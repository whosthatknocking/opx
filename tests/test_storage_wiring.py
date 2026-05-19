"""Tests for the storage-enabled branches of fetcher.py and check_positions.py."""
# pylint: disable=duplicate-code

import builtins
from contextlib import ExitStack, nullcontext
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from conftest import make_runtime_config
from opx_chain.fetcher import acquire_fetcher_lock, release_fetcher_lock
from opx_chain.providers.base import ProviderQuotaError
from opx_chain.runlog import logger_name
from opx_chain.storage.memory import MemoryBackend
from opx_chain.validate import ValidationFinding


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ticker_df(ticker: str = "TSLA") -> pd.DataFrame:
    return pd.DataFrame({
        "underlying_symbol": [ticker] * 2,
        "strike": [100.0, 110.0],
        "expiration_date": ["2026-06-20", "2026-06-20"],
        "passes_primary_screen": [True, True],
    })


def _fetcher_patches(tmp_path: Path, config, backend, ticker_df=None, validation_findings=None):
    """Return a list of patch context managers for a minimal fetcher run."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    if ticker_df is None:
        ticker_df = _make_ticker_df()
    if validation_findings is None:
        validation_findings = []

    (tmp_path / "output").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)

    return [
        patch.object(fetcher, "RUNS_DIR", tmp_path / "output"),
        nullcontext(tmp_path / "logs"),
        patch.object(fetcher, "FETCHER_LOCK_PATH", tmp_path / "logs" / "fetcher.lock"),
        patch.object(fetcher, "acquire_fetcher_lock", return_value=MagicMock()),
        patch.object(fetcher, "release_fetcher_lock"),
        patch.object(fetcher, "get_runtime_config", return_value=config),
        patch.object(fetcher, "set_runtime_config_override"),
        patch.object(fetcher, "create_run_logger",
                     return_value=(MagicMock(), tmp_path / "logs" / "run.log")),
        patch.object(fetcher, "load_positions", return_value=MagicMock(
            stock_tickers=set(), option_keys=set(), empty=True
        )),
        patch.object(fetcher, "fetch_ticker_option_chain", return_value=ticker_df),
        patch.object(fetcher, "validate_export_frame", return_value=validation_findings),
        patch.object(fetcher, "get_storage_backend", return_value=backend),
    ]


# ---------------------------------------------------------------------------
# fetcher storage wiring
# ---------------------------------------------------------------------------

def test_fetcher_calls_write_dataset_when_storage_enabled(tmp_path: Path):
    """When storage is enabled, fetcher must call write_dataset after write_options_csv."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 0
    datasets = backend.list_datasets()
    assert len(datasets) == 1


def test_fetcher_records_fetch_row_counts_from_dataframe_attrs(tmp_path: Path):
    """Storage ticker metadata must preserve raw and normalized fetch counts."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    ticker_df = _make_ticker_df()
    ticker_df.attrs["raw_row_count"] = 5
    ticker_df.attrs["normalized_row_count"] = 3
    ticker_df.attrs["filtered_row_count"] = 1
    patches = _fetcher_patches(tmp_path, config, backend, ticker_df=ticker_df)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 0
    run_id = backend.list_datasets()[0].run_id
    ticker_result = backend._ticker_results[run_id][0]  # pylint: disable=protected-access
    assert ticker_result.raw_row_count == 5
    assert ticker_result.normalized_row_count == 3
    assert ticker_result.kept_row_count == 2
    assert ticker_result.filtered_row_count == 1


def test_fetcher_records_ticker_error_status_from_dataframe_attrs(tmp_path: Path):
    """Per-ticker fetch failures must persist as errors, not skipped tickers."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, tickers=("BAD", "GOOD"))
    error_df = pd.DataFrame()
    error_df.attrs["fetch_status"] = "error"
    error_df.attrs["fetch_error_summary"] = "RuntimeError: provider exploded for BAD"
    ok_df = _make_ticker_df("GOOD")
    patches = _fetcher_patches(tmp_path, config, backend, ticker_df=ok_df)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], \
         patch.object(fetcher, "fetch_ticker_option_chain", side_effect=[error_df, ok_df]), \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 0
    run_id = backend.list_datasets()[0].run_id
    ticker_results = backend._ticker_results[run_id]  # pylint: disable=protected-access
    by_ticker = {result.ticker: result for result in ticker_results}
    assert by_ticker["BAD"].status == "error"
    assert by_ticker["BAD"].error_summary == "RuntimeError: provider exploded for BAD"
    assert by_ticker["GOOD"].status == "ok"


def test_fetcher_records_validation_findings_when_storage_enabled(tmp_path: Path):
    """Storage-backed fetch runs must persist grouped validation summaries."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    findings = [
        ValidationFinding(
            severity="warning",
            code="MISSING_FIELD",
            message="bid is missing",
            row_index=0,
            contract_symbol="TSLA260620C00100000",
            field="bid",
        ),
        ValidationFinding(
            severity="warning",
            code="MISSING_FIELD",
            message="ask is missing",
            row_index=1,
            field="ask",
        ),
        ValidationFinding(
            severity="error",
            code="DUPLICATE_CONTRACT",
            message="duplicate contract row",
            contract_symbol="TSLA260620C00100000",
        ),
    ]
    patches = _fetcher_patches(tmp_path, config, backend, validation_findings=findings)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 0
    run_id = backend.list_datasets()[0].run_id
    records = {
        (record.severity, record.code): record
        for record in backend._validations[run_id]  # pylint: disable=protected-access
    }
    assert records[("warning", "MISSING_FIELD")].count == 2
    assert records[("error", "DUPLICATE_CONTRACT")].count == 1
    assert '"field": "bid"' in records[("warning", "MISSING_FIELD")].sample


def test_fetcher_finalizes_run_on_success(tmp_path: Path):
    """Successful fetch must finalize the run with status=complete."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        fetcher.main([])

    run_id = backend.list_datasets()[0].run_id
    run = backend._runs[run_id]  # pylint: disable=protected-access
    assert run.status == "complete"


def test_fetcher_snapshots_positions_only_after_success(tmp_path: Path):
    """Successful storage-backed runs must persist positions.csv as a sidecar."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol\nTSLA\n", encoding="utf-8")
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main(["--positions", str(positions_file)])

    assert result == 0
    run_id = backend.list_datasets()[0].run_id
    artifacts = backend._artifacts[run_id]  # pylint: disable=protected-access
    sidecars = [artifact for artifact in artifacts if artifact.artifact_type == "sidecar"]
    assert len(sidecars) == 1
    assert sidecars[0].location.endswith("/positions.csv")


def test_fetcher_records_run_log_reference_artifact(tmp_path: Path):
    """Successful storage-backed runs must register the shared run log reference."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 0
    run_id = backend.list_datasets()[0].run_id
    artifacts = backend._artifacts[run_id]  # pylint: disable=protected-access
    run_logs = [artifact for artifact in artifacts if artifact.artifact_type == "run_log"]
    assert len(run_logs) == 1
    assert run_logs[0].location.endswith("/run_log_reference.json")


def test_fetcher_logs_storage_run_id_on_start(tmp_path: Path):
    """The shared run log must use the storage UUID, not a parallel timestamp id."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(patcher) for patcher in patches]
        result = fetcher.main([])

    assert result == 0
    run_id = backend.list_datasets()[0].run_id
    logger = mocks[7].return_value[0]  # create_run_logger return value
    run_started = [
        call for call in logger.info.call_args_list
        if call.args and call.args[0].startswith("run_started ")
    ]
    assert len(run_started) == 1
    assert run_started[0].args[1] == run_id


def test_fetcher_fails_run_on_no_data(tmp_path: Path):
    """When no data is fetched, the run must be marked as failed."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend, ticker_df=pd.DataFrame())

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 1
    runs = list(backend._runs.values())  # pylint: disable=protected-access
    assert len(runs) == 1
    assert runs[0].status == "failed"


def test_fetcher_does_not_snapshot_positions_when_run_fails(tmp_path: Path):
    """Failed runs must not leave behind a positions sidecar artifact."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol\nTSLA\n", encoding="utf-8")
    patches = _fetcher_patches(tmp_path, config, backend, ticker_df=pd.DataFrame())

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main(["--positions", str(positions_file)])

    assert result == 1
    assert not backend._artifacts  # pylint: disable=protected-access


def test_fetcher_quota_error_fails_run_without_writing_dataset(tmp_path: Path):
    """A mid-loop ProviderQuotaError must mark the run failed and write no dataset."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[11]:
        with patch.object(
            fetcher, "fetch_ticker_option_chain",
            side_effect=ProviderQuotaError("daily request limit reached"),
        ):
            result = fetcher.main([])

    assert result == 1
    assert not backend.list_datasets()
    runs = list(backend._runs.values())  # pylint: disable=protected-access
    assert len(runs) == 1
    assert runs[0].status == "failed"
    assert "request limit" in (runs[0].error_summary or "")


def test_fetcher_artifact_failure_fails_without_writing_dataset(tmp_path: Path):
    """Storage artifact failures must not publish a dataset for a failed run."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    class ArtifactFailingBackend(MemoryBackend):
        """Memory backend that fails before the dataset commit point."""

        def __init__(self):
            super().__init__()
            self.write_dataset_called = False

        def write_artifact(self, run_id, artifact):
            raise OSError(f"cannot write {artifact.filename}")

        def write_dataset(self, run_id, dataset):
            self.write_dataset_called = True
            return super().write_dataset(run_id, dataset)

    backend = ArtifactFailingBackend()
    config = make_runtime_config(storage_enabled=True)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol\nTSLA\n", encoding="utf-8")
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main(["--positions", str(positions_file)])

    assert result == 1
    assert not backend.write_dataset_called
    assert not backend.list_datasets()
    runs = list(backend._runs.values())  # pylint: disable=protected-access
    assert len(runs) == 1
    assert runs[0].status == "failed"


def test_fetcher_rolls_back_partial_artifacts_before_dataset_publication(tmp_path: Path):
    """A second artifact failure must remove any earlier run artifacts."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    class SecondArtifactFailingBackend(MemoryBackend):
        """Memory backend that fails after the first artifact is written."""

        def __init__(self):
            super().__init__()
            self.artifact_attempts = 0
            self.delete_run_artifacts_called = False
            self.write_dataset_called = False

        def write_artifact(self, run_id, artifact):
            self.artifact_attempts += 1
            if self.artifact_attempts == 2:
                raise OSError(f"cannot write {artifact.filename}")
            return super().write_artifact(run_id, artifact)

        def delete_run_artifacts(self, run_id):
            self.delete_run_artifacts_called = True
            return super().delete_run_artifacts(run_id)

        def write_dataset(self, run_id, dataset):
            self.write_dataset_called = True
            return super().write_dataset(run_id, dataset)

    backend = SecondArtifactFailingBackend()
    config = make_runtime_config(storage_enabled=True)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol\nTSLA\n", encoding="utf-8")
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main(["--positions", str(positions_file)])

    assert result == 1
    assert backend.artifact_attempts == 2
    assert backend.delete_run_artifacts_called
    assert not backend.write_dataset_called
    assert not backend.list_datasets()
    assert not backend._artifacts  # pylint: disable=protected-access
    runs = list(backend._runs.values())  # pylint: disable=protected-access
    assert len(runs) == 1
    assert runs[0].status == "failed"


def test_fetcher_rolls_back_partial_artifacts_on_keyboard_interrupt(tmp_path: Path):
    """KeyboardInterrupt before dataset publication must remove earlier artifacts."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    class InterruptingArtifactBackend(MemoryBackend):
        """Memory backend that interrupts after writing the first artifact."""

        def __init__(self):
            super().__init__()
            self.artifact_attempts = 0
            self.delete_run_artifacts_called = False
            self.write_dataset_called = False

        def write_artifact(self, run_id, artifact):
            self.artifact_attempts += 1
            if self.artifact_attempts == 2:
                raise KeyboardInterrupt
            return super().write_artifact(run_id, artifact)

        def delete_run_artifacts(self, run_id):
            self.delete_run_artifacts_called = True
            return super().delete_run_artifacts(run_id)

        def write_dataset(self, run_id, dataset):
            self.write_dataset_called = True
            return super().write_dataset(run_id, dataset)

    backend = InterruptingArtifactBackend()
    config = make_runtime_config(storage_enabled=True)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol\nTSLA\n", encoding="utf-8")
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main(["--positions", str(positions_file)])

    assert result == 130
    assert backend.artifact_attempts == 2
    assert backend.delete_run_artifacts_called
    assert not backend.write_dataset_called
    assert not backend.list_datasets()
    assert not backend._artifacts  # pylint: disable=protected-access
    runs = list(backend._runs.values())  # pylint: disable=protected-access
    assert len(runs) == 1
    assert runs[0].status == "interrupted"


def test_fetcher_rolls_back_partial_artifacts_on_sigterm(tmp_path: Path, monkeypatch):
    """SIGTERM should route through the interrupted cleanup path."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    previous_handler = object()
    signal_calls = []
    captured = {}

    def fake_signal(signum, handler):
        signal_calls.append((signum, handler))
        captured["handler"] = handler

    monkeypatch.setattr(fetcher.signal, "getsignal", lambda signum: previous_handler)
    monkeypatch.setattr(fetcher.signal, "signal", fake_signal)

    class SigtermArtifactBackend(MemoryBackend):
        """Memory backend that triggers the installed SIGTERM handler."""

        def __init__(self):
            super().__init__()
            self.artifact_attempts = 0
            self.delete_run_artifacts_called = False
            self.write_dataset_called = False

        def write_artifact(self, run_id, artifact):
            self.artifact_attempts += 1
            if self.artifact_attempts == 2:
                captured["handler"](fetcher.signal.SIGTERM, None)
            return super().write_artifact(run_id, artifact)

        def delete_run_artifacts(self, run_id):
            self.delete_run_artifacts_called = True
            return super().delete_run_artifacts(run_id)

        def write_dataset(self, run_id, dataset):
            self.write_dataset_called = True
            return super().write_dataset(run_id, dataset)

    backend = SigtermArtifactBackend()
    config = make_runtime_config(storage_enabled=True)
    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol\nTSLA\n", encoding="utf-8")
    patches = _fetcher_patches(tmp_path, config, backend)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main(["--positions", str(positions_file)])

    assert result == 130
    assert backend.artifact_attempts == 2
    assert backend.delete_run_artifacts_called
    assert not backend.write_dataset_called
    assert not backend.list_datasets()
    assert not backend._artifacts  # pylint: disable=protected-access
    runs = list(backend._runs.values())  # pylint: disable=protected-access
    assert len(runs) == 1
    assert runs[0].status == "interrupted"
    assert signal_calls[0] == (
        fetcher.signal.SIGTERM,
        fetcher._raise_keyboard_interrupt_on_sigterm,  # pylint: disable=protected-access
    )
    assert signal_calls[-1] == (fetcher.signal.SIGTERM, previous_handler)


def test_fetcher_skips_storage_when_disabled(tmp_path: Path):
    """When storage is disabled, write_dataset must never be called."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=False)
    patches = _fetcher_patches(tmp_path, config, backend=None)

    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patches[5], patches[6], patches[7], patches[8], patches[9], \
         patches[10], patches[11]:
        result = fetcher.main([])

    assert result == 0
    assert not backend.list_datasets()


# ---------------------------------------------------------------------------
# check_positions storage wiring
# ---------------------------------------------------------------------------

def test_check_positions_uses_storage_when_enabled(tmp_path: Path):
    """opx-check must use list_datasets when storage is enabled."""
    from datetime import datetime, timezone  # pylint: disable=import-outside-toplevel
    from opx_chain import check_positions as cp  # pylint: disable=import-outside-toplevel
    from opx_chain.storage.models import DatasetRecord  # pylint: disable=import-outside-toplevel

    artifact = tmp_path / "ds.csv"
    artifact.write_text(
        "underlying_symbol,strike,expiration_date,passes_primary_screen\n"
        "TSLA,100.0,2026-06-20,True\n",
        encoding="utf-8",
    )
    record = DatasetRecord(
        dataset_id="ds-id", run_id="run-1",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        provider="yfinance", schema_version=1, row_count=1,
        format="csv", location=str(artifact), content_hash="a" * 64,
    )
    mock_backend = MagicMock()
    mock_backend.list_datasets.return_value = [record]

    positions_file = tmp_path / "positions.csv"
    positions_file.write_text(
        "Symbol,Expiration Date,Option Type,Strike\n", encoding="utf-8"
    )

    with (
        patch.object(cp, "get_storage_backend", return_value=mock_backend),
        patch.object(cp, "get_runtime_config", return_value=make_runtime_config()),
    ):
        result = cp.main(["--positions", str(positions_file)])

    assert result == 0


def test_check_positions_prefers_csv_over_parquet_dataset(tmp_path: Path):
    """opx-check must skip parquet records and use the newest CSV dataset."""
    from datetime import datetime, timezone  # pylint: disable=import-outside-toplevel
    from opx_chain import check_positions as cp  # pylint: disable=import-outside-toplevel
    from opx_chain.storage.models import DatasetRecord  # pylint: disable=import-outside-toplevel

    parquet_record = DatasetRecord(
        dataset_id="parquet-id",
        run_id="run-1",
        created_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        provider="yfinance",
        schema_version=1,
        row_count=5,
        format="parquet",
        location="/fake/output/parquet-id.parquet",
        content_hash="a" * 64,
    )
    csv_record = DatasetRecord(
        dataset_id="csv-id",
        run_id="run-1",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        provider="yfinance",
        schema_version=1,
        row_count=2,
        format="csv",
        location=str(tmp_path / "csv-id.csv"),
        content_hash="b" * 64,
    )
    (tmp_path / "csv-id.csv").write_text(
        "underlying_symbol,strike,expiration_date,passes_primary_screen\n"
        "TSLA,100.0,2026-06-20,True\n",
        encoding="utf-8",
    )

    mock_backend = MagicMock()
    mock_backend.list_datasets.return_value = [parquet_record, csv_record]

    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol,Expiration Date,Option Type,Strike\n", encoding="utf-8")

    with (
        patch.object(cp, "get_storage_backend", return_value=mock_backend),
        patch.object(cp, "get_runtime_config", return_value=make_runtime_config()),
    ):
        result = cp.main(["--positions", str(positions_file)])

    assert result == 0
    mock_backend.list_datasets.assert_called_once_with(limit=100)


def test_check_positions_skips_records_with_missing_artifact(tmp_path: Path):
    """opx-check must skip storage records whose artifact file no longer exists."""
    from datetime import datetime, timezone  # pylint: disable=import-outside-toplevel
    from opx_chain import check_positions as cp  # pylint: disable=import-outside-toplevel
    from opx_chain.storage.models import DatasetRecord  # pylint: disable=import-outside-toplevel

    stale_record = DatasetRecord(
        dataset_id="stale-id",
        run_id="run-1",
        created_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        provider="yfinance",
        schema_version=1,
        row_count=5,
        format="csv",
        location="/old/workspace/output/stale-id.csv",
        content_hash="a" * 64,
    )
    current_record = DatasetRecord(
        dataset_id="current-id",
        run_id="run-2",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        provider="yfinance",
        schema_version=1,
        row_count=2,
        format="csv",
        location=str(tmp_path / "current-id.csv"),
        content_hash="b" * 64,
    )
    (tmp_path / "current-id.csv").write_text(
        "underlying_symbol,strike,expiration_date,passes_primary_screen\n"
        "TSLA,100.0,2026-06-20,True\n",
        encoding="utf-8",
    )

    mock_backend = MagicMock()
    mock_backend.list_datasets.return_value = [stale_record, current_record]

    positions_file = tmp_path / "positions.csv"
    positions_file.write_text("Symbol,Expiration Date,Option Type,Strike\n", encoding="utf-8")

    with (
        patch.object(cp, "get_storage_backend", return_value=mock_backend),
        patch.object(cp, "get_runtime_config", return_value=make_runtime_config()),
    ):
        result = cp.main(["--positions", str(positions_file)])

    assert result == 0


# ---------------------------------------------------------------------------
# --dry-run
# ---------------------------------------------------------------------------

def test_dry_run_makes_no_api_calls_and_no_writes(tmp_path: Path):
    """--dry-run must not call fetch_ticker_option_chain or write any output."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        result = fetcher.main(["--dry-run"])

    assert result == 0
    mocks[3].assert_not_called()  # acquire_fetcher_lock
    mocks[4].assert_not_called()  # release_fetcher_lock
    mock_fetch = mocks[9]  # fetch_ticker_option_chain
    mock_fetch.assert_not_called()
    assert not backend.list_datasets()


def test_dry_run_logger_uses_stdlib_null_logger():
    """Dry-run logging should keep the complete stdlib logger method surface."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    logger = fetcher._dry_run_logger()  # pylint: disable=protected-access

    assert logger.name == logger_name("fetcher.dry_run")
    assert logger.propagate is False
    assert any(isinstance(handler, fetcher.logging.NullHandler) for handler in logger.handlers)
    logger.debug("debug calls should be supported")
    logger.critical("critical calls should be supported")
    logger.log(fetcher.logging.INFO, "generic log calls should be supported")


def test_fetcher_lock_blocks_second_holder(tmp_path: Path):
    """Fetcher locks must remain non-blocking without requiring fcntl imports."""
    lock_path = tmp_path / "fetcher.lock"
    first = acquire_fetcher_lock(lock_path)
    assert first is not None
    try:
        assert acquire_fetcher_lock(lock_path) is None
    finally:
        release_fetcher_lock(first, lock_path)
    assert lock_path.exists()
    second = acquire_fetcher_lock(lock_path)
    assert second is not None
    release_fetcher_lock(second, lock_path)


def test_dry_run_prints_would_fetch_summary(tmp_path: Path, capsys):
    """--dry-run must print the tickers it would fetch and storage backend class."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, tickers=("AAPL", "TSLA"))
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        fetcher.main(["--dry-run"])

    captured = capsys.readouterr()
    assert "DRY RUN" in captured.out
    assert "AAPL" in captured.out
    assert "TSLA" in captured.out
    assert "Dry-run complete" in captured.out


# ---------------------------------------------------------------------------
# run_fetch API
# ---------------------------------------------------------------------------

def test_run_fetch_passes_positions_path(tmp_path: Path):
    """run_fetch must forward positions_path to load_positions."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    positions_file = tmp_path / "custom_positions.csv"
    positions_file.write_text("", encoding="utf-8")

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(positions_path=positions_file)

    mock_load = mocks[8]
    mock_load.assert_called_once()
    called_path = mock_load.call_args[0][0]
    assert called_path == positions_file.expanduser()


def test_run_fetch_tickers_override_replaces_config_tickers(tmp_path: Path):
    """run_fetch(tickers=...) must use the supplied tickers, not config.tickers."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, tickers=("NVDA", "MSFT"))
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(tickers=("AAPL",))

    # set_runtime_config_override is called twice: once to set, once to clear (None)
    mock_set_config = mocks[6]
    set_call = mock_set_config.call_args_list[0]
    assert set_call[0][0].tickers == ("AAPL",)


def test_run_fetch_data_provider_override_replaces_config_provider(tmp_path: Path):
    """run_fetch(data_provider=...) must use the supplied provider for this run."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, data_provider="yfinance")
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(data_provider="marketdata")

    mock_set_config = mocks[6]
    set_call = mock_set_config.call_args_list[0]
    assert set_call[0][0].data_provider == "marketdata"


def test_run_fetch_data_provider_override_rejects_unknown_provider(tmp_path: Path):
    """Provider overrides should fail before opening a fetcher run."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True, data_provider="yfinance")
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        try:
            fetcher.run_fetch(data_provider="bad-provider")
        except ValueError as exc:
            assert "unsupported data provider" in str(exc)
        else:  # pragma: no cover - defensive assertion branch
            raise AssertionError("expected invalid data provider to raise ValueError")

    mocks[3].assert_not_called()  # acquire_fetcher_lock


def test_run_fetch_max_expiration_override_updates_derived_date(tmp_path: Path):
    """run_fetch(max_expiration_weeks=...) must keep the derived filter date in sync."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(
        storage_enabled=True,
        max_expiration_weeks=14,
        max_expiration="2026-06-30",
    )
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(max_expiration_weeks=4)

    mock_set_config = mocks[6]
    set_call = mock_set_config.call_args_list[0]
    active_config = set_call[0][0]
    assert active_config.max_expiration_weeks == 4
    assert active_config.max_expiration == "2026-04-17"


def test_run_fetch_max_expiration_override_can_disable_filter(tmp_path: Path):
    """run_fetch(max_expiration_weeks=0) should disable the max-expiration filter."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(
        storage_enabled=True,
        max_expiration_weeks=14,
        max_expiration="2026-06-30",
    )
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(max_expiration_weeks=0)

    mock_set_config = mocks[6]
    set_call = mock_set_config.call_args_list[0]
    active_config = set_call[0][0]
    assert active_config.max_expiration_weeks == 0
    assert active_config.max_expiration is None


def test_run_fetch_dry_run_makes_no_api_calls_and_no_writes(tmp_path: Path):
    """run_fetch(dry_run=True) should match the CLI dry-run zero-call behavior."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    backend = MemoryBackend()
    config = make_runtime_config(storage_enabled=True)
    patches = _fetcher_patches(tmp_path, config, backend)

    with ExitStack() as stack:
        mocks = [stack.enter_context(p) for p in patches]
        fetcher.run_fetch(dry_run=True)

    mocks[3].assert_not_called()  # acquire_fetcher_lock
    mocks[4].assert_not_called()  # release_fetcher_lock
    mock_fetch = mocks[9]  # fetch_ticker_option_chain
    mock_fetch.assert_not_called()
    assert not backend.list_datasets()
    assert not list(backend._runs.values())  # pylint: disable=protected-access


def test_run_fetch_dry_run_checks_parquet_dependency_before_api_calls(tmp_path: Path):
    """Dry-run should fail fast when parquet output is configured without pyarrow."""
    from opx_chain import fetcher  # pylint: disable=import-outside-toplevel

    config = make_runtime_config(
        storage_enabled=True,
        storage_dataset_format="parquet",
        storage_dir=tmp_path,
    )
    fetcher_patches = _fetcher_patches(tmp_path, config, MemoryBackend())
    patches = [
        patcher
        for patcher in fetcher_patches
        if getattr(patcher, "attribute", None) != "get_storage_backend"
    ]
    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "pyarrow":
            raise ImportError("missing pyarrow")
        return original_import(name, *args, **kwargs)

    with ExitStack() as stack:
        mocks = {
            getattr(patcher, "attribute", ""): stack.enter_context(patcher)
            for patcher in patches
        }
        with patch("builtins.__import__", side_effect=fake_import):
            result = fetcher.main(["--dry-run"])

    mock_fetch = mocks["fetch_ticker_option_chain"]
    assert result == 1
    mock_fetch.assert_not_called()


def test_check_positions_falls_back_to_scan_when_disabled(tmp_path: Path):
    """opx-check must fall back to directory scanning when storage is disabled."""
    from opx_chain import check_positions as cp  # pylint: disable=import-outside-toplevel

    positions_file = tmp_path / "positions.csv"
    positions_file.write_text(
        "Symbol,Expiration Date,Option Type,Strike\n", encoding="utf-8"
    )

    with (
        patch.object(cp, "get_storage_backend", return_value=None),
        patch.object(cp, "find_latest_output", return_value=None),
        patch.object(cp, "get_runtime_config", return_value=make_runtime_config()),
    ):
        result = cp.main(["--positions", str(positions_file)])

    assert result == 1
