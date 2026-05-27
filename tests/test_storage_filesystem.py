"""Tests for FilesystemBackend and get_storage_backend factory."""
# pylint: disable=duplicate-code,too-many-lines

import hashlib
import inspect
import json
import os
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from conftest import make_runtime_config
import opx_chain.storage.filesystem as filesystem_mod
from opx_chain.storage.base import StorageBackend
from opx_chain.storage.factory import get_storage_backend
from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage.models import (
    ArtifactWrite,
    DatasetHandle,
    DatasetRecord,
    DatasetWrite,
    RunContext,
    RunSummary,
    TickerFetchResult,
    ValidationRecord,
)
from opx_chain.version import __version__


def _make_backend(
    tmp_path: Path,
    max_runs_retained: int = 0,
    dataset_format: str = "csv",
) -> FilesystemBackend:
    return FilesystemBackend(
        runs_dir=tmp_path / "runs",
        debug_dir=tmp_path / "debug",
        max_runs_retained=max_runs_retained,
        dataset_format=dataset_format,
    )


def _make_context(**kwargs) -> RunContext:
    defaults = {
        "provider": "yfinance",
        "tickers": ("TSLA",),
        "config_fingerprint": "abc123",
        "positions_fingerprint": "",
    }
    return RunContext(**{**defaults, **kwargs})


def _make_dataframe(rows: int = 3) -> pd.DataFrame:
    return pd.DataFrame(
        {"underlying_symbol": ["TSLA"] * rows, "strike": [100.0, 110.0, 120.0][:rows]}
    )


def _write(backend: FilesystemBackend, run_id: str, rows: int = 3, provider: str = "yfinance"):
    return backend.write_dataset(
        run_id,
        DatasetWrite(data=_make_dataframe(rows), provider=provider, schema_version=1),
    )


def _meta_path(record: DatasetRecord) -> Path:
    return Path(record.location).with_suffix(".meta.json")


def _touch_future_mtime(path: Path) -> None:
    future = datetime.now(tz=timezone.utc).timestamp() + 3600
    os.utime(path, (future, future))


def _record_ticker(backend: FilesystemBackend, run_id: str, ticker: str) -> None:
    backend.record_ticker_result(
        run_id,
        TickerFetchResult(
            ticker=ticker,
            raw_row_count=50,
            normalized_row_count=48,
            kept_row_count=40,
            filtered_row_count=8,
            expiration_count=4,
            status="ok",
        ),
    )


class _SlowReadFilesystemBackend(FilesystemBackend):
    """Filesystem backend variant that tracks overlapping run sidecar reads."""

    def __init__(self, runs_dir: Path, debug_dir: Path) -> None:
        super().__init__(runs_dir=runs_dir, debug_dir=debug_dir)
        self._active_read_count = 0
        self.max_active_read_count = 0
        self._read_count_lock = threading.Lock()

    def _read_run(self, run_id: str) -> dict:
        with self._read_count_lock:
            self._active_read_count += 1
            self.max_active_read_count = max(
                self.max_active_read_count,
                self._active_read_count,
            )
        try:
            data = super()._read_run(run_id)
            time.sleep(0.01)
            return data
        finally:
            with self._read_count_lock:
                self._active_read_count -= 1


def _run_concurrently(actions: list[Callable[[], None]]) -> None:
    start = threading.Barrier(len(actions) + 1)
    errors = []
    errors_lock = threading.Lock()

    def worker(action: Callable[[], None]) -> None:
        try:
            start.wait(timeout=5)
            action()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            with errors_lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker, args=(action,)) for action in actions]
    for thread in threads:
        thread.start()
    start.wait(timeout=5)
    for thread in threads:
        thread.join(timeout=5)

    assert not [thread for thread in threads if thread.is_alive()]
    assert not errors, [repr(error) for error in errors]


# ---------------------------------------------------------------------------
# Protocol satisfaction
# ---------------------------------------------------------------------------

def test_filesystem_backend_satisfies_protocol(tmp_path: Path):
    """FilesystemBackend must satisfy the StorageBackend runtime-checkable protocol."""
    assert isinstance(_make_backend(tmp_path), StorageBackend)


# ---------------------------------------------------------------------------
# Run lifecycle
# ---------------------------------------------------------------------------

def test_create_run_writes_sidecar(tmp_path: Path):
    """create_run must write a JSON sidecar to runs_dir/{run_id}/run.json."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())

    assert (tmp_path / "runs" / run_id / "run.json").exists()


def test_create_run_initial_status_is_running(tmp_path: Path):
    """Newly created run sidecar must have status=running."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context(tickers=("TSLA", "NVDA")))

    run = backend.get_run(run_id)
    assert run.status == "running"
    assert run.finished_at is None
    assert run.tickers == ("TSLA", "NVDA")
    assert run.script_version == __version__


@pytest.mark.parametrize(
    ("stored_tickers", "expected"),
    [
        ("TSLA", ()),
        (["TSLA", True, 7, ""], ()),
        (None, ()),
        ({"symbol": "TSLA"}, ()),
        (["tsla", "NVDA"], ("TSLA", "NVDA")),
    ],
)
def test_get_run_sanitizes_retained_ticker_payload(
    tmp_path: Path,
    stored_tickers,
    expected: tuple[str, ...],
):
    """Retained run sidecars must not return malformed ticker tuples."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    run_path = tmp_path / "runs" / run_id / "run.json"
    data = json.loads(run_path.read_text(encoding="utf-8"))
    data["tickers"] = stored_tickers
    run_path.write_text(json.dumps(data), encoding="utf-8")

    run = backend.get_run(run_id)

    assert run.tickers == expected


def test_get_run_sanitizes_missing_retained_ticker_payload(tmp_path: Path):
    """Legacy run sidecars without ticker metadata should read back as an empty universe."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    run_path = tmp_path / "runs" / run_id / "run.json"
    data = json.loads(run_path.read_text(encoding="utf-8"))
    data.pop("tickers")
    run_path.write_text(json.dumps(data), encoding="utf-8")

    run = backend.get_run(run_id)

    assert not run.tickers


def test_finalize_run_sets_status_complete(tmp_path: Path):
    """finalize_run must update status to complete and set finished_at."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    backend.finalize_run(run_id, RunSummary(status="complete"))

    run = backend.get_run(run_id)
    assert run.status == "complete"
    assert run.finished_at is not None
    assert run.error_summary is None


def test_fail_run_sets_status_and_error(tmp_path: Path):
    """fail_run must update status to failed and persist the error message."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    backend.fail_run(run_id, "network error")

    run = backend.get_run(run_id)
    assert run.status == "failed"
    assert run.error_summary == "network error"


def test_terminal_run_status_is_not_overwritten(tmp_path: Path):
    """Late lifecycle calls must not demote already terminal run records."""
    backend = _make_backend(tmp_path)

    complete_run_id = backend.create_run(_make_context())
    backend.finalize_run(complete_run_id, RunSummary(status="complete"))
    completed = backend.get_run(complete_run_id)
    backend.fail_run(complete_run_id, "post-finalize error")
    backend.finalize_run(
        complete_run_id,
        RunSummary(status="interrupted", error_summary="interrupted"),
    )

    after_late_calls = backend.get_run(complete_run_id)
    assert after_late_calls.status == "complete"
    assert after_late_calls.finished_at == completed.finished_at
    assert after_late_calls.error_summary is None

    failed_run_id = backend.create_run(_make_context())
    backend.fail_run(failed_run_id, "network error")
    failed = backend.get_run(failed_run_id)
    backend.finalize_run(failed_run_id, RunSummary(status="complete"))

    after_finalize = backend.get_run(failed_run_id)
    assert after_finalize.status == "failed"
    assert after_finalize.finished_at == failed.finished_at
    assert after_finalize.error_summary == "network error"


def test_record_ticker_result_persisted(tmp_path: Path):
    """record_ticker_result must persist the result in the run sidecar."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    result = TickerFetchResult(
        ticker="TSLA",
        raw_row_count=50,
        normalized_row_count=48,
        kept_row_count=40,
        filtered_row_count=8,
        expiration_count=4,
        status="ok",
    )
    backend.record_ticker_result(run_id, result)

    ticker_results = backend.get_ticker_results(run_id)
    assert len(ticker_results) == 1
    assert ticker_results[0].ticker == "TSLA"
    assert ticker_results[0].kept_row_count == 40


def test_record_validation_persisted(tmp_path: Path):
    """record_validation must append validation summaries to the run sidecar."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())

    backend.record_validation(ValidationRecord(
        run_id=run_id,
        severity="error",
        code="DUPLICATE_CONTRACT",
        count=1,
        sample='{"contract_symbol": "TSLA260620C00100000"}',
    ))

    data = json.loads((tmp_path / "runs" / run_id / "run.json").read_text(encoding="utf-8"))
    assert data["validations"] == [{
        "severity": "error",
        "code": "DUPLICATE_CONTRACT",
        "count": 1,
        "sample": '{"contract_symbol": "TSLA260620C00100000"}',
    }]


def test_run_sidecar_rejects_non_finite_json_writes(tmp_path: Path):
    """Run sidecar writes must not emit non-standard NaN/Infinity JSON."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())

    with pytest.raises(ValueError, match="nonnegative integer"):
        backend.record_validation(ValidationRecord(
            run_id=run_id,
            severity="warning",
            code="BAD_NUMERIC",
            count=float("nan"),
            sample=None,
        ))

    data = json.loads((tmp_path / "runs" / run_id / "run.json").read_text(encoding="utf-8"))
    assert data["validations"] == []


def test_run_sidecar_rejects_non_finite_json_reads(tmp_path: Path):
    """Run sidecar reads must reject non-standard NaN/Infinity JSON."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    run_path = tmp_path / "runs" / run_id / "run.json"
    run_path.write_text('{"run_id":"bad","started_at":NaN}', encoding="utf-8")

    with pytest.raises(ValueError, match="non-finite JSON value is not allowed: NaN"):
        backend.get_run(run_id)


def test_filesystem_backend_uses_strict_json_helpers() -> None:
    """Filesystem metadata paths should not use permissive default JSON helpers."""
    source = inspect.getsource(filesystem_mod.FilesystemBackend)

    assert "json.load" not in source
    assert "json.loads" not in source
    assert "json.dumps" not in source
    assert "loads_strict_json" in source
    assert "dumps_strict_json" in source


def test_record_ticker_result_serializes_concurrent_sidecar_updates(tmp_path: Path):
    """Concurrent ticker-result writes must not drop run sidecar updates."""
    backend = _SlowReadFilesystemBackend(
        runs_dir=tmp_path / "runs",
        debug_dir=tmp_path / "debug",
    )
    run_id = backend.create_run(_make_context())
    tickers = [f"TK{chr(ord('A') + index)}" for index in range(12)]

    _run_concurrently([
        lambda ticker=ticker: _record_ticker(backend, run_id, ticker)
        for ticker in tickers
    ])

    assert backend.max_active_read_count == 1
    assert sorted(result.ticker for result in backend.get_ticker_results(run_id)) == tickers


def test_record_validation_serializes_concurrent_sidecar_updates(tmp_path: Path):
    """Concurrent validation writes must not drop run sidecar updates."""
    backend = _SlowReadFilesystemBackend(
        runs_dir=tmp_path / "runs",
        debug_dir=tmp_path / "debug",
    )
    run_id = backend.create_run(_make_context())
    codes = [f"VALIDATION_{i:02d}" for i in range(12)]

    _run_concurrently([
        lambda code=code: backend.record_validation(ValidationRecord(
            run_id=run_id,
            severity="warning",
            code=code,
            count=1,
            sample=code,
        ))
        for code in codes
    ])

    data = json.loads((tmp_path / "runs" / run_id / "run.json").read_text(encoding="utf-8"))
    assert backend.max_active_read_count == 1
    assert sorted(row["code"] for row in data["validations"]) == codes


# ---------------------------------------------------------------------------
# Dataset write and read
# ---------------------------------------------------------------------------

def test_write_dataset_creates_csv_and_meta(tmp_path: Path):
    """write_dataset must create both the artifact CSV and its .meta.json."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    assert Path(record.location).exists()
    assert (tmp_path / "runs" / run_id / "output" / f"{record.dataset_id}.meta.json").exists()


def test_write_dataset_returns_correct_record(tmp_path: Path):
    """DatasetRecord returned by write_dataset must have correct field values."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    df = _make_dataframe()
    record = backend.write_dataset(
        run_id, DatasetWrite(data=df, provider="yfinance", schema_version=1)
    )

    assert isinstance(record, DatasetRecord)
    assert record.run_id == run_id
    assert record.row_count == len(df)
    assert record.format == "csv"
    assert len(record.content_hash) == 64
    assert Path(record.location).is_absolute()
    assert record.script_version == __version__


def test_content_hash_matches_artifact_bytes(tmp_path: Path):
    """content_hash must equal SHA-256 of the written artifact file."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    actual_hash = hashlib.sha256(Path(record.location).read_bytes()).hexdigest()
    assert record.content_hash == actual_hash


def test_write_dataset_hashes_serialized_bytes_without_readback(monkeypatch, tmp_path: Path):
    """Dataset writes should not re-read the artifact file just to compute its hash."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())

    def fail_read_bytes(self: Path) -> bytes:  # pylint: disable=unused-argument
        raise AssertionError("dataset artifact was read back from disk")

    monkeypatch.setattr(Path, "read_bytes", fail_read_bytes)

    record = _write(backend, run_id)

    assert len(record.content_hash) == 64


def test_write_dataset_removes_artifact_when_meta_write_fails(monkeypatch, tmp_path: Path):
    """A partial dataset publish must not leave an orphaned artifact file."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())

    def fail_write_meta(_record: DatasetRecord) -> None:
        raise OSError("meta write failed")

    monkeypatch.setattr(backend, "_write_meta", fail_write_meta)

    with pytest.raises(OSError, match="meta write failed"):
        _write(backend, run_id)

    output_dir = tmp_path / "runs" / run_id / "output"
    assert not list(output_dir.glob("*.csv"))
    assert not list(output_dir.glob("*.meta.json"))
    assert backend.get_run(run_id).dataset_id is None
    assert not backend.list_datasets()


def test_write_dataset_rolls_back_late_publish_failure(monkeypatch, tmp_path: Path):
    """Rollback must clear metadata and run references after later publish failures."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())

    def fail_prune() -> None:
        raise OSError("prune failed")

    monkeypatch.setattr(backend, "_prune_datasets", fail_prune)

    with pytest.raises(OSError, match="prune failed"):
        _write(backend, run_id)

    output_dir = tmp_path / "runs" / run_id / "output"
    assert not list(output_dir.glob("*.csv"))
    assert not list(output_dir.glob("*.meta.json"))
    assert backend.get_run(run_id).dataset_id is None
    assert not backend.list_datasets()


def test_get_dataset_returns_handle(tmp_path: Path):
    """get_dataset must return a DatasetHandle matching the written record."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    handle = backend.get_dataset(record.dataset_id)

    assert isinstance(handle, DatasetHandle)
    assert handle.dataset_id == record.dataset_id
    assert handle.run_id == record.run_id
    assert handle.provider == record.provider
    assert handle.content_hash == record.content_hash
    assert handle.created_at == record.created_at
    assert handle.script_version == record.script_version


def test_get_dataset_uses_index_before_directory_glob(tmp_path: Path, monkeypatch):
    """get_dataset should use the dataset index instead of scanning run dirs."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    def fail_find_meta_path(_dataset_id):
        raise AssertionError("get_dataset should not glob when the index has the record")

    monkeypatch.setattr(backend, "_find_meta_path", fail_find_meta_path)

    handle = backend.get_dataset(record.dataset_id)

    assert handle.dataset_id == record.dataset_id


def test_filesystem_backend_has_no_dead_private_helpers():
    """FilesystemBackend should not carry unused private path/read helpers."""
    source = inspect.getsource(FilesystemBackend)

    assert "def _read_meta" not in source
    assert "def _artifact_path" not in source


def test_get_dataset_raises_for_unknown_id(tmp_path: Path):
    """get_dataset must raise KeyError for an unrecognised dataset_id."""
    backend = _make_backend(tmp_path)
    with pytest.raises(KeyError):
        backend.get_dataset("no-such-id")


def test_list_datasets_most_recent_first(tmp_path: Path):
    """list_datasets must return records newest first."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    r1 = _write(backend, run_id, rows=1)
    r2 = _write(backend, run_id, rows=2)

    records = backend.list_datasets()

    assert records[0].dataset_id == r2.dataset_id
    assert records[1].dataset_id == r1.dataset_id


def test_list_datasets_limit(tmp_path: Path):
    """list_datasets must honour the limit parameter."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    for _ in range(5):
        _write(backend, run_id)

    assert len(backend.list_datasets(limit=2)) == 2


def test_list_datasets_uses_index_without_reparsing_meta_files(tmp_path: Path, monkeypatch):
    """The filesystem backend should not parse every meta file for a limited listing."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    for _ in range(5):
        _write(backend, run_id)

    meta_reads = 0
    original_read_text = Path.read_text

    def tracking_read_text(self, *args, **kwargs):
        nonlocal meta_reads
        if self.name.endswith(".meta.json"):
            meta_reads += 1
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", tracking_read_text)

    assert len(backend.list_datasets(limit=1)) == 1
    assert meta_reads == 0


def test_dataset_index_is_written_compactly(tmp_path: Path):
    """The machine-read dataset index should not pay pretty-print storage overhead."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    index_text = (tmp_path / "runs" / "datasets.index.json").read_text(encoding="utf-8")

    assert json.loads(index_text)[0]["dataset_id"] == record.dataset_id
    assert "\n" not in index_text
    assert ": " not in index_text


def test_list_datasets_scan_skips_malformed_meta_shape(tmp_path: Path):
    """Fallback meta-file scans should skip JSON with the wrong shape."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)
    (tmp_path / "runs" / "datasets.index.json").unlink()
    malformed_dir = tmp_path / "runs" / "bad-run-id" / "output"
    malformed_dir.mkdir(parents=True, exist_ok=True)
    (malformed_dir / "bad.meta.json").write_text("[]", encoding="utf-8")

    records = backend.list_datasets()

    assert [item.dataset_id for item in records] == [record.dataset_id]


def test_list_and_get_dataset_skip_records_with_missing_run_sidecar(tmp_path: Path):
    """Dataset records whose owning run sidecar is gone must not remain visible."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)
    (tmp_path / "runs" / run_id / "run.json").unlink()

    assert not backend.list_datasets()
    with pytest.raises(KeyError, match="dataset not found"):
        backend.get_dataset(record.dataset_id)


def test_list_datasets_filter_provider(tmp_path: Path):
    """list_datasets must filter by provider when the argument is given."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    _write(backend, run_id, provider="yfinance")
    _write(backend, run_id, provider="marketdata")

    results = backend.list_datasets(provider="yfinance")

    assert len(results) == 1
    assert results[0].provider == "yfinance"


def test_list_datasets_filter_ticker(tmp_path: Path):
    """list_datasets must filter by ticker before applying the limit."""
    backend = _make_backend(tmp_path)
    tsla_run_id = backend.create_run(_make_context(tickers=("TSLA",)))
    _record_ticker(backend, tsla_run_id, "TSLA")
    tsla_record = _write(backend, tsla_run_id)
    aapl_run_id = backend.create_run(_make_context(tickers=("AAPL",)))
    _record_ticker(backend, aapl_run_id, "AAPL")
    _write(backend, aapl_run_id)

    results = backend.list_datasets(limit=1, ticker="tsla")

    assert [record.dataset_id for record in results] == [tsla_record.dataset_id]


def test_list_datasets_filter_ticker_uses_run_context_tickers(tmp_path: Path):
    """Ticker filtering should work before any per-ticker result rows are recorded."""
    backend = _make_backend(tmp_path)
    tsla_run_id = backend.create_run(_make_context(tickers=("TSLA",)))
    tsla_record = _write(backend, tsla_run_id)
    aapl_run_id = backend.create_run(_make_context(tickers=("AAPL",)))
    _write(backend, aapl_run_id)

    results = backend.list_datasets(limit=1, ticker="tsla")

    assert [record.dataset_id for record in results] == [tsla_record.dataset_id]


def test_list_datasets_empty_when_no_runs_dir(tmp_path: Path):
    """list_datasets must return empty list when runs_dir does not exist."""
    backend = _make_backend(tmp_path)
    assert not backend.list_datasets()


@pytest.mark.parametrize("bad_limit", [-1, True, 1.5, "2", None, [], {}])
def test_list_datasets_rejects_malformed_limit(tmp_path: Path, bad_limit):
    """list_datasets must reject non-integer and negative limits consistently."""
    backend = _make_backend(tmp_path)
    with pytest.raises(ValueError, match="limit"):
        backend.list_datasets(limit=bad_limit)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"provider": ""},
        {"provider": []},
        {"ticker": ""},
        {"ticker": []},
        {"since": "2026-01-01"},
        {"until": True},
    ],
)
def test_list_datasets_rejects_malformed_filters(tmp_path: Path, kwargs):
    """list_datasets must reject malformed filter shapes at the storage boundary."""
    backend = _make_backend(tmp_path)
    with pytest.raises(ValueError):
        backend.list_datasets(**kwargs)


@pytest.mark.parametrize("ticker", ["%", "____"])
def test_list_datasets_malformed_ticker_filter_is_no_match(tmp_path: Path, ticker):
    """Malformed string ticker filters must not behave like wildcards."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context(tickers=("TSLA",)))
    _write(backend, run_id)

    assert not backend.list_datasets(ticker=ticker)


def test_write_dataset_links_run(tmp_path: Path):
    """write_dataset must update the run sidecar's dataset_id field."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    run = backend.get_run(run_id)
    assert run.dataset_id == record.dataset_id


# ---------------------------------------------------------------------------
# Artifact write
# ---------------------------------------------------------------------------

def test_write_artifact_creates_file(tmp_path: Path):
    """write_artifact must write the content bytes to disk."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    payload = ArtifactWrite(
        artifact_type="debug_payload", content=b"payload", filename="data.json"
    )

    record = backend.write_artifact(run_id, payload)

    assert Path(record.location).read_bytes() == b"payload"
    assert len(record.content_hash) == 64


@pytest.mark.parametrize(
    "artifact,expected_path",
    [
        (
            ArtifactWrite("debug_payload", b"payload", "data.json"),
            None,
        ),
        (
            ArtifactWrite("sidecar", b"positions", "positions.csv"),
            Path("runs/missing-run/positions.csv"),
        ),
    ],
)
def test_write_artifact_missing_run_does_not_write_payload(
    tmp_path: Path,
    artifact: ArtifactWrite,
    expected_path: Path | None,
):
    """Missing-run artifact writes must fail before creating unmanaged files."""
    backend = _make_backend(tmp_path)

    with pytest.raises(KeyError, match="run not found"):
        backend.write_artifact("missing-run", artifact)

    if expected_path is not None:
        assert not (tmp_path / expected_path).exists()
    assert not (tmp_path / "debug").exists()


def test_write_sidecar_artifact_stays_under_run_dir(tmp_path: Path):
    """Sidecar artifacts must live under the run directory, not the debug directory."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    payload = ArtifactWrite(
        artifact_type="sidecar", content=b"positions", filename="positions.csv"
    )

    record = backend.write_artifact(run_id, payload)

    assert Path(record.location) == (tmp_path / "runs" / run_id / "positions.csv").resolve()
    assert Path(record.location).read_bytes() == b"positions"


def test_delete_run_artifacts_preserves_run_and_removes_payloads(tmp_path: Path):
    """Rollback cleanup must remove sidecars, debug artifacts, and output files."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    sidecar = backend.write_artifact(run_id, ArtifactWrite(
        artifact_type="sidecar",
        content=b"positions",
        filename="positions.csv",
    ))
    debug = backend.write_artifact(run_id, ArtifactWrite(
        artifact_type="run_log",
        content=b"{}",
        filename="run_log_reference.json",
    ))
    output_dir = tmp_path / "runs" / run_id / "output"
    output_dir.mkdir(parents=True)
    output_file = output_dir / "options_engine_output.csv"
    output_file.write_text("partial", encoding="utf-8")

    backend.delete_run_artifacts(run_id)

    assert (tmp_path / "runs" / run_id / "run.json").exists()
    assert not Path(sidecar.location).exists()
    assert not Path(debug.location).exists()
    assert not Path(debug.location).parent.exists()
    assert not output_file.exists()
    assert not output_dir.exists()


# ---------------------------------------------------------------------------
# Retention pruning
# ---------------------------------------------------------------------------

def test_pruning_removes_oldest_when_limit_exceeded(tmp_path: Path):
    """Datasets beyond max_runs_retained must be pruned after each write."""
    backend = _make_backend(tmp_path, max_runs_retained=2)
    run_id = backend.create_run(_make_context())
    r1 = _write(backend, run_id)
    r2 = _write(backend, run_id)
    r3 = _write(backend, run_id)

    records = backend.list_datasets()
    ids = {r.dataset_id for r in records}

    assert len(records) == 2
    assert r1.dataset_id not in ids
    assert r2.dataset_id in ids
    assert r3.dataset_id in ids
    assert backend.get_run(run_id).dataset_id == r3.dataset_id


def test_pruning_uses_created_at_not_meta_file_mtime(tmp_path: Path):
    """Retention must use semantic dataset age, not mutable filesystem mtimes."""
    backend = _make_backend(tmp_path, max_runs_retained=2)
    run_id = backend.create_run(_make_context())
    r1 = _write(backend, run_id)
    r2 = _write(backend, run_id)
    _touch_future_mtime(_meta_path(r1))

    r3 = _write(backend, run_id)

    ids = {record.dataset_id for record in backend.list_datasets()}
    assert r1.dataset_id not in ids
    assert r2.dataset_id in ids
    assert r3.dataset_id in ids


def test_pruning_selects_only_oldest_excess_meta_files(tmp_path: Path, monkeypatch):
    """Filesystem pruning must avoid fully sorting every retained meta file."""
    calls = []
    original_nsmallest = filesystem_mod.nsmallest

    def tracking_nsmallest(count, iterable, *, key=None):
        items = list(iterable)
        calls.append((count, len(items), key is not None))
        return original_nsmallest(count, items, key=key)

    monkeypatch.setattr(filesystem_mod, "nsmallest", tracking_nsmallest)
    backend = _make_backend(tmp_path, max_runs_retained=2)
    run_id = backend.create_run(_make_context())
    _write(backend, run_id)
    _write(backend, run_id)
    _write(backend, run_id)

    assert calls == [(1, 3, True)]


def test_pruning_clears_dataset_id_for_pruned_run(tmp_path: Path):
    """A pruned dataset must not remain advertised by its run sidecar."""
    backend = _make_backend(tmp_path, max_runs_retained=1)
    old_run_id = backend.create_run(_make_context())
    old_record = _write(backend, old_run_id)
    new_run_id = backend.create_run(_make_context(provider="marketdata"))
    new_record = _write(backend, new_run_id, provider="marketdata")

    assert backend.get_run(old_run_id).dataset_id is None
    assert backend.get_run(new_run_id).dataset_id == new_record.dataset_id
    with pytest.raises(KeyError, match="dataset not found"):
        backend.get_dataset(old_record.dataset_id)


def test_pruning_removes_artifact_file(tmp_path: Path):
    """Pruning must delete the artifact CSV in addition to the meta file."""
    backend = _make_backend(tmp_path, max_runs_retained=1)
    run_id = backend.create_run(_make_context())
    r1 = _write(backend, run_id)
    _write(backend, run_id)

    assert not Path(r1.location).exists()


def test_pruning_removes_positions_sidecar_for_pruned_run(tmp_path: Path):
    """Pruning must also remove a run's positions snapshot sidecar."""
    backend = _make_backend(tmp_path, max_runs_retained=1)
    run_id = backend.create_run(_make_context())
    record = backend.write_artifact(run_id, ArtifactWrite(
        artifact_type="sidecar",
        content=b"positions",
        filename="positions.csv",
    ))
    _write(backend, run_id)
    next_run_id = backend.create_run(_make_context(provider="marketdata"))
    _write(backend, next_run_id, provider="marketdata")

    assert not Path(record.location).exists()


def test_pruning_removes_all_sidecars_for_pruned_run(tmp_path: Path):
    """Pruning must remove every sidecar file, not just positions.csv."""
    backend = _make_backend(tmp_path, max_runs_retained=1)
    run_id = backend.create_run(_make_context())
    positions = backend.write_artifact(run_id, ArtifactWrite(
        artifact_type="sidecar",
        content=b"positions",
        filename="positions.csv",
    ))
    manifest = backend.write_artifact(run_id, ArtifactWrite(
        artifact_type="sidecar",
        content=b"manifest",
        filename="manifest.json",
    ))
    _write(backend, run_id)
    next_run_id = backend.create_run(_make_context(provider="marketdata"))
    _write(backend, next_run_id, provider="marketdata")

    assert not Path(positions.location).exists()
    assert not Path(manifest.location).exists()
    assert (tmp_path / "runs" / run_id / "run.json").exists()


def test_pruning_removes_run_log_artifact_for_pruned_run(tmp_path: Path):
    """Pruning must remove debug-dir run-log artifacts owned by a pruned run."""
    backend = _make_backend(tmp_path, max_runs_retained=1)
    run_id = backend.create_run(_make_context())
    record = backend.write_artifact(run_id, ArtifactWrite(
        artifact_type="run_log",
        content=b'{"path": "/tmp/opx_runs.log"}',
        filename="run_log_reference.json",
    ))
    artifact_path = Path(record.location)
    artifact_dir = artifact_path.parent
    _write(backend, run_id)
    next_run_id = backend.create_run(_make_context(provider="marketdata"))
    _write(backend, next_run_id, provider="marketdata")

    assert not artifact_path.exists()
    assert not artifact_dir.exists()
    assert (tmp_path / "runs" / run_id / "run.json").exists()


def test_no_pruning_when_max_runs_retained_zero(tmp_path: Path):
    """When max_runs_retained = 0 (default), no datasets are ever pruned."""
    backend = _make_backend(tmp_path, max_runs_retained=0)
    run_id = backend.create_run(_make_context())
    for _ in range(5):
        _write(backend, run_id)

    assert len(backend.list_datasets()) == 5


def test_orphan_dataset_artifacts_are_swept_on_backend_init(tmp_path: Path):
    """Startup cleanup must remove dataset artifacts that lack metadata."""
    run_id = "orphan-run"
    output_dir = tmp_path / "runs" / run_id / "output"
    output_dir.mkdir(parents=True)
    orphan = output_dir / "orphan.csv"
    orphan.write_text("ticker,strike\nTSLA,100\n", encoding="utf-8")

    _make_backend(tmp_path)

    assert not orphan.exists()


def test_orphan_dataset_artifacts_are_swept_after_successful_write(tmp_path: Path):
    """Retention cleanup must sweep orphan artifacts even with retention disabled."""
    backend = _make_backend(tmp_path, max_runs_retained=0)
    run_id = backend.create_run(_make_context())
    output_dir = tmp_path / "runs" / run_id / "output"
    output_dir.mkdir(parents=True)
    orphan = output_dir / "orphan.csv"
    orphan.write_text("ticker,strike\nTSLA,100\n", encoding="utf-8")

    _write(backend, run_id)

    assert not orphan.exists()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def test_factory_returns_none_when_storage_disabled():
    """get_storage_backend must return None when storage_enabled = False."""
    config = make_runtime_config(storage_enabled=False)
    assert get_storage_backend(config) is None


def test_factory_returns_filesystem_backend_when_enabled(tmp_path: Path):
    """get_storage_backend must return a FilesystemBackend when enabled."""
    config = make_runtime_config(
        storage_enabled=True,
        storage_backend="filesystem",
        storage_dir=tmp_path,
        debug_dump_dir=tmp_path / "debug",
    )
    backend = get_storage_backend(config)
    assert isinstance(backend, FilesystemBackend)


def test_factory_reuses_filesystem_backend_for_same_config(tmp_path: Path):
    """Repeated factory calls with the same config must reuse the backend."""
    config = make_runtime_config(
        storage_enabled=True,
        storage_backend="filesystem",
        storage_dir=tmp_path,
        debug_dump_dir=tmp_path / "debug",
    )

    assert get_storage_backend(config) is get_storage_backend(config)


def test_factory_rebuilds_filesystem_backend_when_config_changes(tmp_path: Path):
    """Storage-affecting config changes must produce a separate backend."""
    config = make_runtime_config(
        storage_enabled=True,
        storage_backend="filesystem",
        storage_dir=tmp_path / "one",
        debug_dump_dir=tmp_path / "debug",
    )
    changed = make_runtime_config(
        storage_enabled=True,
        storage_backend="filesystem",
        storage_dir=tmp_path / "two",
        debug_dump_dir=tmp_path / "debug",
    )

    assert get_storage_backend(config) is not get_storage_backend(changed)


# ---------------------------------------------------------------------------
# Parquet format
# ---------------------------------------------------------------------------

def test_write_dataset_parquet_creates_parquet_file(tmp_path: Path):
    """write_dataset with payload format='parquet' must create a parquet artifact."""
    pytest.importorskip("pyarrow")
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = backend.write_dataset(
        run_id,
        DatasetWrite(
            data=_make_dataframe(),
            provider="yfinance",
            schema_version=1,
            format="parquet",
        ),
    )

    assert record.format == "parquet"
    assert Path(record.location).suffix == ".parquet"
    assert Path(record.location).exists()


def test_write_dataset_parquet_is_readable(tmp_path: Path):
    """A parquet artifact written by FilesystemBackend must be readable by pandas."""
    pytest.importorskip("pyarrow")
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    df = _make_dataframe()
    record = backend.write_dataset(
        run_id,
        DatasetWrite(data=df, provider="yfinance", schema_version=1, format="parquet"),
    )

    result = pd.read_parquet(record.location)
    assert list(result.columns) == list(df.columns)
    assert len(result) == len(df)


def test_write_dataset_uses_payload_format_over_backend_default(tmp_path: Path):
    """DatasetWrite.format must control serialization even when backend default differs."""
    backend = _make_backend(tmp_path, dataset_format="parquet")
    run_id = backend.create_run(_make_context())
    record = backend.write_dataset(
        run_id,
        DatasetWrite(data=_make_dataframe(), provider="yfinance", schema_version=1, format="csv"),
    )

    assert record.format == "csv"
    assert Path(record.location).suffix == ".csv"
    assert pd.read_csv(record.location).shape[0] == record.row_count


def test_factory_passes_dataset_format_to_backend(tmp_path: Path):
    """get_storage_backend must preflight storage_dataset_format from config."""
    config = make_runtime_config(
        storage_enabled=True,
        storage_backend="filesystem",
        storage_dataset_format="parquet",
        storage_dir=tmp_path,
        debug_dump_dir=tmp_path / "debug",
    )
    backend = get_storage_backend(config)
    assert isinstance(backend, FilesystemBackend)
    assert "_dataset_format" not in vars(backend)
    assert "_serializer" not in vars(backend)


# ---------------------------------------------------------------------------
# get_run error path
# ---------------------------------------------------------------------------

def test_get_run_raises_for_unknown_id(tmp_path: Path):
    """get_run must raise KeyError when the run sidecar does not exist."""
    backend = _make_backend(tmp_path)
    with pytest.raises(KeyError, match="run not found"):
        backend.get_run("no-such-run")


# ---------------------------------------------------------------------------
# list_datasets date range filters
# ---------------------------------------------------------------------------

def test_list_datasets_since_excludes_older_records(tmp_path: Path):
    """list_datasets(since=T) must exclude records whose created_at is before T."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    future = record.created_at + timedelta(seconds=1)
    results = backend.list_datasets(since=future)

    assert not results


def test_list_datasets_until_excludes_newer_records(tmp_path: Path):
    """list_datasets(until=T) must exclude records whose created_at is after T."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    past = record.created_at - timedelta(seconds=1)
    results = backend.list_datasets(until=past)

    assert not results


def test_list_datasets_normalizes_legacy_naive_created_at(tmp_path: Path):
    """Legacy naive dataset timestamps should not crash date filtering."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)
    meta_path = _meta_path(record)
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    metadata["created_at"] = "2026-05-27T12:00:00"
    meta_path.write_text(json.dumps(metadata), encoding="utf-8")
    (tmp_path / "runs" / "datasets.index.json").unlink()

    results = backend.list_datasets(
        since=datetime(2026, 5, 27, 11, 59, tzinfo=timezone.utc),
        until=datetime(2026, 5, 27, 12, 1, tzinfo=timezone.utc),
    )

    assert [item.dataset_id for item in results] == [record.dataset_id]
    assert results[0].created_at == datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)


def test_list_datasets_orders_by_created_at_not_meta_file_mtime(tmp_path: Path):
    """Filesystem listing order should match SQLite's created_at ordering."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    r1 = _write(backend, run_id)
    r2 = _write(backend, run_id)
    _touch_future_mtime(_meta_path(r1))

    results = backend.list_datasets()

    assert [record.dataset_id for record in results[:2]] == [r2.dataset_id, r1.dataset_id]


# ---------------------------------------------------------------------------
# Pruning resilience
# ---------------------------------------------------------------------------

def test_prune_tolerates_corrupt_meta_file(tmp_path: Path):
    """A corrupt meta JSON must be silently skipped and removed during pruning."""
    backend = _make_backend(tmp_path, max_runs_retained=1)
    run_id = backend.create_run(_make_context())
    _write(backend, run_id)

    corrupt_dir = tmp_path / "runs" / "fake-run-id" / "output"
    corrupt_dir.mkdir(parents=True, exist_ok=True)
    corrupt_meta = corrupt_dir / "corrupt.meta.json"
    corrupt_meta.write_text("not-valid-json", encoding="utf-8")

    _write(backend, run_id)

    assert not corrupt_meta.exists()


# ---------------------------------------------------------------------------
# get_serializer error path
# ---------------------------------------------------------------------------

def test_get_serializer_raises_for_unknown_format():
    """get_serializer must raise ValueError for an unrecognised format name."""
    from opx_chain.storage.serializers import get_serializer  # pylint: disable=import-outside-toplevel
    with pytest.raises(ValueError, match="Unsupported dataset format"):
        get_serializer("avro")


# ---------------------------------------------------------------------------
# count_runs_today
# ---------------------------------------------------------------------------

def test_count_runs_today_counts_same_provider_only(tmp_path: Path):
    """count_runs_today must count complete runs for the given provider, not others."""
    backend = _make_backend(tmp_path)
    market_run_1 = backend.create_run(_make_context(provider="marketdata"))
    market_run_2 = backend.create_run(_make_context(provider="marketdata"))
    market_running = backend.create_run(_make_context(provider="marketdata"))
    market_failed = backend.create_run(_make_context(provider="marketdata"))
    yahoo_run = backend.create_run(_make_context(provider="yfinance"))

    backend.finalize_run(market_run_1, RunSummary(status="complete"))
    backend.finalize_run(market_run_2, RunSummary(status="complete"))
    backend.fail_run(market_failed, "failed")
    backend.finalize_run(yahoo_run, RunSummary(status="complete"))

    assert backend.count_runs_today("marketdata") == 2
    assert backend.count_runs_today("yfinance") == 1
    assert backend.get_run(market_running).status == "running"


def test_count_runs_today_returns_zero_when_no_runs(tmp_path: Path):
    """count_runs_today must return 0 when no runs exist for that provider."""
    backend = _make_backend(tmp_path)
    assert backend.count_runs_today("marketdata") == 0


@pytest.mark.parametrize("provider", [None, "", [], {}])
def test_count_runs_today_rejects_malformed_provider(tmp_path: Path, provider):
    """count_runs_today must reject malformed provider values consistently."""
    backend = _make_backend(tmp_path)
    with pytest.raises(ValueError, match="provider"):
        backend.count_runs_today(provider)


def test_count_runs_today_reuses_process_cache(tmp_path: Path, monkeypatch):
    """Repeated same-day counts should not rescan every run sidecar."""
    backend = _make_backend(tmp_path)
    market_run = backend.create_run(_make_context(provider="marketdata"))
    backend.finalize_run(market_run, RunSummary(status="complete"))

    assert backend.count_runs_today("marketdata") == 1

    def fail_read_text(*_args, **_kwargs):
        raise AssertionError("count_runs_today should use cached count")

    monkeypatch.setattr(Path, "read_text", fail_read_text)

    assert backend.count_runs_today("marketdata") == 1


def test_count_runs_today_cache_invalidates_on_run_write(tmp_path: Path):
    """Run-sidecar writes must clear cached daily counts."""
    backend = _make_backend(tmp_path)
    market_run = backend.create_run(_make_context(provider="marketdata"))

    assert backend.count_runs_today("marketdata") == 0

    backend.finalize_run(market_run, RunSummary(status="complete"))

    assert backend.count_runs_today("marketdata") == 1


def test_count_runs_today_handles_legacy_naive_started_at(tmp_path: Path):
    """Legacy naive run timestamps should be normalized before day counting."""
    backend = _make_backend(tmp_path)
    market_run = backend.create_run(_make_context(provider="marketdata"))
    backend.finalize_run(market_run, RunSummary(status="complete"))
    run_path = tmp_path / "runs" / market_run / "run.json"
    data = json.loads(run_path.read_text(encoding="utf-8"))
    data["started_at"] = datetime.now(tz=timezone.utc).replace(tzinfo=None).isoformat()
    run_path.write_text(json.dumps(data), encoding="utf-8")

    assert backend.count_runs_today("marketdata") == 1


def test_interrupt_stale_runs_marks_old_running_sidecars(tmp_path: Path):
    """Stale running run sidecars should converge to interrupted."""
    backend = _make_backend(tmp_path)
    stale_run = backend.create_run(_make_context(provider="marketdata"))
    fresh_run = backend.create_run(_make_context(provider="marketdata"))
    stale_path = tmp_path / "runs" / stale_run / "run.json"
    data = json.loads(stale_path.read_text(encoding="utf-8"))
    data["started_at"] = (datetime.now(tz=timezone.utc) - timedelta(minutes=5)).isoformat()
    stale_path.write_text(json.dumps(data), encoding="utf-8")

    count = backend.interrupt_stale_runs(
        datetime.now(tz=timezone.utc) - timedelta(seconds=30),
        "process_terminated_uncleanly",
    )

    assert count == 1
    stale_record = backend.get_run(stale_run)
    assert stale_record.status == "interrupted"
    assert stale_record.finished_at is not None
    assert stale_record.error_summary == "process_terminated_uncleanly"
    assert backend.get_run(fresh_run).status == "running"
