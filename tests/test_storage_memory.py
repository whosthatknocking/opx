"""Tests for MemoryBackend: protocol satisfaction and write roundtrips."""

import inspect
import io
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from conftest import make_option_chain_frame
from opx_chain import SCHEMA_VERSION
from opx_chain.storage.base import StorageBackend
from opx_chain.storage._disk import content_hash_for_bytes
from opx_chain.storage.memory import MemoryBackend
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


def _make_context(**kwargs):
    defaults = {
        "provider": "yfinance",
        "tickers": ("TSLA",),
        "config_fingerprint": "abc123",
        "positions_fingerprint": "",
    }
    return RunContext(**{**defaults, **kwargs})


def _make_dataframe(rows=3):
    return make_option_chain_frame(rows=rows)


def _write(backend, run_id, rows=3, provider="yfinance"):
    record = backend.write_dataset(
        run_id,
        DatasetWrite(
            data=make_option_chain_frame(rows=rows, provider=provider),
            provider=provider,
            schema_version=SCHEMA_VERSION,
        ),
    )
    backend.finalize_run(run_id, RunSummary(status="complete"))
    return record


def _record_ticker(backend, run_id, ticker):
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


# ---------------------------------------------------------------------------
# Protocol satisfaction
# ---------------------------------------------------------------------------

def test_memory_backend_satisfies_protocol():
    """MemoryBackend must satisfy the StorageBackend runtime-checkable protocol."""
    assert isinstance(MemoryBackend(), StorageBackend)


# ---------------------------------------------------------------------------
# Full run lifecycle roundtrip
# ---------------------------------------------------------------------------

def test_create_run_returns_string_id():
    """create_run must return a non-empty string identifier."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context(tickers=("TSLA", "NVDA")))
    assert isinstance(run_id, str) and run_id
    assert backend.get_run(run_id).tickers == ("TSLA", "NVDA")
    assert backend.get_run(run_id).script_version == __version__


def test_write_dataset_roundtrip():
    """write_dataset must return a DatasetRecord with correct fields."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    df = _make_dataframe()
    payload = DatasetWrite(data=df, provider="yfinance", schema_version=SCHEMA_VERSION)

    record = backend.write_dataset(run_id, payload)

    assert isinstance(record, DatasetRecord)
    assert record.run_id == run_id
    assert record.row_count == len(df)
    assert record.schema_version == SCHEMA_VERSION
    assert record.provider == "yfinance"
    assert record.format == "csv"
    assert len(record.content_hash) == 64
    assert record.location.startswith("memory://")
    assert record.script_version == __version__


def test_get_dataset_returns_handle():
    """get_dataset must return a DatasetHandle matching the DatasetRecord."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    handle = backend.get_dataset(record.dataset_id)

    assert isinstance(handle, DatasetHandle)
    assert handle.dataset_id == record.dataset_id
    assert handle.run_id == record.run_id
    assert handle.provider == record.provider
    assert handle.schema_version == record.schema_version
    assert handle.content_hash == record.content_hash
    assert handle.created_at == record.created_at
    assert handle.row_count == record.row_count
    assert handle.format == record.format
    assert handle.script_version == record.script_version


def test_get_dataset_raises_for_unknown_id():
    """get_dataset must raise KeyError for an unrecognised dataset_id."""
    backend = MemoryBackend()
    with pytest.raises(KeyError):
        backend.get_dataset("no-such-id")


def test_list_datasets_most_recent_first():
    """list_datasets must return datasets newest-first."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    r1 = _write(backend, run_id, rows=1)
    r2 = _write(backend, run_id, rows=2)

    records = backend.list_datasets()

    assert records[0].dataset_id == r2.dataset_id
    assert records[1].dataset_id == r1.dataset_id


def test_list_datasets_limit():
    """list_datasets must honour the limit parameter."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    for _ in range(5):
        _write(backend, run_id)

    assert len(backend.list_datasets(limit=2)) == 2


def test_list_datasets_filter_provider():
    """list_datasets must filter by provider when the argument is given."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    _write(backend, run_id, provider="yfinance")
    _write(backend, run_id, provider="marketdata")

    results = backend.list_datasets(provider="yfinance")

    assert len(results) == 1
    assert results[0].provider == "yfinance"


def test_list_datasets_filter_ticker():
    """list_datasets must filter by ticker before applying the limit."""
    backend = MemoryBackend()
    tsla_run_id = backend.create_run(_make_context(tickers=("TSLA",)))
    _record_ticker(backend, tsla_run_id, "TSLA")
    tsla_record = _write(backend, tsla_run_id)
    aapl_run_id = backend.create_run(_make_context(tickers=("AAPL",)))
    _record_ticker(backend, aapl_run_id, "AAPL")
    _write(backend, aapl_run_id)

    results = backend.list_datasets(limit=1, ticker="tsla")

    assert [record.dataset_id for record in results] == [tsla_record.dataset_id]


def test_list_datasets_filter_ticker_uses_run_context_tickers():
    """Ticker filtering should work before any per-ticker result rows are recorded."""
    backend = MemoryBackend()
    tsla_run_id = backend.create_run(_make_context(tickers=("TSLA",)))
    tsla_record = _write(backend, tsla_run_id)
    aapl_run_id = backend.create_run(_make_context(tickers=("AAPL",)))
    _write(backend, aapl_run_id)

    results = backend.list_datasets(limit=1, ticker="tsla")

    assert [record.dataset_id for record in results] == [tsla_record.dataset_id]


def test_list_datasets_empty():
    """list_datasets on a fresh backend must return an empty list."""
    assert not MemoryBackend().list_datasets()


@pytest.mark.parametrize("bad_limit", [-1, True, 1.5, "2", None, [], {}])
def test_list_datasets_rejects_malformed_limit(bad_limit):
    """list_datasets must reject non-integer and negative limits consistently."""
    backend = MemoryBackend()
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
def test_list_datasets_rejects_malformed_filters(kwargs):
    """list_datasets must reject malformed filter shapes at the storage boundary."""
    backend = MemoryBackend()
    with pytest.raises(ValueError):
        backend.list_datasets(**kwargs)


@pytest.mark.parametrize("ticker", ["%", "____"])
def test_list_datasets_malformed_ticker_filter_is_no_match(ticker):
    """Malformed string ticker filters must not behave like wildcards."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context(tickers=("TSLA",)))
    _write(backend, run_id)

    assert not backend.list_datasets(ticker=ticker)


# ---------------------------------------------------------------------------
# Ticker results
# ---------------------------------------------------------------------------

def test_record_ticker_result_stored():
    """record_ticker_result must persist the result under the run_id."""
    backend = MemoryBackend()
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

    stored = backend.get_ticker_results(run_id)
    assert len(stored) == 1
    assert stored[0].ticker == "TSLA"
    assert stored[0].kept_row_count == 40


def test_get_ticker_results_returns_empty_list_for_run_without_results():
    """get_ticker_results should return an empty list when a run has no ticker rows."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())

    assert not backend.get_ticker_results(run_id)


def test_get_ticker_results_raises_for_unknown_run():
    """get_ticker_results should match get_run semantics for unknown run IDs."""
    backend = MemoryBackend()

    with pytest.raises(KeyError, match="run not found"):
        backend.get_ticker_results("missing-run")


def test_record_validation_stored():
    """record_validation must persist grouped validation findings under the run_id."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    record = ValidationRecord(
        run_id=run_id,
        severity="warning",
        code="MISSING_FIELD",
        count=2,
        sample='{"field": "bid"}',
    )

    backend.record_validation(record)

    stored = backend._validations[run_id]  # pylint: disable=protected-access
    assert stored == [record]


# ---------------------------------------------------------------------------
# Artifact write
# ---------------------------------------------------------------------------

def test_write_artifact_roundtrip():
    """write_artifact must return an ArtifactRecord with a valid content_hash."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    payload = ArtifactWrite(
        artifact_type="debug_payload", content=b"hello", filename="debug.json"
    )

    record = backend.write_artifact(run_id, payload)

    assert record.run_id == run_id
    assert record.artifact_type == "debug_payload"
    assert len(record.content_hash) == 64
    assert "debug.json" in record.location


def test_delete_run_artifacts_removes_artifacts():
    """delete_run_artifacts must discard artifacts while preserving the run."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    backend.write_artifact(
        run_id,
        ArtifactWrite(artifact_type="sidecar", content=b"positions", filename="positions.csv"),
    )

    backend.delete_run_artifacts(run_id)

    assert backend.get_run(run_id).run_id == run_id
    assert run_id not in backend._artifacts  # pylint: disable=protected-access


def test_delete_run_artifacts_preserves_dataset_bytes():
    """Artifact cleanup must not confuse artifact IDs with dataset byte keys."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    dataset = _write(backend, run_id)
    backend.write_artifact(
        run_id,
        ArtifactWrite(artifact_type="sidecar", content=b"positions", filename="positions.csv"),
    )

    backend.delete_run_artifacts(run_id)

    assert dataset.dataset_id in backend._dataset_bytes  # pylint: disable=protected-access
    assert backend.get_dataset(dataset.dataset_id).dataset_id == dataset.dataset_id


def test_memory_pruning_removes_old_dataset_records_bytes_and_links():
    """Memory retention should mirror persisted backends for dataset cleanup."""
    backend = MemoryBackend(max_runs_retained=1)
    old_run_id = backend.create_run(_make_context(provider="yfinance"))
    old_record = _write(backend, old_run_id, provider="yfinance")
    backend.write_artifact(
        old_run_id,
        ArtifactWrite(artifact_type="sidecar", content=b"positions", filename="positions.csv"),
    )
    new_run_id = backend.create_run(_make_context(provider="marketdata"))
    new_record = _write(backend, new_run_id, provider="marketdata")

    assert [record.dataset_id for record in backend.list_datasets(limit=10)] == [
        new_record.dataset_id
    ]
    assert old_record.dataset_id not in backend._dataset_bytes  # pylint: disable=protected-access
    assert new_record.dataset_id in backend._dataset_bytes  # pylint: disable=protected-access
    assert backend.get_run(old_run_id).dataset_id is None
    assert backend.get_run(new_run_id).dataset_id == new_record.dataset_id
    assert old_run_id not in backend._artifacts  # pylint: disable=protected-access
    with pytest.raises(KeyError):
        backend.get_dataset(old_record.dataset_id)


# ---------------------------------------------------------------------------
# Run lifecycle transitions
# ---------------------------------------------------------------------------

def test_finalize_run_sets_status_complete():
    """finalize_run must update status, finished_at, and clear error_summary."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    backend.finalize_run(run_id, RunSummary(status="complete"))

    run = backend._runs[run_id]  # pylint: disable=protected-access
    assert run.status == "complete"
    assert run.finished_at is not None
    assert run.error_summary is None


def test_fail_run_sets_status_and_error():
    """fail_run must mark the run as failed and store the error message."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    backend.fail_run(run_id, "provider timeout")

    run = backend._runs[run_id]  # pylint: disable=protected-access
    assert run.status == "failed"
    assert run.error_summary == "provider timeout"
    assert run.finished_at is not None


def test_terminal_run_status_is_not_overwritten():
    """Late lifecycle calls must not demote already terminal run records."""
    backend = MemoryBackend()

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


def test_write_dataset_links_run_to_dataset_id():
    """write_dataset must update the run record's dataset_id field."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    assert backend._runs[run_id].dataset_id == record.dataset_id  # pylint: disable=protected-access


def test_write_dataset_parquet_stores_matching_bytes():
    """MemoryBackend must not store CSV bytes under parquet metadata."""
    pytest.importorskip("pyarrow")
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    df = _make_dataframe()
    record = backend.write_dataset(
        run_id,
        DatasetWrite(
            data=df,
            provider="yfinance",
            schema_version=SCHEMA_VERSION,
            format="parquet",
        ),
    )

    content = backend._dataset_bytes[record.dataset_id]  # pylint: disable=protected-access
    result = pd.read_parquet(io.BytesIO(content))

    assert record.format == "parquet"
    assert record.location.endswith(".parquet")
    assert list(result.columns) == list(df.columns)
    assert len(result) == len(df)


def test_write_dataset_uses_shared_serializer_bytes_path():
    """MemoryBackend should delegate exact-byte work to the shared gate."""
    source = inspect.getsource(MemoryBackend.write_dataset)

    assert "prepare_option_chain_dataset" in source
    assert "prepared.content" in source
    assert "to_csv" not in source
    assert "to_parquet" not in source


def test_write_dataset_rejects_unknown_format():
    """MemoryBackend should reject unsupported dataset formats through serializers."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())

    with pytest.raises(ValueError, match="Unsupported dataset format"):
        backend.write_dataset(
            run_id,
            DatasetWrite(
                data=_make_dataframe(),
                provider="yfinance",
                schema_version=SCHEMA_VERSION,
                format="avro",
            ),
        )


def test_content_hash_is_deterministic():
    """Identical DataFrames written twice must produce the same content_hash."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    df = _make_dataframe()
    def make_write():
        return DatasetWrite(
            data=df.copy(),
            provider="yfinance",
            schema_version=SCHEMA_VERSION,
        )

    r1 = backend.write_dataset(run_id, make_write())
    r2 = backend.write_dataset(run_id, make_write())

    assert r1.content_hash == r2.content_hash


def test_content_hash_helper_uses_sha256():
    """Storage backends share the same content-hash algorithm."""
    assert content_hash_for_bytes(b"abc") == (
        "ba7816bf8f01cfea414140de5dae2223"
        "b00361a396177a9cb410ff61f20015ad"
    )


def test_schema_version_constant_is_positive_int():
    """SCHEMA_VERSION must be importable from opx and be a positive integer."""
    assert isinstance(SCHEMA_VERSION, int) and SCHEMA_VERSION >= 1


# ---------------------------------------------------------------------------
# get_run
# ---------------------------------------------------------------------------

def test_get_run_returns_record():
    """get_run must return the RunRecord via the public API."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context(provider="marketdata"))

    run = backend.get_run(run_id)

    assert run.run_id == run_id
    assert run.provider == "marketdata"
    assert run.status == "running"
    assert run.finished_at is None


def test_get_run_raises_for_unknown_id():
    """get_run must raise KeyError for a run_id that was never created."""
    backend = MemoryBackend()
    with pytest.raises(KeyError):
        backend.get_run("no-such-run")


# ---------------------------------------------------------------------------
# list_datasets date range filters
# ---------------------------------------------------------------------------

def test_list_datasets_since_excludes_older_records():
    """list_datasets(since=T) must exclude records created before T."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    future = record.created_at + timedelta(seconds=1)
    results = backend.list_datasets(since=future)

    assert not results


def test_list_datasets_since_includes_records_at_boundary():
    """list_datasets(since=T) must include a record created exactly at T."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    results = backend.list_datasets(since=record.created_at)

    assert len(results) == 1
    assert results[0].dataset_id == record.dataset_id


def test_list_datasets_until_excludes_newer_records():
    """list_datasets(until=T) must exclude records created after T."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    past = record.created_at - timedelta(seconds=1)
    results = backend.list_datasets(until=past)

    assert not results


def test_list_datasets_until_includes_records_at_boundary():
    """list_datasets(until=T) must include a record created exactly at T."""
    backend = MemoryBackend()
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    results = backend.list_datasets(until=record.created_at)

    assert len(results) == 1
    assert results[0].dataset_id == record.dataset_id


# ---------------------------------------------------------------------------
# count_runs_today
# ---------------------------------------------------------------------------

def test_count_runs_today_counts_same_provider_only():
    """count_runs_today must count complete runs for the given provider, not others."""
    backend = MemoryBackend()
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


def test_count_runs_today_returns_zero_when_no_runs():
    """count_runs_today must return 0 when no runs exist for that provider."""
    backend = MemoryBackend()
    assert backend.count_runs_today("marketdata") == 0


@pytest.mark.parametrize("provider", [None, "", [], {}])
def test_count_runs_today_rejects_malformed_provider(provider):
    """count_runs_today must reject malformed provider values consistently."""
    backend = MemoryBackend()
    with pytest.raises(ValueError, match="provider"):
        backend.count_runs_today(provider)


def test_interrupt_stale_runs_marks_old_running_records():
    """Stale running memory records should converge to interrupted."""
    backend = MemoryBackend()
    stale_run = backend.create_run(_make_context(provider="marketdata"))
    fresh_run = backend.create_run(_make_context(provider="marketdata"))
    backend._runs[stale_run].started_at = (  # pylint: disable=protected-access
        datetime.now(tz=timezone.utc) - timedelta(minutes=5)
    )

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
