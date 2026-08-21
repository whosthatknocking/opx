"""Tests for SqliteIndexedBackend and factory sqlite path."""
# pylint: disable=duplicate-code,too-many-lines

import gc
import hashlib
import sqlite3
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from conftest import make_option_chain_frame, make_runtime_config
from opx_chain import SCHEMA_VERSION
from opx_chain.storage.base import StorageBackend
from opx_chain.storage.factory import get_storage_backend
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
import opx_chain.storage.sqlite_indexed as sqlite_indexed_mod
from opx_chain.storage.sqlite_indexed import SqliteIndexedBackend


def _make_backend(
    tmp_path: Path,
    max_runs_retained: int = 0,
    dataset_format: str = "csv",
) -> SqliteIndexedBackend:
    return SqliteIndexedBackend(
        db_path=tmp_path / "opx-chain.db",
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
    return make_option_chain_frame(rows=rows)


def _write(backend: SqliteIndexedBackend, run_id: str, rows: int = 3, provider: str = "yfinance"):
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


def _record_ticker(backend: SqliteIndexedBackend, run_id: str, ticker: str) -> None:
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

def test_sqlite_backend_satisfies_protocol(tmp_path: Path):
    """SqliteIndexedBackend must satisfy the StorageBackend runtime-checkable protocol."""
    assert isinstance(_make_backend(tmp_path), StorageBackend)


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

def test_schema_initialises_on_first_connect(tmp_path: Path):
    """Constructor must create all tables and seed schema_version."""
    backend = _make_backend(tmp_path)
    conn = sqlite3.connect(str(tmp_path / "opx-chain.db"))
    master = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    tables = {r[0] for r in master}
    assert {"runs", "datasets", "ticker_results", "artifacts", "_schema_meta"}.issubset(tables)
    conn.close()
    run_id = backend.create_run(_make_context())
    assert run_id


def test_schema_migration_updates_version_and_applies_sql(tmp_path: Path, monkeypatch):
    """Existing databases must run migrations when the backend schema version advances."""
    _make_backend(tmp_path)
    next_version = sqlite_indexed_mod._SCHEMA_VERSION + 1  # pylint: disable=protected-access
    monkeypatch.setattr(sqlite_indexed_mod, "_SCHEMA_VERSION", next_version)
    monkeypatch.setattr(
        sqlite_indexed_mod,
        "_SCHEMA_MIGRATIONS",
        {next_version: "ALTER TABLE runs ADD COLUMN migration_marker TEXT;"},
    )

    _make_backend(tmp_path)

    conn = sqlite3.connect(str(tmp_path / "opx-chain.db"))
    try:
        version = conn.execute(
            "SELECT value FROM _schema_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(runs)").fetchall()
        }
    finally:
        conn.close()

    assert version == str(next_version)
    assert "migration_marker" in columns


def test_schema_migration_recovers_partial_add_column_state(tmp_path: Path):
    """Idempotent migrations must recover when a previous ALTER partially succeeded."""
    db_path = tmp_path / "opx-chain.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE _schema_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO _schema_meta VALUES ('schema_version', '1');
            CREATE TABLE runs (
                run_id                TEXT PRIMARY KEY,
                started_at            TEXT NOT NULL,
                finished_at           TEXT,
                status                TEXT NOT NULL,
                provider              TEXT NOT NULL,
                tickers               TEXT NOT NULL DEFAULT '[]',
                script_version        TEXT NOT NULL DEFAULT 'unknown',
                config_fingerprint    TEXT NOT NULL,
                positions_fingerprint TEXT NOT NULL,
                dataset_id            TEXT,
                error_summary         TEXT
            );
            CREATE TABLE datasets (
                dataset_id      TEXT PRIMARY KEY,
                run_id          TEXT NOT NULL REFERENCES runs(run_id),
                created_at      TEXT NOT NULL,
                provider        TEXT NOT NULL,
                schema_version  INTEGER NOT NULL,
                row_count       INTEGER NOT NULL,
                format          TEXT NOT NULL,
                location        TEXT NOT NULL,
                content_hash    TEXT NOT NULL
            );
            """
        )
        conn.commit()
    finally:
        conn.close()

    _make_backend(tmp_path)

    conn = sqlite3.connect(db_path)
    try:
        version = conn.execute(
            "SELECT value FROM _schema_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        run_columns = {row[1] for row in conn.execute("PRAGMA table_info(runs)").fetchall()}
        dataset_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(datasets)").fetchall()
        }
    finally:
        conn.close()

    assert version == str(sqlite_indexed_mod._SCHEMA_VERSION)  # pylint: disable=protected-access
    assert {"tickers", "script_version"}.issubset(run_columns)
    assert {"script_version", "created_at_sort_key"}.issubset(dataset_columns)


def test_schema_backfills_dataset_created_at_sort_key(tmp_path: Path):
    """Startup should repair retained rows missing the normalized created-at sort key."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE datasets SET created_at = ?, created_at_sort_key = NULL "
            "WHERE dataset_id = ?",
            ("2026-05-27T12:00:00Z", record.dataset_id),
        )
        conn.commit()
    finally:
        conn.close()
    backend.close()

    _make_backend(tmp_path)

    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        sort_key = conn.execute(
            "SELECT created_at_sort_key FROM datasets WHERE dataset_id = ?",
            (record.dataset_id,),
        ).fetchone()[0]
    finally:
        conn.close()

    assert sort_key is not None


def test_list_datasets_commits_created_at_sort_key_backfill(tmp_path: Path):
    """Listing-triggered sort-key repair must not leave a pooled write transaction open."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)
    db_path = tmp_path / "opx-chain.db"

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE datasets SET created_at = ?, created_at_sort_key = NULL "
            "WHERE dataset_id = ?",
            ("2026-05-27T12:00:00Z", record.dataset_id),
        )
        conn.commit()
    finally:
        conn.close()

    records = backend.list_datasets(limit=1)

    assert [dataset.dataset_id for dataset in records] == [record.dataset_id]
    assert backend._connection is not None  # pylint: disable=protected-access
    assert not backend._connection.in_transaction  # pylint: disable=protected-access

    conn = sqlite3.connect(db_path, timeout=0.1)
    try:
        sort_key = conn.execute(
            "SELECT created_at_sort_key FROM datasets WHERE dataset_id = ?",
            (record.dataset_id,),
        ).fetchone()[0]
        conn.execute("UPDATE _schema_meta SET value = value WHERE key = 'schema_version'")
        conn.commit()
    finally:
        conn.close()

    assert sort_key is not None


def test_table_columns_rejects_unsafe_table_identifier(tmp_path: Path):
    """Migration idempotence checks must not interpolate arbitrary table names."""
    backend = _make_backend(tmp_path)
    conn = sqlite3.connect(str(tmp_path / "opx-chain.db"))
    conn.row_factory = sqlite3.Row
    try:
        assert "run_id" in backend._table_columns(conn, "runs")  # pylint: disable=protected-access
        with pytest.raises(ValueError, match="Unsafe SQLite table identifier"):
            backend._table_columns(  # pylint: disable=protected-access
                conn,
                'runs"; DROP TABLE runs; --',
            )
        runs = conn.execute("SELECT name FROM sqlite_master WHERE name = 'runs'").fetchone()
    finally:
        conn.close()

    assert runs is not None


def test_schema_migration_fails_when_required_step_is_missing(tmp_path: Path, monkeypatch):
    """A schema-version bump without a migration must fail instead of silently reusing v1."""
    _make_backend(tmp_path)
    next_version = sqlite_indexed_mod._SCHEMA_VERSION + 1  # pylint: disable=protected-access
    monkeypatch.setattr(sqlite_indexed_mod, "_SCHEMA_VERSION", next_version)
    monkeypatch.setattr(sqlite_indexed_mod, "_SCHEMA_MIGRATIONS", {})

    with pytest.raises(RuntimeError, match="schema migration missing"):
        _make_backend(tmp_path)


def test_sqlite_backend_reuses_connection_per_instance(tmp_path: Path, monkeypatch):
    """A backend instance should amortize SQLite connect/PRAGMA setup work."""
    connect_calls = []
    original_connect = sqlite_indexed_mod.sqlite3.connect

    def counting_connect(*args, **kwargs):
        connect_calls.append((args, kwargs))
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite_indexed_mod.sqlite3, "connect", counting_connect)
    backend = _make_backend(tmp_path)

    run_id = backend.create_run(_make_context())
    _record_ticker(backend, run_id, "TSLA")
    record = _write(backend, run_id)
    backend.get_run(run_id)
    backend.get_ticker_results(run_id)
    backend.list_datasets()
    backend.get_dataset(record.dataset_id)

    assert len(connect_calls) == 1
    assert connect_calls[0][1]["check_same_thread"] is False


def test_sqlite_backend_close_reopens_connection(tmp_path: Path, monkeypatch):
    """Closing the pooled connection should make the next operation reconnect."""
    connect_calls = []
    original_connect = sqlite_indexed_mod.sqlite3.connect

    def counting_connect(*args, **kwargs):
        connect_calls.append((args, kwargs))
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite_indexed_mod.sqlite3, "connect", counting_connect)
    backend = _make_backend(tmp_path)
    backend.close()
    backend.create_run(_make_context())

    assert len(connect_calls) == 2


# ---------------------------------------------------------------------------
# Run lifecycle
# ---------------------------------------------------------------------------

def test_create_run_initial_status_is_running(tmp_path: Path):
    """Newly created run must have status=running."""
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
        ("not-json", ()),
        ('"TSLA"', ()),
        ('["TSLA", true, 7, ""]', ()),
        ('["tsla", "NVDA"]', ("TSLA", "NVDA")),
    ],
)
def test_get_run_sanitizes_retained_ticker_json(
    tmp_path: Path,
    stored_tickers: str,
    expected: tuple[str, ...],
):
    """Retained run ticker metadata must not leak parser errors or malformed members."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE runs SET tickers = ? WHERE run_id = ?",
            (stored_tickers, run_id),
        )
        conn.commit()
    finally:
        conn.close()

    run = backend.get_run(run_id)

    assert run.tickers == expected


def test_finalize_run_sets_status_complete(tmp_path: Path):
    """finalize_run must update status to complete."""
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
    """record_ticker_result must persist the result in SQLite."""
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
    """record_validation must insert validation summaries into SQLite."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())

    backend.record_validation(ValidationRecord(
        run_id=run_id,
        severity="warning",
        code="MISSING_GREEKS",
        count=3,
        sample='{"field": "delta"}',
    ))

    conn = sqlite3.connect(str(tmp_path / "opx-chain.db"))
    try:
        row = conn.execute(
            "SELECT run_id, severity, code, count, sample FROM validations"
        ).fetchone()
    finally:
        conn.close()

    assert row == (run_id, "warning", "MISSING_GREEKS", 3, '{"field": "delta"}')


# ---------------------------------------------------------------------------
# Dataset write and read
# ---------------------------------------------------------------------------

def test_write_dataset_creates_artifact(tmp_path: Path):
    """write_dataset must create the artifact file on disk."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    assert Path(record.location).exists()


def test_write_dataset_removes_artifact_when_index_write_fails(
    tmp_path: Path,
    monkeypatch,
):
    """A failed visibility transaction keeps the validated row non-discoverable."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())

    original_prune = backend._prune_datasets  # pylint: disable=protected-access

    def fail_prune(_conn):
        raise sqlite3.OperationalError("index write failed")

    monkeypatch.setattr(backend, "_prune_datasets", fail_prune)

    record = backend.write_dataset(
        run_id,
        DatasetWrite(
            data=_make_dataframe(),
            provider="yfinance",
            schema_version=SCHEMA_VERSION,
        ),
    )
    with pytest.raises(sqlite3.OperationalError, match="index write failed"):
        backend.finalize_run(run_id, RunSummary(status="complete"))

    assert Path(record.location).exists()
    assert backend.get_run(run_id).status == "running"
    assert not backend.list_datasets()

    monkeypatch.setattr(backend, "_prune_datasets", original_prune)
    backend.finalize_run(run_id, RunSummary(status="complete"))
    assert backend.get_dataset(record.dataset_id).dataset_id == record.dataset_id


def test_write_dataset_returns_correct_record(tmp_path: Path):
    """DatasetRecord returned by write_dataset must have correct field values."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    df = _make_dataframe()
    record = backend.write_dataset(
        run_id,
        DatasetWrite(
            data=df,
            provider="yfinance",
            schema_version=SCHEMA_VERSION,
        ),
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
    assert handle.script_version == record.script_version


def test_get_dataset_raises_for_unknown_id(tmp_path: Path):
    """get_dataset must raise KeyError for an unrecognised dataset_id."""
    backend = _make_backend(tmp_path)
    with pytest.raises(KeyError):
        backend.get_dataset("no-such-id")


def test_get_dataset_reports_corrupt_created_at_stably(tmp_path: Path):
    """Corrupt dataset timestamps must not leak raw parser errors."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE datasets SET created_at = ? WHERE dataset_id = ?",
            ("zzzz-not-a-timestamp", record.dataset_id),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(ValueError, match="dataset metadata corrupt"):
        backend.get_dataset(record.dataset_id)


def test_list_datasets_most_recent_first(tmp_path: Path):
    """list_datasets must return records newest first."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    r1 = _write(backend, run_id, rows=1)
    r2 = _write(backend, run_id, rows=2)

    records = backend.list_datasets()

    assert records[0].dataset_id == r2.dataset_id
    assert records[1].dataset_id == r1.dataset_id


def test_list_datasets_skips_corrupt_created_at(tmp_path: Path):
    """Corrupt retained dataset timestamps should be skipped during discovery."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    good_record = _write(backend, run_id)
    bad_record = _write(backend, run_id)
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE datasets SET created_at = ? WHERE dataset_id = ?",
            ("zzzz-not-a-timestamp", bad_record.dataset_id),
        )
        conn.commit()
    finally:
        conn.close()

    records = backend.list_datasets(limit=10)

    assert [record.dataset_id for record in records] == [good_record.dataset_id]


def test_list_datasets_normalizes_legacy_naive_created_at(tmp_path: Path):
    """Legacy naive dataset timestamps should not leak or break date filtering."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE datasets SET created_at = ?, created_at_sort_key = NULL "
            "WHERE dataset_id = ?",
            ("2026-05-27T12:00:00", record.dataset_id),
        )
        conn.commit()
    finally:
        conn.close()

    records = backend.list_datasets(
        since=datetime(2026, 5, 27, 11, 59, tzinfo=timezone.utc),
        until=datetime(2026, 5, 27, 12, 1, tzinfo=timezone.utc),
    )

    assert [item.dataset_id for item in records] == [record.dataset_id]
    assert records[0].created_at == datetime(2026, 5, 27, 12, 0, tzinfo=timezone.utc)


def test_list_datasets_orders_same_second_legacy_timestamps(tmp_path: Path):
    """Parsed UTC timestamps, not raw ISO text, should determine recency."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    older = _write(backend, run_id)
    newer = _write(backend, run_id)
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE datasets SET created_at = ?, created_at_sort_key = NULL "
            "WHERE dataset_id = ?",
            ("2026-05-27T12:00:00Z", older.dataset_id),
        )
        conn.execute(
            "UPDATE datasets SET created_at = ?, created_at_sort_key = NULL "
            "WHERE dataset_id = ?",
            ("2026-05-27T12:00:00.500000+00:00", newer.dataset_id),
        )
        conn.commit()
    finally:
        conn.close()

    records = backend.list_datasets(limit=2)

    assert [record.dataset_id for record in records] == [
        newer.dataset_id,
        older.dataset_id,
    ]


def test_list_datasets_limit(tmp_path: Path):
    """list_datasets must honour the limit parameter."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    for _ in range(5):
        _write(backend, run_id)

    assert len(backend.list_datasets(limit=2)) == 2


def test_list_datasets_filter_provider(tmp_path: Path):
    """list_datasets must filter by provider when supplied."""
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


@pytest.mark.parametrize(
    "stored_tickers",
    [
        '"TSLA"',
        "null",
        '["TSLA", true]',
        '{"TSLA": true}',
    ],
)
def test_list_datasets_filter_ticker_sanitizes_retained_run_tickers(
    tmp_path: Path,
    stored_tickers: str,
):
    """Ticker-filter listing must not match corrupt retained run ticker metadata."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context(tickers=("TSLA",)))
    _write(backend, run_id)
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE runs SET tickers = ? WHERE run_id = ?",
            (stored_tickers, run_id),
        )
        conn.commit()
    finally:
        conn.close()

    assert not backend.list_datasets(ticker="TSLA")


def test_list_datasets_filter_ticker_uses_ticker_result_when_run_tickers_corrupt(
    tmp_path: Path,
):
    """Valid per-ticker results should remain the fallback for corrupt run tickers."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context(tickers=("TSLA",)))
    _record_ticker(backend, run_id, "TSLA")
    record = _write(backend, run_id)
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE runs SET tickers = ? WHERE run_id = ?",
            ('["TSLA", true]', run_id),
        )
        conn.commit()
    finally:
        conn.close()

    results = backend.list_datasets(ticker="TSLA")

    assert [result.dataset_id for result in results] == [record.dataset_id]


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
    """Malformed string ticker filters must not behave like SQL wildcards."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context(tickers=("TSLA",)))
    _write(backend, run_id)

    assert not backend.list_datasets(ticker=ticker)


def test_write_dataset_links_run(tmp_path: Path):
    """write_dataset must update the run's dataset_id field."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    record = _write(backend, run_id)

    run = backend.get_run(run_id)
    assert run.dataset_id == record.dataset_id


def test_write_dataset_uses_payload_format_over_backend_default(tmp_path: Path):
    """DatasetWrite.format must control serialization even when backend default differs."""
    backend = _make_backend(tmp_path, dataset_format="parquet")
    run_id = backend.create_run(_make_context())
    record = backend.write_dataset(
        run_id,
        DatasetWrite(
            data=_make_dataframe(),
            provider="yfinance",
            schema_version=SCHEMA_VERSION,
            format="csv",
        ),
    )

    assert record.format == "csv"
    assert Path(record.location).suffix == ".csv"
    assert pd.read_csv(record.location).shape[0] == record.row_count


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


def test_write_artifact_removes_debug_file_when_index_write_fails(tmp_path: Path):
    """A failed debug artifact DB insert must not leave an unindexed payload file."""
    backend = _make_backend(tmp_path)
    payload = ArtifactWrite(
        artifact_type="debug_payload", content=b"payload", filename="data.json"
    )

    with pytest.raises(KeyError, match="run not found"):
        backend.write_artifact("missing-run", payload)

    debug_dir = tmp_path / "debug"
    assert not debug_dir.exists() or not list(debug_dir.rglob("*"))


def test_write_artifact_removes_sidecar_when_index_write_fails(tmp_path: Path):
    """A failed sidecar DB insert must not leave an unindexed run artifact file."""
    backend = _make_backend(tmp_path)
    payload = ArtifactWrite(
        artifact_type="sidecar", content=b"positions", filename="positions.csv"
    )

    with pytest.raises(KeyError, match="run not found"):
        backend.write_artifact("missing-run", payload)

    assert not (tmp_path / "runs" / "missing-run" / "positions.csv").exists()


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


def test_write_sidecar_artifact_duplicate_does_not_overwrite_file(tmp_path: Path):
    """Duplicate stable sidecar IDs must fail before replacing retained bytes."""
    backend = _make_backend(tmp_path)
    run_id = backend.create_run(_make_context())
    first = backend.write_artifact(
        run_id,
        ArtifactWrite("sidecar", b"original", "positions.csv"),
    )

    with pytest.raises(ValueError, match="artifact already exists"):
        backend.write_artifact(
            run_id,
            ArtifactWrite("sidecar", b"replacement", "positions.csv"),
        )

    assert Path(first.location).read_bytes() == b"original"


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

    assert backend.get_run(run_id).run_id == run_id
    assert not Path(sidecar.location).exists()
    assert not Path(debug.location).exists()
    assert not Path(debug.location).parent.exists()
    assert not output_file.exists()
    assert not output_dir.exists()
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM artifacts WHERE run_id = ?",
            (run_id,),
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 0


def test_delete_run_artifacts_defers_payload_deletes_until_commit(
    tmp_path: Path,
    monkeypatch,
):
    """A rollback during cleanup must not leave DB rows pointing to missing files."""
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

    def fail_payload_staging(_run_id: str):
        raise RuntimeError("rollback before commit")

    monkeypatch.setattr(backend, "_stage_run_payload_deletes", fail_payload_staging)

    with pytest.raises(RuntimeError, match="rollback before commit"):
        backend.delete_run_artifacts(run_id)

    assert Path(sidecar.location).exists()
    assert Path(debug.location).exists()
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM artifacts WHERE run_id = ?",
            (run_id,),
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 2


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


def test_pruning_uses_created_at_sort_key_index(tmp_path: Path):
    """Retention pruning should remain bounded by the normalized timestamp index."""
    _make_backend(tmp_path, max_runs_retained=7)
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT dataset_id, run_id, location, created_at, created_at_sort_key "
            "FROM datasets "
            "ORDER BY created_at_sort_key DESC, dataset_id DESC "
            "LIMIT -1 OFFSET ?",
            (7,),
        ).fetchall()
    finally:
        conn.close()

    details = " ".join(str(row[-1]) for row in plan)
    assert "idx_datasets_created_at_sort_key" in details


def test_list_datasets_limit_uses_created_at_sort_key_index(tmp_path: Path):
    """Small dataset listings should use the normalized timestamp index and limit."""
    _make_backend(tmp_path)
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT d.*, r.tickers AS run_tickers "
            "FROM datasets d LEFT JOIN runs r ON r.run_id = d.run_id "
            "ORDER BY d.created_at_sort_key DESC, d.dataset_id DESC "
            "LIMIT ?",
            (1,),
        ).fetchall()
    finally:
        conn.close()

    details = " ".join(str(row[-1]) for row in plan)
    assert "idx_datasets_created_at_sort_key" in details


def test_pruning_clears_dataset_id_for_pruned_run(tmp_path: Path):
    """A pruned dataset must not remain advertised by its run record."""
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
    """Pruning must delete the artifact file on disk."""
    backend = _make_backend(tmp_path, max_runs_retained=1)
    run_id = backend.create_run(_make_context())
    r1 = _write(backend, run_id)
    _write(backend, run_id)

    assert not Path(r1.location).exists()


def test_pruning_defers_file_deletes_until_commit(tmp_path: Path, monkeypatch):
    """A rollback during pruning must preserve files still referenced by SQLite."""
    backend = _make_backend(tmp_path, max_runs_retained=1)
    old_run_id = backend.create_run(_make_context())
    old_record = _write(backend, old_run_id)
    new_run_id = backend.create_run(_make_context(provider="marketdata"))
    original_prune = backend._prune_datasets  # pylint: disable=protected-access

    def fail_after_staging(conn):
        pending = original_prune(conn)
        assert pending
        assert Path(old_record.location).exists()
        raise RuntimeError("rollback before commit")

    monkeypatch.setattr(backend, "_prune_datasets", fail_after_staging)

    with pytest.raises(RuntimeError, match="rollback before commit"):
        _write(backend, new_run_id, provider="marketdata")

    assert Path(old_record.location).exists()
    assert backend.get_dataset(old_record.dataset_id).location == old_record.location


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


def test_pruning_removes_run_log_artifact_for_pruned_run(tmp_path: Path):
    """Pruning must remove debug-dir run-log artifact rows and files."""
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

    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        remaining = conn.execute(
            "SELECT COUNT(*) FROM artifacts WHERE artifact_id = ?",
            (record.artifact_id,),
        ).fetchone()[0]
    finally:
        conn.close()

    assert remaining == 0
    assert not artifact_path.exists()
    assert not artifact_dir.exists()


def test_pruning_uses_parsed_created_at_order(tmp_path: Path):
    """Retention should keep the newest parsed timestamp across legacy ISO forms."""
    backend = _make_backend(tmp_path, max_runs_retained=0)
    run_id = backend.create_run(_make_context())
    older = _write(backend, run_id, rows=1)
    newer = _write(backend, run_id, rows=2)
    backend._max_runs_retained = 1  # pylint: disable=protected-access
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            "UPDATE datasets SET created_at = ?, created_at_sort_key = NULL "
            "WHERE dataset_id = ?",
            ("2026-05-27T12:00:00Z", older.dataset_id),
        )
        conn.execute(
            "UPDATE datasets SET created_at = ?, created_at_sort_key = NULL "
            "WHERE dataset_id = ?",
            ("2026-05-27T12:00:00.500000+00:00", newer.dataset_id),
        )
        pending = backend._prune_datasets(conn)  # pylint: disable=protected-access
        conn.commit()
    finally:
        conn.close()
    backend._delete_deferred_paths(pending)  # pylint: disable=protected-access

    records = backend.list_datasets(limit=10)

    assert [record.dataset_id for record in records] == [newer.dataset_id]


def test_no_pruning_when_max_runs_retained_zero(tmp_path: Path):
    """When max_runs_retained = 0 (default), no datasets are ever pruned."""
    backend = _make_backend(tmp_path, max_runs_retained=0)
    run_id = backend.create_run(_make_context())
    for _ in range(5):
        _write(backend, run_id)

    assert len(backend.list_datasets()) == 5


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def test_factory_returns_sqlite_backend_when_configured(tmp_path: Path):
    """get_storage_backend must return SqliteIndexedBackend when backend=sqlite."""
    config = make_runtime_config(
        storage_enabled=True,
        storage_backend="sqlite",
        storage_dir=tmp_path,
        debug_dump_dir=tmp_path / "debug",
    )
    backend = get_storage_backend(config)
    assert isinstance(backend, SqliteIndexedBackend)


def test_factory_reuses_sqlite_backend_for_same_config(tmp_path: Path):
    """Repeated factory calls with the same sqlite config must reuse the backend."""
    config = make_runtime_config(
        storage_enabled=True,
        storage_backend="sqlite",
        storage_dir=tmp_path,
        debug_dump_dir=tmp_path / "debug",
    )

    assert get_storage_backend(config) is get_storage_backend(config)


def test_sqlite_connections_are_closed_after_operations(tmp_path: Path):
    """Backend methods must not leak sqlite connections that warn at GC time."""
    result = TickerFetchResult(
        ticker="TSLA",
        raw_row_count=4,
        normalized_row_count=4,
        kept_row_count=4,
        filtered_row_count=0,
        expiration_count=1,
        status="ok",
    )

    # Flush objects left by earlier tests before this test starts attributing
    # ResourceWarnings to the backend operations below.
    gc.collect()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        backend = _make_backend(tmp_path)
        run_id = backend.create_run(_make_context())
        backend.record_ticker_result(run_id, result)
        _write(backend, run_id)
        backend.get_run(run_id)
        backend.get_ticker_results(run_id)
        backend.list_datasets()
        backend.get_dataset(backend.list_datasets()[0].dataset_id)
        del backend
        gc.collect()

    resource_warnings = [
        warning for warning in caught
        if issubclass(warning.category, ResourceWarning)
    ]
    assert not resource_warnings


# ---------------------------------------------------------------------------
# get_run error path
# ---------------------------------------------------------------------------

def test_get_run_raises_for_unknown_id(tmp_path: Path):
    """get_run must raise KeyError when the run_id is not in the database."""
    backend = _make_backend(tmp_path)
    with pytest.raises(KeyError):
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


def test_count_runs_today_skips_malformed_started_at(tmp_path: Path):
    """Malformed retained run timestamps must not count as today's completed run."""
    backend = _make_backend(tmp_path)
    good_run = backend.create_run(_make_context(provider="marketdata"))
    bad_run = backend.create_run(_make_context(provider="marketdata"))
    backend.finalize_run(good_run, RunSummary(status="complete"))
    backend.finalize_run(bad_run, RunSummary(status="complete"))
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE runs SET started_at = ? WHERE run_id = ?",
            ("zzzz-not-a-timestamp", bad_run),
        )
        conn.commit()
    finally:
        conn.close()

    assert backend.count_runs_today("marketdata") == 1


def test_count_runs_today_handles_legacy_naive_started_at(tmp_path: Path):
    """Legacy naive run timestamps should be normalized before day counting."""
    backend = _make_backend(tmp_path)
    market_run = backend.create_run(_make_context(provider="marketdata"))
    backend.finalize_run(market_run, RunSummary(status="complete"))
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE runs SET started_at = ? WHERE run_id = ?",
            (datetime.now(tz=timezone.utc).replace(tzinfo=None).isoformat(), market_run),
        )
        conn.commit()
    finally:
        conn.close()

    assert backend.count_runs_today("marketdata") == 1


def test_count_runs_today_uses_composite_index(tmp_path: Path):
    """count_runs_today must use the provider/status/started_at index."""
    _make_backend(tmp_path)
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN "
            f"{sqlite_indexed_mod._COUNT_RUNS_TODAY_SQL}",  # pylint: disable=protected-access
            ("marketdata", "2026-01-01"),
        ).fetchall()
    finally:
        conn.close()

    details = " ".join(str(row[-1]) for row in plan)
    assert "idx_runs_provider_status_started" in details


def test_schema_migration_adds_count_runs_today_index(tmp_path: Path):
    """v3 databases must be upgraded with the count_runs_today index."""
    _make_backend(tmp_path)
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute("DROP INDEX IF EXISTS idx_runs_provider_status_started")
        conn.execute("UPDATE _schema_meta SET value = '3' WHERE key = 'schema_version'")
        conn.commit()
    finally:
        conn.close()

    _make_backend(tmp_path)

    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        version = conn.execute(
            "SELECT value FROM _schema_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        index_row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
            ("idx_runs_provider_status_started",),
        ).fetchone()
    finally:
        conn.close()

    assert version == str(sqlite_indexed_mod._SCHEMA_VERSION)  # pylint: disable=protected-access
    assert index_row is not None


def test_interrupt_stale_runs_marks_old_running_rows(tmp_path: Path):
    """Stale running SQLite rows should converge to interrupted."""
    backend = _make_backend(tmp_path)
    stale_run = backend.create_run(_make_context(provider="marketdata"))
    fresh_run = backend.create_run(_make_context(provider="marketdata"))
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE runs SET started_at = ? WHERE run_id = ?",
            (
                (datetime.now(tz=timezone.utc) - timedelta(minutes=5)).isoformat(),
                stale_run,
            ),
        )
        conn.commit()
    finally:
        conn.close()

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


def test_interrupt_stale_runs_compares_parsed_legacy_z_timestamp(tmp_path: Path):
    """Legacy Z timestamps should not evade stale recovery through text ordering."""
    backend = _make_backend(tmp_path)
    stale_run = backend.create_run(_make_context(provider="marketdata"))
    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE runs SET started_at = ? WHERE run_id = ?",
            ("2026-05-27T12:00:00Z", stale_run),
        )
        conn.commit()
    finally:
        conn.close()

    count = backend.interrupt_stale_runs(
        datetime(2026, 5, 27, 12, 0, 0, 500000, tzinfo=timezone.utc),
        "process_terminated_uncleanly",
    )

    assert count == 1
    assert backend.get_run(stale_run).status == "interrupted"
