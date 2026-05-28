"""SQLite-indexed StorageBackend implementation."""
# pylint: disable=duplicate-code

from __future__ import annotations

import json
import re
import shutil
import sqlite3
import threading
import weakref
from contextlib import contextmanager
from dataclasses import dataclass
import uuid
from datetime import datetime, timezone
from pathlib import Path

from opx_chain.timestamps import datetime_to_iso, iso_to_datetime, utc_now
from opx_chain.storage.models import (
    ArtifactRecord,
    ArtifactWrite,
    DatasetHandle,
    DatasetRecord,
    DatasetWrite,
    RunContext,
    RunRecord,
    RunSummary,
    TickerFetchResult,
    TickerRunRecord,
    UNKNOWN_SCRIPT_VERSION,
    ValidationRecord,
    record_to_handle,
)
from opx_chain.storage.atomic import atomic_write_bytes
from opx_chain.storage._disk import (
    content_hash_for_bytes,
    retained_path_under_roots,
    resolve_child_path,
    write_artifact_bytes,
    write_dataset_artifact,
)
from opx_chain.storage.serializers import get_serializer
from opx_chain.storage.validation import (
    INVALID_TICKER_FILTER,
    sanitize_retained_run_tickers,
    validate_artifact_write,
    validate_dataset_list_filters,
    validate_dataset_id,
    validate_dataset_write,
    validate_required_text,
    validate_run_context,
    validate_run_id,
    validate_run_summary,
    validate_stale_run_inputs,
    validate_ticker_fetch_result,
    validate_validation_record,
)


def _unlink_orphaned_file(path: Path, *, remove_empty_parent: bool = False) -> None:
    """Best-effort cleanup for files written before their SQLite row commits."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        return
    if remove_empty_parent:
        try:
            path.parent.rmdir()
        except OSError:
            pass


@dataclass(frozen=True)
class _DeferredDelete:
    """Filesystem path to delete only after the SQLite transaction commits."""

    path: Path
    recursive: bool = False
    remove_empty_parent: bool = False


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS _schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    run_id                TEXT PRIMARY KEY,
    started_at            TEXT NOT NULL,
    finished_at           TEXT,
    status                TEXT NOT NULL,
    provider              TEXT NOT NULL,
    script_version        TEXT NOT NULL DEFAULT 'unknown',
    tickers               TEXT NOT NULL DEFAULT '[]',
    config_fingerprint    TEXT NOT NULL,
    positions_fingerprint TEXT NOT NULL,
    dataset_id            TEXT,
    error_summary         TEXT
);

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id      TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES runs(run_id),
    created_at      TEXT NOT NULL,
    created_at_sort_key INTEGER,
    provider        TEXT NOT NULL,
    script_version  TEXT NOT NULL DEFAULT 'unknown',
    schema_version  INTEGER NOT NULL,
    row_count       INTEGER NOT NULL,
    format          TEXT NOT NULL,
    location        TEXT NOT NULL,
    content_hash    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ticker_results (
    run_id               TEXT NOT NULL REFERENCES runs(run_id),
    ticker               TEXT NOT NULL,
    raw_row_count        INTEGER NOT NULL,
    normalized_row_count INTEGER NOT NULL,
    kept_row_count       INTEGER NOT NULL,
    filtered_row_count   INTEGER NOT NULL,
    expiration_count     INTEGER NOT NULL,
    status               TEXT NOT NULL,
    error_summary        TEXT,
    PRIMARY KEY (run_id, ticker)
);

CREATE TABLE IF NOT EXISTS validations (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id   TEXT NOT NULL REFERENCES runs(run_id),
    severity TEXT NOT NULL,
    code     TEXT NOT NULL,
    count    INTEGER NOT NULL,
    sample   TEXT
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id   TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL REFERENCES runs(run_id),
    artifact_type TEXT NOT NULL,
    location      TEXT NOT NULL,
    content_hash  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_datasets_created_at ON datasets(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_datasets_run_id     ON datasets(run_id);
CREATE INDEX IF NOT EXISTS idx_runs_status         ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_provider_status_started
    ON runs(provider, status, started_at);
"""

_SCHEMA_VERSION = 5
_SCHEMA_MIGRATIONS: dict[int, str] = {
    2: "ALTER TABLE runs ADD COLUMN tickers TEXT NOT NULL DEFAULT '[]';",
    3: """
       ALTER TABLE runs ADD COLUMN script_version TEXT NOT NULL DEFAULT 'unknown';
       ALTER TABLE datasets ADD COLUMN script_version TEXT NOT NULL DEFAULT 'unknown';
       """,
    4: """
       CREATE INDEX IF NOT EXISTS idx_runs_provider_status_started
           ON runs(provider, status, started_at);
       """,
    5: """
       ALTER TABLE datasets ADD COLUMN created_at_sort_key INTEGER;
       CREATE INDEX IF NOT EXISTS idx_datasets_created_at_sort_key
           ON datasets(created_at_sort_key DESC, dataset_id DESC);
       """,
}

_COUNT_RUNS_TODAY_SQL = (
    "SELECT started_at FROM runs "
    "WHERE provider = ? AND status = 'complete' AND started_at >= ?"
)


def _utc_sort_key(value: datetime | None) -> datetime:
    """Return a timezone-aware UTC datetime suitable for stable comparisons."""
    if value is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _datetime_to_sort_key(value: datetime) -> int:
    """Return a UTC microsecond sort key suitable for SQLite ordering."""
    normalized = _utc_sort_key(value)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return int((normalized - epoch).total_seconds() * 1_000_000)


def _created_at_text_to_sort_key(value: str) -> int | None:
    """Return a retained timestamp sort key, preserving malformed rows as null."""
    try:
        parsed = iso_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed is None:
        return None
    return _datetime_to_sort_key(parsed)


_ADD_COLUMN_RE = re.compile(
    r"^ALTER\s+TABLE\s+(?P<table>[A-Za-z_][A-Za-z0-9_]*)\s+"
    r"ADD\s+COLUMN\s+(?P<column>[A-Za-z_][A-Za-z0-9_]*)\b",
    re.IGNORECASE,
)
_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SqliteIndexedBackend:
    """StorageBackend that stores run/dataset metadata in SQLite and artifacts on disk."""

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        db_path: Path,
        runs_dir: Path,
        debug_dir: Path,
        max_runs_retained: int = 0,
        dataset_format: str = "csv",
    ) -> None:
        """Initialise with the SQLite db path, runs directory, and retention limit."""
        self._db_path = Path(db_path)
        self._runs_dir = Path(runs_dir)
        self._debug_dir = Path(debug_dir)
        self._max_runs_retained = max_runs_retained
        self._connection: sqlite3.Connection | None = None
        self._connection_finalizer: weakref.finalize | None = None
        self._connection_lock = threading.RLock()
        get_serializer(dataset_format)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    def _connection_for_use(self) -> sqlite3.Connection:
        """Return the pooled connection, creating it on first use."""
        if self._connection is None:
            self._connection = self._connect()
            self._connection_finalizer = weakref.finalize(self, self._connection.close)
        return self._connection

    @contextmanager
    def _open_connection(self):
        """Yield the pooled SQLite connection under the backend lock."""
        with self._connection_lock:
            conn = self._connection_for_use()
            try:
                yield conn
            except Exception:
                conn.rollback()
                raise

    def close(self) -> None:
        """Close the pooled SQLite connection, if one has been opened."""
        with self._connection_lock:
            if self._connection is None:
                return
            self._connection.close()
            if self._connection_finalizer is not None:
                self._connection_finalizer.detach()
                self._connection_finalizer = None
            self._connection = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:  # pragma: no cover  # pylint: disable=broad-exception-caught
            pass

    def _init_schema(self) -> None:
        with self._open_connection() as conn:
            conn.executescript(_SCHEMA_SQL)
            current_version = self._read_schema_version(conn)
            if current_version is None:
                conn.execute(
                    "INSERT INTO _schema_meta VALUES ('schema_version', ?)",
                    (str(_SCHEMA_VERSION),),
                )
            elif current_version > _SCHEMA_VERSION:
                raise RuntimeError(
                    "SQLite storage schema version "
                    f"{current_version} is newer than supported version {_SCHEMA_VERSION}"
                )
            elif current_version < _SCHEMA_VERSION:
                self._migrate_schema(conn, current_version, _SCHEMA_VERSION)
            self._ensure_dataset_sort_key_index(conn)
            self._backfill_dataset_sort_keys(conn)
            conn.commit()

    def _read_schema_version(self, conn: sqlite3.Connection) -> int | None:
        row = conn.execute(
            "SELECT value FROM _schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        if row is None:
            return None
        try:
            return int(row["value"])
        except ValueError as exc:
            raise RuntimeError(
                "SQLite storage schema version is not an integer: "
                f"{row['value']!r}"
            ) from exc

    def _table_columns(self, conn: sqlite3.Connection, table_name: str) -> set[str]:
        if not _SQL_IDENTIFIER_RE.fullmatch(table_name):
            raise ValueError(f"Unsafe SQLite table identifier: {table_name!r}")
        quoted_table_name = f'"{table_name}"'
        return {
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({quoted_table_name})").fetchall()
        }

    def _migration_statements(self, migration: str) -> list[str]:
        return [statement.strip() for statement in migration.split(";") if statement.strip()]

    def _execute_migration_statement(
        self,
        conn: sqlite3.Connection,
        statement: str,
    ) -> None:
        match = _ADD_COLUMN_RE.match(statement)
        if match and match.group("column") in self._table_columns(conn, match.group("table")):
            return
        conn.execute(statement)

    def _migrate_schema(
        self,
        conn: sqlite3.Connection,
        current_version: int,
        target_version: int,
    ) -> None:
        for next_version in range(current_version + 1, target_version + 1):
            migration = _SCHEMA_MIGRATIONS.get(next_version)
            if migration is None:
                raise RuntimeError(
                    "SQLite storage schema migration missing: "
                    f"{current_version}->{target_version}"
                )
            for statement in self._migration_statements(migration):
                self._execute_migration_statement(conn, statement)
            conn.execute(
                "UPDATE _schema_meta SET value = ? WHERE key = 'schema_version'",
                (str(next_version),),
            )

    def _backfill_dataset_sort_keys(self, conn: sqlite3.Connection) -> bool:
        if "created_at_sort_key" not in self._table_columns(conn, "datasets"):
            return False
        rows = conn.execute(
            "SELECT dataset_id, created_at FROM datasets WHERE created_at_sort_key IS NULL"
        ).fetchall()
        updates = [
            (sort_key, row["dataset_id"])
            for row in rows
            if (sort_key := _created_at_text_to_sort_key(row["created_at"])) is not None
        ]
        if updates:
            conn.executemany(
                "UPDATE datasets SET created_at_sort_key = ? WHERE dataset_id = ?",
                updates,
            )
            return True
        return False

    def _ensure_dataset_sort_key_index(self, conn: sqlite3.Connection) -> None:
        if "created_at_sort_key" not in self._table_columns(conn, "datasets"):
            return
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_datasets_created_at_sort_key "
            "ON datasets(created_at_sort_key DESC, dataset_id DESC)"
        )

    def _backfill_dataset_sort_keys_for_listing(self, conn: sqlite3.Connection) -> None:
        if self._backfill_dataset_sort_keys(conn):
            conn.commit()

    def _row_tickers(self, value: str | None) -> tuple[str, ...]:
        try:
            decoded = json.loads(value or "[]")
        except json.JSONDecodeError:
            return ()
        return sanitize_retained_run_tickers(decoded)

    def _sidecar_path(self, run_id: str, filename: str) -> Path:
        return resolve_child_path(self._runs_dir, run_id, filename)

    def _stage_sidecar_file_deletes(self, run_id: str) -> list[_DeferredDelete]:
        run_dir = resolve_child_path(self._runs_dir, run_id)
        try:
            entries = list(run_dir.iterdir())
        except OSError:
            return []
        return [
            _DeferredDelete(entry)
            for entry in entries
            if entry.is_file() and entry.name != "run.json"
        ]

    def _stage_run_payload_deletes(self, run_id: str) -> list[_DeferredDelete]:
        run_dir = resolve_child_path(self._runs_dir, run_id)
        try:
            entries = list(run_dir.iterdir())
        except OSError:
            return []
        pending: list[_DeferredDelete] = []
        for entry in entries:
            if entry.name == "run.json":
                continue
            if entry.is_dir():
                pending.append(_DeferredDelete(entry, recursive=True))
            elif entry.is_file():
                pending.append(_DeferredDelete(entry))
        return pending

    def _stage_artifact_file_delete(self, location: str) -> _DeferredDelete | None:
        path = retained_path_under_roots(location, (self._runs_dir, self._debug_dir))
        if path is None:
            return None
        try:
            remove_empty_parent = (
                path.parent.parent.resolve() == self._debug_dir.resolve()
            )
        except OSError:
            remove_empty_parent = False
        return _DeferredDelete(path, remove_empty_parent=remove_empty_parent)

    def _delete_deferred_paths(self, pending: list[_DeferredDelete]) -> None:
        for item in pending:
            if item.recursive:
                shutil.rmtree(item.path, ignore_errors=True)
            else:
                _unlink_orphaned_file(
                    item.path,
                    remove_empty_parent=item.remove_empty_parent,
                )

    def _stage_run_artifact_deletes(
        self,
        conn: sqlite3.Connection,
        run_id: str,
    ) -> list[_DeferredDelete]:
        rows = conn.execute(
            "SELECT artifact_id, location FROM artifacts "
            "WHERE run_id = ?",
            (run_id,),
        ).fetchall()
        pending: list[_DeferredDelete] = []
        for row in rows:
            staged = self._stage_artifact_file_delete(row["location"])
            if staged is not None:
                pending.append(staged)
            conn.execute("DELETE FROM artifacts WHERE artifact_id = ?", (row["artifact_id"],))
        pending.extend(self._stage_sidecar_file_deletes(run_id))
        return pending

    def _require_run_id(self, conn: sqlite3.Connection, run_id: str) -> str:
        """Return a validated run id when the SQLite run row exists."""
        run_id = validate_run_id(run_id)
        row = conn.execute("SELECT 1 FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            raise KeyError(f"run not found: {run_id}")
        return run_id

    def delete_run_artifacts(self, run_id: str) -> None:
        """Delete storage-managed artifacts for a run while preserving run metadata."""
        run_id = validate_run_id(run_id)
        pending: list[_DeferredDelete]
        with self._open_connection() as conn:
            pending = self._stage_run_artifact_deletes(conn, run_id)
            pending.extend(self._stage_run_payload_deletes(run_id))
            conn.commit()
        self._delete_deferred_paths(pending)

    def _prune_datasets(self, conn: sqlite3.Connection) -> list[_DeferredDelete]:
        if self._max_runs_retained <= 0:
            return []
        self._backfill_dataset_sort_keys(conn)
        rows = conn.execute(
            "SELECT dataset_id, run_id, location, created_at, created_at_sort_key "
            "FROM datasets "
            "ORDER BY created_at_sort_key DESC, dataset_id DESC "
            "LIMIT -1 OFFSET ?",
            (self._max_runs_retained,),
        ).fetchall()
        pending: list[_DeferredDelete] = []
        for row in rows:
            dataset_path = retained_path_under_roots(row["location"], (self._runs_dir,))
            if dataset_path is not None:
                pending.append(_DeferredDelete(dataset_path))
            pending.extend(self._stage_run_artifact_deletes(conn, row["run_id"]))
            conn.execute(
                "UPDATE runs SET dataset_id = NULL WHERE run_id = ? AND dataset_id = ?",
                (row["run_id"], row["dataset_id"]),
            )
            conn.execute("DELETE FROM datasets WHERE dataset_id = ?", (row["dataset_id"],))
            remaining = conn.execute(
                "SELECT COUNT(*) FROM datasets WHERE run_id = ?",
                (row["run_id"],),
            ).fetchone()[0]
            if remaining == 0:
                pending.extend(self._stage_run_payload_deletes(row["run_id"]))
        return pending

    # ------------------------------------------------------------------
    # StorageBackend protocol
    # ------------------------------------------------------------------

    def create_run(self, context: RunContext) -> str:
        """Insert a new run row and return its run_id."""
        context = validate_run_context(context)
        run_id = str(uuid.uuid4())
        with self._open_connection() as conn:
            conn.execute(
                """INSERT INTO runs
                   (run_id, started_at, finished_at, status, provider, script_version, tickers,
                    config_fingerprint, positions_fingerprint, dataset_id, error_summary)
                   VALUES (?, ?, NULL, 'running', ?, ?, ?, ?, ?, NULL, NULL)""",
                (
                    run_id,
                    datetime_to_iso(utc_now()),
                    context.provider,
                    context.script_version,
                    json.dumps(list(context.tickers)),
                    context.config_fingerprint,
                    context.positions_fingerprint,
                ),
            )
            conn.commit()
        return run_id

    def record_ticker_result(self, run_id: str, result: TickerFetchResult) -> None:
        """Insert or replace a per-ticker result row."""
        result = validate_ticker_fetch_result(result)
        with self._open_connection() as conn:
            run_id = self._require_run_id(conn, run_id)
            conn.execute(
                """INSERT OR REPLACE INTO ticker_results
                   (run_id, ticker, raw_row_count, normalized_row_count,
                    kept_row_count, filtered_row_count, expiration_count,
                    status, error_summary)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    result.ticker,
                    result.raw_row_count,
                    result.normalized_row_count,
                    result.kept_row_count,
                    result.filtered_row_count,
                    result.expiration_count,
                    result.status,
                    result.error_summary,
                ),
            )
            conn.commit()

    def record_validation(self, record: ValidationRecord) -> None:
        """Insert a validation summary record for a run."""
        record = validate_validation_record(record)
        with self._open_connection() as conn:
            self._require_run_id(conn, record.run_id)
            conn.execute(
                """INSERT INTO validations
                   (run_id, severity, code, count, sample)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    record.run_id,
                    record.severity,
                    record.code,
                    record.count,
                    record.sample,
                ),
            )
            conn.commit()

    def write_dataset(self, run_id: str, dataset: DatasetWrite) -> DatasetRecord:
        """Serialize the DataFrame, store metadata in SQLite, and return a DatasetRecord."""
        dataset = validate_dataset_write(dataset)
        with self._open_connection() as conn:
            run_id = self._require_run_id(conn, run_id)
        output_dir = resolve_child_path(self._runs_dir, run_id) / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        serializer = get_serializer(dataset.format)
        dataset_id, artifact_path, content_hash = write_dataset_artifact(
            dataset.data, output_dir, dataset.format, serializer
        )
        now = utc_now()
        record = DatasetRecord(
            dataset_id=dataset_id,
            run_id=run_id,
            created_at=now,
            provider=dataset.provider,
            script_version=dataset.script_version,
            schema_version=dataset.schema_version,
            row_count=len(dataset.data),
            format=dataset.format,
            location=str(artifact_path),
            content_hash=content_hash,
        )
        try:
            with self._open_connection() as conn:
                conn.execute(
                    """INSERT INTO datasets
                       (dataset_id, run_id, created_at, created_at_sort_key, provider,
                        script_version, schema_version, row_count, format, location, content_hash)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        dataset_id,
                        run_id,
                        datetime_to_iso(now),
                        _datetime_to_sort_key(now),
                        dataset.provider,
                        dataset.script_version,
                        dataset.schema_version,
                        len(dataset.data),
                        dataset.format,
                        str(artifact_path),
                        content_hash,
                    ),
                )
                conn.execute(
                    "UPDATE runs SET dataset_id = ? WHERE run_id = ?",
                    (dataset_id, run_id),
                )
                pending_deletes = self._prune_datasets(conn)
                conn.commit()
        except Exception:
            _unlink_orphaned_file(artifact_path)
            raise
        self._delete_deferred_paths(pending_deletes)
        return record

    def write_artifact(self, run_id: str, artifact: ArtifactWrite) -> ArtifactRecord:
        """Write artifact bytes to disk and record metadata in SQLite."""
        artifact = validate_artifact_write(artifact)
        with self._open_connection() as conn:
            run_id = self._require_run_id(conn, run_id)
        if artifact.artifact_type == "sidecar":
            dest = self._sidecar_path(run_id, artifact.filename)
            existed_before_write = dest.exists()
            artifact_id = f"{run_id}:{artifact.filename}"
            with self._open_connection() as conn:
                existing = conn.execute(
                    "SELECT 1 FROM artifacts WHERE artifact_id = ?",
                    (artifact_id,),
                ).fetchone()
            if existing is not None:
                raise ValueError(f"artifact already exists: {artifact_id}")
            atomic_write_bytes(dest, artifact.content)
            content_hash = content_hash_for_bytes(artifact.content)
            remove_empty_parent = False
        else:
            artifact_id, dest, content_hash = write_artifact_bytes(
                artifact.content, self._debug_dir, artifact.filename
            )
            existed_before_write = False
            remove_empty_parent = True
        try:
            with self._open_connection() as conn:
                conn.execute(
                    """INSERT INTO artifacts
                       (artifact_id, run_id, artifact_type, location, content_hash)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        artifact_id,
                        run_id,
                        artifact.artifact_type,
                        str(dest.resolve()),
                        content_hash,
                    ),
                )
                conn.commit()
        except Exception:
            if not existed_before_write:
                _unlink_orphaned_file(dest, remove_empty_parent=remove_empty_parent)
            raise
        return ArtifactRecord(
            artifact_id=artifact_id,
            run_id=run_id,
            artifact_type=artifact.artifact_type,
            location=str(dest.resolve()),
            content_hash=content_hash,
        )

    def list_datasets(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
        self,
        limit: int = 50,
        provider: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        ticker: str | None = None,
    ) -> list[DatasetRecord]:
        """Return dataset records from SQLite, newest first."""
        filters = validate_dataset_list_filters(
            limit=limit,
            provider=provider,
            since=since,
            until=until,
            ticker=ticker,
        )
        if filters.limit == 0 or filters.ticker == INVALID_TICKER_FILTER:
            return []

        sql = (
            "SELECT d.*, r.tickers AS run_tickers "
            "FROM datasets d LEFT JOIN runs r ON r.run_id = d.run_id"
        )
        params: list = []
        conditions: list[str] = []
        if filters.provider is not None:
            conditions.append("d.provider = ?")
            params.append(filters.provider)
        if filters.since is not None:
            conditions.append("d.created_at_sort_key >= ?")
            params.append(_datetime_to_sort_key(filters.since))
        if filters.until is not None:
            conditions.append("d.created_at_sort_key <= ?")
            params.append(_datetime_to_sort_key(filters.until))
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY d.created_at_sort_key DESC, d.dataset_id DESC"
        if filters.ticker is None:
            sql += " LIMIT ?"
            params.append(filters.limit)
        with self._open_connection() as conn:
            self._backfill_dataset_sort_keys_for_listing(conn)
            rows = conn.execute(sql, params).fetchall()
            records: list[DatasetRecord] = []
            for row in rows:
                if filters.ticker is not None and not self._row_has_ticker(
                    conn,
                    row,
                    filters.ticker,
                ):
                    continue
                try:
                    record = self._row_to_record(row)
                except ValueError:
                    continue
                if filters.since is not None and record.created_at < filters.since:
                    continue
                if filters.until is not None and record.created_at > filters.until:
                    continue
                records.append(record)
                if len(records) >= filters.limit:
                    break
        return records

    def get_dataset(self, dataset_id: str) -> DatasetHandle:
        """Return a DatasetHandle for the given dataset_id."""
        dataset_id = validate_dataset_id(dataset_id)
        with self._open_connection() as conn:
            row = conn.execute(
                "SELECT * FROM datasets WHERE dataset_id = ?", (dataset_id,)
            ).fetchone()
        if row is None:
            raise KeyError(f"dataset not found: {dataset_id}")
        try:
            record = self._row_to_record(row)
        except ValueError as exc:
            raise ValueError(f"dataset metadata corrupt: {dataset_id}") from exc
        return record_to_handle(record)

    def finalize_run(self, run_id: str, summary: RunSummary) -> None:
        """Update the run row with a completion status."""
        summary = validate_run_summary(summary)
        with self._open_connection() as conn:
            run_id = self._require_run_id(conn, run_id)
            conn.execute(
                "UPDATE runs SET status = ?, finished_at = ?, error_summary = ? "
                "WHERE run_id = ? AND status = 'running'",
                (summary.status, datetime_to_iso(utc_now()), summary.error_summary, run_id),
            )
            conn.commit()

    def fail_run(self, run_id: str, error: str) -> None:
        """Update the run row with a failed status and error message."""
        error = validate_required_text(error, name="error")
        with self._open_connection() as conn:
            run_id = self._require_run_id(conn, run_id)
            conn.execute(
                "UPDATE runs SET status = 'failed', finished_at = ?, error_summary = ? "
                "WHERE run_id = ? AND status = 'running'",
                (datetime_to_iso(utc_now()), error, run_id),
            )
            conn.commit()

    def interrupt_stale_runs(self, cutoff: datetime, error_summary: str) -> int:
        """Mark running runs older than cutoff as interrupted."""
        cutoff, error_summary = validate_stale_run_inputs(cutoff, error_summary)
        with self._open_connection() as conn:
            rows = conn.execute(
                "SELECT run_id, started_at FROM runs WHERE status = 'running'"
            ).fetchall()
            stale_run_ids = []
            for row in rows:
                try:
                    started_at = iso_to_datetime(row["started_at"])
                except (TypeError, ValueError):
                    continue
                if started_at is not None and started_at < cutoff:
                    stale_run_ids.append(row["run_id"])
            if stale_run_ids:
                conn.executemany(
                    "UPDATE runs "
                    "SET status = 'interrupted', finished_at = ?, error_summary = ? "
                    "WHERE run_id = ? AND status = 'running'",
                    [
                        (datetime_to_iso(utc_now()), error_summary, run_id)
                        for run_id in stale_run_ids
                    ],
                )
            conn.commit()
        return len(stale_run_ids)

    def get_run(self, run_id: str) -> RunRecord:
        """Return a RunRecord for the given run_id."""
        run_id = validate_run_id(run_id)
        with self._open_connection() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        if row is None:
            raise KeyError(f"run not found: {run_id}")
        return RunRecord(
            run_id=row["run_id"],
            started_at=iso_to_datetime(row["started_at"]),
            finished_at=iso_to_datetime(row["finished_at"]),
            status=row["status"],
            provider=row["provider"],
            script_version=row["script_version"] or UNKNOWN_SCRIPT_VERSION,
            tickers=self._row_tickers(row["tickers"]),
            config_fingerprint=row["config_fingerprint"],
            positions_fingerprint=row["positions_fingerprint"],
            dataset_id=row["dataset_id"],
            error_summary=row["error_summary"],
        )

    def count_runs_today(self, provider: str) -> int:
        """Return the number of complete runs started today (US/Eastern) for the provider."""
        from opx_chain.config import US_MARKET_TIMEZONE  # pylint: disable=import-outside-toplevel
        provider = validate_required_text(provider, name="provider")
        now_et = datetime.now(tz=US_MARKET_TIMEZONE)
        midnight_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        since_utc = midnight_et.astimezone(timezone.utc)
        since_utc_floor_text = since_utc.date().isoformat()
        with self._open_connection() as conn:
            rows = conn.execute(
                _COUNT_RUNS_TODAY_SQL,
                (provider, since_utc_floor_text),
            ).fetchall()
        count = 0
        for row in rows:
            try:
                started_at = iso_to_datetime(row["started_at"])
            except (TypeError, ValueError):
                continue
            if started_at is not None and started_at >= since_utc:
                count += 1
        return count

    def get_ticker_results(self, run_id: str) -> list[TickerRunRecord]:
        """Return per-ticker results for a run."""
        run_id = validate_run_id(run_id)
        with self._open_connection() as conn:
            run_exists = conn.execute(
                "SELECT 1 FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run_exists is None:
                raise KeyError(f"run not found: {run_id}")
            rows = conn.execute(
                "SELECT * FROM ticker_results WHERE run_id = ?", (run_id,)
            ).fetchall()
        return [
            TickerRunRecord(
                run_id=row["run_id"],
                ticker=row["ticker"],
                raw_row_count=row["raw_row_count"],
                normalized_row_count=row["normalized_row_count"],
                kept_row_count=row["kept_row_count"],
                filtered_row_count=row["filtered_row_count"],
                expiration_count=row["expiration_count"],
                status=row["status"],
                error_summary=row["error_summary"],
            )
            for row in rows
        ]

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> DatasetRecord:
        return DatasetRecord(
            dataset_id=row["dataset_id"],
            run_id=row["run_id"],
            created_at=iso_to_datetime(row["created_at"]),
            provider=row["provider"],
            script_version=row["script_version"] or UNKNOWN_SCRIPT_VERSION,
            schema_version=row["schema_version"],
            row_count=row["row_count"],
            format=row["format"],
            location=row["location"],
            content_hash=row["content_hash"],
        )

    @staticmethod
    def _row_has_ticker(
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        expected: str,
    ) -> bool:
        try:
            run_tickers = json.loads(row["run_tickers"] or "[]")
        except (TypeError, ValueError):
            run_tickers = []
        if expected in sanitize_retained_run_tickers(run_tickers):
            return True
        ticker_row = conn.execute(
            "SELECT 1 FROM ticker_results WHERE run_id = ? AND UPPER(ticker) = ?",
            (row["run_id"], expected),
        ).fetchone()
        return ticker_row is not None
