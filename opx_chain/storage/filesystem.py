"""Filesystem-based StorageBackend implementation."""

from __future__ import annotations

import shutil
import threading
import uuid
from datetime import datetime, timezone
from heapq import nsmallest
from pathlib import Path

from opx_chain.json_utils import dumps_strict_json, loads_strict_json
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
from opx_chain.storage.atomic import atomic_write_bytes, atomic_write_text
from opx_chain.storage._disk import (
    content_hash_for_bytes,
    retained_path_under_roots,
    resolve_child_path,
    write_artifact_bytes,
    write_dataset_artifact,
)
from opx_chain.storage.serializers import get_serializer

_DATASET_ARTIFACT_SUFFIXES = {".csv", ".parquet"}


def _dt_sort_key(value: datetime | None) -> datetime:
    """Return a timezone-aware UTC datetime suitable for stable ordering."""
    if value is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class FilesystemBackend:
    """StorageBackend that writes metadata as JSON sidecars and artifacts as files.

    Run metadata lands in runs_dir/{run_id}/run.json.
    Dataset artifacts land in runs_dir/{run_id}/output/ as {dataset_id}.csv (or .parquet).
    Dataset metadata lands alongside as {dataset_id}.meta.json.
    Debug artifacts land in debug_dir as {artifact_id}/{filename}; sidecars may
    live directly under runs_dir/{run_id}/.
    """

    def __init__(
        self,
        runs_dir: Path,
        debug_dir: Path,
        max_runs_retained: int = 0,
        dataset_format: str = "csv",
    ) -> None:
        """Initialise with the runs directory, debug directory, and optional retention limit."""
        self._runs_dir = runs_dir
        self._debug_dir = debug_dir
        self._max_runs_retained = max_runs_retained
        self._run_sidecar_lock = threading.RLock()
        self._daily_count_cache: dict[tuple[str, str], int] = {}
        get_serializer(dataset_format)
        self._sweep_orphan_dataset_artifacts()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_output_dir(self, run_id: str) -> Path:
        return resolve_child_path(self._runs_dir, run_id) / "output"

    def _run_path(self, run_id: str) -> Path:
        return resolve_child_path(self._runs_dir, run_id) / "run.json"

    def _dataset_index_path(self) -> Path:
        return self._runs_dir / "datasets.index.json"

    def _meta_path(self, dataset_id: str, run_id: str) -> Path:
        return self._run_output_dir(run_id) / f"{dataset_id}.meta.json"

    def _sidecar_path(self, run_id: str, filename: str) -> Path:
        return resolve_child_path(self._runs_dir, run_id, filename)

    def _delete_sidecar_files(self, run_id: str) -> None:
        run_dir = resolve_child_path(self._runs_dir, run_id)
        try:
            entries = list(run_dir.iterdir())
        except OSError:
            return
        for entry in entries:
            if entry.is_file() and entry.name != "run.json":
                entry.unlink(missing_ok=True)

    def _delete_run_payloads(self, run_id: str) -> None:
        run_dir = resolve_child_path(self._runs_dir, run_id)
        try:
            entries = list(run_dir.iterdir())
        except OSError:
            return
        for entry in entries:
            if entry.name == "run.json":
                continue
            if entry.is_dir():
                shutil.rmtree(entry, ignore_errors=True)
            elif entry.is_file():
                entry.unlink(missing_ok=True)

    def _delete_artifact_path(self, location: str) -> None:
        path = retained_path_under_roots(location, (self._runs_dir, self._debug_dir))
        if path is None:
            return
        path.unlink(missing_ok=True)
        try:
            if path.parent.parent.resolve() == self._debug_dir.resolve():
                path.parent.rmdir()
        except OSError:
            pass

    def _record_artifact(self, record: ArtifactRecord) -> None:
        with self._run_sidecar_lock:
            data = self._read_run(record.run_id)
            artifacts = [
                item
                for item in data.get("artifacts", [])
                if item.get("artifact_id") != record.artifact_id
            ]
            artifacts.append(
                {
                    "artifact_id": record.artifact_id,
                    "artifact_type": record.artifact_type,
                    "location": record.location,
                    "content_hash": record.content_hash,
                }
            )
            data["artifacts"] = artifacts
            self._write_run(record.run_id, data)

    def _delete_run_artifacts(self, run_id: str) -> None:
        try:
            data = self._read_run(run_id)
        except (OSError, ValueError):
            data = {}
        for artifact in data.get("artifacts", []):
            location = artifact.get("location")
            if location:
                self._delete_artifact_path(location)
        # Preserve cleanup for sidecars written before artifact metadata existed.
        self._delete_sidecar_files(run_id)

    def delete_run_artifacts(self, run_id: str) -> None:
        """Delete storage-managed artifacts for a run while preserving run metadata."""
        self._delete_run_artifacts(run_id)
        self._delete_run_payloads(run_id)

    def _read_run(self, run_id: str) -> dict:
        path = self._run_path(run_id)
        return loads_strict_json(path.read_text(encoding="utf-8"))

    def _run_has_ticker(self, run_id: str, ticker: str) -> bool:
        try:
            data = self._read_run(run_id)
        except (OSError, ValueError):
            return False
        expected = ticker.upper()
        run_tickers = {str(symbol).upper() for symbol in data.get("tickers", [])}
        if expected in run_tickers:
            return True
        return any(
            str(row.get("ticker", "")).upper() == expected
            for row in data.get("ticker_results", [])
        )

    def _write_run(self, run_id: str, data: dict) -> None:
        path = self._run_path(run_id)
        atomic_write_text(path, dumps_strict_json(data, indent=2))
        self._daily_count_cache.clear()

    def _find_meta_path(self, dataset_id: str) -> Path:
        """Scan all run dirs to locate a dataset meta file by dataset_id."""
        matches = list(self._runs_dir.glob(f"*/output/{dataset_id}.meta.json"))
        if not matches:
            raise KeyError(f"dataset not found: {dataset_id}")
        return matches[0]

    def _find_dataset_record(self, dataset_id: str) -> DatasetRecord:
        """Return a dataset record by id, preferring the dataset index."""
        for record in self._dataset_records():
            if record.dataset_id == dataset_id:
                return record
        meta_path = self._find_meta_path(dataset_id)
        data = loads_strict_json(meta_path.read_text(encoding="utf-8"))
        return self._meta_to_record(data)

    def _write_meta(self, record: DatasetRecord) -> None:
        path = self._meta_path(record.dataset_id, record.run_id)
        atomic_write_text(path, dumps_strict_json(self._record_to_meta(record), indent=2))

    @staticmethod
    def _record_to_meta(record: DatasetRecord) -> dict:
        return {
            "dataset_id": record.dataset_id,
            "run_id": record.run_id,
            "created_at": datetime_to_iso(record.created_at),
            "provider": record.provider,
            "schema_version": record.schema_version,
            "row_count": record.row_count,
            "format": record.format,
            "location": record.location,
            "content_hash": record.content_hash,
            "script_version": record.script_version,
        }

    @staticmethod
    def _meta_to_record(data: dict) -> DatasetRecord:
        return DatasetRecord(
            dataset_id=data["dataset_id"],
            run_id=data["run_id"],
            created_at=iso_to_datetime(data["created_at"]),
            provider=data["provider"],
            schema_version=data["schema_version"],
            row_count=data["row_count"],
            format=data["format"],
            location=data["location"],
            content_hash=data["content_hash"],
            script_version=data.get("script_version", UNKNOWN_SCRIPT_VERSION),
        )

    def _scan_dataset_records(self) -> list[DatasetRecord]:
        records = []
        for meta_path in self._runs_dir.glob("*/output/*.meta.json"):
            try:
                records.append(
                    self._meta_to_record(loads_strict_json(meta_path.read_text(encoding="utf-8")))
                )
            except (OSError, TypeError, KeyError, ValueError):
                continue
        records.sort(key=lambda record: _dt_sort_key(record.created_at), reverse=True)
        return records

    def _write_dataset_index(self, records: list[DatasetRecord]) -> None:
        self._runs_dir.mkdir(parents=True, exist_ok=True)
        data = [self._record_to_meta(record) for record in records]
        atomic_write_text(
            self._dataset_index_path(),
            dumps_strict_json(data, separators=(",", ":")),
        )

    def _load_dataset_index(self) -> list[DatasetRecord] | None:
        index_path = self._dataset_index_path()
        if not index_path.exists():
            return None
        try:
            data = loads_strict_json(index_path.read_text(encoding="utf-8"))
            records = [self._meta_to_record(item) for item in data]
        except (OSError, TypeError, KeyError, ValueError):
            return None
        records = [
            record
            for record in records
            if self._meta_path(record.dataset_id, record.run_id).exists()
        ]
        records.sort(key=lambda record: _dt_sort_key(record.created_at), reverse=True)
        return records

    def _dataset_records(self) -> list[DatasetRecord]:
        records = self._load_dataset_index()
        if records is not None:
            return records
        records = self._scan_dataset_records()
        try:
            self._write_dataset_index(records)
        except OSError:
            pass
        return records

    def _append_dataset_index(self, record: DatasetRecord) -> None:
        records = [
            item
            for item in self._dataset_records()
            if item.dataset_id != record.dataset_id
        ]
        records.append(record)
        records.sort(key=lambda item: _dt_sort_key(item.created_at), reverse=True)
        try:
            self._write_dataset_index(records)
        except OSError:
            pass

    def _clear_run_dataset_reference(self, run_id: str, dataset_id: str) -> None:
        with self._run_sidecar_lock:
            try:
                data = self._read_run(run_id)
            except (OSError, KeyError, ValueError):
                return
            if data.get("dataset_id") != dataset_id:
                return
            data["dataset_id"] = None
            try:
                self._write_run(run_id, data)
            except OSError:
                pass

    def _sweep_orphan_dataset_artifacts(self) -> int:
        """Remove dataset artifacts that have no matching metadata sidecar."""
        if not self._runs_dir.exists():
            return 0
        removed = 0
        for artifact_path in self._runs_dir.glob("*/output/*"):
            if (
                not artifact_path.is_file()
                or artifact_path.name.endswith(".meta.json")
                or artifact_path.suffix not in _DATASET_ARTIFACT_SUFFIXES
                or artifact_path.with_suffix(".meta.json").exists()
            ):
                continue
            try:
                artifact_path.unlink(missing_ok=True)
                removed += 1
            except OSError:
                pass
        return removed

    def _rollback_dataset_write(
        self,
        record: DatasetRecord | None,
        artifact_path: Path | None,
    ) -> None:
        """Best-effort cleanup for partial dataset publish failures."""
        if artifact_path is not None:
            try:
                artifact_path.unlink(missing_ok=True)
            except OSError:
                pass
        if record is None:
            return
        try:
            self._meta_path(record.dataset_id, record.run_id).unlink(missing_ok=True)
        except OSError:
            pass
        self._clear_run_dataset_reference(record.run_id, record.dataset_id)
        try:
            self._write_dataset_index(self._scan_dataset_records())
        except OSError:
            pass

    def _meta_created_at_sort_key(self, meta_path: Path) -> datetime:
        try:
            data = loads_strict_json(meta_path.read_text(encoding="utf-8"))
            return _dt_sort_key(iso_to_datetime(data.get("created_at")))
        except (OSError, TypeError, ValueError):
            return datetime.min.replace(tzinfo=timezone.utc)

    def _run_has_dataset_metadata(self, run_id: str) -> bool:
        return any(self._run_output_dir(run_id).glob("*.meta.json"))

    def _prune_datasets(self) -> None:
        self._sweep_orphan_dataset_artifacts()
        if self._max_runs_retained <= 0:
            return
        meta_files = list(self._runs_dir.glob("*/output/*.meta.json"))
        excess = len(meta_files) - self._max_runs_retained
        if excess <= 0:
            return
        for meta_path in nsmallest(excess, meta_files, key=self._meta_created_at_sort_key):
            try:
                data = loads_strict_json(meta_path.read_text(encoding="utf-8"))
                artifact = meta_path.parent / Path(data["location"]).name
                if artifact.exists():
                    artifact.unlink()
                self._delete_run_artifacts(meta_path.parent.parent.name)
                self._clear_run_dataset_reference(data["run_id"], data["dataset_id"])
            except (OSError, KeyError, ValueError):
                pass
            meta_path.unlink(missing_ok=True)
            if not self._run_has_dataset_metadata(meta_path.parent.parent.name):
                self._delete_run_payloads(meta_path.parent.parent.name)
        try:
            self._write_dataset_index(self._scan_dataset_records())
        except OSError:
            pass

    # ------------------------------------------------------------------
    # StorageBackend protocol
    # ------------------------------------------------------------------

    def create_run(self, context: RunContext) -> str:
        """Create a run sidecar JSON and return its run_id."""
        run_id = str(uuid.uuid4())
        data = {
            "run_id": run_id,
            "started_at": datetime_to_iso(utc_now()),
            "finished_at": None,
            "status": "running",
            "provider": context.provider,
            "script_version": context.script_version,
            "tickers": list(context.tickers),
            "config_fingerprint": context.config_fingerprint,
            "positions_fingerprint": context.positions_fingerprint,
            "dataset_id": None,
            "error_summary": None,
            "ticker_results": [],
            "validations": [],
        }
        self._write_run(run_id, data)
        return run_id

    def record_ticker_result(self, run_id: str, result: TickerFetchResult) -> None:
        """Append a per-ticker result to the run sidecar."""
        with self._run_sidecar_lock:
            data = self._read_run(run_id)
            data["ticker_results"].append({
                "ticker": result.ticker,
                "raw_row_count": result.raw_row_count,
                "normalized_row_count": result.normalized_row_count,
                "kept_row_count": result.kept_row_count,
                "filtered_row_count": result.filtered_row_count,
                "expiration_count": result.expiration_count,
                "status": result.status,
                "error_summary": result.error_summary,
            })
            self._write_run(run_id, data)

    def record_validation(self, record: ValidationRecord) -> None:
        """Append a validation summary record to the run sidecar."""
        with self._run_sidecar_lock:
            data = self._read_run(record.run_id)
            data.setdefault("validations", []).append({
                "severity": record.severity,
                "code": record.code,
                "count": record.count,
                "sample": record.sample,
            })
            self._write_run(record.run_id, data)

    def write_dataset(self, run_id: str, dataset: DatasetWrite) -> DatasetRecord:
        """Serialize the DataFrame, compute its hash, and write metadata."""
        output_dir = self._run_output_dir(run_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        serializer = get_serializer(dataset.format)
        record = None
        artifact_path = None
        try:
            dataset_id, artifact_path, content_hash = write_dataset_artifact(
                dataset.data, output_dir, dataset.format, serializer
            )
            record = DatasetRecord(
                dataset_id=dataset_id,
                run_id=run_id,
                created_at=utc_now(),
                provider=dataset.provider,
                schema_version=dataset.schema_version,
                row_count=len(dataset.data),
                format=dataset.format,
                location=str(artifact_path),
                content_hash=content_hash,
                script_version=dataset.script_version,
            )
            self._write_meta(record)
            with self._run_sidecar_lock:
                data = self._read_run(run_id)
                data["dataset_id"] = dataset_id
                self._write_run(run_id, data)
            self._append_dataset_index(record)
            self._prune_datasets()
            return record
        except Exception:  # pylint: disable=broad-exception-caught
            self._rollback_dataset_write(record, artifact_path)
            raise

    def write_artifact(self, run_id: str, artifact: ArtifactWrite) -> ArtifactRecord:
        """Write artifact bytes to disk and return an ArtifactRecord."""
        if artifact.artifact_type == "sidecar":
            dest = self._sidecar_path(run_id, artifact.filename)
            atomic_write_bytes(dest, artifact.content)
            artifact_id = f"{run_id}:{artifact.filename}"
            content_hash = content_hash_for_bytes(artifact.content)
        else:
            artifact_id, dest, content_hash = write_artifact_bytes(
                artifact.content, self._debug_dir, artifact.filename
            )
        record = ArtifactRecord(
            artifact_id=artifact_id,
            run_id=run_id,
            artifact_type=artifact.artifact_type,
            location=str(dest.resolve()),
            content_hash=content_hash,
        )
        self._record_artifact(record)
        return record

    def list_datasets(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        limit: int = 50,
        provider: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        ticker: str | None = None,
    ) -> list[DatasetRecord]:
        """Return dataset records from meta files, newest first."""
        if not self._runs_dir.exists():
            return []
        results = []
        for record in self._dataset_records():
            if provider is not None and record.provider != provider:
                continue
            if since is not None and record.created_at < since:
                break
            if since is None and len(results) >= limit:
                break
            if until is not None and record.created_at > until:
                continue
            if ticker is not None and not self._run_has_ticker(record.run_id, ticker):
                continue
            results.append(record)
            if len(results) >= limit:
                break
        return results[:limit]

    def get_dataset(self, dataset_id: str) -> DatasetHandle:
        """Return a DatasetHandle by loading the dataset's meta file."""
        return record_to_handle(self._find_dataset_record(dataset_id))

    def finalize_run(self, run_id: str, summary: RunSummary) -> None:
        """Update the run sidecar with a completion status."""
        with self._run_sidecar_lock:
            data = self._read_run(run_id)
            if data.get("status") != "running":
                return
            data["status"] = summary.status
            data["finished_at"] = datetime_to_iso(utc_now())
            data["error_summary"] = summary.error_summary
            self._write_run(run_id, data)

    def fail_run(self, run_id: str, error: str) -> None:
        """Update the run sidecar with a failed status and error message."""
        with self._run_sidecar_lock:
            data = self._read_run(run_id)
            if data.get("status") != "running":
                return
            data["status"] = "failed"
            data["finished_at"] = datetime_to_iso(utc_now())
            data["error_summary"] = error
            self._write_run(run_id, data)

    def interrupt_stale_runs(self, cutoff: datetime, error_summary: str) -> int:
        """Mark running runs older than cutoff as interrupted."""
        interrupted = 0
        if not self._runs_dir.exists():
            return interrupted
        for run_path in self._runs_dir.glob("*/run.json"):
            try:
                with self._run_sidecar_lock:
                    data = loads_strict_json(run_path.read_text(encoding="utf-8"))
                    started_at = iso_to_datetime(data.get("started_at"))
                    if data.get("status") != "running" or started_at is None:
                        continue
                    if _dt_sort_key(started_at) >= _dt_sort_key(cutoff):
                        continue
                    data["status"] = "interrupted"
                    data["finished_at"] = datetime_to_iso(utc_now())
                    data["error_summary"] = error_summary
                    self._write_run(data["run_id"], data)
                    interrupted += 1
            except (OSError, KeyError, ValueError):
                continue
        return interrupted

    def get_run(self, run_id: str) -> RunRecord:
        """Return a RunRecord by loading the run sidecar."""
        try:
            data = self._read_run(run_id)
        except FileNotFoundError as exc:
            raise KeyError(f"run not found: {run_id}") from exc
        return RunRecord(
            run_id=data["run_id"],
            started_at=iso_to_datetime(data["started_at"]),
            finished_at=iso_to_datetime(data.get("finished_at")),
            status=data["status"],
            provider=data["provider"],
            script_version=data.get("script_version", UNKNOWN_SCRIPT_VERSION),
            tickers=tuple(data.get("tickers", ())),
            config_fingerprint=data["config_fingerprint"],
            positions_fingerprint=data["positions_fingerprint"],
            dataset_id=data.get("dataset_id"),
            error_summary=data.get("error_summary"),
        )

    def count_runs_today(self, provider: str) -> int:
        """Return the number of complete runs started today (US/Eastern) for the provider."""
        from opx_chain.config import US_MARKET_TIMEZONE  # pylint: disable=import-outside-toplevel
        now_et = datetime.now(tz=US_MARKET_TIMEZONE)
        cache_key = (provider, now_et.date().isoformat())
        cached = self._daily_count_cache.get(cache_key)
        if cached is not None:
            return cached
        midnight_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        since_utc = midnight_et.astimezone(timezone.utc)
        count = 0
        if not self._runs_dir.exists():
            return count
        for run_path in self._runs_dir.glob("*/run.json"):
            try:
                data = loads_strict_json(run_path.read_text(encoding="utf-8"))
                if data.get("provider") != provider:
                    continue
                started_at_str = data.get("started_at", "")
                if not started_at_str:
                    continue
                started_at = iso_to_datetime(started_at_str)
                if data.get("status") == "complete" and started_at >= since_utc:
                    count += 1
            except (OSError, ValueError):
                continue
        self._daily_count_cache[cache_key] = count
        return count

    def get_ticker_results(self, run_id: str) -> list[TickerRunRecord]:
        """Return per-ticker results stored in the run sidecar."""
        try:
            data = self._read_run(run_id)
        except FileNotFoundError as exc:
            raise KeyError(f"run not found: {run_id}") from exc
        return [
            TickerRunRecord(
                run_id=run_id,
                ticker=r["ticker"],
                raw_row_count=r["raw_row_count"],
                normalized_row_count=r["normalized_row_count"],
                kept_row_count=r["kept_row_count"],
                filtered_row_count=r["filtered_row_count"],
                expiration_count=r["expiration_count"],
                status=r["status"],
                error_summary=r.get("error_summary"),
            )
            for r in data.get("ticker_results", [])
        ]
