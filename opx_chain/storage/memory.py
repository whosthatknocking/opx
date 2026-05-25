"""In-memory storage backend for testing."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from opx_chain.storage._disk import content_hash_for_bytes
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
    ValidationRecord,
    record_to_handle,
)
from opx_chain.storage.serializers import get_serializer
from opx_chain.storage.validation import (
    INVALID_TICKER_FILTER,
    validate_dataset_list_filters,
    validate_required_text,
)


class MemoryBackend:
    """StorageBackend backed entirely by in-memory dicts.

    Writes no files. Used in tests that exercise the storage-enabled
    branches of fetcher.py and opx-check.
    """

    def __init__(self, max_runs_retained: int = 0) -> None:
        """Initialise empty in-memory stores."""
        self._max_runs_retained = max_runs_retained
        self._runs: dict[str, RunRecord] = {}
        self._datasets: list[DatasetRecord] = []
        self._ticker_results: dict[str, list[TickerRunRecord]] = {}
        self._validations: dict[str, list[ValidationRecord]] = {}
        self._artifacts: dict[str, list[ArtifactRecord]] = {}
        self._dataset_bytes: dict[str, bytes] = {}

    def _prune_datasets(self) -> None:
        """Drop oldest dataset records and bytes beyond the retention limit."""
        if self._max_runs_retained <= 0:
            return
        excess = len(self._datasets) - self._max_runs_retained
        if excess <= 0:
            return
        oldest = sorted(
            enumerate(self._datasets),
            key=lambda item: (item[1].created_at, item[0]),
        )[:excess]
        pruned_ids = {record.dataset_id for _, record in oldest}
        for _, record in oldest:
            self._dataset_bytes.pop(record.dataset_id, None)
            run = self._runs.get(record.run_id)
            if run is not None and run.dataset_id == record.dataset_id:
                run.dataset_id = None
            self.delete_run_artifacts(record.run_id)
        self._datasets = [
            record for record in self._datasets
            if record.dataset_id not in pruned_ids
        ]

    def create_run(self, context: RunContext) -> str:
        """Open a new run record and return its run_id."""
        run_id = str(uuid.uuid4())
        self._runs[run_id] = RunRecord(
            run_id=run_id,
            started_at=datetime.now(tz=timezone.utc),
            finished_at=None,
            status="running",
            provider=context.provider,
            script_version=context.script_version,
            tickers=context.tickers,
            config_fingerprint=context.config_fingerprint,
            positions_fingerprint=context.positions_fingerprint,
            dataset_id=None,
            error_summary=None,
        )
        return run_id

    def record_ticker_result(self, run_id: str, result: TickerFetchResult) -> None:
        """Append a per-ticker fetch result to the run."""
        record = TickerRunRecord(
            run_id=run_id,
            ticker=result.ticker,
            raw_row_count=result.raw_row_count,
            normalized_row_count=result.normalized_row_count,
            kept_row_count=result.kept_row_count,
            filtered_row_count=result.filtered_row_count,
            expiration_count=result.expiration_count,
            status=result.status,
            error_summary=result.error_summary,
        )
        self._ticker_results.setdefault(run_id, []).append(record)

    def record_validation(self, record: ValidationRecord) -> None:
        """Append a validation summary record under its run_id."""
        self._validations.setdefault(record.run_id, []).append(record)

    def write_dataset(self, run_id: str, dataset: DatasetWrite) -> DatasetRecord:
        """Serialize the DataFrame in memory and record the dataset."""
        dataset_id = str(uuid.uuid4())
        serializer = get_serializer(dataset.format)
        content = serializer.serialize_bytes(dataset.data)
        content_hash = content_hash_for_bytes(content)
        record = DatasetRecord(
            dataset_id=dataset_id,
            run_id=run_id,
            created_at=datetime.now(tz=timezone.utc),
            provider=dataset.provider,
            schema_version=dataset.schema_version,
            row_count=len(dataset.data),
            format=dataset.format,
            location=f"memory://datasets/{dataset_id}.{dataset.format}",
            content_hash=content_hash,
            script_version=dataset.script_version,
        )
        self._datasets.append(record)
        self._dataset_bytes[dataset_id] = content
        if run_id in self._runs:
            self._runs[run_id].dataset_id = dataset_id
        self._prune_datasets()
        return record

    def write_artifact(self, run_id: str, artifact: ArtifactWrite) -> ArtifactRecord:
        """Store artifact bytes in memory and return an ArtifactRecord."""
        artifact_id = str(uuid.uuid4())
        content_hash = content_hash_for_bytes(artifact.content)
        record = ArtifactRecord(
            artifact_id=artifact_id,
            run_id=run_id,
            artifact_type=artifact.artifact_type,
            location=f"memory://artifacts/{artifact_id}/{artifact.filename}",
            content_hash=content_hash,
        )
        self._artifacts.setdefault(run_id, []).append(record)
        return record

    def delete_run_artifacts(self, run_id: str) -> None:
        """Delete artifacts associated with a run."""
        self._artifacts.pop(run_id, None)

    def list_datasets(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        limit: int = 50,
        provider: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        ticker: str | None = None,
    ) -> list[DatasetRecord]:
        """Return datasets in reverse chronological order, newest first."""
        filters = validate_dataset_list_filters(
            limit=limit, provider=provider, since=since, until=until, ticker=ticker
        )
        if filters.ticker == INVALID_TICKER_FILTER:
            return []
        results = list(reversed(self._datasets))
        if filters.provider is not None:
            results = [r for r in results if r.provider == filters.provider]
        if filters.since is not None:
            results = [r for r in results if r.created_at >= filters.since]
        if filters.until is not None:
            results = [r for r in results if r.created_at <= filters.until]
        if filters.ticker is not None:
            expected = filters.ticker
            results = [
                record for record in results
                if (
                    (run := self._runs.get(record.run_id)) is not None
                    and expected in {symbol.upper() for symbol in run.tickers}
                )
                or any(
                    row.ticker.upper() == expected
                    for row in self._ticker_results.get(record.run_id, [])
                )
            ]
        return results[:filters.limit]

    def get_dataset(self, dataset_id: str) -> DatasetHandle:
        """Return a DatasetHandle for the given dataset_id."""
        for record in self._datasets:
            if record.dataset_id == dataset_id:
                return record_to_handle(record)
        raise KeyError(f"dataset not found: {dataset_id}")

    def get_run(self, run_id: str) -> RunRecord:
        """Return the RunRecord for the given run_id."""
        if run_id not in self._runs:
            raise KeyError(f"run not found: {run_id}")
        return self._runs[run_id]

    def get_ticker_results(self, run_id: str) -> list[TickerRunRecord]:
        """Return per-ticker results stored for a run."""
        if run_id not in self._runs:
            raise KeyError(f"run not found: {run_id}")
        return list(self._ticker_results.get(run_id, []))

    def finalize_run(self, run_id: str, summary: RunSummary) -> None:
        """Mark run as complete or interrupted with the given summary."""
        if run_id in self._runs:
            run = self._runs[run_id]
            if run.status != "running":
                return
            run.status = summary.status
            run.finished_at = datetime.now(tz=timezone.utc)
            run.error_summary = summary.error_summary

    def fail_run(self, run_id: str, error: str) -> None:
        """Mark run as failed with the given error message."""
        if run_id in self._runs:
            run = self._runs[run_id]
            if run.status != "running":
                return
            run.status = "failed"
            run.finished_at = datetime.now(tz=timezone.utc)
            run.error_summary = error

    def interrupt_stale_runs(self, cutoff: datetime, error_summary: str) -> int:
        """Mark running runs older than cutoff as interrupted."""
        interrupted = 0
        for run in self._runs.values():
            if run.status != "running" or run.started_at >= cutoff:
                continue
            run.status = "interrupted"
            run.finished_at = datetime.now(tz=timezone.utc)
            run.error_summary = error_summary
            interrupted += 1
        return interrupted

    def count_runs_today(self, provider: str) -> int:
        """Return the number of complete runs started today (US/Eastern) for the provider."""
        from opx_chain.config import US_MARKET_TIMEZONE  # pylint: disable=import-outside-toplevel
        provider = validate_required_text(provider, name="provider")
        now_et = datetime.now(tz=US_MARKET_TIMEZONE)
        midnight_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        since_utc = midnight_et.astimezone(timezone.utc)
        return sum(
            1
            for run in self._runs.values()
            if (
                run.provider == provider
                and run.status == "complete"
                and run.started_at >= since_utc
            )
        )
