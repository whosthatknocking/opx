"""Tests for durable IV-history backfill from retained datasets."""

from datetime import datetime, timezone

import pandas as pd

from conftest import make_runtime_config
from opx_chain.iv_history import IVHistoryStore
from opx_chain.iv_history_backfill import (
    format_backfill_result,
    run_iv_history_backfill,
)
from opx_chain.storage.models import DatasetRecord


def _dataset(path, *, ticker: str = "TSLA", quote_time: str | None = None) -> None:
    rows = []
    for dte, expiration, iv in (
        (14, "2026-06-05", 0.24),
        (14, "2026-06-05", 0.28),
        (28, "2026-06-19", 0.31),
    ):
        rows.append(
            {
                "underlying_symbol": ticker,
                "expiration_date": expiration,
                "days_to_expiration": dte,
                "option_type": "PUT",
                "delta": -0.2,
                "implied_volatility": iv,
                "option_quote_time": quote_time,
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)


class FakeStorage:
    """Storage stub that exposes retained option-chain datasets."""

    def __init__(self, records):
        self.records = list(records)

    def list_datasets(self, **kwargs):  # pylint: disable=unused-argument
        """Return configured records."""
        return self.records

    def get_dataset(self, dataset_id: str):
        """Return one configured record by dataset id."""
        for record in self.records:
            if record.dataset_id == dataset_id:
                return record
        raise KeyError(dataset_id)


def _record(path, *, dataset_id: str = "dataset-1") -> DatasetRecord:
    return DatasetRecord(
        dataset_id=dataset_id,
        run_id="run-1",
        created_at=datetime(2026, 5, 22, 20, 0, tzinfo=timezone.utc),
        provider="marketdata",
        schema_version=2,
        row_count=3,
        format="csv",
        location=str(path),
        content_hash="hash",
    )


def test_iv_history_backfill_ingests_retained_dataset(tmp_path):
    """Backfill should derive IV aggregates without provider API calls."""
    path = tmp_path / "chain.csv"
    _dataset(path)
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=datetime(2026, 5, 22, tzinfo=timezone.utc).date(),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        config=config,
        store=store,
        storage=FakeStorage([_record(path)]),
    )

    assert result.rows[0].status == "INGESTED"
    assert result.rows[0].source_rows == 3
    assert result.rows[0].stored_rows > 0
    assert store.stats(provider="marketdata", ticker="TSLA").observation_dates == 1


def test_iv_history_backfill_prefers_quote_date_over_dataset_created_at(tmp_path):
    """Weekend backfills should store IV under the quote date when available."""
    path = tmp_path / "chain.csv"
    _dataset(path, quote_time="2026-05-22T20:00:00Z")
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=datetime(2026, 5, 24, tzinfo=timezone.utc).date(),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        config=config,
        store=store,
        storage=FakeStorage([_record(path)]),
    )

    assert result.rows[0].observation_date == "2026-05-22"
    assert store.stats(provider="marketdata", ticker="TSLA").latest_date.isoformat() == "2026-05-22"


def test_iv_history_backfill_dry_run_does_not_write(tmp_path):
    """Dry run should compute aggregate rows but leave the durable store untouched."""
    path = tmp_path / "chain.csv"
    _dataset(path)
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=datetime(2026, 5, 22, tzinfo=timezone.utc).date(),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        config=config,
        dry_run=True,
        store=store,
        storage=FakeStorage([_record(path)]),
    )

    assert result.rows[0].status == "DRY_RUN"
    assert result.rows[0].stored_rows > 0
    assert store.stats(provider="marketdata", ticker="TSLA").row_count == 0
    assert "mode: dry-run" in format_backfill_result(result)


def test_iv_history_backfill_skips_already_ingested_dataset(tmp_path):
    """Existing sync records should prevent duplicate ingest unless refreshed."""
    path = tmp_path / "chain.csv"
    _dataset(path)
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(data_provider="marketdata", tickers=("TSLA",))
    storage = FakeStorage([_record(path)])

    first = run_iv_history_backfill(
        providers=("marketdata",),
        config=config,
        store=store,
        storage=storage,
    )
    second = run_iv_history_backfill(
        providers=("marketdata",),
        config=config,
        store=store,
        storage=storage,
    )

    assert first.rows[0].status == "INGESTED"
    assert second.rows[0].status == "SKIPPED"
