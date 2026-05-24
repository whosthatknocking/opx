"""Tests for durable IV-history backfill from retained and historical data."""

from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

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


class HistoricalProvider:
    """Provider stub that exposes historical option-chain snapshots."""

    name = "marketdata"

    def __init__(self) -> None:
        self.prepared: list[str] = []
        self.calls: list[tuple[str, date]] = []

    def prepare_ticker_fetch(self, ticker: str) -> None:
        """Record ticker preparation."""
        self.prepared.append(ticker)

    def load_historical_option_chain_frame(
        self,
        ticker: str,
        *,
        observation_date: date,
    ) -> pd.DataFrame:
        """Return a minimal historical option-chain frame."""
        self.calls.append((ticker, observation_date))
        expiration = (observation_date + timedelta(days=21)).isoformat()
        return pd.DataFrame(
            [
                {
                    "underlying_symbol": ticker,
                    "expiration_date": expiration,
                    "option_type": "PUT",
                    "delta": -0.20,
                    "implied_volatility": 0.30,
                },
                {
                    "underlying_symbol": ticker,
                    "expiration_date": expiration,
                    "option_type": "CALL",
                    "delta": 0.20,
                    "implied_volatility": 0.34,
                },
            ]
        )


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


def test_iv_history_historical_dry_run_estimates_requests_without_fetch(tmp_path):
    """Historical dry-run should show provider calls without consuming API quota."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    provider = HistoricalProvider()
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA", "NVDA"),
        today=date(2026, 5, 22),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        fetch_historical=True,
        sessions=2,
        config=config,
        store=store,
        dry_run=True,
        provider_factory=lambda _provider_name: provider,
    )

    assert not provider.calls
    assert result.fetch_historical is True
    assert result.estimated_requests == 4
    assert [row.status for row in result.rows] == ["WOULD_FETCH"] * 4
    assert "source: historical provider fetch" in format_backfill_result(result)
    assert "estimated_provider_requests: 4" in format_backfill_result(result)


def test_iv_history_historical_sessions_roll_weekend_end_date(tmp_path):
    """Historical session planning should count back from the prior business day."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=date(2026, 5, 24),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        fetch_historical=True,
        sessions=2,
        config=config,
        store=store,
        dry_run=True,
    )

    assert [row.observation_date for row in result.rows] == [
        "2026-05-21",
        "2026-05-22",
    ]
    assert result.estimated_requests == 2


def test_iv_history_historical_fetch_ingests_marketdata_snapshots(tmp_path):
    """Historical fetch should write provider/ticker/date IV aggregates."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    provider = HistoricalProvider()
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=date(2026, 5, 22),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        fetch_historical=True,
        sessions=2,
        config=config,
        store=store,
        provider_factory=lambda _provider_name: provider,
    )

    assert [row.status for row in result.rows] == ["INGESTED", "INGESTED"]
    assert provider.prepared == ["TSLA"]
    assert provider.calls == [
        ("TSLA", date(2026, 5, 21)),
        ("TSLA", date(2026, 5, 22)),
    ]
    stats = store.stats(provider="marketdata", ticker="TSLA")
    assert stats.observation_dates == 2
    assert stats.latest_date == date(2026, 5, 22)


def test_iv_history_historical_fetch_skips_existing_date_without_refresh(tmp_path):
    """Existing provider/ticker/date coverage should not be refetched by default."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    provider = HistoricalProvider()
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=date(2026, 5, 22),
    )

    first = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        fetch_historical=True,
        sessions=1,
        config=config,
        store=store,
        provider_factory=lambda _provider_name: provider,
    )
    second = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        fetch_historical=True,
        sessions=1,
        config=config,
        store=store,
        provider_factory=lambda _provider_name: provider,
    )

    assert first.rows[0].status == "INGESTED"
    assert second.rows[0].status == "SKIPPED"
    assert provider.calls == [("TSLA", date(2026, 5, 22))]


def test_iv_history_historical_fetch_rejects_unsupported_provider(tmp_path):
    """Only providers with historical option-chain snapshots should be accepted."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(data_provider="yfinance", tickers=("TSLA",))

    with pytest.raises(ValueError, match="historical IV fetch is only available"):
        run_iv_history_backfill(
            providers=("yfinance",),
            fetch_historical=True,
            config=config,
            store=store,
            dry_run=True,
        )
