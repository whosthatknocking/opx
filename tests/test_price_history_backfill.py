"""Tests for the provider-scoped price-history backfill command."""

from datetime import date

import pandas as pd

from conftest import make_runtime_config
from opx_chain.price_history import PriceHistoryStore
from opx_chain.price_history_backfill import (
    format_backfill_result,
    main,
    run_price_history_backfill,
)


def _history(end: str = "2026-03-20", periods: int = 20) -> pd.DataFrame:
    dates = pd.bdate_range(end=end, periods=periods)
    closes = [100.0 + index for index in range(periods)]
    return pd.DataFrame(
        {
            "date": dates,
            "open": [close - 0.5 for close in closes],
            "high": [close + 1.0 for close in closes],
            "low": [close - 1.0 for close in closes],
            "close": closes,
            "volume": [1000 + index for index in range(periods)],
        }
    )


class BackfillProvider:
    """Provider stub with independent provider identity."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.prepared: list[str] = []
        self.calls: list[tuple[str, int]] = []

    def prepare_ticker_fetch(self, ticker: str) -> None:
        """Record ticker preparation."""
        self.prepared.append(ticker)

    def load_price_history(self, ticker: str, *, lookback_days: int) -> pd.DataFrame:
        """Return deterministic daily OHLCV rows."""
        self.calls.append((ticker, lookback_days))
        return _history(periods=lookback_days)


def test_price_history_backfill_stores_providers_independently(tmp_path):
    """Backfill rows should be keyed by provider, ticker, and trading date."""
    store = PriceHistoryStore(tmp_path / "price-history.db")
    providers = {
        "marketdata": BackfillProvider("marketdata"),
        "yfinance": BackfillProvider("yfinance"),
    }
    config = make_runtime_config(
        tickers=("AAA", "BBB"),
        today=date(2026, 3, 20),
        price_context_lookback_days=5,
        provider_price_context_ttl=86400,
    )

    result = run_price_history_backfill(
        providers=("marketdata", "yfinance"),
        tickers=("AAA",),
        config=config,
        store=store,
        provider_factory=lambda provider_name: providers[provider_name],
    )

    assert result.providers == ("marketdata", "yfinance")
    assert result.tickers == ("AAA",)
    assert [row.status for row in result.rows] == ["FETCHED", "FETCHED"]
    assert providers["marketdata"].calls == [("AAA", 5)]
    assert providers["yfinance"].calls == [("AAA", 5)]
    assert store.stats(provider="marketdata", ticker="AAA").row_count == 5
    assert store.stats(provider="yfinance", ticker="AAA").row_count == 5


def test_price_history_backfill_dry_run_reports_coverage_without_fetch(tmp_path):
    """Dry run should inspect local coverage without provider calls or writes."""
    store = PriceHistoryStore(tmp_path / "price-history.db")
    store.upsert_bars(provider="yfinance", ticker="AAA", history=_history(periods=3))
    provider = BackfillProvider("yfinance")
    config = make_runtime_config(
        tickers=("AAA",),
        today=date(2026, 3, 20),
        price_context_lookback_days=5,
    )

    result = run_price_history_backfill(
        providers=("yfinance",),
        config=config,
        store=store,
        provider_factory=lambda _provider_name: provider,
        dry_run=True,
    )

    assert not provider.calls
    assert result.rows[0].status == "DRY_RUN"
    assert result.rows[0].stored_row_count == 3
    assert "mode: dry-run" in format_backfill_result(result)


def test_price_history_backfill_cli_rejects_unsupported_provider(capsys):
    """The CLI should fail clearly for providers without price-history backfill support."""
    result = main(["--providers", "massive", "--dry-run"])

    captured = capsys.readouterr()
    assert result == 1
    assert "price-history backfill is only available" in captured.out
