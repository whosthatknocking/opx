"""Analyst forecast provider contract tests."""

from __future__ import annotations
# pylint: disable=missing-module-docstring,missing-class-docstring
# pylint: disable=missing-function-docstring,duplicate-code

from datetime import date, datetime, timezone

import pytest

from opx_chain.analyst_forecast import (
    ANALYST_FORECAST_SCHEMA_VERSION,
    fetch_analyst_forecasts,
)


class FakeForecastProvider:
    name = "yfinance"

    def prepare_ticker_fetch(self, _ticker: str) -> None:
        return None

    def load_analyst_forecast(self, ticker: str) -> dict:
        if ticker == "ERR":
            raise RuntimeError("provider down")
        if ticker == "MISS":
            return {"price_targets": {}, "recommendations_summary": []}
        return {
            "price_targets": {
                "current": 101.0,
                "low": 90,
                "mean": 125.5,
                "median": 124,
                "high": 150,
            },
            "recommendations_summary": [
                {
                    "period": "-1m",
                    "strongBuy": 10,
                    "buy": 5,
                    "hold": 1,
                    "sell": 0,
                    "strongSell": 0,
                },
                {
                    "period": "0m",
                    "strongBuy": 12,
                    "buy": 26,
                    "hold": 8,
                    "sell": 1,
                    "strongSell": 0,
                },
            ],
        }


def test_fetch_analyst_forecasts_normalizes_yfinance_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        "opx_chain.analyst_forecast.get_data_provider_by_name",
        lambda _name: FakeForecastProvider(),
    )

    payload = fetch_analyst_forecasts(
        ["googl"],
        fetched_at=datetime(2026, 6, 4, 14, 34, 41, tzinfo=timezone.utc),
        trading_date=date(2026, 6, 4),
    )

    assert payload["schema_version"] == ANALYST_FORECAST_SCHEMA_VERSION
    assert payload["status"] == "ok"
    assert payload["source_quality"] == "research_fallback"
    row = payload["forecasts"][0]
    assert row["ticker"] == "GOOGL"
    assert row["target_low"] == 90.0
    assert row["target_mean"] == 125.5
    assert row["target_median"] == 124.0
    assert row["target_high"] == 150.0
    assert row["analyst_count"] is None
    assert row["recommendation_count"] == 47
    assert row["rating_counts"] == {
        "strong_buy": 12,
        "buy": 26,
        "hold": 8,
        "sell": 1,
        "strong_sell": 0,
    }
    assert row["consensus_rating"] == "buy"


def test_fetch_analyst_forecasts_degrades_missing_and_provider_errors(monkeypatch) -> None:
    monkeypatch.setattr(
        "opx_chain.analyst_forecast.get_data_provider_by_name",
        lambda _name: FakeForecastProvider(),
    )

    payload = fetch_analyst_forecasts(
        ["GOOGL", "MISS", "ERR"],
        fetched_at=datetime(2026, 6, 4, 14, 34, 41, tzinfo=timezone.utc),
        trading_date=date(2026, 6, 4),
    )

    assert payload["status"] == "partial"
    by_ticker = {row["ticker"]: row for row in payload["forecasts"]}
    assert by_ticker["MISS"]["status"] == "missing"
    assert by_ticker["MISS"]["warnings"][0]["code"] == "missing_price_targets"
    assert by_ticker["ERR"]["status"] == "error"
    assert by_ticker["ERR"]["warnings"][0]["severity"] == "error"
    assert payload["errors"][0]["ticker"] == "ERR"


def test_fetch_analyst_forecasts_reports_ambiguous_recommendation_period(monkeypatch) -> None:
    class AmbiguousProvider(FakeForecastProvider):
        def load_analyst_forecast(self, _ticker: str) -> dict:
            return {
                "price_targets": {"mean": 125},
                "recommendations_summary": [
                    {"buy": 2, "hold": 1},
                    {"buy": 1, "hold": 3},
                ],
            }

    monkeypatch.setattr(
        "opx_chain.analyst_forecast.get_data_provider_by_name",
        lambda _name: AmbiguousProvider(),
    )

    payload = fetch_analyst_forecasts(
        ["GOOGL"],
        fetched_at=datetime(2026, 6, 4, 14, 34, 41, tzinfo=timezone.utc),
    )

    row = payload["forecasts"][0]
    assert row["recommendation_count"] is None
    assert row["consensus_rating"] is None
    assert row["warnings"][0]["code"] == "ambiguous_recommendation_period"


def test_fetch_analyst_forecasts_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="analyst_forecast_provider"):
        fetch_analyst_forecasts(["GOOGL"], provider="factset")
    with pytest.raises(ValueError, match="tickers must be a list, tuple, or set"):
        fetch_analyst_forecasts("NVDA")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="invalid analyst forecast ticker"):
        fetch_analyst_forecasts(["bad ticker"])
    with pytest.raises(ValueError, match="timezone-aware"):
        fetch_analyst_forecasts(["GOOGL"], fetched_at=datetime(2026, 6, 4, 14, 0))
    with pytest.raises(ValueError, match="trading_date must be a date"):
        fetch_analyst_forecasts(
            ["GOOGL"],
            trading_date=datetime(2026, 6, 4, 14, 0, tzinfo=timezone.utc),
        )
