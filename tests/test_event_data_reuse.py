"""Event data retained-snapshot reuse tests."""

from __future__ import annotations
# pylint: disable=missing-class-docstring,missing-function-docstring

import json
from datetime import date, datetime, timezone

from opx_chain.event_data import (
    event_data_snapshot_dir,
    run_event_fetch,
    summarize_latest_event_data,
)


class FakeEventProvider:
    name = "yfinance"

    def prepare_ticker_fetch(self, _ticker: str) -> None:
        return None

    def load_ticker_events(self, _ticker: str) -> dict:
        return {
            "next_earnings_date": "2026-06-05",
            "next_earnings_date_is_estimated": True,
            "next_earnings_date_source": "yfinance",
            "next_earnings_date_confidence": "estimated",
            "next_ex_div_date": "2026-06-03",
            "next_ex_div_date_source": "yfinance",
            "next_ex_div_date_confidence": "confirmed",
            "dividend_amount": 0.25,
        }


def test_event_data_reuse_records_authoritative_over_stale_success_metadata(
    tmp_path,
    monkeypatch,
) -> None:
    snapshot_dir = event_data_snapshot_dir(tmp_path)
    snapshot_dir.mkdir(parents=True)
    (snapshot_dir / "stale-success-metadata.json").write_text(
        json.dumps(
            {
                "artifact_type": "event_data_snapshot",
                "schema_version": 1,
                "event_snapshot_id": "stale-success-metadata",
                "provider": "yfinance",
                "resolved_provider": "yfinance",
                "status": "partial",
                "fetched_at": "2026-06-01T14:00:00Z",
                "trading_date": "2026-06-01",
                "freshness_policy": "trading_day",
                "fresh_through_trading_date": "2026-06-01",
                "ticker_universe_source": "caller_supplied_tickers",
                "tickers_requested": ["TSLA"],
                "tickers_succeeded": ["TSLA"],
                "tickers_failed": [],
                "tickers_no_known_event": [],
                "status_counts": {"provider_error": 1},
                "records": [
                    {
                        "ticker": "TSLA",
                        "provider_status": "provider_error",
                        "provider_error": "provider down",
                    }
                ],
                "canonical_events": [],
            }
        ),
        encoding="utf-8",
    )
    calls: list[str] = []

    def fake_provider(name: str):
        calls.append(name)
        return FakeEventProvider()

    summary = summarize_latest_event_data(
        provider="yfinance",
        chain_provider="marketdata",
        tickers=("TSLA",),
        trading_date=date(2026, 6, 1),
        base_dir=tmp_path,
    )
    monkeypatch.setattr("opx_chain.event_data.get_data_provider_by_name", fake_provider)
    result = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="auto",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 5, tzinfo=timezone.utc),
    )

    assert summary["available"] is True
    assert summary["reusable"] is False
    assert summary["covered_required_tickers"] == []
    assert summary["missing_tickers"] == ["TSLA"]
    assert summary["auto_would_reuse"] is False
    assert summary["provider_api_call_expected"] is True
    assert calls == ["yfinance"]
    assert result.reused is False
    assert result.status == "ready"
    assert result.snapshot_id != "stale-success-metadata"
