"""Event data provider contract tests."""

from __future__ import annotations
# pylint: disable=missing-module-docstring,missing-class-docstring
# pylint: disable=missing-function-docstring,duplicate-code,too-many-lines

import json
import math
import os
from dataclasses import replace
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import pytest

from opx_chain.config import get_runtime_config, set_runtime_config_override
from opx_chain.event_data import (
    clear_event_columns,
    event_data_snapshot_dir,
    overlay_event_snapshot,
    run_event_fetch,
    summarize_latest_event_data,
)


class FakeEventProvider:
    name = "yfinance"

    def prepare_ticker_fetch(self, _ticker: str) -> None:
        return None

    def load_ticker_events(self, ticker: str) -> dict:
        if ticker == "ERR":
            raise RuntimeError("provider down")
        if ticker == "NONE":
            return {
                "next_earnings_date": None,
                "next_earnings_date_is_estimated": None,
                "next_earnings_date_source": None,
                "next_earnings_date_confidence": None,
                "next_ex_div_date": None,
                "next_ex_div_date_source": None,
                "next_ex_div_date_confidence": None,
                "dividend_amount": np.nan,
            }
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


class FailingEventProvider:
    name = "yfinance"

    def prepare_ticker_fetch(self, _ticker: str) -> None:
        return None

    def load_ticker_events(self, _ticker: str) -> dict:
        raise RuntimeError("provider down")


class MalformedDateProvider:
    name = "yfinance"

    def __init__(self, *, earnings: str | None = "not-a-date", ex_div: str | None = None) -> None:
        self.earnings = earnings
        self.ex_div = ex_div

    def prepare_ticker_fetch(self, _ticker: str) -> None:
        return None

    def load_ticker_events(self, _ticker: str) -> dict:
        return {
            "next_earnings_date": self.earnings,
            "next_earnings_date_is_estimated": True,
            "next_earnings_date_source": "yfinance",
            "next_earnings_date_confidence": "estimated",
            "next_ex_div_date": self.ex_div,
            "next_ex_div_date_source": "yfinance",
            "next_ex_div_date_confidence": "confirmed",
            "dividend_amount": 0.25,
        }


def test_run_event_fetch_writes_and_reuses_same_trading_day_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    calls = []

    def fake_provider(name: str):
        calls.append(name)
        return FakeEventProvider()

    monkeypatch.setattr("opx_chain.event_data.get_data_provider_by_name", fake_provider)

    first = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="fetch_latest",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA", "NONE"),
        ticker_universe_source="new_run_portfolio_and_ticker_intents",
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )
    second = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="auto",
        trading_date=date(2026, 6, 1),
        tickers=("NONE", "TSLA"),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 5, tzinfo=timezone.utc),
    )

    assert calls == ["yfinance"]
    assert first.reused is False
    assert first.status == "ready"
    assert first.path and first.path.exists()
    assert first.payload["ticker_universe_source"] == "new_run_portfolio_and_ticker_intents"
    assert second.reused is True
    assert second.snapshot_id == first.snapshot_id
    assert second.payload["status_counts"] == {"no_known_event": 1, "ready": 1}


def test_run_event_fetch_disabled_skips_same_as_chain_resolution(tmp_path) -> None:
    result = run_event_fetch(
        enabled=False,
        provider="same_as_chain",
        chain_provider=None,
        fetch_mode="auto",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        ticker_universe_source="new_run_portfolio_and_ticker_intents",
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )

    assert result.status == "disabled"
    assert result.provider is None
    assert result.resolved_provider is None
    assert result.path is None
    assert result.payload["provider"] is None
    assert result.payload["resolved_provider"] is None
    assert result.payload["ticker_universe_source"] == "new_run_portfolio_and_ticker_intents"
    assert result.payload["tickers_requested"] == ["TSLA"]


@pytest.mark.parametrize("bad_enabled", ["false", "0", 0, 1, None])
def test_run_event_fetch_rejects_non_bool_enabled_values(
    monkeypatch,
    tmp_path,
    bad_enabled,
) -> None:
    def fail_provider(_name):
        raise AssertionError("provider should not be resolved for invalid enabled")

    monkeypatch.setattr(
        "opx_chain.event_data.get_data_provider_by_name",
        fail_provider,
    )

    with pytest.raises(ValueError, match="enabled must be true or false"):
        run_event_fetch(
            enabled=bad_enabled,  # type: ignore[arg-type]
            provider="same_as_chain",
            chain_provider=None,
            fetch_mode="auto",
            trading_date=date(2026, 6, 1),
            tickers=("TSLA",),
            ticker_universe_source="new_run_portfolio_and_ticker_intents",
            base_dir=tmp_path,
            now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
        )


@pytest.mark.parametrize("bad_provider", [False, 0])
def test_run_event_fetch_rejects_falsey_non_string_provider(
    monkeypatch,
    tmp_path,
    bad_provider,
) -> None:
    def fail_provider(_name):
        raise AssertionError("provider should not be resolved for invalid provider")

    monkeypatch.setattr(
        "opx_chain.event_data.get_data_provider_by_name",
        fail_provider,
    )

    with pytest.raises(ValueError, match="event_data_provider must be a string"):
        run_event_fetch(
            provider=bad_provider,  # type: ignore[arg-type]
            chain_provider="marketdata",
            fetch_mode="auto",
            trading_date=date(2026, 6, 1),
            tickers=("TSLA",),
            ticker_universe_source="new_run_portfolio_and_ticker_intents",
            base_dir=tmp_path,
            now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
        )


@pytest.mark.parametrize("bad_fetch_mode", [False, 0])
def test_run_event_fetch_rejects_falsey_non_string_fetch_mode(
    monkeypatch,
    tmp_path,
    bad_fetch_mode,
) -> None:
    def fail_provider(_name):
        raise AssertionError("provider should not be resolved for invalid fetch mode")

    monkeypatch.setattr(
        "opx_chain.event_data.get_data_provider_by_name",
        fail_provider,
    )

    with pytest.raises(ValueError, match="event_data_fetch_mode must be a string"):
        run_event_fetch(
            provider="yfinance",
            chain_provider="marketdata",
            fetch_mode=bad_fetch_mode,  # type: ignore[arg-type]
            trading_date=date(2026, 6, 1),
            tickers=("TSLA",),
            ticker_universe_source="new_run_portfolio_and_ticker_intents",
            base_dir=tmp_path,
            now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
        )


@pytest.mark.parametrize("bad_member", [True, False, math.nan, math.inf, None, 0])
def test_run_event_fetch_rejects_non_string_ticker_members(
    monkeypatch,
    tmp_path,
    bad_member,
) -> None:
    def fail_provider(_name):
        raise AssertionError("provider should not be resolved for invalid tickers")

    monkeypatch.setattr(
        "opx_chain.event_data.get_data_provider_by_name",
        fail_provider,
    )

    with pytest.raises(ValueError, match="ticker members must be strings"):
        run_event_fetch(
            provider="yfinance",
            chain_provider="marketdata",
            fetch_mode="fetch_latest",
            trading_date=date(2026, 6, 1),
            tickers=(bad_member,),  # type: ignore[arg-type]
            ticker_universe_source="new_run_portfolio_and_ticker_intents",
            base_dir=tmp_path,
            now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
        )


def test_run_event_fetch_not_supported_preserves_ticker_universe_source(tmp_path) -> None:
    result = run_event_fetch(
        provider="same_as_chain",
        chain_provider="massive",
        fetch_mode="fetch_latest",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        ticker_universe_source="new_run_portfolio_and_ticker_intents",
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )

    assert result.status == "not_supported"
    assert result.provider == "same_as_chain"
    assert result.resolved_provider == "massive"
    assert result.payload["ticker_universe_source"] == "new_run_portfolio_and_ticker_intents"


def test_run_event_fetch_rejects_naive_now_before_provider_calls(tmp_path, monkeypatch) -> None:
    calls: list[str] = []

    def fake_provider(name: str):
        calls.append(name)
        return FakeEventProvider()

    monkeypatch.setattr("opx_chain.event_data.get_data_provider_by_name", fake_provider)

    with pytest.raises(ValueError, match="now must be timezone-aware UTC"):
        run_event_fetch(
            provider="yfinance",
            chain_provider="marketdata",
            fetch_mode="fetch_latest",
            trading_date=date(2026, 6, 1),
            tickers=("TSLA",),
            base_dir=tmp_path,
            now=datetime(2026, 6, 1, 14, 0),
        )

    assert not calls


def test_run_event_fetch_rejects_datetime_trading_date_before_provider_calls(
    tmp_path,
    monkeypatch,
) -> None:
    calls: list[str] = []

    def fake_provider(name: str):
        calls.append(name)
        return FakeEventProvider()

    monkeypatch.setattr("opx_chain.event_data.get_data_provider_by_name", fake_provider)

    with pytest.raises(ValueError, match="trading_date must be a date"):
        run_event_fetch(
            provider="yfinance",
            chain_provider="marketdata",
            fetch_mode="fetch_latest",
            trading_date=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
            tickers=("TSLA",),
            base_dir=tmp_path,
            now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
        )

    assert not calls


def test_run_event_fetch_payload_preserves_selected_and_resolved_provider(
    tmp_path,
    monkeypatch,
) -> None:
    calls: list[str] = []

    def fake_provider(name: str):
        calls.append(name)
        return FakeEventProvider()

    monkeypatch.setattr("opx_chain.event_data.get_data_provider_by_name", fake_provider)

    result = run_event_fetch(
        provider="same_as_chain",
        chain_provider="yfinance",
        fetch_mode="fetch_latest",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        ticker_universe_source="new_run_portfolio_and_ticker_intents",
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )
    payload = json.loads(result.path.read_text(encoding="utf-8"))
    summary = summarize_latest_event_data(
        provider="same_as_chain",
        chain_provider="yfinance",
        tickers=("TSLA",),
        trading_date=date(2026, 6, 1),
        base_dir=tmp_path,
    )

    assert calls == ["yfinance"]
    assert result.provider == "same_as_chain"
    assert result.resolved_provider == "yfinance"
    assert payload["provider"] == "same_as_chain"
    assert payload["resolved_provider"] == "yfinance"
    assert summary["provider"] == "same_as_chain"
    assert summary["resolved_provider"] == "yfinance"
    assert summary["auto_would_reuse"] is True


def test_run_event_fetch_reports_partial_provider_errors(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "opx_chain.event_data.get_data_provider_by_name",
        lambda _name: FakeEventProvider(),
    )

    result = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="fetch_latest",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA", "ERR"),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )

    assert result.status == "partial"
    assert result.payload["tickers_failed"] == ["ERR"]
    assert result.payload["status_counts"] == {"provider_error": 1, "ready": 1}


def test_summarize_latest_event_data_reports_failed_partial_ticker_non_reusable(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "opx_chain.event_data.get_data_provider_by_name",
        lambda _name: FakeEventProvider(),
    )
    run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="fetch_latest",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA", "ERR"),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )

    summary = summarize_latest_event_data(
        provider="yfinance",
        chain_provider="marketdata",
        tickers=("ERR",),
        trading_date=date(2026, 6, 1),
        base_dir=tmp_path,
    )

    assert summary["available"] is True
    assert summary["reusable"] is False
    assert summary["status"] == "partial"
    assert summary["freshness_label"] == "MISSING"
    assert summary["covered_required_tickers"] == []
    assert summary["missing_tickers"] == ["ERR"]
    assert summary["status_counts"] == {"provider_error": 1, "ready": 1}
    assert summary["auto_would_reuse"] is False
    assert summary["provider_api_call_expected"] is True


def test_run_event_fetch_auto_does_not_reuse_partial_failed_ticker(
    tmp_path,
    monkeypatch,
) -> None:
    calls: list[str] = []

    def fake_provider(name: str):
        calls.append(name)
        return FakeEventProvider()

    monkeypatch.setattr("opx_chain.event_data.get_data_provider_by_name", fake_provider)

    first = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="fetch_latest",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA", "ERR"),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )
    second = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="auto",
        trading_date=date(2026, 6, 1),
        tickers=("ERR",),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 5, tzinfo=timezone.utc),
    )

    assert calls == ["yfinance", "yfinance"]
    assert first.status == "partial"
    assert second.reused is False
    assert second.status == "provider_error"
    assert second.snapshot_id != first.snapshot_id


def test_run_event_fetch_auto_does_not_reuse_missing_snapshot(tmp_path, monkeypatch) -> None:
    snapshot_dir = event_data_snapshot_dir(tmp_path)
    snapshot_dir.mkdir(parents=True)
    (snapshot_dir / "missing-retained.json").write_text(
        json.dumps(
            {
                "artifact_type": "event_data_snapshot",
                "schema_version": 1,
                "event_snapshot_id": "missing-retained",
                "provider": "yfinance",
                "resolved_provider": "yfinance",
                "status": "missing",
                "fetched_at": "2026-06-01T14:00:00Z",
                "trading_date": "2026-06-01",
                "freshness_policy": "trading_day",
                "fresh_through_trading_date": "2026-06-01",
                "ticker_universe_source": "caller_supplied_tickers",
                "tickers_requested": ["TSLA"],
                "tickers_succeeded": [],
                "tickers_failed": [],
                "tickers_no_known_event": [],
                "status_counts": {"missing": 1},
                "records": [],
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

    assert summary["status"] == "missing"
    assert summary["freshness_label"] == "MISSING"
    assert summary["auto_would_reuse"] is False
    assert summary["provider_api_call_expected"] is True
    assert calls == ["yfinance"]
    assert result.reused is False
    assert result.snapshot_id != "missing-retained"


def test_run_event_fetch_auto_does_not_reuse_provider_error_snapshot(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_provider(name: str):
        calls.append(name)
        return FailingEventProvider()

    monkeypatch.setattr("opx_chain.event_data.get_data_provider_by_name", fake_provider)

    first = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="fetch_latest",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )
    second = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="auto",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 5, tzinfo=timezone.utc),
    )

    assert calls == ["yfinance", "yfinance"]
    assert first.status == "provider_error"
    assert first.reused is False
    assert second.status == "provider_error"
    assert second.reused is False
    assert second.snapshot_id != first.snapshot_id


def test_event_data_reuse_rejects_wrong_artifact_type(tmp_path, monkeypatch) -> None:
    snapshot_dir = event_data_snapshot_dir(tmp_path)
    snapshot_dir.mkdir(parents=True)
    (snapshot_dir / "bad.json").write_text(
        json.dumps(
            {
                "artifact_type": "price_context",
                "schema_version": 1,
                "event_snapshot_id": "bad-artifact",
                "provider": "yfinance",
                "status": "ready",
                "trading_date": "2026-06-01",
                "fresh_through_trading_date": "2026-06-01",
                "tickers_requested": ["TSLA"],
                "records": [],
                "canonical_events": [],
            }
        ),
        encoding="utf-8",
    )
    summary = summarize_latest_event_data(
        provider="yfinance",
        chain_provider="marketdata",
        tickers=("TSLA",),
        trading_date=date(2026, 6, 1),
        base_dir=tmp_path,
    )
    calls = []

    def fake_provider(name: str):
        calls.append(name)
        return FakeEventProvider()

    monkeypatch.setattr("opx_chain.event_data.get_data_provider_by_name", fake_provider)
    result = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="auto",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )

    assert summary["available"] is False
    assert summary["reusable"] is False
    assert calls == ["yfinance"]
    assert result.reused is False
    assert result.snapshot_id != "bad-artifact"


def test_event_data_reuse_skips_malformed_retained_tickers(tmp_path, monkeypatch) -> None:
    snapshot_dir = event_data_snapshot_dir(tmp_path)
    snapshot_dir.mkdir(parents=True)
    (snapshot_dir / "bad-tickers.json").write_text(
        json.dumps(
            {
                "artifact_type": "event_data_snapshot",
                "schema_version": 1,
                "event_snapshot_id": "bad-tickers",
                "provider": "yfinance",
                "status": "ready",
                "trading_date": "2026-06-01",
                "fresh_through_trading_date": "2026-06-01",
                "tickers_requested": ["BAD TICKER"],
                "records": [],
                "canonical_events": [],
            }
        ),
        encoding="utf-8",
    )
    summary = summarize_latest_event_data(
        provider="yfinance",
        chain_provider="marketdata",
        tickers=("TSLA",),
        trading_date=date(2026, 6, 1),
        base_dir=tmp_path,
    )
    calls = []

    def fake_provider(name: str):
        calls.append(name)
        return FakeEventProvider()

    monkeypatch.setattr("opx_chain.event_data.get_data_provider_by_name", fake_provider)
    result = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="auto",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )

    assert summary["available"] is False
    assert calls == ["yfinance"]
    assert result.reused is False


def test_summarize_latest_event_data_reports_provider_mismatch(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "opx_chain.event_data.get_data_provider_by_name",
        lambda _name: FakeEventProvider(),
    )
    run_event_fetch(
        provider="marketdata",
        chain_provider="marketdata",
        fetch_mode="fetch_latest",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )

    summary = summarize_latest_event_data(
        provider="yfinance",
        chain_provider="marketdata",
        tickers=("TSLA",),
        trading_date=date(2026, 6, 1),
        base_dir=tmp_path,
    )

    assert summary["available"] is True
    assert summary["reusable"] is False
    assert summary["status"] == "provider_mismatch"
    assert summary["freshness_label"] == "PROVIDER_MISMATCH"
    assert summary["provider"] == "yfinance"
    assert summary["resolved_provider"] == "yfinance"
    assert summary["retained_provider"] == "marketdata"
    assert summary["retained_resolved_provider"] == "marketdata"
    assert summary["covered_required_tickers"] == ["TSLA"]
    assert summary["missing_tickers"] == []
    assert summary["auto_would_reuse"] is False
    assert summary["provider_api_call_expected"] is True


def test_run_event_fetch_auto_requires_same_selected_provider(tmp_path, monkeypatch) -> None:
    calls: list[str] = []

    def fake_provider(name: str):
        calls.append(name)
        return FakeEventProvider()

    monkeypatch.setattr("opx_chain.event_data.get_data_provider_by_name", fake_provider)

    first = run_event_fetch(
        provider="same_as_chain",
        chain_provider="yfinance",
        fetch_mode="fetch_latest",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )
    summary = summarize_latest_event_data(
        provider="yfinance",
        chain_provider="marketdata",
        tickers=("TSLA",),
        trading_date=date(2026, 6, 1),
        base_dir=tmp_path,
    )
    second = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="auto",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 5, tzinfo=timezone.utc),
    )

    assert first.provider == "same_as_chain"
    assert first.resolved_provider == "yfinance"
    assert summary["status"] == "provider_mismatch"
    assert summary["retained_provider"] == "same_as_chain"
    assert summary["retained_resolved_provider"] == "yfinance"
    assert calls == ["yfinance", "yfinance"]
    assert second.reused is False
    assert second.snapshot_id != first.snapshot_id


def test_malformed_provider_earnings_date_is_invalid_payload(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "opx_chain.event_data.get_data_provider_by_name",
        lambda _name: MalformedDateProvider(earnings="not-a-date"),
    )
    result = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="fetch_latest",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )
    df = pd.DataFrame([{"underlying_symbol": "TSLA"}])
    overlaid = overlay_event_snapshot(df, result, trading_date=date(2026, 6, 1))
    summary = summarize_latest_event_data(
        provider="yfinance",
        chain_provider="marketdata",
        tickers=("TSLA",),
        trading_date=date(2026, 6, 1),
        base_dir=tmp_path,
    )

    assert result.status == "invalid_payload"
    assert result.payload["status_counts"] == {"invalid_payload": 1}
    assert result.payload["tickers_failed"] == ["TSLA"]
    assert result.payload["canonical_events"] == []
    assert result.payload["records"][0]["provider_status"] == "invalid_payload"
    assert result.payload["records"][0]["provider_message"].startswith("invalid_payload:")
    assert result.payload["records"][0]["event_fields"]["next_earnings_date"] is None
    assert summary["reusable"] is False
    assert summary["freshness_label"] == "MISSING"
    assert overlaid.loc[0, "next_earnings_date"] is None
    assert overlaid.loc[0, "event_data_status"] == "invalid_payload"


def test_malformed_provider_ex_dividend_date_is_invalid_payload(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "opx_chain.event_data.get_data_provider_by_name",
        lambda _name: MalformedDateProvider(earnings=None, ex_div="bad-date"),
    )
    result = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="fetch_latest",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )

    assert result.status == "invalid_payload"
    assert result.payload["canonical_events"] == []
    assert result.payload["records"][0]["event_fields"]["next_ex_div_date"] is None


def test_overlay_event_snapshot_recomputes_event_flags(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "opx_chain.event_data.get_data_provider_by_name",
        lambda _name: FakeEventProvider(),
    )
    result = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="fetch_latest",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )
    df = pd.DataFrame(
        [
            {
                "underlying_symbol": "TSLA",
                "expiration_date": "2026-06-12",
                "days_to_expiration": 11,
            }
        ]
    )

    overlaid = overlay_event_snapshot(df, result, trading_date=date(2026, 6, 1))

    assert overlaid.loc[0, "next_earnings_date"] == "2026-06-05"
    assert overlaid.loc[0, "days_to_earnings"] == 4
    assert bool(overlaid.loc[0, "earnings_within_5d"]) is True
    assert overlaid.loc[0, "next_ex_div_date"] == "2026-06-03"
    assert overlaid.loc[0, "days_to_ex_div"] == 2
    assert bool(overlaid.loc[0, "ex_div_within_3d"]) is True
    assert overlaid.loc[0, "event_data_provider"] == "yfinance"
    assert overlaid.loc[0, "event_data_snapshot_id"] == result.snapshot_id
    assert overlaid.loc[0, "event_data_status"] == "ready"


def test_clear_event_columns_marks_disabled() -> None:
    df = pd.DataFrame(
        [
            {
                "underlying_symbol": "TSLA",
                "next_earnings_date": "2026-06-05",
                "dividend_amount": 0.25,
                "event_data_status": "ready",
                "event_data_provider": "yfinance",
            }
        ]
    )

    cleared = clear_event_columns(df)

    assert cleared.loc[0, "next_earnings_date"] is None
    assert np.isnan(cleared.loc[0, "dividend_amount"])
    assert cleared.loc[0, "event_data_status"] == "disabled"
    assert cleared.loc[0, "event_data_provider"] is None


def test_clear_event_columns_creates_missing_canonical_fields() -> None:
    df = pd.DataFrame([{"underlying_symbol": "TSLA"}])

    cleared = clear_event_columns(df)

    assert "next_earnings_date" in cleared.columns
    assert "next_ex_div_date" in cleared.columns
    assert "event_risk_score" in cleared.columns
    assert "event_data_status" in cleared.columns
    assert cleared.loc[0, "event_data_status"] == "disabled"
    assert cleared.loc[0, "next_earnings_date"] is None


def test_summarize_latest_event_data_reports_reuse(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "opx_chain.event_data.get_data_provider_by_name",
        lambda _name: FakeEventProvider(),
    )
    config = replace(get_runtime_config(), today=date(2026, 6, 1))
    previous = get_runtime_config()
    try:
        set_runtime_config_override(config)
        run_event_fetch(
            provider="yfinance",
            chain_provider="marketdata",
            fetch_mode="fetch_latest",
            trading_date=date(2026, 6, 1),
            tickers=("TSLA",),
            base_dir=tmp_path,
            now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
        )
        summary = summarize_latest_event_data(
            provider="yfinance",
            chain_provider="marketdata",
            tickers=("TSLA",),
            trading_date=date(2026, 6, 1),
            base_dir=tmp_path,
        )
    finally:
        set_runtime_config_override(previous)

    assert summary["available"] is True
    assert summary["reusable"] is True
    assert summary["freshness_label"] == "CURRENT_TRADING_DAY"
    assert summary["status"] == "ready"
    assert summary["ticker_universe_source"] == "caller_supplied_tickers"
    assert summary["provider_api_call_expected"] is False


def test_event_data_retained_lookup_orders_by_payload_fetched_at(
    tmp_path,
    monkeypatch,
) -> None:
    snapshot_dir = event_data_snapshot_dir(tmp_path)
    snapshot_dir.mkdir(parents=True)

    def write_snapshot(snapshot_id: str, fetched_at: str, mtime: float) -> None:
        path = snapshot_dir / f"{snapshot_id}.json"
        path.write_text(
            json.dumps(
                {
                    "artifact_type": "event_data_snapshot",
                    "schema_version": 1,
                    "event_snapshot_id": snapshot_id,
                    "provider": "yfinance",
                    "resolved_provider": "yfinance",
                    "status": "ready",
                    "fetched_at": fetched_at,
                    "trading_date": "2026-06-01",
                    "freshness_policy": "trading_day",
                    "fresh_through_trading_date": "2026-06-01",
                    "ticker_universe_source": "caller_supplied_tickers",
                    "tickers_requested": ["TSLA"],
                    "tickers_succeeded": ["TSLA"],
                    "tickers_failed": [],
                    "tickers_no_known_event": [],
                    "status_counts": {"ready": 1},
                    "records": [
                        {
                            "ticker": "TSLA",
                            "provider_status": "ready",
                            "next_earnings_date": "2026-06-05",
                        }
                    ],
                    "canonical_events": [],
                }
            ),
            encoding="utf-8",
        )
        os.utime(path, (mtime, mtime))

    write_snapshot("older-payload-newer-mtime", "2026-06-01T14:00:00Z", 200.0)
    write_snapshot("newer-payload-older-mtime", "2026-06-01T15:00:00Z", 100.0)

    summary = summarize_latest_event_data(
        provider="yfinance",
        chain_provider="marketdata",
        tickers=("TSLA",),
        trading_date=date(2026, 6, 1),
        base_dir=tmp_path,
    )

    calls: list[str] = []

    def fake_provider(name: str):
        calls.append(name)
        return FakeEventProvider()

    monkeypatch.setattr("opx_chain.event_data.get_data_provider_by_name", fake_provider)
    result = run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="auto",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 16, 0, tzinfo=timezone.utc),
    )

    assert summary["event_snapshot_id"] == "newer-payload-older-mtime"
    assert summary["fetched_at"] == "2026-06-01T15:00:00Z"
    assert summary["auto_would_reuse"] is True
    assert result.reused is True
    assert result.snapshot_id == "newer-payload-older-mtime"
    assert result.fetched_at == "2026-06-01T15:00:00Z"
    assert not calls


def test_summarize_latest_event_data_reports_stale_retained_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "opx_chain.event_data.get_data_provider_by_name",
        lambda _name: FakeEventProvider(),
    )
    run_event_fetch(
        provider="yfinance",
        chain_provider="marketdata",
        fetch_mode="fetch_latest",
        trading_date=date(2026, 6, 1),
        tickers=("TSLA",),
        ticker_universe_source="new_run_portfolio_and_ticker_intents",
        base_dir=tmp_path,
        now=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
    )

    summary = summarize_latest_event_data(
        provider="yfinance",
        chain_provider="marketdata",
        tickers=("TSLA",),
        trading_date=date(2026, 6, 2),
        base_dir=tmp_path,
    )

    assert summary["available"] is True
    assert summary["reusable"] is False
    assert summary["status"] == "stale"
    assert summary["freshness_label"] == "STALE"
    assert summary["event_snapshot_id"]
    assert summary["snapshot_trading_date"] == "2026-06-01"
    assert summary["fresh_through_trading_date"] == "2026-06-01"
    assert summary["snapshot_age_days"] == 1
    assert summary["ticker_universe_source"] == "new_run_portfolio_and_ticker_intents"
    assert summary["auto_would_reuse"] is False
    assert summary["provider_api_call_expected"] is True


def test_summarize_latest_event_data_reports_missing_impact_fields(tmp_path) -> None:
    summary = summarize_latest_event_data(
        provider="yfinance",
        chain_provider="marketdata",
        tickers=("TSLA",),
        trading_date=date(2026, 6, 1),
        base_dir=tmp_path,
    )

    assert summary["available"] is False
    assert summary["status"] == "missing"
    assert summary["auto_would_reuse"] is False
    assert summary["provider_api_call_expected"] is True
    assert summary["covered_required_tickers"] == []
    assert summary["missing_tickers"] == ["TSLA"]
    assert summary["record_count"] == 0


def test_summarize_latest_event_data_rejects_datetime_trading_date(tmp_path) -> None:
    with pytest.raises(ValueError, match="trading_date must be a date"):
        summarize_latest_event_data(
            provider="yfinance",
            chain_provider="marketdata",
            tickers=("TSLA",),
            trading_date=datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc),
            base_dir=tmp_path,
        )


def test_summarize_latest_event_data_reports_not_supported_impact_fields(tmp_path) -> None:
    summary = summarize_latest_event_data(
        provider="same_as_chain",
        chain_provider="massive",
        tickers=("TSLA",),
        trading_date=date(2026, 6, 1),
        base_dir=tmp_path,
    )

    assert summary["available"] is False
    assert summary["status"] == "not_supported"
    assert summary["provider"] == "same_as_chain"
    assert summary["resolved_provider"] == "massive"
    assert summary["auto_would_reuse"] is False
    assert summary["provider_api_call_expected"] is False
    assert summary["covered_required_tickers"] == []
    assert summary["missing_tickers"] == ["TSLA"]
