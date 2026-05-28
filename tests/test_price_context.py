"""Price-context calculation tests."""

import ast
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from opx_chain.price_context import (
    PRICE_CONTEXT_FIELDS,
    TECHNICAL_CONTEXT_FIELDS,
    PriceContextStatus,
    blank_price_context,
    compute_price_context,
    normalize_price_history_frame,
)


def test_price_context_status_contract_values():
    """Price-context status values are a chain-owned artifact contract."""
    assert PriceContextStatus.FRESH.value == "FRESH"
    assert PriceContextStatus.STALE.value == "STALE"
    assert PriceContextStatus.MISSING.value == "MISSING"
    assert PriceContextStatus.ERROR.value == "ERROR"


def _history(start: str = "2025-07-01", periods: int = 220) -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=periods)
    closes = [100.0 + index * 0.2 for index in range(periods)]
    return pd.DataFrame(
        {
            "Date": dates,
            "Open": [close - 0.5 for close in closes],
            "High": [close + 1.0 for close in closes],
            "Low": [close - 1.5 for close in closes],
            "Close": closes,
            "Volume": [1000 + index for index in range(periods)],
        }
    )


def test_compute_price_context_derives_daily_ohlcv_boundaries():
    """Daily OHLCV history should produce deterministic flat context fields."""
    history = _history()

    context = compute_price_context(
        history,
        source="unit",
        today=date(2026, 5, 5),
        max_age_days=7,
    )

    assert set(PRICE_CONTEXT_FIELDS).issubset(context)
    assert context["price_context_staleness_status"] == PriceContextStatus.FRESH.value
    assert context["price_context_source"] == "unit"
    assert context["price_context_lookback_trading_days"] == 220
    assert context["price_context_as_of"] == "2026-05-04"
    assert context["20d_high"] == pytest.approx(144.8)
    assert context["20d_low"] == pytest.approx(138.5)
    assert context["50dma"] == pytest.approx(138.9)
    assert context["200dma"] == pytest.approx(123.9)
    assert context["rsi_14"] == pytest.approx(100.0)
    assert context["ema_20"] == pytest.approx(141.9)
    assert context["ema_50"] == pytest.approx(138.900768)
    assert context["ema_cloud_state"] == "BULLISH"
    assert context["price_vs_ema50_pct"] == pytest.approx(3.527145)
    assert context["support_1"] == pytest.approx(143.633333)
    assert context["support_2"] == pytest.approx(138.9)
    assert context["resistance_1"] == pytest.approx(144.8)
    assert context["vwap"] > 0
    assert context["volume_profile_high_volume_node"] > 0
    assert context["pre_earnings_move_pct"] is None


def test_compute_price_context_classifies_bearish_ema_cloud():
    """EMA cloud state should be deterministic and provider-agnostic."""
    history = _history(periods=90)
    history["Close"] = list(reversed(history["Close"].tolist()))
    history["Open"] = history["Close"] + 0.5
    history["High"] = history["Close"] + 1.0
    history["Low"] = history["Close"] - 1.5

    context = compute_price_context(
        history,
        source="unit",
        today=date(2025, 11, 4),
        max_age_days=7,
    )

    assert context["price_context_staleness_status"] == PriceContextStatus.FRESH.value
    assert context["ema_20"] < context["ema_50"]
    assert context["ema_cloud_state"] == "BEARISH"
    assert context["price_vs_ema50_pct"] < 0


def test_compute_price_context_blanks_stale_numeric_fields():
    """Stale price history should inform status without exporting stale levels."""
    context = compute_price_context(
        _history(periods=20),
        source="unit",
        today=date(2026, 6, 1),
        max_age_days=7,
    )

    assert context["price_context_staleness_status"] == PriceContextStatus.STALE.value
    assert context["price_context_as_of"] == "2025-07-28"
    assert context["price_context_age_days"] > 7
    assert all(context[field] is None for field in PRICE_CONTEXT_FIELDS)
    assert all(
        context[field] is None
        for field in TECHNICAL_CONTEXT_FIELDS
        if field != "ema_cloud_state"
    )
    assert context["ema_cloud_state"] == "UNKNOWN"


def test_compute_price_context_blanks_future_daily_history():
    """Future-dated history should be suspect instead of exported as fresh context."""
    context = compute_price_context(
        _history(start="2026-05-06", periods=3),
        source="unit",
        today=date(2026, 5, 5),
        max_age_days=7,
    )

    assert context["price_context_staleness_status"] == PriceContextStatus.ERROR.value
    assert context["price_context_as_of"] == "2026-05-08"
    assert context["price_context_age_days"] == -3
    assert context["price_context_lookback_trading_days"] == 3
    assert all(context[field] is None for field in PRICE_CONTEXT_FIELDS)
    assert all(
        context[field] is None
        for field in TECHNICAL_CONTEXT_FIELDS
        if field != "ema_cloud_state"
    )
    assert context["ema_cloud_state"] == "UNKNOWN"


def test_compute_price_context_returns_blank_payload_for_missing_history():
    """Missing or malformed history should not raise."""
    context = compute_price_context(
        pd.DataFrame({"Close": [100.0]}),
        source="unit",
        today=date(2026, 5, 5),
        max_age_days=7,
    )

    assert context == blank_price_context(source="unit")


def test_price_context_normalization_drops_boolean_ohlcv_rows():
    """Boolean OHLCV values should not become one-dollar price-context levels."""
    history = _history(periods=30)
    for column in ("High", "Low", "Close", "Volume"):
        history[column] = history[column].astype(object)
        history.loc[history.index[-1], column] = True

    normalized = normalize_price_history_frame(history)
    context = compute_price_context(
        history,
        source="unit",
        today=date(2025, 8, 11),
        max_age_days=7,
    )

    assert len(normalized) == 29
    assert normalized["date"].max().date().isoformat() == "2025-08-08"
    assert context["price_context_as_of"] == "2025-08-08"
    assert context["support_1"] != 1.0
    assert context["resistance_1"] != 1.0


def test_price_context_normalization_drops_close_outside_daily_range():
    """Daily bars with close outside high/low should not produce context levels."""
    history = _history(periods=30)
    history.loc[history.index[-1], "Close"] = history.loc[history.index[-1], "High"] + 100

    normalized = normalize_price_history_frame(history)

    assert len(normalized) == 29
    assert normalized["date"].max().date().isoformat() == "2025-08-08"


def test_price_context_normalization_parses_integer_yyyymmdd_dates():
    """Compact numeric daily dates should not become 1970 nanosecond timestamps."""
    history = pd.DataFrame(
        {
            "date": [20260501, 20260504, 20260505],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "volume": [1000, 1000, 1000],
        }
    )

    normalized = normalize_price_history_frame(history)
    context = compute_price_context(
        history,
        source="unit",
        today=date(2026, 5, 5),
        max_age_days=7,
    )

    assert normalized["date"].dt.date.astype(str).tolist() == [
        "2026-05-01",
        "2026-05-04",
        "2026-05-05",
    ]
    assert context["price_context_as_of"] == "2026-05-05"
    assert context["price_context_staleness_status"] == PriceContextStatus.FRESH.value


def test_price_context_normalization_handles_mixed_iso_and_numeric_epoch_dates():
    """A numeric timestamp row should not cause valid ISO date rows to be dropped."""
    history = pd.DataFrame(
        {
            "date": ["2026-05-01", 1777852800, "2026-05-05"],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "volume": [1000, 1000, 1000],
        }
    )

    normalized = normalize_price_history_frame(history)

    assert normalized["date"].dt.date.astype(str).tolist() == [
        "2026-05-01",
        "2026-05-04",
        "2026-05-05",
    ]


def test_price_context_normalization_supports_high_resolution_epoch_dates():
    """Epoch seconds, milliseconds, microseconds, and nanoseconds should parse identically."""
    epoch_seconds = 1777939200
    history = pd.DataFrame(
        {
            "date": [
                epoch_seconds,
                epoch_seconds * 1_000,
                epoch_seconds * 1_000_000,
                epoch_seconds * 1_000_000_000,
            ],
            "high": [101.0, 102.0, 103.0, 104.0],
            "low": [99.0, 100.0, 101.0, 102.0],
            "close": [100.0, 101.0, 102.0, 103.0],
            "volume": [1000, 1000, 1000, 1000],
        }
    )

    normalized = normalize_price_history_frame(history)

    assert normalized["date"].dt.date.astype(str).tolist() == ["2026-05-05"]
    assert normalized["close"].tolist() == [103.0]


def test_blank_price_context_accepts_status_enum():
    """Callers should use the canonical enum without leaking enum objects to JSON."""
    context = blank_price_context(source="unit", status=PriceContextStatus.ERROR)

    assert context["price_context_staleness_status"] == PriceContextStatus.ERROR.value


def test_price_context_status_producers_use_status_contract():
    """Production emit sites should not bypass PriceContextStatus for status values."""
    root = Path(__file__).resolve().parents[1]
    allowed_path = root / "opx_chain" / "price_context.py"
    status_values = {status.value for status in PriceContextStatus}
    offenders: list[str] = []

    for path in (root / "opx_chain").rglob("*.py"):
        if path == allowed_path:
            continue
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.keyword)
                and node.arg == "status"
                and isinstance(node.value, ast.Constant)
                and node.value.value in status_values
            ):
                offenders.append(f"{path}:{node.lineno} inline status keyword")
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values, strict=False):
                    if (
                        isinstance(key, ast.Constant)
                        and key.value == "price_context_staleness_status"
                        and isinstance(value, ast.Constant)
                        and value.value in status_values
                    ):
                        offenders.append(
                            f"{path}:{value.lineno} inline price-context status value"
                        )

    assert not offenders
