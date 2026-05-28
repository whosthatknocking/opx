"""Daily-OHLCV price-context calculations for standalone artifacts."""

from __future__ import annotations

from datetime import date
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd

from opx_chain.utils import finite_float_or_none, finite_numeric_series

PRICE_CONTEXT_FIELDS: tuple[str, ...] = (
    "support_1",
    "support_2",
    "resistance_1",
    "resistance_2",
    "20d_high",
    "20d_low",
    "50dma",
    "200dma",
    "vwap",
    "volume_profile_high_volume_node",
    "gap_fill_level",
    "pre_earnings_move_pct",
)
TECHNICAL_CONTEXT_NUMERIC_FIELDS: tuple[str, ...] = (
    "rsi_14",
    "ema_20",
    "ema_50",
    "price_vs_ema50_pct",
)
TECHNICAL_CONTEXT_FIELDS: tuple[str, ...] = (
    *TECHNICAL_CONTEXT_NUMERIC_FIELDS,
    "ema_cloud_state",
)
PRICE_CONTEXT_METADATA_FIELDS: tuple[str, ...] = (
    "price_context_as_of",
    "price_context_age_days",
    "price_context_source",
    "price_context_lookback_trading_days",
    "price_context_calculation_method",
    "price_context_staleness_status",
)
PRICE_CONTEXT_RECORD_FIELDS: tuple[str, ...] = (
    *PRICE_CONTEXT_FIELDS,
    *TECHNICAL_CONTEXT_FIELDS,
    *PRICE_CONTEXT_METADATA_FIELDS,
)
PRICE_CONTEXT_CALCULATION_METHOD = "daily_ohlcv_v1"
PRICE_CONTEXT_SCHEMA_VERSION = 2

EMA_CLOUD_BULLISH = "BULLISH"
EMA_CLOUD_BEARISH = "BEARISH"
EMA_CLOUD_TRANSITION = "TRANSITION"
EMA_CLOUD_UNKNOWN = "UNKNOWN"


class PriceContextStatus(str, Enum):
    """Status vocabulary for the price_context_staleness_status artifact field."""

    FRESH = "FRESH"
    STALE = "STALE"
    MISSING = "MISSING"
    ERROR = "ERROR"


def blank_price_context(
    *,
    source: str | None = None,
    status: PriceContextStatus | str = PriceContextStatus.MISSING,
    method: str = PRICE_CONTEXT_CALCULATION_METHOD,
) -> dict[str, Any]:
    """Return a JSON-safe blank price-context payload."""
    status_value = status.value if isinstance(status, PriceContextStatus) else str(status)
    return {
        **{field: None for field in PRICE_CONTEXT_FIELDS},
        **{field: None for field in TECHNICAL_CONTEXT_NUMERIC_FIELDS},
        "ema_cloud_state": EMA_CLOUD_UNKNOWN,
        "price_context_as_of": None,
        "price_context_age_days": None,
        "price_context_source": source,
        "price_context_lookback_trading_days": 0,
        "price_context_calculation_method": method,
        "price_context_staleness_status": status_value,
    }


def _positive_float(value: Any) -> float | None:
    resolved = finite_float_or_none(value)
    if resolved is None or resolved <= 0:
        return None
    return resolved


def _rounded(value: Any) -> float | None:
    resolved = _positive_float(value)
    return None if resolved is None else round(resolved, 6)


def _rounded_any(value: Any) -> float | None:
    resolved = finite_float_or_none(value)
    return None if resolved is None else round(resolved, 6)


def _column_by_alias(frame: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    normalized = {str(column).strip().lower(): column for column in frame.columns}
    for alias in aliases:
        if alias in normalized:
            return normalized[alias]
    return None


def _normalize_dates(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(series, utc=True, errors="coerce")
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        max_value = numeric.max()
        if max_value > 100_000_000_000:
            return pd.to_datetime(numeric, unit="ms", utc=True, errors="coerce")
        if max_value > 100_000_000:
            return pd.to_datetime(numeric, unit="s", utc=True, errors="coerce")
    return pd.to_datetime(series, utc=True, errors="coerce")


def _normalize_history_frame(history: pd.DataFrame) -> pd.DataFrame:
    """Normalize provider-specific OHLCV history into date/open/high/low/close/volume."""
    if not isinstance(history, pd.DataFrame) or history.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    frame = history.copy()
    date_column = _column_by_alias(frame, ("date", "datetime", "timestamp", "time", "t"))
    if date_column is None:
        frame = frame.reset_index()
        date_column = _column_by_alias(
            frame,
            ("date", "datetime", "timestamp", "time", "t", "index"),
        )

    column_map = {
        "date": date_column,
        "open": _column_by_alias(frame, ("open", "o")),
        "high": _column_by_alias(frame, ("high", "h")),
        "low": _column_by_alias(frame, ("low", "l")),
        "close": _column_by_alias(frame, ("close", "c", "adj close", "adj_close")),
        "volume": _column_by_alias(frame, ("volume", "v")),
    }
    if any(column_map[key] is None for key in ("date", "high", "low", "close")):
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    normalized = pd.DataFrame(
        {
            "date": _normalize_dates(frame[column_map["date"]]),
            "open": finite_numeric_series(frame[column_map["open"]])
            if column_map["open"] is not None else np.nan,
            "high": finite_numeric_series(frame[column_map["high"]]),
            "low": finite_numeric_series(frame[column_map["low"]]),
            "close": finite_numeric_series(frame[column_map["close"]]),
            "volume": finite_numeric_series(frame[column_map["volume"]])
            if column_map["volume"] is not None else np.nan,
        }
    )
    normalized = normalized.dropna(subset=["date", "high", "low", "close"])
    normalized = normalized.loc[
        (normalized["high"] > 0)
        & (normalized["low"] > 0)
        & (normalized["close"] > 0)
        & (normalized["high"] >= normalized["low"])
        & (normalized["close"] >= normalized["low"])
        & (normalized["close"] <= normalized["high"])
    ]
    return normalized.sort_values("date").drop_duplicates(subset=["date"], keep="last")


def normalize_price_history_frame(history: pd.DataFrame) -> pd.DataFrame:
    """Normalize provider-specific daily OHLCV history for storage and calculations."""
    return _normalize_history_frame(history)


def _unique_levels(levels: list[float]) -> list[float]:
    """Deduplicate nearby levels while preserving sorted priority."""
    unique: list[float] = []
    for level in levels:
        if _positive_float(level) is None:
            continue
        if any(abs(level - existing) / existing <= 0.001 for existing in unique):
            continue
        unique.append(level)
    return unique


def _levels_around_spot(  # pylint: disable=too-many-arguments
    *,
    spot: float,
    twenty_day_high: float | None,
    twenty_day_low: float | None,
    fifty_dma: float | None,
    two_hundred_dma: float | None,
    volume_node: float | None,
) -> tuple[float | None, float | None, float | None, float | None]:
    candidates = [
        value
        for value in (
            twenty_day_high,
            twenty_day_low,
            fifty_dma,
            two_hundred_dma,
            volume_node,
        )
        if _positive_float(value) is not None
    ]
    below = _unique_levels(sorted((value for value in candidates if value <= spot), reverse=True))
    above = _unique_levels(sorted(value for value in candidates if value >= spot))
    support_1 = below[0] if below else twenty_day_low
    support_2 = below[1] if len(below) > 1 else None
    resistance_1 = above[0] if above else twenty_day_high
    resistance_2 = above[1] if len(above) > 1 else None
    return support_1, support_2, resistance_1, resistance_2


def _rolling_vwap(history: pd.DataFrame, window: int = 20) -> float | None:
    recent = history.tail(window)
    if recent.empty or "volume" not in recent.columns:
        return None
    volume = finite_numeric_series(recent["volume"])
    valid = volume.notna() & (volume > 0)
    if not valid.any():
        return None
    typical = (recent["high"] + recent["low"] + recent["close"]) / 3.0
    return _rounded((typical[valid] * volume[valid]).sum() / volume[valid].sum())


def _ema(close: pd.Series, span: int) -> float | None:
    values = finite_numeric_series(close).dropna()
    if len(values) < span:
        return None
    return _rounded(values.ewm(span=span, adjust=False).mean().iloc[-1])


def _rsi_14(close: pd.Series) -> float | None:
    values = finite_numeric_series(close).dropna()
    if len(values) < 15:
        return None
    delta = values.diff().dropna().tail(14)
    if len(delta) < 14:
        return None
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = finite_float_or_none(gains.mean())
    avg_loss = finite_float_or_none(losses.mean())
    if avg_gain is None or avg_loss is None:
        return None
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return _rounded_any(100.0 - (100.0 / (1.0 + rs)))


def _ema_cloud_state(
    *,
    latest_close: float,
    ema_20: float | None,
    ema_50: float | None,
) -> str:
    if ema_20 is None or ema_50 is None:
        return EMA_CLOUD_UNKNOWN
    if ema_20 > ema_50 and latest_close > ema_50:
        return EMA_CLOUD_BULLISH
    if ema_20 < ema_50 and latest_close < ema_50:
        return EMA_CLOUD_BEARISH
    return EMA_CLOUD_TRANSITION


def _volume_node(history: pd.DataFrame, window: int = 60) -> float | None:
    recent = history.tail(window)
    if recent.empty or "volume" not in recent.columns:
        return None
    volume = finite_numeric_series(recent["volume"])
    valid = volume.notna() & (volume > 0)
    if not valid.any():
        return None
    row = recent.loc[volume[valid].idxmax()]
    return _rounded((row["high"] + row["low"] + row["close"]) / 3.0)


def _latest_unfilled_gap(history: pd.DataFrame, window: int = 60) -> float | None:
    recent = history.tail(window).reset_index(drop=True)
    if len(recent) < 2:
        return None
    for index in range(len(recent) - 1, 0, -1):
        previous = recent.iloc[index - 1]
        current = recent.iloc[index]
        subsequent = recent.iloc[index:]
        if current["low"] > previous["high"]:
            gap_level = previous["high"]
            if recent.iloc[-1]["close"] > gap_level and subsequent["low"].min() > gap_level:
                return _rounded(gap_level)
        if current["high"] < previous["low"]:
            gap_level = previous["low"]
            if recent.iloc[-1]["close"] < gap_level and subsequent["high"].max() < gap_level:
                return _rounded(gap_level)
    return None


def _age_days(as_of: pd.Timestamp, today: date) -> int | None:
    if pd.isna(as_of):
        return None
    return (today - as_of.date()).days


def compute_price_context(  # pylint: disable=too-many-locals
    history: pd.DataFrame,
    *,
    source: str,
    today: date,
    max_age_days: int,
) -> dict[str, Any]:
    """Compute optional price-context fields from daily OHLCV history.

    Stale or missing histories return metadata plus blank numeric fields. The
    option-chain fetch can therefore inform the operator without letting stale
    levels influence downstream candidate tags.
    """
    normalized = _normalize_history_frame(history)
    if normalized.empty:
        return blank_price_context(source=source)

    as_of = normalized["date"].max()
    age_days = _age_days(as_of, today)
    if age_days is None:
        return blank_price_context(source=source)
    if age_days < 0:
        context = blank_price_context(source=source, status=PriceContextStatus.ERROR)
        context.update(
            {
                "price_context_as_of": as_of.date().isoformat(),
                "price_context_age_days": age_days,
                "price_context_lookback_trading_days": int(len(normalized)),
            }
        )
        return context
    if age_days > max_age_days:
        context = blank_price_context(source=source, status=PriceContextStatus.STALE)
        context.update(
            {
                "price_context_as_of": as_of.date().isoformat(),
                "price_context_age_days": age_days,
                "price_context_lookback_trading_days": int(len(normalized)),
            }
        )
        return context

    latest_close = _positive_float(normalized.iloc[-1]["close"])
    if latest_close is None:
        return blank_price_context(source=source)

    twenty_day = normalized.tail(20)
    twenty_day_high = _rounded(twenty_day["high"].max()) if len(twenty_day) >= 2 else None
    twenty_day_low = _rounded(twenty_day["low"].min()) if len(twenty_day) >= 2 else None
    fifty_dma = _rounded(normalized["close"].tail(50).mean()) if len(normalized) >= 50 else None
    two_hundred_dma = (
        _rounded(normalized["close"].tail(200).mean()) if len(normalized) >= 200 else None
    )
    rsi_14 = _rsi_14(normalized["close"])
    ema_20 = _ema(normalized["close"], 20)
    ema_50 = _ema(normalized["close"], 50)
    price_vs_ema50_pct = (
        _rounded_any((latest_close - ema_50) / ema_50 * 100.0)
        if ema_50 is not None and ema_50 > 0
        else None
    )
    vwap = _rolling_vwap(normalized)
    volume_node = _volume_node(normalized)
    gap_fill_level = _latest_unfilled_gap(normalized)
    support_1, support_2, resistance_1, resistance_2 = _levels_around_spot(
        spot=latest_close,
        twenty_day_high=twenty_day_high,
        twenty_day_low=twenty_day_low,
        fifty_dma=fifty_dma,
        two_hundred_dma=two_hundred_dma,
        volume_node=volume_node,
    )

    return {
        "support_1": _rounded(support_1),
        "support_2": _rounded(support_2),
        "resistance_1": _rounded(resistance_1),
        "resistance_2": _rounded(resistance_2),
        "20d_high": twenty_day_high,
        "20d_low": twenty_day_low,
        "50dma": fifty_dma,
        "200dma": two_hundred_dma,
        "rsi_14": rsi_14,
        "ema_20": ema_20,
        "ema_50": ema_50,
        "ema_cloud_state": _ema_cloud_state(
            latest_close=latest_close,
            ema_20=ema_20,
            ema_50=ema_50,
        ),
        "price_vs_ema50_pct": price_vs_ema50_pct,
        "vwap": vwap,
        "volume_profile_high_volume_node": volume_node,
        "gap_fill_level": gap_fill_level,
        "pre_earnings_move_pct": None,
        "price_context_as_of": as_of.date().isoformat(),
        "price_context_age_days": age_days,
        "price_context_source": source,
        "price_context_lookback_trading_days": int(len(normalized)),
        "price_context_calculation_method": PRICE_CONTEXT_CALCULATION_METHOD,
        "price_context_staleness_status": PriceContextStatus.FRESH.value,
    }
