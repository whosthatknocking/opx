"""Public volatility feature helpers for downstream advisory consumers."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from math import sqrt
from typing import Any

import numpy as np
import pandas as pd

from opx_chain.metrics import add_iv_state_level, add_iv_state_term
from opx_chain.price_context import normalize_price_history_frame
from opx_chain.price_history import PriceHistoryStore
from opx_chain.utils import finite_float_or_none


VOLATILITY_FEATURE_SCHEMA_VERSION = 1
VOLATILITY_FEATURE_METHOD = "vrp_lite_features_v1"
PRICE_VOL_METHOD = "close_to_close_rv_v1"
IV_FEATURE_METHOD = "current_chain_with_optional_history_v1"
MIN_IV_HISTORY_OBSERVATIONS = 20

SOURCE_READY = "READY"
SOURCE_PARTIAL = "PARTIAL"
SOURCE_INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
SOURCE_STALE = "STALE"
SOURCE_MISSING = "MISSING"
SOURCE_ERROR = "ERROR"

RV_WINDOWS: tuple[int, ...] = (3, 5, 10)
DTE_BUCKETS: tuple[tuple[str, int | None, int | None], ...] = (
    ("0_7", 0, 7),
    ("8_14", 8, 14),
    ("15_30", 15, 30),
    ("31_45", 31, 45),
    ("46_90", 46, 90),
    ("91_PLUS", 91, None),
)


def _date_value(value: Any) -> date | None:
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return pd.Timestamp(value).date()
    except (TypeError, ValueError):
        return None


def _date_to_iso(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _rounded(value: Any, digits: int = 6) -> float | None:
    resolved = finite_float_or_none(value)
    if resolved is None:
        return None
    return round(float(resolved), digits)


def _percentile_rank(values: pd.Series, current: float | None) -> float | None:
    current_value = finite_float_or_none(current)
    if current_value is None:
        return None
    clean = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return None
    return round(float((clean <= current_value).mean() * 100.0), 2)


def dte_bucket(days_to_expiration: Any) -> str | None:
    """Return the public DTE bucket label for an option row."""
    dte = finite_float_or_none(days_to_expiration)
    if dte is None:
        return None
    dte_int = int(round(dte))
    for label, lower, upper in DTE_BUCKETS:
        if lower is not None and dte_int < lower:
            continue
        if upper is not None and dte_int > upper:
            continue
        return label
    return None


def _ticker_filter(frame: pd.DataFrame, ticker: str) -> pd.Series:
    ticker_key = ticker.upper().strip()
    for column in ("underlying_symbol", "ticker", "symbol"):
        if column in frame.columns:
            return frame[column].astype(str).str.upper().str.strip() == ticker_key
    return pd.Series([True] * len(frame), index=frame.index)


def _representative_iv(frame: pd.DataFrame) -> float | None:
    if "implied_volatility" not in frame.columns:
        return None
    valid = pd.to_numeric(frame["implied_volatility"], errors="coerce").replace(
        [np.inf, -np.inf], np.nan
    )
    usable = frame.loc[valid > 0].copy()
    if usable.empty:
        return None

    if {"expiration_date", "strike_distance_pct"}.issubset(usable.columns):
        for expiration in sorted(usable["expiration_date"].dropna().unique()):
            exp_rows = usable[usable["expiration_date"] == expiration].copy()
            distances = pd.to_numeric(exp_rows["strike_distance_pct"], errors="coerce").abs()
            distances = distances.replace([np.inf, -np.inf], np.nan)
            if distances.dropna().empty:
                continue
            return _rounded(exp_rows.loc[distances.idxmin(), "implied_volatility"])

    return _rounded(valid[valid > 0].median())


def _safe_first(frame: pd.DataFrame, column: str) -> str | None:
    if column not in frame.columns:
        return None
    values = frame[column].dropna()
    if values.empty:
        return None
    value = str(values.iloc[0]).strip()
    return value or None


def _with_iv_state_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if "iv_state_level" not in result.columns:
        result = add_iv_state_level(result)
    if "iv_state_term" not in result.columns:
        result = add_iv_state_term(result)
    return result


def _historical_iv_frame(
    history: pd.DataFrame,
    ticker: str,
    *,
    as_of: date | None = None,
    lookback_days: int | None = None,
) -> pd.DataFrame:
    if not isinstance(history, pd.DataFrame) or history.empty:
        return pd.DataFrame()
    frame = history.copy()
    frame = frame.loc[_ticker_filter(frame, ticker)].copy()
    if frame.empty:
        return frame
    if "dte_bucket" not in frame.columns and "days_to_expiration" in frame.columns:
        frame["dte_bucket"] = frame["days_to_expiration"].map(dte_bucket)
    for date_column in ("observation_date", "date"):
        if as_of is None or date_column not in frame.columns:
            continue
        dates = pd.to_datetime(frame[date_column], utc=True, errors="coerce").dt.date
        frame = frame.loc[dates <= as_of].copy()
        if lookback_days is not None:
            start_date = as_of - timedelta(days=max(int(lookback_days), 0))
            frame = frame.loc[dates.loc[frame.index] >= start_date].copy()
        break
    iv_column = None
    for candidate in ("representative_iv", "median_iv", "implied_volatility", "iv"):
        if candidate in frame.columns:
            iv_column = candidate
            break
    if iv_column is None:
        return pd.DataFrame()
    frame["representative_iv"] = pd.to_numeric(frame[iv_column], errors="coerce")
    frame = frame.replace([np.inf, -np.inf], np.nan).dropna(subset=["representative_iv"])
    frame = frame.loc[frame["representative_iv"] > 0]
    return frame


def _ticker_wide_iv_history(history: pd.DataFrame) -> pd.DataFrame:
    if "dte_bucket" not in history.columns:
        return history
    wide_rows = history[history["dte_bucket"] == "ALL"]
    return wide_rows if not wide_rows.empty else history


def build_iv_features(  # pylint: disable=too-many-arguments,too-many-locals
    chain: pd.DataFrame,
    *,
    ticker: str,
    as_of: date | None = None,
    iv_history: pd.DataFrame | None = None,
    iv_history_source_method: str | None = None,
    iv_lookback_days: int = 365,
    min_iv_history_observations: int = MIN_IV_HISTORY_OBSERVATIONS,
) -> dict[str, Any]:
    """Build ticker-wide and DTE-bucket IV features from current chain data.

    `iv_history` is optional because durable historical-IV storage may be
    populated later. When it is absent, this helper returns current-chain
    observations and explicitly leaves percentile fields blank rather than
    pretending current cross-section rank is a historical percentile.
    """
    ticker_key = ticker.upper().strip()
    if not isinstance(chain, pd.DataFrame) or chain.empty:
        return {
            "ticker": ticker_key,
            "as_of": _date_to_iso(as_of),
            "source_status": SOURCE_MISSING,
            "unknown_reason": "missing_option_chain",
            "method": IV_FEATURE_METHOD,
            "representative_iv": None,
            "iv_percentile_1y": None,
            "iv_source_method": "unavailable",
            "iv_history_observation_count": 0,
            "iv_state_level": "UNKNOWN",
            "iv_state_term": "UNKNOWN",
            "dte_buckets": {},
        }

    ticker_frame = chain.loc[_ticker_filter(chain, ticker_key)].copy()
    if ticker_frame.empty:
        return {
            "ticker": ticker_key,
            "as_of": _date_to_iso(as_of),
            "source_status": SOURCE_MISSING,
            "unknown_reason": "ticker_not_in_option_chain",
            "method": IV_FEATURE_METHOD,
            "representative_iv": None,
            "iv_percentile_1y": None,
            "iv_source_method": "unavailable",
            "iv_history_observation_count": 0,
            "iv_state_level": "UNKNOWN",
            "iv_state_term": "UNKNOWN",
            "dte_buckets": {},
        }

    ticker_frame = _with_iv_state_columns(ticker_frame)
    representative_iv = _representative_iv(ticker_frame)
    valid_iv = pd.to_numeric(
        ticker_frame.get("implied_volatility", pd.Series(dtype=float)),
        errors="coerce",
    ).replace([np.inf, -np.inf], np.nan)
    current_observation_count = int((valid_iv > 0).sum())
    history = (
        _historical_iv_frame(
            iv_history,
            ticker_key,
            as_of=as_of,
            lookback_days=iv_lookback_days,
        )
        if iv_history is not None
        else pd.DataFrame()
    )
    wide_history = _ticker_wide_iv_history(history)
    history_iv = wide_history.get("representative_iv", pd.Series(dtype=float))
    history_observation_count = int(len(history_iv.dropna()))
    iv_percentile = _percentile_rank(history_iv, representative_iv)

    if "days_to_expiration" in ticker_frame.columns:
        ticker_frame["dte_bucket"] = ticker_frame["days_to_expiration"].map(dte_bucket)
    else:
        ticker_frame["dte_bucket"] = None

    dte_buckets: dict[str, dict[str, Any]] = {}
    for bucket, _, _ in DTE_BUCKETS:
        current_rows = ticker_frame[ticker_frame["dte_bucket"] == bucket]
        current_iv = pd.to_numeric(
            current_rows.get("implied_volatility", pd.Series(dtype=float)),
            errors="coerce",
        ).replace([np.inf, -np.inf], np.nan)
        current_iv = current_iv[current_iv > 0]
        current_median = _rounded(current_iv.median())
        history_rows = (
            history[history.get("dte_bucket") == bucket]
            if "dte_bucket" in history.columns
            else pd.DataFrame()
        )
        history_values = history_rows.get("representative_iv", pd.Series(dtype=float))
        history_observations = int(len(history_values.dropna()))
        dte_buckets[bucket] = {
            "current_observation_count": int(len(current_iv)),
            "history_observation_count": history_observations,
            "representative_iv": current_median,
            "iv_percentile": (
                _percentile_rank(history_values, current_median)
                if history_observations >= int(min_iv_history_observations)
                else None
            ),
        }

    source_status = SOURCE_PARTIAL
    unknown_reason = "missing_iv_history"
    iv_source_method = "current_chain_proxy"
    if not history.empty:
        has_enough_history = history_observation_count >= int(min_iv_history_observations)
        source_status = (
            SOURCE_READY
            if iv_percentile is not None and has_enough_history
            else SOURCE_PARTIAL
        )
        unknown_reason = (
            None
            if iv_percentile is not None and has_enough_history
            else "insufficient_iv_history"
        )
        iv_source_method = iv_history_source_method or "current_chain_plus_history"
    elif representative_iv is None:
        source_status = SOURCE_MISSING
        unknown_reason = "missing_current_iv"
        iv_source_method = "unavailable"

    return {
        "ticker": ticker_key,
        "as_of": _date_to_iso(as_of),
        "source_status": source_status,
        "unknown_reason": unknown_reason,
        "method": IV_FEATURE_METHOD,
        "representative_iv": representative_iv,
        "iv_percentile_1y": iv_percentile,
        "iv_source_method": iv_source_method,
        "iv_history_observation_count": history_observation_count,
        "current_chain_observation_count": current_observation_count,
        "iv_state_level": _safe_first(ticker_frame, "iv_state_level") or "UNKNOWN",
        "iv_state_term": _safe_first(ticker_frame, "iv_state_term") or "UNKNOWN",
        "dte_buckets": dte_buckets,
    }


def _rv_series(log_returns: pd.Series, window: int) -> pd.Series:
    return log_returns.rolling(window).apply(
        lambda values: sqrt(float(np.square(values).sum())),
        raw=True,
    )


def build_price_volatility_features(  # pylint: disable=too-many-arguments,too-many-locals
    history: pd.DataFrame,
    *,
    ticker: str,
    provider: str | None = None,
    as_of: date | None = None,
    min_context_sessions: int = 90,
    max_stale_days: int = 7,
) -> dict[str, Any]:
    """Build close-to-close realized-volatility features from daily OHLCV bars."""
    ticker_key = ticker.upper().strip()
    normalized = normalize_price_history_frame(history)
    if as_of is not None and not normalized.empty:
        normalized = normalized[
            pd.to_datetime(normalized["date"], utc=True).dt.date <= as_of
        ]
    normalized = normalized.sort_values("date").drop_duplicates(
        subset=["date"],
        keep="last",
    )
    close = pd.to_numeric(
        normalized.get("close", pd.Series(dtype=float)),
        errors="coerce",
    )
    close = close.replace([np.inf, -np.inf], np.nan)
    valid = normalized.loc[close > 0].copy()
    valid["close"] = close[close > 0]

    base: dict[str, Any] = {
        "ticker": ticker_key,
        "provider": provider,
        "as_of": _date_to_iso(as_of),
        "source_status": SOURCE_MISSING,
        "unknown_reason": "missing_price_history",
        "method": PRICE_VOL_METHOD,
        "newest_completed_session": None,
        "price_history_lookback_sessions": int(len(valid)),
        "stale_session_days": None,
    }
    for window in RV_WINDOWS:
        base[f"rv_{window}d"] = None
        base[f"rv_{window}d_percentile_1y"] = None

    if valid.empty or len(valid) < 2:
        return base

    newest_session = _date_value(valid["date"].iloc[-1])
    base["newest_completed_session"] = _date_to_iso(newest_session)
    if as_of is not None and newest_session is not None:
        base["stale_session_days"] = max((as_of - newest_session).days, 0)

    log_returns = np.log(valid["close"] / valid["close"].shift(1)).replace(
        [np.inf, -np.inf], np.nan
    ).dropna()
    for window in RV_WINDOWS:
        series = _rv_series(log_returns, window).dropna()
        current = None if series.empty else float(series.iloc[-1])
        base[f"rv_{window}d"] = _rounded(current)
        base[f"rv_{window}d_percentile_1y"] = _percentile_rank(series, current)

    if len(valid) < max(RV_WINDOWS) + 1:
        base["source_status"] = SOURCE_INSUFFICIENT_HISTORY
        base["unknown_reason"] = "insufficient_returns_for_rv_windows"
    elif len(valid) < min_context_sessions:
        base["source_status"] = SOURCE_INSUFFICIENT_HISTORY
        base["unknown_reason"] = "insufficient_context_history"
    elif (
        base["stale_session_days"] is not None
        and int(base["stale_session_days"]) > max_stale_days
    ):
        base["source_status"] = SOURCE_STALE
        base["unknown_reason"] = "stale_price_history"
    else:
        base["source_status"] = SOURCE_READY
        base["unknown_reason"] = None
    return base


def load_price_volatility_features(  # pylint: disable=too-many-arguments
    store: PriceHistoryStore,
    *,
    provider: str,
    ticker: str,
    as_of: date,
    lookback_days: int = 260,
    min_context_sessions: int = 90,
    max_stale_days: int = 7,
) -> dict[str, Any]:
    """Load stored daily bars and derive realized-volatility features."""
    history = store.load_recent_bars(
        provider=provider,
        ticker=ticker,
        lookback_days=lookback_days,
        end_date=as_of,
    )
    return build_price_volatility_features(
        history,
        ticker=ticker,
        provider=provider,
        as_of=as_of,
        min_context_sessions=min_context_sessions,
        max_stale_days=max_stale_days,
    )


def build_ticker_volatility_features(  # pylint: disable=too-many-arguments,too-many-locals
    *,
    ticker: str,
    chain: pd.DataFrame,
    price_history: pd.DataFrame | None = None,
    price_history_store: PriceHistoryStore | None = None,
    provider: str | None = None,
    as_of: date | None = None,
    iv_history: pd.DataFrame | None = None,
    iv_history_store: Any | None = None,
    iv_lookback_days: int = 365,
    min_iv_history_observations: int = MIN_IV_HISTORY_OBSERVATIONS,
    price_lookback_days: int = 260,
    min_context_sessions: int = 90,
    max_stale_days: int = 7,
) -> dict[str, Any]:
    """Build a JSON-safe ticker feature snapshot for volatility advisory use."""
    ticker_key = ticker.upper().strip()
    if price_history is None and price_history_store is not None and provider and as_of:
        price_features = load_price_volatility_features(
            price_history_store,
            provider=provider,
            ticker=ticker_key,
            as_of=as_of,
            lookback_days=price_lookback_days,
            min_context_sessions=min_context_sessions,
            max_stale_days=max_stale_days,
        )
    else:
        price_features = build_price_volatility_features(
            price_history if price_history is not None else pd.DataFrame(),
            ticker=ticker_key,
            provider=provider,
            as_of=as_of,
            min_context_sessions=min_context_sessions,
            max_stale_days=max_stale_days,
        )

    iv_history_source_method = None
    if iv_history is None and iv_history_store is not None and provider and as_of:
        iv_history = iv_history_store.load_history(
            provider=provider,
            ticker=ticker_key,
            lookback_days=iv_lookback_days,
            end_date=as_of,
        )
        iv_history_source_method = "durable_iv_history"

    iv_features = build_iv_features(
        chain,
        ticker=ticker_key,
        as_of=as_of,
        iv_history=iv_history,
        iv_history_source_method=iv_history_source_method,
        iv_lookback_days=iv_lookback_days,
        min_iv_history_observations=min_iv_history_observations,
    )
    source_statuses = {
        str(price_features.get("source_status") or SOURCE_MISSING),
        str(iv_features.get("source_status") or SOURCE_MISSING),
    }
    if SOURCE_ERROR in source_statuses:
        source_status = SOURCE_ERROR
    elif source_statuses == {SOURCE_READY}:
        source_status = SOURCE_READY
    elif source_statuses <= {SOURCE_MISSING}:
        source_status = SOURCE_MISSING
    else:
        source_status = SOURCE_PARTIAL

    return {
        "schema_version": VOLATILITY_FEATURE_SCHEMA_VERSION,
        "method": VOLATILITY_FEATURE_METHOD,
        "ticker": ticker_key,
        "as_of": _date_to_iso(as_of),
        "provider": provider,
        "source_status": source_status,
        "price": price_features,
        "iv": iv_features,
    }


__all__ = [
    "DTE_BUCKETS",
    "IV_FEATURE_METHOD",
    "MIN_IV_HISTORY_OBSERVATIONS",
    "PRICE_VOL_METHOD",
    "RV_WINDOWS",
    "SOURCE_ERROR",
    "SOURCE_INSUFFICIENT_HISTORY",
    "SOURCE_MISSING",
    "SOURCE_PARTIAL",
    "SOURCE_READY",
    "SOURCE_STALE",
    "VOLATILITY_FEATURE_METHOD",
    "VOLATILITY_FEATURE_SCHEMA_VERSION",
    "build_iv_features",
    "build_price_volatility_features",
    "build_ticker_volatility_features",
    "dte_bucket",
    "load_price_volatility_features",
]
