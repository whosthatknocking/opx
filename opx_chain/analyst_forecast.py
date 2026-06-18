"""Provider-neutral analyst forecast facts."""
# pylint: disable=line-too-long,too-many-return-statements,too-many-locals,duplicate-code

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pandas as pd

from opx_chain.providers import get_data_provider_by_name
from opx_chain.providers.base import date_arg
from opx_chain.tickers import is_valid_ticker
from opx_chain.timestamps import format_utc_z_seconds
from opx_chain.utils import finite_float_or_none

ANALYST_FORECAST_SCHEMA_VERSION = 1
ANALYST_FORECAST_SUPPORTED_PROVIDERS = frozenset({"yfinance"})
ANALYST_FORECAST_SOURCE_QUALITY = {
    "yfinance": "research_fallback",
}

_RATING_KEYS = {
    "strongbuy": "strong_buy",
    "strong_buy": "strong_buy",
    "strong buy": "strong_buy",
    "buy": "buy",
    "hold": "hold",
    "sell": "sell",
    "strongsell": "strong_sell",
    "strong_sell": "strong_sell",
    "strong sell": "strong_sell",
}
_PERIOD_ORDER = ("0m", "-1m", "-2m", "-3m")


def normalize_analyst_forecast_provider(provider: str | None, *, default: str = "yfinance") -> str:
    """Return a supported analyst-forecast provider id."""
    resolved = str(provider or default).strip().lower()
    if resolved not in ANALYST_FORECAST_SUPPORTED_PROVIDERS:
        allowed = ", ".join(sorted(ANALYST_FORECAST_SUPPORTED_PROVIDERS))
        raise ValueError(f"analyst_forecast_provider must be one of: {allowed}")
    return resolved


def _normalize_tickers(tickers: list[str] | tuple[str, ...] | set[str]) -> tuple[str, ...]:
    if not isinstance(tickers, (list, tuple, set)):
        raise ValueError("analyst forecast tickers must be a list, tuple, or set")
    normalized: list[str] = []
    for raw in tickers:
        if not isinstance(raw, str):
            raise ValueError("analyst forecast ticker members must be strings")
        ticker = raw.strip().upper()
        if not ticker:
            continue
        if not is_valid_ticker(ticker):
            raise ValueError(f"invalid analyst forecast ticker: {ticker!r}")
        normalized.append(ticker)
    return tuple(dict.fromkeys(sorted(normalized)))


def _warning(code: str, message: str, *, ticker: str | None = None, severity: str = "warning") -> dict[str, Any]:
    payload: dict[str, Any] = {
        "code": code,
        "severity": severity,
        "message": message,
    }
    if ticker:
        payload["ticker"] = ticker
    return payload


def _target_value(payload: dict[str, Any], key: str) -> float | None:
    lower = {str(raw_key).strip().lower(): value for raw_key, value in payload.items()}
    parsed = finite_float_or_none(lower.get(key))
    return parsed if parsed is not None and parsed > 0 else None


def _records(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, pd.DataFrame):
        return [
            {str(key): item for key, item in record.items()}
            for record in value.where(pd.notna(value), None).to_dict(orient="records")
        ]
    if isinstance(value, pd.Series):
        return [{str(key): item for key, item in value.where(pd.notna(value), None).to_dict().items()}]
    if isinstance(value, dict):
        if any(key in value for key in ("period", "strongBuy", "strong_buy", "buy", "hold", "sell")):
            return [{str(key): item for key, item in value.items()}]
        return [
            {str(key): item for key, item in record.items()}
            for record in value.values()
            if isinstance(record, dict)
        ]
    if isinstance(value, list):
        return [
            {str(key): item for key, item in record.items()}
            for record in value
            if isinstance(record, dict)
        ]
    return []


def _choose_recommendation_row(rows: list[dict[str, Any]], warnings: list[dict[str, Any]], ticker: str) -> dict[str, Any] | None:
    if not rows:
        return None
    by_period = {
        str(row.get("period") or row.get("Period") or "").strip(): row
        for row in rows
        if str(row.get("period") or row.get("Period") or "").strip()
    }
    for period in _PERIOD_ORDER:
        if period in by_period:
            return by_period[period]
    if not by_period and len(rows) == 1:
        return rows[0]
    if not by_period and len(rows) > 1:
        warnings.append(
            _warning(
                "ambiguous_recommendation_period",
                "Recommendation summary contained multiple unlabeled periods; rating context was omitted.",
                ticker=ticker,
            )
        )
    return None


def _rating_counts(row: dict[str, Any] | None) -> dict[str, int] | None:
    if not row:
        return None
    counts: dict[str, int] = {}
    for raw_key, value in row.items():
        key = _RATING_KEYS.get(str(raw_key).strip().replace("-", "_").lower())
        if not key:
            continue
        try:
            count = int(value)
        except (TypeError, ValueError):
            continue
        if count < 0:
            continue
        counts[key] = count
    if not counts:
        return None
    return {key: counts.get(key, 0) for key in ("strong_buy", "buy", "hold", "sell", "strong_sell")}


def _consensus_rating(counts: dict[str, int] | None, direct: Any = None) -> str | None:
    if isinstance(direct, str):
        normalized = _RATING_KEYS.get(direct.strip().replace("-", "_").lower())
        if normalized:
            return normalized
    if not counts:
        return None
    total = sum(counts.values())
    if total <= 0:
        return None
    weighted = (
        counts.get("strong_buy", 0) * 1
        + counts.get("buy", 0) * 2
        + counts.get("hold", 0) * 3
        + counts.get("sell", 0) * 4
        + counts.get("strong_sell", 0) * 5
    ) / total
    if weighted <= 1.5:
        return "strong_buy"
    if weighted <= 2.5:
        return "buy"
    if weighted <= 3.5:
        return "hold"
    if weighted <= 4.5:
        return "sell"
    return "strong_sell"


def _normalize_row(ticker: str, payload: dict[str, Any], *, fetched_at: str) -> dict[str, Any]:
    warnings: list[dict[str, Any]] = []
    targets = payload.get("price_targets")
    if not isinstance(targets, dict):
        targets = payload
    target_low = _target_value(targets, "low")
    target_mean = _target_value(targets, "mean")
    target_median = _target_value(targets, "median")
    target_high = _target_value(targets, "high")
    target_values = [target_low, target_mean, target_median, target_high]

    recommendation_rows = _records(
        payload.get("recommendations_summary")
        if "recommendations_summary" in payload
        else payload.get("recommendations")
    )
    chosen = _choose_recommendation_row(recommendation_rows, warnings, ticker)
    counts = _rating_counts(payload.get("rating_counts") if isinstance(payload.get("rating_counts"), dict) else chosen)
    recommendation_count = sum(counts.values()) if counts is not None else None
    direct_rating = payload.get("consensus_rating") or payload.get("recommendation_key")

    status = "ok" if any(value is not None for value in target_values) else "missing"
    if status == "missing":
        warnings.append(
            _warning(
                "missing_price_targets",
                "Provider did not return usable analyst price-target fields.",
                ticker=ticker,
            )
        )
    return {
        "ticker": ticker,
        "status": status,
        "as_of": str(payload.get("as_of") or fetched_at[:10]),
        "as_of_source": str(payload.get("as_of_source") or "fetched_at_fallback"),
        "horizon_months": 12,
        "currency": str(payload.get("currency") or "USD"),
        "target_low": target_low,
        "target_mean": target_mean,
        "target_median": target_median,
        "target_high": target_high,
        "analyst_count": None,
        "consensus_rating": _consensus_rating(counts, direct_rating),
        "recommendation_count": recommendation_count,
        "rating_counts": counts,
        "warnings": warnings,
    }


def _payload_status(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "missing"
    ok = sum(1 for row in rows if row.get("status") == "ok")
    if ok == len(rows):
        return "ok"
    if ok:
        return "partial"
    if any(row.get("status") == "error" for row in rows):
        return "error"
    return "missing"


def fetch_analyst_forecasts(
    tickers: list[str],
    *,
    provider: str = "yfinance",
    fetched_at: datetime | None = None,
    trading_date: date | None = None,
) -> dict[str, Any]:
    """Fetch provider-neutral analyst forecast facts for ticker symbols."""
    provider_id = normalize_analyst_forecast_provider(provider)
    if fetched_at is not None and fetched_at.tzinfo is None:
        raise ValueError("fetched_at must be timezone-aware UTC")
    generated_dt = fetched_at or datetime.now(timezone.utc)
    generated_at = format_utc_z_seconds(generated_dt)
    resolved_trading_date = (
        date_arg(trading_date, name="trading_date")
        if trading_date is not None
        else generated_dt.date()
    )
    normalized_tickers = _normalize_tickers(tickers)
    provider_impl = get_data_provider_by_name(provider_id)
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for ticker in normalized_tickers:
        try:
            if hasattr(provider_impl, "prepare_ticker_fetch"):
                provider_impl.prepare_ticker_fetch(ticker)
            raw = provider_impl.load_analyst_forecast(ticker)
            if not isinstance(raw, dict):
                raw = {}
            row = _normalize_row(ticker, raw, fetched_at=generated_at)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            message = f"{type(exc).__name__}: {exc}"
            error = _warning("provider_error", message, ticker=ticker, severity="error")
            errors.append(error)
            row = {
                "ticker": ticker,
                "status": "error",
                "as_of": generated_at[:10],
                "as_of_source": "fetched_at_fallback",
                "horizon_months": 12,
                "currency": "USD",
                "target_low": None,
                "target_mean": None,
                "target_median": None,
                "target_high": None,
                "analyst_count": None,
                "consensus_rating": None,
                "recommendation_count": None,
                "rating_counts": None,
                "warnings": [error],
            }
        rows.append(row)
    return {
        "schema_type": "analyst_forecast",
        "schema_version": ANALYST_FORECAST_SCHEMA_VERSION,
        "provider": provider_id,
        "source_quality": ANALYST_FORECAST_SOURCE_QUALITY[provider_id],
        "generated_at": generated_at,
        "trading_date": resolved_trading_date.isoformat(),
        "status": _payload_status(rows),
        "warnings": [],
        "errors": errors,
        "forecasts": rows,
    }
