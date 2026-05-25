"""Input-boundary helpers shared by storage backend implementations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from numbers import Integral
from typing import Any

from opx_chain.tickers import is_valid_ticker


INVALID_TICKER_FILTER = ""


@dataclass(frozen=True)
class DatasetListFilters:
    """Validated filters for listing retained datasets."""

    limit: int
    provider: str | None
    since: datetime | None
    until: datetime | None
    ticker: str | None


def validate_list_limit(value: Any) -> int:
    """Return a stable nonnegative integer dataset-list limit."""
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise ValueError("list_datasets limit must be a nonnegative integer")
    limit = int(value)
    if limit < 0:
        raise ValueError("list_datasets limit must be a nonnegative integer")
    return limit


def validate_optional_text_filter(value: Any, *, name: str) -> str | None:
    """Validate an optional nonblank string filter."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonblank string")
    return value.strip()


def validate_required_text(value: Any, *, name: str) -> str:
    """Validate a required nonblank string boundary value."""
    text = validate_optional_text_filter(value, name=name)
    if text is None:
        raise ValueError(f"{name} must be a nonblank string")
    return text


def validate_optional_datetime_filter(value: Any, *, name: str) -> datetime | None:
    """Validate and normalize an optional datetime filter."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, datetime):
        raise ValueError(f"{name} must be a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def validate_ticker_filter(value: Any) -> str | None:
    """Validate a dataset ticker filter and return normalized text.

    String values that are not valid ticker symbols are a valid no-match filter:
    they should return no datasets instead of behaving like wildcards.
    """
    text = validate_optional_text_filter(value, name="ticker")
    if text is None:
        return None
    normalized = text.upper()
    if not is_valid_ticker(normalized):
        return INVALID_TICKER_FILTER
    return normalized


def validate_dataset_list_filters(
    *,
    limit: Any,
    provider: Any,
    since: Any,
    until: Any,
    ticker: Any,
) -> DatasetListFilters:
    """Validate all list_datasets query inputs with one shared boundary path."""
    return DatasetListFilters(
        limit=validate_list_limit(limit),
        provider=validate_optional_text_filter(provider, name="provider"),
        since=validate_optional_datetime_filter(since, name="since"),
        until=validate_optional_datetime_filter(until, name="until"),
        ticker=validate_ticker_filter(ticker),
    )
