"""Strict validation for direct runtime helper arguments."""

from __future__ import annotations

from datetime import datetime


def strict_bool_arg(value: object, *, name: str) -> bool:
    """Require a real boolean for public helper mode flags."""

    if not isinstance(value, bool):
        raise ValueError(f"{name} must be true or false")
    return value


def timezone_aware_datetime_arg(value: object, *, name: str) -> datetime:
    """Require a timezone-aware datetime for public helper timestamps."""

    if not isinstance(value, datetime):
        raise ValueError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware UTC")
    return value
