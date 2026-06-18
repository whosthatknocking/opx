"""Strict validation for direct runtime helper arguments."""

from __future__ import annotations


def strict_bool_arg(value: object, *, name: str) -> bool:
    """Require a real boolean for public helper mode flags."""

    if not isinstance(value, bool):
        raise ValueError(f"{name} must be true or false")
    return value
