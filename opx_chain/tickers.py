"""Ticker symbol validation helpers."""

from __future__ import annotations

import re

_VALID_TICKER_RE = re.compile(r"^[A-Z]+(?:\.[A-Z]+)?$")


def is_valid_ticker(value: str) -> bool:
    """Return True when a normalized ticker symbol is valid for fetch/position use."""
    return 0 < len(value) <= 10 and bool(_VALID_TICKER_RE.fullmatch(value))
