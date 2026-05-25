"""Load and parse portfolio positions for filter bypass and ticker inclusion."""

from __future__ import annotations

import csv
import hashlib
import math
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from opx_chain.json_utils import dumps_strict_json
from opx_chain.option_types import OPTION_TYPE_CALL, OPTION_TYPE_PUT
from opx_chain.paths import get_default_positions_path
from opx_chain.tickers import is_valid_ticker


DEFAULT_POSITIONS_PATH = get_default_positions_path()
STRIKE_MATCH_TOLERANCE = 0.01  # max abs difference when matching strikes across data sources

_OPTION_RE = re.compile(r"^-?([A-Z.]+)(\d{2})(\d{2})(\d{2})([CP])(\d+\.?\d*)$")
_SKIP_SYMBOLS = {"SPAXX**"}
_SKIP_PREFIXES = ("Pending",)


@dataclass(frozen=True)
class OptionPositionKey:
    """Identifies a specific option contract held in the portfolio."""

    ticker: str
    expiration_date: str  # ISO format: YYYY-MM-DD
    option_type: str      # "call" or "put"
    strike: float


@dataclass(frozen=True)
class PositionSet:
    """Parsed portfolio positions used to guide fetch and filter behavior."""

    stock_tickers: frozenset[str]
    option_keys: frozenset[OptionPositionKey]

    @property
    def empty(self) -> bool:
        """Return True when the parsed positions set contains no stock or option entries."""
        return not self.stock_tickers and not self.option_keys

    @property
    def tickers(self) -> frozenset[str]:
        """Return every ticker represented by stock rows or held option rows."""
        return self.stock_tickers | frozenset(key.ticker for key in self.option_keys)


EMPTY_POSITION_SET = PositionSet(frozenset(), frozenset())


def resolve_positions_path(
    path: Path | None = None,
    *,
    default: Path | None = None,
) -> Path:
    """Return the expanded positions CSV path using the configured default."""
    return (path or default or DEFAULT_POSITIONS_PATH).expanduser()


def _option_key_fingerprint_value(key: OptionPositionKey) -> list[object]:
    """Return the canonical JSON representation for one parsed option position."""
    return [key.ticker, key.expiration_date, key.option_type, key.strike]


def position_set_fingerprint(position_set: PositionSet) -> str:
    """Return a stable SHA-256 fingerprint for parsed positions."""
    payload = {
        "stock_tickers": sorted(position_set.stock_tickers),
        "option_keys": sorted(
            _option_key_fingerprint_value(key)
            for key in position_set.option_keys
        ),
    }
    serialized = dumps_strict_json(payload, sort_keys=True)
    return hashlib.sha256(serialized.encode()).hexdigest()


def positions_fingerprint(
    positions_path: Path,
    position_set: PositionSet | None = None,
) -> str:
    """Return SHA-256 of canonical parsed positions, or empty string if absent."""
    if not positions_path.exists():
        return ""
    return position_set_fingerprint(position_set or load_positions(positions_path))


def _parse_option_symbol(raw: str) -> OptionPositionKey | None:
    """Parse a Fidelity-style option symbol into a structured key, or return None."""
    clean = raw.strip().replace(" ", "").upper()
    m = _OPTION_RE.match(clean)
    if not m:
        return None
    ticker, yy, mm, dd, cp, strike_str = m.groups()
    is_occ_padded_strike = strike_str.isdigit() and len(strike_str) == 8
    if is_occ_padded_strike or not is_valid_ticker(ticker):
        # Fidelity exports use a plain decimal strike; OCC uses an 8-digit
        # strike scaled by 1000, which this parser intentionally does not decode.
        return None
    try:
        strike = float(strike_str)
    except ValueError:
        return None
    if not math.isfinite(strike):
        return None
    try:
        expiration_date = date(2000 + int(yy), int(mm), int(dd)).isoformat()
    except ValueError:
        return None
    return OptionPositionKey(
        ticker=ticker,
        expiration_date=expiration_date,
        option_type=OPTION_TYPE_CALL if cp == "C" else OPTION_TYPE_PUT,
        strike=strike,
    )


def _parse_stock_ticker(raw: str) -> str | None:
    """Normalize and validate a stock ticker from a positions row."""
    ticker = raw.strip().upper()
    if is_valid_ticker(ticker):
        return ticker
    return None


def load_positions(path: Path | None = None) -> PositionSet:
    """Load the portfolio positions CSV and return parsed stock tickers and option keys.

    Returns an empty PositionSet when the file does not exist. If the file exists
    but cannot be parsed, prints a warning to stderr and returns an empty PositionSet.
    """
    resolved = resolve_positions_path(path)
    if not resolved.exists():
        return EMPTY_POSITION_SET

    stock_tickers: set[str] = set()
    option_keys: set[OptionPositionKey] = set()

    try:
        with resolved.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None or "Symbol" not in reader.fieldnames:
                print(
                    f"Warning: positions file {resolved} missing required 'Symbol' "
                    f"column (found: {reader.fieldnames}); returning empty positions.",
                    file=sys.stderr,
                )
                return EMPTY_POSITION_SET
            for row in reader:
                symbol = (row.get("Symbol") or "").strip()
                if not symbol or symbol in _SKIP_SYMBOLS:
                    continue
                if any(symbol.startswith(p) for p in _SKIP_PREFIXES):
                    continue
                if symbol.startswith("-") or " -" in symbol:
                    key = _parse_option_symbol(symbol)
                    if key:
                        option_keys.add(key)
                else:
                    ticker = _parse_stock_ticker(symbol)
                    if ticker:
                        stock_tickers.add(ticker)
    except (OSError, csv.Error, UnicodeDecodeError) as exc:
        print(
            f"Warning: failed to parse positions file {resolved}: {exc}; "
            "returning empty positions.",
            file=sys.stderr,
        )
        return EMPTY_POSITION_SET

    return PositionSet(frozenset(stock_tickers), frozenset(option_keys))
