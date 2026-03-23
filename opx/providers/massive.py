"""Massive provider implementation backed by the official Massive client."""

from __future__ import annotations

from functools import lru_cache
import time
from typing import Any

import numpy as np
import pandas as pd
from massive import RESTClient

from opx.config import get_provider_credentials
from opx.normalize import normalize_vendor_option_frame
from opx.providers.base import DataProvider, OptionChainFrames
from opx.utils import coerce_float, normalize_timestamp

MAX_RETRIES = 3
BACKOFF_SECONDS = 1.0


def _coalesce(*values: Any) -> Any:
    """Return the first value that is not None and not NaN-like."""
    for value in values:
        if value is None:
            continue
        if isinstance(value, float) and np.isnan(value):
            continue
        return value
    return None


def _get_field(value: Any, *path: str) -> Any:
    """Read nested attributes or dict keys without assuming one payload shape."""
    current = value
    for part in path:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(part)
        else:
            current = getattr(current, part, None)
    return current


def _normalize_contract_type(value: Any) -> str | None:
    """Map Massive contract types to canonical call/put labels."""
    normalized = str(value).strip().lower() if value is not None else ""
    if normalized in {"call", "c"}:
        return "call"
    if normalized in {"put", "p"}:
        return "put"
    return None


class MassiveProvider(DataProvider):
    """Market-data provider backed by the official Massive/Polygon REST client."""

    name = "massive"

    @property
    def external_logger_names(self) -> tuple[str, ...]:
        """Expose urllib3 logging used underneath the official client."""
        return ("urllib3",)

    def _api_key(self) -> str:
        credentials = get_provider_credentials(self.name)
        return credentials["api_key"]

    @lru_cache(maxsize=1)
    def _client(self) -> RESTClient:
        """Construct the official Massive REST client once per provider instance."""
        return RESTClient(api_key=self._api_key(), retries=MAX_RETRIES, pagination=True)

    def _fetch_snapshot_results(self, ticker: str) -> tuple[Any, ...]:
        """Load snapshot rows from the official client with exponential backoff."""
        last_error: Exception | None = None

        for attempt in range(MAX_RETRIES + 1):
            try:
                results = self._client().list_snapshot_options_chain(
                    ticker.upper(),
                    params={"limit": 250},
                )
                return tuple(results)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                last_error = exc
                message = str(exc).lower()
                if "401" in message or "403" in message or "auth" in message:
                    raise RuntimeError(
                        "Massive authentication failed. Check [providers.massive] api_key "
                        "in ~/.config/opx/config.toml."
                    ) from exc
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(BACKOFF_SECONDS * (2 ** attempt))

        raise RuntimeError("Massive snapshot request failed without a response.") from last_error

    @lru_cache(maxsize=32)
    def _snapshot_results(self, ticker: str) -> tuple[Any, ...]:
        """Cache snapshot results once per ticker for the current process."""
        return self._fetch_snapshot_results(ticker)

    @lru_cache(maxsize=1)
    def load_vix_snapshot(self) -> dict:
        """Massive VIX support is not implemented in this phase."""
        return {
            "vix_level": np.nan,
            "vix_quote_time": pd.NaT,
        }

    def load_underlying_snapshot(self, ticker: str) -> dict:
        """Infer the underlying snapshot from the option snapshot payload."""
        results = self._snapshot_results(ticker)
        if not results:
            return {
                "underlying_price": np.nan,
                "underlying_price_time": pd.NaT,
                "underlying_market_state": None,
                "underlying_day_change_pct": np.nan,
                "historical_volatility": np.nan,
                "vix_level": np.nan,
                "vix_quote_time": pd.NaT,
            }

        first = results[0]
        underlying_price = coerce_float(
            _coalesce(
                _get_field(first, "underlying_asset", "price"),
                _get_field(first, "underlying_asset", "value"),
            )
        )
        underlying_price_time = normalize_timestamp(
            _coalesce(
                _get_field(first, "underlying_asset", "last_updated"),
                _get_field(first, "day", "last_updated"),
                _get_field(first, "last_trade", "sip_timestamp"),
                _get_field(first, "last_quote", "sip_timestamp"),
            )
        )
        previous_close = coerce_float(_get_field(first, "day", "previous_close"))
        if pd.notna(underlying_price) and pd.notna(previous_close) and previous_close > 0:
            underlying_day_change_pct = (underlying_price - previous_close) / previous_close
        else:
            underlying_day_change_pct = coerce_float(
                _coalesce(
                    _get_field(first, "underlying_asset", "change_percent"),
                    _get_field(first, "day", "change_percent"),
                )
            )
        vix_snapshot = self.load_vix_snapshot()

        return {
            "underlying_price": underlying_price,
            "underlying_price_time": underlying_price_time,
            "underlying_market_state": None,
            "underlying_day_change_pct": underlying_day_change_pct,
            "historical_volatility": np.nan,
            "vix_level": vix_snapshot["vix_level"],
            "vix_quote_time": vix_snapshot["vix_quote_time"],
        }

    def list_option_expirations(self, ticker: str) -> list[str]:
        """Return distinct expiration dates present in the Massive snapshot payload."""
        expirations = {
            _get_field(result, "details", "expiration_date")
            for result in self._snapshot_results(ticker)
        }
        return sorted(expiration for expiration in expirations if expiration)

    def load_option_chain(self, ticker: str, expiration_date: str) -> OptionChainFrames:
        """Filter the snapshot payload down to one expiration and split by side."""
        rows = []
        for result in self._snapshot_results(ticker):
            if _get_field(result, "details", "expiration_date") != expiration_date:
                continue
            option_type = _normalize_contract_type(_get_field(result, "details", "contract_type"))
            if option_type is None:
                continue
            row = {
                "contract_symbol": _coalesce(
                    _get_field(result, "details", "ticker"),
                    _get_field(result, "ticker"),
                ),
                "option_type": option_type,
                "strike": _get_field(result, "details", "strike_price"),
                "expiration_date": expiration_date,
                "contract_size": _coalesce(
                    _get_field(result, "details", "shares_per_contract"),
                    "REGULAR",
                ),
                "option_quote_time": _coalesce(
                    _get_field(result, "last_quote", "sip_timestamp"),
                    _get_field(result, "last_trade", "sip_timestamp"),
                    _get_field(result, "day", "last_updated"),
                ),
                "bid": _coalesce(
                    _get_field(result, "last_quote", "bid"),
                    _get_field(result, "last_quote", "bid_price"),
                ),
                "ask": _coalesce(
                    _get_field(result, "last_quote", "ask"),
                    _get_field(result, "last_quote", "ask_price"),
                ),
                "last_trade_price": _coalesce(
                    _get_field(result, "last_trade", "price"),
                    _get_field(result, "day", "close"),
                ),
                "volume": _get_field(result, "day", "volume"),
                "open_interest": _get_field(result, "open_interest"),
                "implied_volatility": _get_field(result, "implied_volatility"),
                "change": _get_field(result, "day", "change"),
                "percent_change": _get_field(result, "day", "change_percent"),
                "is_in_the_money": _get_field(result, "details", "in_the_money"),
                "delta": _get_field(result, "greeks", "delta"),
                "gamma": _get_field(result, "greeks", "gamma"),
                "theta": _get_field(result, "greeks", "theta"),
                "vega": _get_field(result, "greeks", "vega"),
            }
            rows.append(row)

        frame = pd.DataFrame(rows)
        if frame.empty:
            empty = pd.DataFrame()
            return OptionChainFrames(calls=empty, puts=empty)

        calls = frame[frame["option_type"] == "call"].copy()
        puts = frame[frame["option_type"] == "put"].copy()
        return OptionChainFrames(calls=calls, puts=puts)

    def normalize_option_frame(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        df: pd.DataFrame,
        underlying_price: float,
        expiration_date: str,
        option_type: str,
        ticker: str,
    ) -> pd.DataFrame:
        """Normalize a Massive frame into the canonical options schema."""
        frame = normalize_vendor_option_frame(
            df=df,
            underlying_price=underlying_price,
            expiration_date=expiration_date,
            option_type=option_type,
            ticker=ticker,
            data_source=self.name,
        )
        for greek_column in ["delta", "gamma", "theta", "vega"]:
            if greek_column in frame.columns:
                frame[greek_column] = pd.to_numeric(frame[greek_column], errors="coerce")
        return frame
