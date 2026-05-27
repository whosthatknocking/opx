"""Normalization filter tests for zero bids, strike bands, spread limits, and position bypass."""

import pandas as pd
import pytest

from opx_chain.normalize import (
    apply_post_download_filters,
    filter_strikes_near_spot,
    filter_wide_spread_quotes,
    filter_zero_bid_quotes,
    normalize_vendor_option_frame,
)
from opx_chain.positions import OptionPositionKey, STRIKE_MATCH_TOLERANCE


class NonMatchingTickerStrike:
    """Sentinel strike that fails if non-matching ticker keys are evaluated."""

    def __rsub__(self, _value):
        raise AssertionError("non-matching ticker key should not evaluate strike math")

    def __repr__(self) -> str:
        return "NonMatchingTickerStrike()"


def test_filter_zero_bid_quotes_keeps_only_finite_positive_bid_rows():
    """Only rows with finite positive bids should remain."""
    frame = pd.DataFrame(
        [
            {"contract_symbol": "ZERO", "bid": 0.0},
            {"contract_symbol": "STRING_ZERO", "bid": "0"},
            {"contract_symbol": "VALID", "bid": 0.5},
            {"contract_symbol": "STRING_VALID", "bid": "0.25"},
            {"contract_symbol": "MISSING", "bid": float("nan")},
            {"contract_symbol": "NONE", "bid": None},
            {"contract_symbol": "INF", "bid": float("inf")},
        ]
    )

    result = filter_zero_bid_quotes(frame)

    assert result["contract_symbol"].tolist() == ["VALID", "STRING_VALID"]


def test_filter_strikes_near_spot_keeps_only_rows_within_configured_band(monkeypatch):
    """Only strikes inside the configured percentage band should survive."""
    def _config():
        return type("Config", (), {"max_strike_distance_pct": 0.30})()

    monkeypatch.setattr("opx_chain.normalize.get_runtime_config", _config)
    frame = pd.DataFrame(
        [
            {"strike": 69.9},
            {"strike": 70.0},
            {"strike": 100.0},
            {"strike": 130.0},
            {"strike": 130.1},
        ]
    )

    result = filter_strikes_near_spot(frame, underlying_price=100.0)

    assert result["strike"].tolist() == [70.0, 100.0, 130.0]


def test_filter_wide_spread_quotes_keeps_rows_at_the_cutoff(monkeypatch: pytest.MonkeyPatch):
    """The configured spread cutoff should exclude only rows above the threshold."""
    def make_config():
        return type("Config", (), {"max_spread_pct_of_mid": 0.25})()

    monkeypatch.setattr(
        "opx_chain.normalize.get_runtime_config",
        make_config,
    )
    frame = pd.DataFrame(
        [
            {"contract_symbol": "TIGHT", "bid_ask_spread_pct_of_mid": 0.10},
            {"contract_symbol": "EDGE", "bid_ask_spread_pct_of_mid": 0.25},
            {"contract_symbol": "WIDE", "bid_ask_spread_pct_of_mid": 0.30},
        ]
    )

    result = filter_wide_spread_quotes(frame)

    assert result["contract_symbol"].tolist() == ["TIGHT", "EDGE"]


def test_normalize_vendor_option_frame_allows_missing_quote_timestamp(monkeypatch):
    """Provider rows without quote timestamps should carry nullable freshness."""
    def make_config():
        return type("Config", (), {
            "today": pd.Timestamp("2026-05-27").date(),
            "risk_free_rate": 0.04,
        })()

    monkeypatch.setattr("opx_chain.normalize.get_runtime_config", make_config)
    frame = pd.DataFrame([
        {
            "contractSymbol": "TSLA260619C00100000",
            "strike": 100,
            "bid": 1.0,
            "ask": 1.2,
        }
    ])

    result = normalize_vendor_option_frame(
        frame,
        underlying_price=110.0,
        expiration_date="2026-06-19",
        option_type="call",
        ticker="TSLA",
        data_source="unit",
    )

    assert "option_quote_time" in result.columns
    assert pd.isna(result.loc[0, "option_quote_time"])


def test_apply_post_download_filters_position_keys_bypass_all_filters(monkeypatch):
    """Rows matching a portfolio position must survive even when they fail every filter."""
    def make_config():
        return type("Config", (), {
            "enable_filters": True,
            "max_strike_distance_pct": 0.10,
            "max_spread_pct_of_mid": 0.25,
        })()

    monkeypatch.setattr("opx_chain.normalize.get_runtime_config", make_config)

    frame = pd.DataFrame([
        {
            "contract_symbol": "POSITION",
            "underlying_symbol": "TSLA",
            "expiration_date": "2026-08-21",
            "option_type": "put",
            "strike": 360.0,
            "bid": 0.0,                        # would be dropped by zero-bid filter
            "bid_ask_spread_pct_of_mid": 0.90,  # would be dropped by spread filter
            "underlying_price": 391.0,
        },
        {
            "contract_symbol": "NORMAL",
            "underlying_symbol": "TSLA",
            "expiration_date": "2026-08-21",
            "option_type": "call",
            "strike": 400.0,
            "bid": 5.0,
            "bid_ask_spread_pct_of_mid": 0.10,
            "underlying_price": 391.0,
        },
        {
            "contract_symbol": "FILTERED",
            "underlying_symbol": "TSLA",
            "expiration_date": "2026-08-21",
            "option_type": "call",
            "strike": 400.0,
            "bid": 0.0,  # dropped — not a position
            "bid_ask_spread_pct_of_mid": 0.10,
            "underlying_price": 391.0,
        },
    ])

    position_keys = frozenset([
        OptionPositionKey(
            ticker="TSLA", expiration_date="2026-08-21", option_type="put", strike=360.0
        )
    ])

    result = apply_post_download_filters(frame, underlying_price=391.0, position_keys=position_keys)

    assert "POSITION" in result["contract_symbol"].values
    assert "NORMAL" in result["contract_symbol"].values
    assert "FILTERED" not in result["contract_symbol"].values


def test_apply_post_download_filters_no_position_keys_behaves_normally(monkeypatch):
    """Without position keys the zero-bid filter must still drop bid==0 rows."""
    def make_config():
        return type("Config", (), {
            "enable_filters": True,
            "max_strike_distance_pct": 0.35,
            "max_spread_pct_of_mid": 0.25,
        })()

    monkeypatch.setattr("opx_chain.normalize.get_runtime_config", make_config)

    frame = pd.DataFrame([
        {
            "contract_symbol": "ZERO",
            "underlying_symbol": "TSLA",
            "expiration_date": "2026-08-21",
            "option_type": "put",
            "strike": 360.0,
            "bid": 0.0,
            "bid_ask_spread_pct_of_mid": 0.10,
            "underlying_price": 391.0,
        },
    ])

    result = apply_post_download_filters(frame, underlying_price=391.0)

    assert result.empty


def test_filter_wide_spread_quotes_drops_nan_spread_rows(monkeypatch: pytest.MonkeyPatch):
    """Rows with NaN bid_ask_spread_pct_of_mid are removed (no valid quote to evaluate)."""
    def make_config():
        return type("Config", (), {"max_spread_pct_of_mid": 0.25})()

    monkeypatch.setattr("opx_chain.normalize.get_runtime_config", make_config)
    frame = pd.DataFrame([
        {"contract_symbol": "TIGHT", "bid_ask_spread_pct_of_mid": 0.10},
        {"contract_symbol": "NO_QUOTE", "bid_ask_spread_pct_of_mid": float("nan")},
    ])

    result = filter_wide_spread_quotes(frame)

    assert result["contract_symbol"].tolist() == ["TIGHT"]


def test_position_bypass_uses_strike_match_tolerance(monkeypatch):
    """Positions with a strike differing by just under STRIKE_MATCH_TOLERANCE must be matched."""
    def make_config():
        return type("Config", (), {
            "enable_filters": True,
            "max_strike_distance_pct": 0.05,
            "max_spread_pct_of_mid": 0.25,
        })()

    monkeypatch.setattr("opx_chain.normalize.get_runtime_config", make_config)

    near_miss = STRIKE_MATCH_TOLERANCE - 0.001
    frame = pd.DataFrame([
        {
            "contract_symbol": "FUZZ",
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-06-20",
            "option_type": "call",
            "strike": 200.0 + near_miss,
            "bid": 0.0,                        # would be dropped by zero-bid filter
            "bid_ask_spread_pct_of_mid": 0.90,  # would be dropped by spread filter
            "underlying_price": 200.0,
        },
    ])
    position_keys = frozenset([
        OptionPositionKey(
            ticker="AAPL", expiration_date="2026-06-20", option_type="call", strike=200.0
        )
    ])

    result = apply_post_download_filters(frame, underlying_price=200.0, position_keys=position_keys)

    assert "FUZZ" in result["contract_symbol"].values


def test_position_bypass_prefilters_position_keys_by_ticker(monkeypatch):
    """Position matching should not evaluate keys for tickers absent from the frame."""
    def make_config():
        return type("Config", (), {
            "enable_filters": True,
            "max_strike_distance_pct": 0.35,
            "max_spread_pct_of_mid": 0.25,
        })()

    monkeypatch.setattr("opx_chain.normalize.get_runtime_config", make_config)
    frame = pd.DataFrame([
        {
            "contract_symbol": "TSLA_PUT",
            "underlying_symbol": "TSLA",
            "expiration_date": "2026-08-21",
            "option_type": "put",
            "strike": 360.0,
            "bid": 5.0,
            "bid_ask_spread_pct_of_mid": 0.10,
            "underlying_price": 391.0,
        },
    ])
    position_keys = frozenset([
        OptionPositionKey(
            ticker="NVDA",
            expiration_date="2026-08-21",
            option_type="put",
            strike=NonMatchingTickerStrike(),
        )
    ])

    result = apply_post_download_filters(frame, underlying_price=391.0, position_keys=position_keys)

    assert result["contract_symbol"].tolist() == ["TSLA_PUT"]
