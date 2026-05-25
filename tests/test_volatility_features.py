"""Tests for public volatility feature helpers."""

# pylint: disable=missing-function-docstring

from datetime import date, datetime

import pandas as pd
import pytest

from opx_chain.price_history import PriceHistoryStore
from opx_chain.volatility_features import (
    SOURCE_INSUFFICIENT_HISTORY,
    SOURCE_MISSING,
    SOURCE_PARTIAL,
    SOURCE_READY,
    build_iv_features,
    build_price_volatility_features,
    build_ticker_volatility_features,
    dte_bucket,
    load_price_volatility_features,
)


def _history(*, periods: int = 120, end: str = "2026-05-22") -> pd.DataFrame:
    dates = pd.bdate_range(end=end, periods=periods)
    closes = [100.0]
    for index in range(1, periods):
        move = 0.002 if index % 5 else -0.006
        closes.append(closes[-1] * (1.0 + move))
    return pd.DataFrame(
        {
            "date": dates,
            "open": [close * 0.999 for close in closes],
            "high": [close * 1.01 for close in closes],
            "low": [close * 0.99 for close in closes],
            "close": closes,
            "volume": [1000 + index for index in range(periods)],
        }
    )


def _chain() -> pd.DataFrame:
    rows = []
    for expiration, dte, ivs in (
        ("2026-06-05", 14, [0.24, 0.26, 0.28, 0.30, 0.32]),
        ("2026-06-19", 28, [0.27, 0.29, 0.31, 0.33, 0.35]),
    ):
        for index, iv in enumerate(ivs):
            rows.append(
                {
                    "underlying_symbol": "TSLA",
                    "expiration_date": expiration,
                    "days_to_expiration": dte,
                    "strike_distance_pct": abs(index - 2) * 0.01,
                    "implied_volatility": iv,
                }
            )
    return pd.DataFrame(rows)


def test_dte_bucket_uses_public_labels() -> None:
    assert dte_bucket(7) == "0_7"
    assert dte_bucket(14) == "8_14"
    assert dte_bucket(45) == "31_45"
    assert dte_bucket(91) == "91_PLUS"
    assert dte_bucket(None) is None


def test_build_price_volatility_features_reports_ready_rv_context() -> None:
    features = build_price_volatility_features(
        _history(),
        ticker="tsla",
        provider="marketdata",
        as_of=date(2026, 5, 22),
    )

    assert features["ticker"] == "TSLA"
    assert features["source_status"] == SOURCE_READY
    assert features["unknown_reason"] is None
    assert features["newest_completed_session"] == "2026-05-22"
    assert features["price_history_lookback_sessions"] == 120
    assert features["rv_3d"] is not None
    assert features["rv_10d"] is not None
    assert 0 <= features["rv_3d_percentile_1y"] <= 100


def test_build_price_volatility_features_reports_insufficient_history() -> None:
    features = build_price_volatility_features(
        _history(periods=6),
        ticker="TSLA",
        provider="marketdata",
        as_of=date(2026, 5, 22),
    )

    assert features["source_status"] == SOURCE_INSUFFICIENT_HISTORY
    assert features["unknown_reason"] == "insufficient_returns_for_rv_windows"
    assert features["rv_10d"] is None


def test_build_price_volatility_features_validates_readiness_parameters() -> None:
    for kwargs in (
        {"min_context_sessions": None},
        {"min_context_sessions": "90"},
        {"min_context_sessions": False},
        {"min_context_sessions": 0},
        {"min_context_sessions": -5},
        {"max_stale_days": None},
        {"max_stale_days": "7"},
        {"max_stale_days": False},
        {"max_stale_days": -1},
    ):
        name = next(iter(kwargs))
        with pytest.raises(ValueError, match=name):
            build_price_volatility_features(
                _history(),
                ticker="TSLA",
                provider="marketdata",
                as_of=date(2026, 5, 22),
                **kwargs,
            )

    features = build_price_volatility_features(
        _history(),
        ticker="TSLA",
        provider="marketdata",
        as_of=date(2026, 5, 22),
        min_context_sessions=121,
        max_stale_days=0,
    )
    assert features["source_status"] == SOURCE_INSUFFICIENT_HISTORY
    assert features["unknown_reason"] == "insufficient_context_history"


def test_build_price_volatility_features_filters_mixed_symbol_history() -> None:
    mixed = pd.concat(
        [
            _history(periods=120).assign(ticker="AAPL", provider="marketdata"),
            _history(periods=6).assign(ticker="TSLA", provider="marketdata"),
        ],
        ignore_index=True,
    )

    features = build_price_volatility_features(
        mixed,
        ticker="TSLA",
        provider="marketdata",
        as_of=date(2026, 5, 22),
    )

    assert features["ticker"] == "TSLA"
    assert features["source_status"] == SOURCE_INSUFFICIENT_HISTORY
    assert features["price_history_lookback_sessions"] == 6
    assert features["unknown_reason"] == "insufficient_returns_for_rv_windows"


def test_build_price_volatility_features_reports_missing_for_absent_ticker() -> None:
    history = _history(periods=120).assign(ticker="AAPL", provider="marketdata")

    features = build_price_volatility_features(
        history,
        ticker="TSLA",
        provider="marketdata",
        as_of=date(2026, 5, 22),
    )

    assert features["source_status"] == SOURCE_MISSING
    assert features["unknown_reason"] == "ticker_not_in_price_history"
    assert features["price_history_lookback_sessions"] == 0


def test_volatility_features_normalize_as_of_boundary() -> None:
    price_features = build_price_volatility_features(
        _history(),
        ticker="TSLA",
        provider="marketdata",
        as_of=datetime(2026, 5, 22, 15, 30),
    )
    iv_features = build_iv_features(
        _chain(),
        ticker="TSLA",
        as_of="2026-05-22",
    )
    combined = build_ticker_volatility_features(
        ticker="TSLA",
        chain=_chain(),
        price_history=_history(),
        provider="marketdata",
        as_of="2026-05-22",
    )

    assert price_features["as_of"] == "2026-05-22"
    assert iv_features["as_of"] == "2026-05-22"
    assert combined["as_of"] == "2026-05-22"


@pytest.mark.parametrize("bad_as_of", [False, 123, "2026-05-22T15:30:00", "bad"])
def test_volatility_features_reject_malformed_as_of_values(bad_as_of) -> None:
    with pytest.raises(ValueError, match="as_of"):
        build_price_volatility_features(
            _history(),
            ticker="TSLA",
            provider="marketdata",
            as_of=bad_as_of,
        )
    with pytest.raises(ValueError, match="as_of"):
        build_iv_features(_chain(), ticker="TSLA", as_of=bad_as_of)
    with pytest.raises(ValueError, match="as_of"):
        build_ticker_volatility_features(
            ticker="TSLA",
            chain=_chain(),
            price_history=_history(),
            provider="marketdata",
            as_of=bad_as_of,
        )


def test_load_price_volatility_features_reads_public_store(tmp_path) -> None:
    store = PriceHistoryStore(tmp_path / "price-history.db")
    store.upsert_bars(provider="marketdata", ticker="TSLA", history=_history())

    features = load_price_volatility_features(
        store,
        provider="marketdata",
        ticker="TSLA",
        as_of=date(2026, 5, 22),
        lookback_days=260,
    )

    assert features["source_status"] == SOURCE_READY
    assert features["rv_5d_percentile_1y"] is not None


@pytest.mark.parametrize("bad_lookback", [None, "260", False, 0, -5, 10.5])
def test_store_backed_volatility_features_validate_lookback(tmp_path, bad_lookback) -> None:
    store = PriceHistoryStore(tmp_path / "price-history.db")
    store.upsert_bars(provider="marketdata", ticker="TSLA", history=_history())

    with pytest.raises(ValueError, match="lookback"):
        load_price_volatility_features(
            store,
            provider="marketdata",
            ticker="TSLA",
            as_of=date(2026, 5, 22),
            lookback_days=bad_lookback,
        )
    with pytest.raises(ValueError, match="price_lookback_days"):
        build_ticker_volatility_features(
            ticker="TSLA",
            chain=_chain(),
            price_history_store=store,
            provider="marketdata",
            as_of=date(2026, 5, 22),
            price_lookback_days=bad_lookback,
        )


def test_store_backed_volatility_features_allow_small_valid_lookback(tmp_path) -> None:
    store = PriceHistoryStore(tmp_path / "price-history.db")
    store.upsert_bars(provider="marketdata", ticker="TSLA", history=_history())

    features = load_price_volatility_features(
        store,
        provider="marketdata",
        ticker="TSLA",
        as_of=date(2026, 5, 22),
        lookback_days=6,
    )

    assert features["source_status"] == SOURCE_INSUFFICIENT_HISTORY
    assert features["price_history_lookback_sessions"] == 6


@pytest.mark.parametrize("bad_ticker", [None, False, 123, "", "   "])
def test_volatility_features_validate_ticker_identity(bad_ticker) -> None:
    with pytest.raises(ValueError, match="ticker"):
        build_price_volatility_features(_history(), ticker=bad_ticker)
    with pytest.raises(ValueError, match="ticker"):
        build_iv_features(_chain(), ticker=bad_ticker)
    with pytest.raises(ValueError, match="ticker"):
        build_ticker_volatility_features(ticker=bad_ticker, chain=_chain())


@pytest.mark.parametrize("bad_ticker", ["BAD/TICKER", "...", "TSLA1", "ABCDEFGHIJK"])
def test_volatility_features_validate_ticker_syntax(bad_ticker) -> None:
    with pytest.raises(ValueError, match="valid stock ticker"):
        build_price_volatility_features(_history(), ticker=bad_ticker)
    with pytest.raises(ValueError, match="valid stock ticker"):
        build_iv_features(_chain(), ticker=bad_ticker)
    with pytest.raises(ValueError, match="valid stock ticker"):
        build_ticker_volatility_features(ticker=bad_ticker, chain=_chain())


def test_volatility_features_allow_lowercase_and_dotted_tickers() -> None:
    chain = _chain().assign(underlying_symbol="BRK.B")
    history = _history().assign(ticker="BRK.B", provider="marketdata")

    price_features = build_price_volatility_features(
        history,
        ticker="brk.b",
        provider="marketdata",
        as_of=date(2026, 5, 22),
    )
    iv_features = build_iv_features(chain, ticker="brk.b", as_of=date(2026, 5, 22))

    assert price_features["ticker"] == "BRK.B"
    assert price_features["source_status"] == SOURCE_READY
    assert iv_features["ticker"] == "BRK.B"
    assert iv_features["source_status"] == SOURCE_PARTIAL


@pytest.mark.parametrize("bad_provider", [None, False, 123, "", "   "])
def test_store_backed_volatility_features_validate_provider_identity(
    tmp_path,
    bad_provider,
) -> None:
    store = PriceHistoryStore(tmp_path / "price-history.db")
    store.upsert_bars(provider="marketdata", ticker="TSLA", history=_history())

    with pytest.raises(ValueError, match="provider"):
        load_price_volatility_features(
            store,
            provider=bad_provider,
            ticker="TSLA",
            as_of=date(2026, 5, 22),
        )
    with pytest.raises(ValueError, match="provider"):
        build_ticker_volatility_features(
            ticker="TSLA",
            chain=_chain(),
            price_history_store=store,
            provider=bad_provider,
            as_of=date(2026, 5, 22),
        )


def test_build_iv_features_leaves_history_percentiles_blank_without_history() -> None:
    features = build_iv_features(_chain(), ticker="TSLA", as_of=date(2026, 5, 22))

    assert features["source_status"] == SOURCE_PARTIAL
    assert features["unknown_reason"] == "missing_iv_history"
    assert features["iv_source_method"] == "current_chain_proxy"
    assert features["representative_iv"] == 0.28
    assert features["iv_percentile_1y"] is None
    assert features["dte_buckets"]["8_14"]["current_observation_count"] == 5
    assert features["dte_buckets"]["8_14"]["iv_percentile"] is None


def test_build_iv_features_rejects_unscoped_option_chain() -> None:
    chain = _chain().drop(columns=["underlying_symbol"])

    features = build_iv_features(chain, ticker="TSLA", as_of=date(2026, 5, 22))

    assert features["source_status"] == SOURCE_MISSING
    assert features["unknown_reason"] == "unscoped_option_chain"


def test_build_iv_features_uses_optional_history_percentiles() -> None:
    history = pd.DataFrame(
        {
            "ticker": ["TSLA"] * 48,
            "representative_iv": [0.20 + index * 0.002 for index in range(48)],
            "dte_bucket": ["8_14"] * 24 + ["15_30"] * 24,
        }
    )

    features = build_iv_features(
        _chain(),
        ticker="TSLA",
        as_of=date(2026, 5, 22),
        iv_history=history,
    )

    assert features["source_status"] == SOURCE_READY
    assert features["iv_source_method"] == "current_chain_plus_history"
    assert features["iv_percentile_1y"] is not None
    assert features["dte_buckets"]["8_14"]["history_observation_count"] == 24
    assert features["dte_buckets"]["8_14"]["iv_percentile"] is not None


def test_build_iv_features_counts_distinct_history_dates_for_readiness() -> None:
    duplicate_date_history = pd.DataFrame(
        {
            "ticker": ["TSLA"] * 25,
            "representative_iv": [0.20 + index * 0.002 for index in range(25)],
            "dte_bucket": ["8_14"] * 25,
            "observation_date": ["2026-05-20"] * 25,
        }
    )

    duplicate_features = build_iv_features(
        _chain(),
        ticker="TSLA",
        as_of=date(2026, 5, 22),
        iv_history=duplicate_date_history,
    )

    assert duplicate_features["source_status"] == SOURCE_PARTIAL
    assert duplicate_features["unknown_reason"] == "insufficient_iv_history"
    assert duplicate_features["iv_history_observation_count"] == 1
    assert duplicate_features["dte_buckets"]["8_14"]["history_observation_count"] == 1
    assert duplicate_features["dte_buckets"]["8_14"]["iv_percentile"] is None

    distinct_date_history = duplicate_date_history.assign(
        observation_date=[f"2026-04-{index:02d}" for index in range(1, 26)]
    )
    distinct_features = build_iv_features(
        _chain(),
        ticker="TSLA",
        as_of=date(2026, 5, 22),
        iv_history=distinct_date_history,
    )

    assert distinct_features["source_status"] == SOURCE_READY
    assert distinct_features["unknown_reason"] is None
    assert distinct_features["iv_history_observation_count"] == 25
    assert distinct_features["dte_buckets"]["8_14"]["history_observation_count"] == 25
    assert distinct_features["dte_buckets"]["8_14"]["iv_percentile"] is not None


def test_build_iv_features_ignores_unscoped_iv_history() -> None:
    history = pd.DataFrame(
        {
            "representative_iv": [0.20 + index * 0.002 for index in range(24)],
            "dte_bucket": ["8_14"] * 24,
        }
    )

    features = build_iv_features(
        _chain(),
        ticker="TSLA",
        as_of=date(2026, 5, 22),
        iv_history=history,
    )

    assert features["source_status"] == SOURCE_PARTIAL
    assert features["unknown_reason"] == "missing_iv_history"
    assert features["iv_history_observation_count"] == 0
    assert features["iv_percentile_1y"] is None


def test_build_iv_features_keeps_sparse_history_partial() -> None:
    history = pd.DataFrame(
        {
            "ticker": ["TSLA"],
            "representative_iv": [0.20],
            "dte_bucket": ["8_14"],
            "observation_date": ["2026-05-21"],
        }
    )

    features = build_iv_features(
        _chain(),
        ticker="TSLA",
        as_of=date(2026, 5, 22),
        iv_history=history,
    )

    assert features["source_status"] == SOURCE_PARTIAL
    assert features["unknown_reason"] == "insufficient_iv_history"
    assert features["iv_percentile_1y"] is not None
    assert features["dte_buckets"]["8_14"]["iv_percentile"] is None


def test_build_iv_features_filters_date_bearing_history_to_lookback() -> None:
    history = pd.DataFrame(
        {
            "ticker": ["TSLA"] * 25,
            "representative_iv": [0.20 + index * 0.002 for index in range(25)],
            "dte_bucket": ["8_14"] * 25,
            "observation_date": ["2025-01-01"] * 25,
        }
    )

    features = build_iv_features(
        _chain(),
        ticker="TSLA",
        as_of=date(2026, 5, 22),
        iv_history=history,
    )

    assert features["source_status"] == SOURCE_PARTIAL
    assert features["iv_history_observation_count"] == 0
    assert features["iv_percentile_1y"] is None


def test_build_ticker_volatility_features_combines_price_and_iv() -> None:
    snapshot = build_ticker_volatility_features(
        ticker="TSLA",
        chain=_chain(),
        price_history=_history(),
        provider="marketdata",
        as_of=date(2026, 5, 22),
    )

    assert snapshot["schema_version"] == 1
    assert snapshot["source_status"] == SOURCE_PARTIAL
    assert snapshot["price"]["source_status"] == SOURCE_READY
    assert snapshot["iv"]["source_status"] == SOURCE_PARTIAL


def test_build_ticker_volatility_features_reports_missing_when_no_inputs() -> None:
    snapshot = build_ticker_volatility_features(
        ticker="TSLA",
        chain=pd.DataFrame(),
        price_history=pd.DataFrame(),
        provider="marketdata",
        as_of=date(2026, 5, 22),
    )

    assert snapshot["source_status"] == SOURCE_MISSING
    assert snapshot["price"]["unknown_reason"] == "missing_price_history"
    assert snapshot["iv"]["unknown_reason"] == "missing_option_chain"
