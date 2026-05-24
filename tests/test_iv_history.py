"""Tests for the durable implied-volatility history store."""

from datetime import date, timedelta

import pandas as pd

from opx_chain.iv_history import IVHistoryStore, build_iv_observation_frame
from opx_chain.price_history import PriceHistoryStore
from opx_chain.volatility_features import (
    SOURCE_READY,
    build_ticker_volatility_features,
)


def _chain(*, ticker: str = "TSLA", iv_offset: float = 0.0) -> pd.DataFrame:
    rows = []
    for expiration, dte, ivs in (
        ("2026-06-05", 14, [0.24, 0.26, 0.28]),
        ("2026-06-19", 28, [0.29, 0.31, 0.33]),
    ):
        for index, iv in enumerate(ivs):
            rows.append(
                {
                    "underlying_symbol": ticker,
                    "expiration_date": expiration,
                    "days_to_expiration": dte,
                    "option_type": "CALL" if index % 2 else "PUT",
                    "delta": 0.15 + index * 0.1,
                    "implied_volatility": iv + iv_offset,
                }
            )
    return pd.DataFrame(rows)


def _price_history(*, periods: int = 120, end: str = "2026-05-22") -> pd.DataFrame:
    dates = pd.bdate_range(end=end, periods=periods)
    closes = [100.0 + index * 0.1 for index in range(periods)]
    return pd.DataFrame(
        {
            "date": dates,
            "open": closes,
            "high": [close + 1.0 for close in closes],
            "low": [close - 1.0 for close in closes],
            "close": closes,
            "volume": [1000 + index for index in range(periods)],
        }
    )


def test_iv_history_store_persists_ticker_and_dte_aggregates(tmp_path):
    """Stored IV observations should preserve ticker-wide and DTE-bucket history."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    observations = build_iv_observation_frame(
        _chain(),
        provider="marketdata",
        dataset_id="dataset-1",
        run_id="run-1",
        observed_at=date(2026, 5, 22),
    )

    stored_rows = store.upsert_observations(observations)
    history = store.load_history(
        provider="marketdata",
        ticker="TSLA",
        lookback_days=365,
        end_date=date(2026, 5, 22),
    )
    stats = store.stats(provider="marketdata", ticker="TSLA")

    assert stored_rows == len(observations)
    assert {"ALL", "8_14", "15_30"} <= set(history["dte_bucket"])
    assert stats.observation_dates == 1
    assert stats.latest_date == date(2026, 5, 22)


def test_ticker_volatility_features_use_durable_iv_history(tmp_path):
    """The public feature builder should load historical IV from the durable store."""
    iv_store = IVHistoryStore(tmp_path / "iv-history.db")
    price_store = PriceHistoryStore(tmp_path / "price-history.db")
    price_store.upsert_bars(
        provider="marketdata",
        ticker="TSLA",
        history=_price_history(),
    )
    start_day = date(2026, 4, 20)
    for index in range(25):
        observed_at = start_day + timedelta(days=index)
        observations = build_iv_observation_frame(
            _chain(iv_offset=index * 0.002),
            provider="marketdata",
            dataset_id=f"dataset-{index}",
            run_id=f"run-{index}",
            observed_at=observed_at,
        )
        iv_store.upsert_observations(observations)

    snapshot = build_ticker_volatility_features(
        ticker="TSLA",
        chain=_chain(iv_offset=0.06),
        price_history_store=price_store,
        iv_history_store=iv_store,
        provider="marketdata",
        as_of=date(2026, 5, 22),
    )

    assert snapshot["source_status"] == SOURCE_READY
    assert snapshot["iv"]["source_status"] == SOURCE_READY
    assert snapshot["iv"]["iv_source_method"] == "durable_iv_history"
    assert snapshot["iv"]["iv_history_observation_count"] == 25
    assert snapshot["iv"]["iv_percentile_1y"] is not None
