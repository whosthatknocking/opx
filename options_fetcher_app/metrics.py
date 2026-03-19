import numpy as np

from options_fetcher_app.config import (
    MAX_SPREAD_PCT_OF_MID,
    MIN_BID,
    MIN_OPEN_INTEREST,
    MIN_VOLUME,
    RISK_FREE_RATE,
    STALE_QUOTE_SECONDS,
)
from options_fetcher_app.greeks import compute_greeks


def classify_days_to_expiration_bucket(days_to_expiration):
    if days_to_expiration <= 10:
        return "Week_1"
    if days_to_expiration <= 18:
        return "Week_2"
    if days_to_expiration <= 26:
        return "Week_3"
    return "Week_4"


def add_quote_quality_metrics(df, underlying_price):
    df["has_valid_underlying"] = underlying_price > 0
    df["has_valid_strike"] = df["strike"] > 0
    df["bid_le_ask"] = df["bid"] <= df["ask"]
    df["has_nonzero_bid"] = df["bid"] > 0
    df["has_nonzero_ask"] = df["ask"] > 0
    df["has_crossed_or_locked_market"] = (
        df["bid"].notna() & df["ask"].notna() & (df["bid"] >= df["ask"])
    )
    df["has_valid_quote"] = (
        df["bid"].notna()
        & df["ask"].notna()
        & (df["bid"] >= 0)
        & (df["ask"] >= 0)
        & df["bid_le_ask"]
    )
    df["has_valid_iv"] = df["implied_volatility"] > 0

    df["mark_price_mid"] = np.where(df["has_valid_quote"], (df["bid"] + df["ask"]) / 2, np.nan)
    df["bid_ask_spread"] = np.where(df["has_valid_quote"], df["ask"] - df["bid"], np.nan)
    df["bid_ask_spread_pct_of_mid"] = np.where(
        df["mark_price_mid"] > 0,
        df["bid_ask_spread"] / df["mark_price_mid"],
        np.nan,
    )
    df["spread_to_strike_pct"] = np.where(
        df["strike"] > 0,
        df["bid_ask_spread"] / df["strike"],
        np.nan,
    )
    df["spread_to_bid_pct"] = np.where(
        df["bid"] > 0,
        df["bid_ask_spread"] / df["bid"],
        np.nan,
    )
    df["oi_to_volume_ratio"] = np.where(
        df["volume"] > 0,
        df["open_interest"] / df["volume"],
        np.nan,
    )

    return df


def add_derived_pricing_metrics(df, underlying_price):
    df["strike_minus_spot"] = df["strike"] - underlying_price
    df["strike_vs_spot_pct"] = np.where(
        underlying_price > 0,
        df["strike_minus_spot"] / underlying_price,
        np.nan,
    )
    df["strike_distance_pct"] = np.abs(df["strike_vs_spot_pct"])

    call_itm_amount = np.maximum(underlying_price - df["strike"], 0)
    put_itm_amount = np.maximum(df["strike"] - underlying_price, 0)
    df["itm_amount"] = np.where(df["option_type"] == "call", call_itm_amount, put_itm_amount)
    df["otm_pct"] = np.where(
        df["option_type"] == "call",
        np.maximum(df["strike"] - underlying_price, 0) / underlying_price,
        np.maximum(underlying_price - df["strike"], 0) / underlying_price,
    )

    df["intrinsic_value"] = df["itm_amount"]
    df["extrinsic_value_bid"] = df["bid"] - df["intrinsic_value"]
    df["extrinsic_value_mid"] = df["mark_price_mid"] - df["intrinsic_value"]
    df["extrinsic_value_ask"] = df["ask"] - df["intrinsic_value"]
    df["extrinsic_pct_mid"] = np.where(
        df["mark_price_mid"] > 0,
        df["extrinsic_value_mid"] / df["mark_price_mid"],
        np.nan,
    )
    df["has_negative_extrinsic_mid"] = df["extrinsic_value_mid"] < 0

    df["premium_reference_price"] = df["mark_price_mid"].fillna(df["bid"]).fillna(df["last_trade_price"])
    df["premium_reference_method"] = np.select(
        [
            df["mark_price_mid"].notna(),
            df["bid"].notna(),
            df["last_trade_price"].notna(),
        ],
        ["mid", "bid", "last_trade_price"],
        default="unavailable",
    )

    df["premium_to_strike"] = np.where(
        df["strike"] > 0,
        df["premium_reference_price"] / df["strike"],
        np.nan,
    )
    df["premium_to_strike_bid"] = np.where(
        df["strike"] > 0,
        df["bid"] / df["strike"],
        np.nan,
    )
    df["premium_to_strike_annualized"] = np.where(
        df["time_to_expiration_years"] > 0,
        df["premium_to_strike"] / df["time_to_expiration_years"],
        np.nan,
    )
    df["premium_per_day"] = np.where(
        df["days_to_expiration"] > 0,
        df["premium_reference_price"] / df["days_to_expiration"],
        np.nan,
    )

    df = compute_greeks(df, underlying_price, RISK_FREE_RATE)

    df["theta_to_premium_ratio"] = np.where(
        df["premium_reference_price"] > 0,
        np.abs(df["theta"]) / df["premium_reference_price"],
        np.nan,
    )
    df["vega_per_day"] = np.where(
        df["days_to_expiration"] > 0,
        df["vega"] / df["days_to_expiration"],
        np.nan,
    )
    df["break_even_if_short"] = np.where(
        df["option_type"] == "call",
        df["strike"] + df["premium_reference_price"],
        df["strike"] - df["premium_reference_price"],
    )

    return df


def add_screening_and_freshness_flags(df, fetched_at):
    df["quote_age_seconds"] = (fetched_at - df["option_quote_time"]).dt.total_seconds()
    df["is_stale_quote"] = np.where(
        df["quote_age_seconds"].notna(),
        df["quote_age_seconds"] > STALE_QUOTE_SECONDS,
        None,
    )
    df["days_bucket"] = df["days_to_expiration"].apply(classify_days_to_expiration_bucket)
    df["near_expiry_near_money_flag"] = (
        (df["days_to_expiration"] <= 14) & (df["strike_distance_pct"] <= 0.03)
    )
    df["is_wide_market"] = df["bid_ask_spread_pct_of_mid"] > MAX_SPREAD_PCT_OF_MID
    df["passes_primary_screen"] = (
        (df["bid"] >= MIN_BID)
        & (df["bid_ask_spread_pct_of_mid"] < MAX_SPREAD_PCT_OF_MID)
        & (df["open_interest"] > MIN_OPEN_INTEREST)
        & (df["volume"] > MIN_VOLUME)
    )
    df["quote_quality_score"] = (
        df["has_valid_quote"].astype(int)
        + df["has_nonzero_bid"].astype(int)
        + df["has_nonzero_ask"].astype(int)
        + df["has_valid_iv"].astype(int)
        + df["has_valid_greeks"].astype(int)
        + (~df["has_crossed_or_locked_market"]).astype(int)
        + (df["is_stale_quote"] == False).fillna(False).astype(int)
    )

    return df
