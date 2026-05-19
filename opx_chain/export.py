"""CSV export helpers for the normalized options dataset."""

from pathlib import Path

import pandas as pd

from opx_chain import SCHEMA_VERSION  # noqa: F401 pylint: disable=unused-import
from opx_chain.schema import QUALITY_FLAG_FIELDS
from opx_chain.storage.atomic import atomic_file_write
from opx_chain.timestamps import UTC_Z_MICROSECONDS_FORMAT


COLUMN_ORDER = [
    "underlying_symbol",
    "contract_symbol",
    "option_type",
    "expiration_date",
    "days_to_expiration",
    "time_to_expiration_years",
    "strike",
    "underlying_price",
    "underlying_day_change_pct",
    "historical_volatility",
    "underlying_price_time",
    "underlying_price_age_seconds",
    "is_stale_underlying_price",
    "next_earnings_date",
    "next_earnings_date_is_estimated",
    "next_earnings_date_source",
    "next_earnings_date_confidence",
    "days_to_earnings",
    "earnings_within_5d",
    "earnings_within_10d",
    "next_ex_div_date",
    "next_ex_div_date_source",
    "next_ex_div_date_confidence",
    "days_to_ex_div",
    "ex_div_within_3d",
    "dividend_amount",
    "event_risk_score",
    "bid",
    "ask",
    "last_trade_price",
    "mark_price_mid",
    "premium_reference_price",
    "premium_reference_method",
    "expected_fill_price",
    "bid_ask_spread",
    "bid_ask_spread_pct_of_mid",
    "spread_to_strike_pct",
    "spread_to_bid_pct",
    "volume",
    "open_interest",
    "oi_to_volume_ratio",
    "listed_strike_increment",
    "implied_volatility",
    "iv_state_level",
    "iv_state_term",
    "change",
    "percent_change",
    "option_quote_time",
    "quote_age_seconds",
    "is_stale_quote",
    "is_in_the_money",
    "strike_minus_spot",
    "strike_vs_spot_pct",
    "strike_distance_pct",
    "itm_amount",
    "otm_pct",
    "intrinsic_value",
    "extrinsic_value_bid",
    "extrinsic_value_mid",
    "extrinsic_value_ask",
    "extrinsic_pct_mid",
    "has_negative_extrinsic_mid",
    "premium_to_strike",
    "premium_to_strike_bid",
    "premium_to_strike_annualized",
    "premium_per_day",
    "iv_adjusted_premium_per_day",
    "estimated_margin_requirement",
    "return_on_margin",
    "return_on_margin_annualized",
    "break_even_if_short",
    "expected_move",
    "expected_move_pct",
    "expected_move_lower_bound",
    "expected_move_upper_bound",
    "delta",
    "delta_abs",
    "delta_safety_pct",
    "delta_itm_proxy",
    "probability_itm",
    "gamma",
    "vega",
    "vega_per_day",
    "theta",
    "theta_dollars_per_day",
    "theta_to_premium_ratio",
    "capital_required",
    "theta_efficiency",
    "theta_efficiency_below_p25",
    *QUALITY_FLAG_FIELDS,
    "days_bucket",
    "near_expiry_near_money_flag",
    "passes_primary_screen",
    "spread_score",
    "dte_score",
    "risk_level",
    "risk_model_inconsistent",
    "quote_quality_score",
    "option_score",
    "score_validation",
    "score_adjustment",
    "final_score",
    "contract_size",
    "data_source",
    "risk_free_rate_used",
]
CANONICAL_EXPORT_COLUMNS = tuple(COLUMN_ORDER)
INTEGER_EXPORT_COLUMNS = (
    "days_to_expiration",
)


def format_export_timestamps(df):
    """Format timestamps consistently so the CSV stays stable across runs."""
    for column in ["option_quote_time", "underlying_price_time"]:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], utc=True, errors="coerce").dt.strftime(
                UTC_Z_MICROSECONDS_FORMAT
            )
    return df


def reorder_export_columns(df):
    """Pin the export to the canonical schema without leaking provider-specific extras."""
    return df.reindex(columns=CANONICAL_EXPORT_COLUMNS)


def coerce_export_column_types(df):
    """Preserve integer semantics for canonical whole-number export fields."""
    df = df.copy()
    for column in INTEGER_EXPORT_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    return df


def prepare_export_frame(ticker_frames) -> pd.DataFrame:
    """Combine fetched frames and apply schema formatting without writing to disk."""
    df = pd.concat(ticker_frames, ignore_index=True)
    df = format_export_timestamps(df)
    df = reorder_export_columns(df)
    df = coerce_export_column_types(df)
    return df


def write_options_csv(ticker_frames, output_path):
    """Combine fetched frames, format the schema, and write the final CSV."""
    output_path = Path(output_path)
    df = prepare_export_frame(ticker_frames)
    atomic_file_write(output_path, lambda tmp_path: df.to_csv(tmp_path, index=False))
    return df
