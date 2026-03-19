from datetime import datetime, timezone

import numpy as np
import pandas as pd
import yfinance as yf

from options_fetcher_app.config import MAX_EXPIRATION, STALE_QUOTE_SECONDS, today
from options_fetcher_app.normalize import enrich_option_frame
from options_fetcher_app.utils import coerce_float, normalize_timestamp


def load_underlying_snapshot(stock):
    """Load the underlying snapshot once per ticker and reuse it for each expiration."""
    fast_info = getattr(stock, "fast_info", {}) or {}
    try:
        info = stock.info
    except Exception:
        info = {}

    last_price = coerce_float(
        fast_info.get("lastPrice")
        or info.get("regularMarketPrice")
        or info.get("previousClose")
    )
    previous_close = coerce_float(
        fast_info.get("previousClose")
        or info.get("previousClose")
    )

    if pd.notna(last_price) and pd.notna(previous_close) and previous_close > 0:
        underlying_day_change_pct = (last_price - previous_close) / previous_close
    else:
        underlying_day_change_pct = np.nan

    return {
        "underlying_price": last_price,
        "underlying_price_time": normalize_timestamp(info.get("regularMarketTime")),
        "underlying_currency": info.get("currency") or fast_info.get("currency"),
        "underlying_market_state": info.get("marketState"),
        "underlying_day_change_pct": underlying_day_change_pct,
    }


def append_underlying_snapshot_fields(df, snapshot, fetched_at):
    """Add underlying snapshot metadata to each option row."""
    df["underlying_price_time"] = snapshot["underlying_price_time"]
    df["underlying_currency"] = snapshot["underlying_currency"]
    df["underlying_market_state"] = snapshot["underlying_market_state"]
    df["underlying_day_change_pct"] = snapshot["underlying_day_change_pct"]
    df["underlying_price_age_seconds"] = (
        (fetched_at - snapshot["underlying_price_time"]).total_seconds()
        if pd.notna(snapshot["underlying_price_time"])
        else np.nan
    )
    df["is_stale_underlying_price"] = np.where(
        pd.notna(df["underlying_price_age_seconds"]),
        df["underlying_price_age_seconds"] > STALE_QUOTE_SECONDS,
        None,
    )
    df["fetch_status"] = "ok"
    df["fetch_error"] = ""
    return df


def fetch_ticker_option_chain(ticker):
    """Fetch and normalize all near-term option chains for one ticker."""
    try:
        fetched_at = pd.Timestamp.now(tz=timezone.utc)
        stock = yf.Ticker(ticker)
        snapshot = load_underlying_snapshot(stock)
        underlying_price = snapshot["underlying_price"]

        if pd.isna(underlying_price) or underlying_price <= 0:
            return pd.DataFrame()

        rows = []
        for expiration_date in stock.options:
            if expiration_date > MAX_EXPIRATION:
                continue

            exp_date = datetime.strptime(expiration_date, "%Y-%m-%d").date()
            if (exp_date - today).days <= 0:
                continue

            chain = stock.option_chain(expiration_date)
            for option_type, option_frame in [("call", chain.calls), ("put", chain.puts)]:
                normalized = enrich_option_frame(
                    df=option_frame,
                    underlying_price=underlying_price,
                    expiration_date=expiration_date,
                    option_type=option_type,
                    ticker=ticker,
                    fetched_at=fetched_at,
                )
                rows.append(append_underlying_snapshot_fields(normalized, snapshot, fetched_at))

        return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

    except Exception as exc:
        print(f"{ticker} error: {exc}")
        return pd.DataFrame()
