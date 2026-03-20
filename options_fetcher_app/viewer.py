import json
import os
import re
import time
from functools import lru_cache
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
from pandas.api.types import is_bool_dtype, is_numeric_dtype


REPO_ROOT = Path(__file__).resolve().parent.parent
STATIC_ROOT = Path(__file__).resolve().parent / "viewer_static"
README_PATH = REPO_ROOT / "README.md"
CSV_PATTERN = "options_engine_output_*.csv"
HIDDEN_COLUMNS = {
    "currency",
    "underlying_currency",
    "roll_from_days_to_expiration",
    "roll_from_expiration_date",
    "roll_days_added",
    "roll_from_premium_reference_price",
    "roll_net_credit",
    "roll_yield",
    "fetch_status",
    "fetch_error",
    "script_version",
    "fetched_at",
}


def discover_csv_files():
    return sorted(REPO_ROOT.glob(CSV_PATTERN), key=lambda path: path.stat().st_mtime, reverse=True)


def resolve_csv_path(csv_name=None):
    files = discover_csv_files()
    if not files:
        raise FileNotFoundError("No CSV files were found in the project root.")

    if not csv_name:
        return files[0]

    candidate = REPO_ROOT / csv_name
    if candidate.exists() and candidate.is_file() and candidate.name.startswith("options_engine_output_"):
        return candidate

    raise FileNotFoundError(f"CSV file not found: {csv_name}")


@lru_cache(maxsize=1)
def load_readme_text():
    return README_PATH.read_text(encoding="utf-8")


@lru_cache(maxsize=1)
def load_field_reference_markdown():
    markdown = load_readme_text()
    start_marker = "## CSV Field Reference"
    start_index = markdown.find(start_marker)
    if start_index == -1:
        return markdown

    remaining = markdown[start_index:]
    next_section_match = re.search(r"^## ", remaining[len(start_marker):], flags=re.MULTILINE)
    if not next_section_match:
        return remaining.strip()

    end_index = len(start_marker) + next_section_match.start()
    return remaining[:end_index].strip()


@lru_cache(maxsize=1)
def extract_field_descriptions():
    descriptions = {}
    pattern = re.compile(r"^- `([^`]+)`: (.+)$")
    for line in load_readme_text().splitlines():
        match = pattern.match(line.strip())
        if match:
            descriptions[match.group(1)] = match.group(2)
    return descriptions


def normalize_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return value.item() if hasattr(value, "item") else value


def is_truthy(value):
    return str(value).strip().lower() in {"true", "1", "yes"}


def coerce_number(series):
    return pd.to_numeric(series, errors="coerce")


def coerce_scalar_number(value):
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(number) else float(number)


def build_freshness_summary(frame, csv_path):
    option_quote_ages = pd.to_numeric(frame.get("quote_age_seconds"), errors="coerce").dropna()
    underlying_quote_ages = pd.to_numeric(frame.get("underlying_price_age_seconds"), errors="coerce").dropna()
    now = time.time()

    summary = {
        "file_age_seconds": max(0.0, now - csv_path.stat().st_mtime),
        "option_quote_age_median_seconds": None,
        "option_quote_age_max_seconds": None,
        "underlying_quote_age_median_seconds": None,
        "underlying_quote_age_max_seconds": None,
    }

    if not option_quote_ages.empty:
        summary["option_quote_age_median_seconds"] = float(option_quote_ages.median())
        summary["option_quote_age_max_seconds"] = float(option_quote_ages.max())

    if not underlying_quote_ages.empty:
        summary["underlying_quote_age_median_seconds"] = float(underlying_quote_ages.median())
        summary["underlying_quote_age_max_seconds"] = float(underlying_quote_ages.max())

    return summary


def format_percent(value):
    return None if value is None else round(value * 100, 1)


def normalize_opportunity(row):
    if row is None:
        return None
    return {
        "contract_symbol": row.get("contract_symbol"),
        "option_type": row.get("option_type"),
        "expiration_date": row.get("expiration_date"),
        "strike": coerce_scalar_number(row.get("strike")),
        "premium_reference_price": coerce_scalar_number(row.get("premium_reference_price")),
        "return_on_margin_annualized_pct": format_percent(coerce_scalar_number(row.get("return_on_margin_annualized"))),
        "probability_itm_pct": format_percent(coerce_scalar_number(row.get("probability_itm"))),
        "delta_abs": coerce_scalar_number(row.get("delta_abs")),
        "strike_distance_pct": format_percent(coerce_scalar_number(row.get("strike_distance_pct"))),
        "quote_quality_score": coerce_scalar_number(row.get("quote_quality_score")),
        "bid_ask_spread_pct_of_mid": format_percent(coerce_scalar_number(row.get("bid_ask_spread_pct_of_mid"))),
        "summary": row.get("_summary"),
    }


def attach_opportunity_summary(frame):
    frame = frame.copy()
    frame["_summary"] = (
        "ROM "
        + (coerce_number(frame["return_on_margin_annualized"]).mul(100).round(1).astype("string").fillna("—"))
        + "% · ITM "
        + (coerce_number(frame["probability_itm"]).mul(100).round(1).astype("string").fillna("—"))
        + "% · spread "
        + (coerce_number(frame["bid_ask_spread_pct_of_mid"]).mul(100).round(1).astype("string").fillna("—"))
        + "%"
    )
    return frame


def pick_profitable_opportunity(frame):
    if frame.empty:
        return None
    candidates = frame.copy()
    if "passes_primary_screen" in candidates.columns:
        screened = candidates[candidates["passes_primary_screen"].map(is_truthy)]
        if not screened.empty:
            candidates = screened
    candidates = attach_opportunity_summary(candidates)
    candidates["_rom"] = coerce_number(candidates.get("return_on_margin_annualized"))
    candidates["_quality"] = coerce_number(candidates.get("quote_quality_score")).fillna(0)
    candidates = candidates.sort_values(by=["_rom", "_quality"], ascending=[False, False], na_position="last")
    return normalize_opportunity(candidates.iloc[0].to_dict()) if not candidates.empty else None


def pick_moderate_risk_opportunity(frame):
    if frame.empty:
        return None
    candidates = frame.copy()
    if "passes_primary_screen" in candidates.columns:
        screened = candidates[candidates["passes_primary_screen"].map(is_truthy)]
        if not screened.empty:
            candidates = screened
    candidates["_itm"] = coerce_number(candidates.get("probability_itm"))
    candidates["_rom"] = coerce_number(candidates.get("return_on_margin_annualized"))
    candidates["_distance"] = coerce_number(candidates.get("strike_distance_pct"))
    candidates["_spread"] = coerce_number(candidates.get("bid_ask_spread_pct_of_mid"))
    moderate = candidates[
        (candidates["_itm"].notna()) & (candidates["_itm"] <= 0.35)
        & (candidates["_distance"].notna()) & (candidates["_distance"] >= 0.03)
        & (candidates["_spread"].notna()) & (candidates["_spread"] < 0.20)
    ]
    if moderate.empty:
        moderate = candidates[(candidates["_itm"].notna()) & (candidates["_itm"] <= 0.45)]
    moderate = attach_opportunity_summary(moderate)
    moderate = moderate.sort_values(by=["_rom", "_itm"], ascending=[False, True], na_position="last")
    return normalize_opportunity(moderate.iloc[0].to_dict()) if not moderate.empty else None


def build_market_context(ticker, underlying_price, day_change_pct):
    if underlying_price is None and day_change_pct is None:
        return f"{ticker} has no recent underlying snapshot in this file."
    if day_change_pct is None:
        return f"{ticker} last underlying price was {underlying_price:.2f}."
    direction = "up" if day_change_pct >= 0 else "down"
    return f"{ticker} last underlying price was {underlying_price:.2f}, {direction} {abs(day_change_pct) * 100:.1f}% versus previous close."


def build_latest_status(day_change_pct, median_iv_pct, historical_volatility_pct):
    if day_change_pct is None and median_iv_pct is None and historical_volatility_pct is None:
        return "Snapshot unavailable"

    status_parts = []
    if day_change_pct is not None:
        move_pct = day_change_pct * 100
        if move_pct > 0.2:
            status_parts.append(f"Up {move_pct:.1f}%")
        elif move_pct < -0.2:
            status_parts.append(f"Down {abs(move_pct):.1f}%")
        else:
            status_parts.append("Flat")

    if median_iv_pct is not None and historical_volatility_pct is not None and historical_volatility_pct > 0:
        iv_hv_ratio = median_iv_pct / historical_volatility_pct
        if iv_hv_ratio >= 1.15:
            status_parts.append("IV rich")
        elif iv_hv_ratio <= 0.9:
            status_parts.append("IV soft")
        else:
            status_parts.append("IV balanced")
    elif median_iv_pct is not None:
        status_parts.append("IV available")

    return " · ".join(status_parts) if status_parts else "Snapshot available"


def build_ticker_summary(ticker, frame):
    underlying_price = coerce_number(frame.get("underlying_price")).dropna()
    day_change = coerce_number(frame.get("underlying_day_change_pct")).dropna()
    implied_volatility = coerce_number(frame.get("implied_volatility")).dropna()
    hv = coerce_number(frame.get("historical_volatility")).dropna()
    profitable = pick_profitable_opportunity(frame)
    moderate = pick_moderate_risk_opportunity(frame)
    underlying_price_value = None if underlying_price.empty else float(underlying_price.iloc[0])
    day_change_value = None if day_change.empty else float(day_change.iloc[0])
    median_iv_value = None if implied_volatility.empty else round(float(implied_volatility.median()) * 100, 1)
    hv_value = None if hv.empty else round(float(hv.iloc[0]) * 100, 1)
    return {
        "ticker": ticker,
        "row_count": int(len(frame.index)),
        "call_count": int((frame.get("option_type") == "call").sum()),
        "put_count": int((frame.get("option_type") == "put").sum()),
        "expiration_count": int(frame.get("expiration_date").nunique()),
        "underlying_price": underlying_price_value,
        "underlying_day_change_pct": format_percent(day_change_value),
        "median_implied_volatility_pct": median_iv_value,
        "historical_volatility_pct": hv_value,
        "iv_hv_ratio": None if median_iv_value is None or hv_value in (None, 0) else round(median_iv_value / hv_value, 2),
        "latest_status": build_latest_status(day_change_value, median_iv_value, hv_value),
        "market_context": build_market_context(ticker, underlying_price_value, day_change_value),
        "profitable_opportunity": profitable,
        "moderate_risk_opportunity": moderate,
    }


def build_summary_payload(csv_name=None):
    csv_path = resolve_csv_path(csv_name)
    frame = pd.read_csv(csv_path)
    visible_columns = [column for column in frame.columns if column not in HIDDEN_COLUMNS]
    frame = frame[visible_columns]
    tickers = sorted(frame["underlying_symbol"].dropna().astype(str).unique())

    ticker_summaries = []
    for ticker in tickers:
        ticker_frame = frame[frame["underlying_symbol"].astype(str) == ticker].copy()
        ticker_summaries.append(build_ticker_summary(ticker, ticker_frame))

    profitable_candidates = [item for item in ticker_summaries if item["profitable_opportunity"]]
    moderate_candidates = [item for item in ticker_summaries if item["moderate_risk_opportunity"]]
    profitable_candidates.sort(
        key=lambda item: item["profitable_opportunity"]["return_on_margin_annualized_pct"] or -10**9,
        reverse=True,
    )
    moderate_candidates.sort(
        key=lambda item: item["moderate_risk_opportunity"]["return_on_margin_annualized_pct"] or -10**9,
        reverse=True,
    )
    return {
        "selected_file": csv_path.name,
        "tickers": ticker_summaries,
        "highlights": {
            "most_profitable": profitable_candidates[0] if profitable_candidates else None,
            "moderate_risk": moderate_candidates[0] if moderate_candidates else None,
        },
    }


def load_csv_payload(csv_name=None):
    csv_path = resolve_csv_path(csv_name)
    frame = pd.read_csv(csv_path)
    freshness_summary = build_freshness_summary(frame, csv_path)
    visible_columns = [column for column in frame.columns if column not in HIDDEN_COLUMNS]
    frame = frame[visible_columns]
    rows = [
        {column: normalize_value(value) for column, value in record.items()}
        for record in frame.to_dict(orient="records")
    ]
    descriptions = extract_field_descriptions()
    columns = [
        {
            "name": column,
            "description": descriptions.get(column, "No README description available for this field."),
            "is_numeric": bool(is_numeric_dtype(frame[column]) and not is_bool_dtype(frame[column])),
        }
        for column in frame.columns
    ]
    return {
        "selected_file": csv_path.name,
        "row_count": len(rows),
        "columns": columns,
        "rows": rows,
        "freshness_summary": freshness_summary,
    }


def make_file_listing():
    files = discover_csv_files()
    return [
        {
            "name": path.name,
            "size_bytes": path.stat().st_size,
            "modified_at": path.stat().st_mtime,
        }
        for path in files
    ]


class ViewerRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_ROOT), **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/files":
            return self.respond_json({"files": make_file_listing()})
        if parsed.path == "/api/data":
            query = parse_qs(parsed.query)
            csv_name = query.get("file", [None])[0]
            try:
                payload = load_csv_payload(csv_name)
            except FileNotFoundError as exc:
                return self.respond_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
            return self.respond_json(payload)
        if parsed.path == "/api/readme":
            return self.respond_json({"markdown": load_field_reference_markdown()})
        if parsed.path == "/api/summary":
            query = parse_qs(parsed.query)
            csv_name = query.get("file", [None])[0]
            try:
                payload = build_summary_payload(csv_name)
            except FileNotFoundError as exc:
                return self.respond_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
            return self.respond_json(payload)
        if parsed.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def respond_json(self, payload, status=HTTPStatus.OK):
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format, *args):
        if os.environ.get("OPTIONS_FETCHER_VIEWER_QUIET") == "1":
            return
        super().log_message(format, *args)


def serve(host="127.0.0.1", port=8000):
    server = ThreadingHTTPServer((host, port), ViewerRequestHandler)
    print(f"CSV viewer running at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main():
    host = os.environ.get("OPTIONS_FETCHER_VIEWER_HOST", "127.0.0.1")
    port = int(os.environ.get("OPTIONS_FETCHER_VIEWER_PORT", "8000"))
    serve(host=host, port=port)


if __name__ == "__main__":
    main()
