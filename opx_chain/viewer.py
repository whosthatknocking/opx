"""Local HTTP viewer for browsing exported options CSV snapshots."""
# pylint: disable=too-many-lines

from __future__ import annotations

import argparse
import ipaddress
import math
import os
import re
import threading
import time
import webbrowser
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import parse_qs, urlparse

import pandas as pd
from pandas.api.types import is_bool_dtype, is_numeric_dtype
from opx_chain.coerce import coerce_bool_or_default
from opx_chain.config import get_runtime_config
from opx_chain.export import CANONICAL_EXPORT_COLUMNS
from opx_chain.json_utils import (
    dumps_sanitized_json,
    to_python_scalar,
)
from opx_chain.integrity import (
    evaluate_option_chain_dataset_facts_status,
    evaluate_option_chain_integrity_status,
    evaluate_option_chain_row_scope_status,
)
from opx_chain.option_types import OPTION_TYPE_CALL, OPTION_TYPE_PUT
from opx_chain.paths import get_runs_dir
from opx_chain.positions import (
    STRIKE_MATCH_TOLERANCE,
    PositionSet,
    load_positions,
    positions_fingerprint,
)
from opx_chain.storage.factory import get_data_dir, get_storage_backend
from opx_chain.storage._disk import retained_path_under_roots
from opx_chain.timestamps import format_utc_z_seconds
from opx_chain.utils import read_dataset_file


_PKG_ROOT = Path(__file__).resolve().parent
STATIC_ROOT = _PKG_ROOT / "viewer_static"
FIELD_REFERENCE_PATH = _PKG_ROOT.parent / "docs" / "FIELD_REFERENCE.md"
RUNS_DIR = get_data_dir() / "runs"
CSV_PATTERN = "options_engine_output_*.csv"
VIEWER_DATASET_DISCOVERY_LIMIT = 10_000
_HOST_LABEL_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")
_DATA_DIR_OVERRIDE: Path | None = None
_CSV_MODE: bool = False
DATASET_CARD_COLUMNS = (
    "premium_reference_method",
    "risk_free_rate_used",
    "data_source",
)
INTEGER_VIEWER_COLUMNS = frozenset({
    "days_to_expiration",
    "days_to_earnings",
    "days_to_ex_div",
    "event_risk_score",
})
REFERENCE_MISSING_DESCRIPTION = "No reference description available for this field."


class FreshnessSummary(TypedDict):
    """File-level freshness statistics exposed to the browser."""

    file_age_seconds: float
    file_modified_at: str
    option_quote_age_median_seconds: float | None
    option_quote_age_max_seconds: float | None
    underlying_quote_age_median_seconds: float | None
    underlying_quote_age_max_seconds: float | None


class DatasetCard(TypedDict):
    """Single dataset-wide card shown above the viewer table."""

    name: str
    value: str
    description: str


class ColumnDefinition(TypedDict):
    """Column metadata used by the frontend table configuration."""

    name: str
    description: str
    is_numeric: bool


class OpportunitySummary(TypedDict):
    """Compact summary of a single highlighted contract opportunity."""

    contract_symbol: str | None
    option_type: str | None
    expiration_date: str | None
    strike: float | None
    premium_reference_price: float | None
    return_on_margin_annualized_pct: float | None
    probability_itm_pct: float | None
    delta_abs: float | None
    strike_distance_pct: float | None
    risk_level: str | None
    spread_score: float | None
    dte_score: float | None
    theta_efficiency: float | None
    quote_quality_score: float | None
    option_score: float | None
    final_score: float | None
    bid_ask_spread_pct_of_mid: float | None
    event_risk_score: float | None
    summary: str | None


class TickerSummary(TypedDict):
    """Per-ticker summary record shown in the Summary tab."""

    ticker: str
    row_count: int
    call_count: int
    put_count: int
    expiration_count: int
    underlying_price: float | None
    underlying_day_change_pct: float | None
    median_implied_volatility_pct: float | None
    historical_volatility_pct: float | None
    iv_hv_ratio: float | None
    next_earnings_date: str | None
    next_earnings_date_is_estimated: bool | None
    event_risk_score: float | None
    latest_status: str
    market_context: str
    profitable_opportunity: OpportunitySummary | None
    moderate_risk_opportunity: OpportunitySummary | None
    high_conviction_call: OpportunitySummary | None
    high_conviction_put: OpportunitySummary | None


class CsvPayload(TypedDict):
    """Serialized table payload returned by the CSV data endpoint."""

    selected_file: str
    row_count: int
    columns: list[ColumnDefinition]
    rows: list[dict[str, Any]]
    freshness_summary: FreshnessSummary
    dataset_cards: list[DatasetCard]


class SummaryHighlights(TypedDict):
    """Top highlighted ticker summaries for the Summary tab header."""

    most_profitable: TickerSummary | None
    moderate_risk: TickerSummary | None


class SummaryPayload(TypedDict):
    """Serialized summary-tab payload for a selected CSV export."""

    selected_file: str
    tickers: list[TickerSummary]
    highlights: SummaryHighlights


def discover_dataset_paths() -> list[Path]:
    """Return dataset paths ordered by most recently modified first.

    When --data-dir was supplied on the CLI, scans that directory for .csv and
    .parquet files. When storage is enabled, queries the storage backend for
    registered artifact locations. Falls back to globbing the output directory
    for timestamped CSV exports matching the standard filename pattern.
    """
    if _DATA_DIR_OVERRIDE is not None:
        candidates = [
            *_DATA_DIR_OVERRIDE.glob("*.csv"),
            *_DATA_DIR_OVERRIDE.glob("*.parquet"),
        ]
        return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)

    if not _CSV_MODE:
        storage = get_storage_backend()
        if storage is not None:
            records = storage.list_datasets(limit=VIEWER_DATASET_DISCOVERY_LIMIT)
            roots = (getattr(storage, "_runs_dir", None) or _runtime_runs_dir(),)
            paths = []
            for record in records:
                path = retained_path_under_roots(record.location, roots)
                if path is not None and path.exists():
                    paths.append(path)
            if paths:
                return paths

    runs_dir = _runtime_runs_dir()
    candidates = [
        *runs_dir.glob(f"*/output/{CSV_PATTERN}"),
        *runs_dir.glob(CSV_PATTERN),
    ]
    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)


def _runtime_runs_dir() -> Path:
    """Return the active runtime runs directory for fallback CSV discovery."""
    config = get_runtime_config()
    return get_runs_dir(config.storage_dir, default_runs_dir=RUNS_DIR)


def resolve_csv_path(csv_name: str | None = None) -> Path:
    """Resolve the requested dataset filename or fall back to the newest export."""
    files = discover_dataset_paths()
    if not files:
        raise FileNotFoundError("No dataset files were found in the output directory.")

    if not csv_name:
        return files[0]

    for candidate in files:
        if candidate.name == csv_name:
            return candidate

    raise FileNotFoundError(f"Dataset file not found: {csv_name}")


def _dataset_integrity_metadata(dataset_path: Path) -> dict[str, str | None]:
    """Return disclosed metadata state without promoting raw viewer reads."""
    default = {
        "dataset_id": None,
        "integrity_status": "unknown",
        "dataset_facts_status": "unknown",
        "row_scope_status": "unknown",
    }
    if _DATA_DIR_OVERRIDE is not None or _CSV_MODE:
        return default
    storage = get_storage_backend()
    if storage is None:
        return default
    selected = dataset_path.expanduser().absolute()
    for record in storage.list_datasets(limit=VIEWER_DATASET_DISCOVERY_LIMIT):
        path = Path(record.location).expanduser().absolute()
        if path != selected:
            continue
        return {
            "dataset_id": record.dataset_id,
            "integrity_status": evaluate_option_chain_integrity_status(record).value,
            "dataset_facts_status": evaluate_option_chain_dataset_facts_status(record).value,
            "row_scope_status": evaluate_option_chain_row_scope_status(record).value,
        }
    return default


def _integrity_dataset_cards(dataset_path: Path) -> list[DatasetCard]:
    """Build explicit integrity-state disclosure for non-authoritative raw views."""
    metadata = _dataset_integrity_metadata(dataset_path)
    return [
        {
            "name": "Integrity Status",
            "value": str(metadata["integrity_status"]),
            "description": (
                "Effective storage integrity metadata for this artifact. The viewer "
                "reads raw rows for inspection and does not validate or promote them."
            ),
        },
        {
            "name": "Dataset Facts Status",
            "value": str(metadata["dataset_facts_status"]),
            "description": (
                "Availability of content-bound neutral facts in storage metadata; "
                "unknown means they must not be treated as verified."
            ),
        },
        {
            "name": "Row Scope Status",
            "value": str(metadata["row_scope_status"]),
            "description": (
                "Availability of provider-neutral acquisition/filter-scope metadata; "
                "unknown means source completeness cannot be asserted."
            ),
        },
    ]


def _positions_sidecar_for_dataset(dataset_path: Path) -> Path | None:
    """Return the run positions sidecar associated with a dataset path."""
    if dataset_path.parent.name != "output":
        return None
    candidate = dataset_path.parent.parent / "positions.csv"
    if candidate.exists() and candidate.is_file():
        return candidate
    return None


def _positions_source_label(positions_path: Path) -> str:
    """Return a non-local-path label for a positions sidecar."""
    if positions_path.name == "positions.csv" and positions_path.parent.name:
        output_dir = positions_path.parent / "output"
        if output_dir.exists() and output_dir.is_dir():
            return f"{positions_path.parent.name}/positions.csv"
    return positions_path.name


def load_field_reference_markdown() -> str:
    """Load the dedicated field-reference document used by the viewer."""
    return load_viewer_markdown("FIELD_REFERENCE.md", FIELD_REFERENCE_PATH)


def load_viewer_markdown(filename: str, source_path: Path) -> str:
    """Load viewer markdown from source checkout or packaged fallback docs."""
    if source_path.exists():
        return source_path.read_text(encoding="utf-8")
    return (
        resources.files("opx_chain")
        .joinpath("docs", filename)
        .read_text(encoding="utf-8")
    )


def extract_field_descriptions() -> dict[str, str]:
    """Parse user-guide bullet entries into per-field viewer descriptions."""
    descriptions: dict[str, str] = {}
    canonical_columns = set(CANONICAL_EXPORT_COLUMNS)
    pattern = re.compile(r"^- `([^`]+)`: (.+)$")
    for line in load_field_reference_markdown().splitlines():
        match = pattern.match(line.strip())
        if match and match.group(1) in canonical_columns:
            descriptions[match.group(1)] = match.group(2)
    return descriptions


def normalize_value(value: Any) -> Any:
    """Convert pandas and NumPy scalar values into JSON-serializable values."""
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    normalized = to_python_scalar(value)
    if isinstance(normalized, float) and not math.isfinite(normalized):
        return None
    return normalized


def normalize_row_value(column: str, value: Any) -> Any:
    """Normalize row values, preserving integer semantics for whole-day fields."""
    normalized = normalize_value(value)
    if column in INTEGER_VIEWER_COLUMNS and normalized is not None:
        number = coerce_scalar_number(normalized)
        return int(number) if number is not None else None
    return normalized


def is_truthy(value: Any) -> bool:
    """Interpret common string and numeric truthy values from CSV content."""
    return coerce_bool_or_default(value, default=False) is True


def coerce_number(series: Any) -> pd.Series:
    """Coerce an arbitrary series-like input into numeric pandas values."""
    if isinstance(series, pd.Series) and is_bool_dtype(series):
        return pd.Series(float("nan"), index=series.index)
    numbers = pd.to_numeric(series, errors="coerce")
    if not isinstance(numbers, pd.Series):
        numbers = pd.Series(numbers)
    if is_bool_dtype(numbers):
        return pd.Series(float("nan"), index=numbers.index)
    finite_mask = numbers.map(
        lambda value: pd.notna(value) and math.isfinite(float(value))
    )
    return numbers.where(finite_mask)


def coerce_scalar_number(value: Any) -> float | None:
    """Coerce a single scalar into a float while preserving missing values."""
    if isinstance(value, bool):
        return None
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return None
    coerced = float(number)
    return coerced if math.isfinite(coerced) else None


def build_freshness_summary(frame: pd.DataFrame, csv_path: Path) -> FreshnessSummary:
    """Build file-level freshness metadata for the current CSV snapshot."""
    _empty = pd.Series(dtype=float)
    _qa_col = frame.get("quote_age_seconds")
    _ua_col = frame.get("underlying_price_age_seconds")
    option_quote_ages = (
        pd.to_numeric(_qa_col, errors="coerce").dropna() if _qa_col is not None else _empty
    )
    underlying_quote_ages = (
        pd.to_numeric(_ua_col, errors="coerce").dropna() if _ua_col is not None else _empty
    )
    now = time.time()
    modified_at = csv_path.stat().st_mtime

    summary: FreshnessSummary = {
        "file_age_seconds": max(0.0, now - modified_at),
        "file_modified_at": format_utc_z_seconds(
            datetime.fromtimestamp(modified_at, tz=timezone.utc)
        ),
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


def get_single_value(frame: pd.DataFrame, column: str) -> str | None:
    """Return a dataset-wide constant value when exactly one non-null value exists."""
    if column not in frame.columns:
        return None
    values = frame[column].dropna().astype(str).unique().tolist()
    return values[0] if len(values) == 1 else None


def build_dataset_cards(frame: pd.DataFrame, descriptions: dict[str, str]) -> list[DatasetCard]:
    """Build header cards for fields that have one dataset-wide value."""
    cards: list[DatasetCard] = []
    for column in DATASET_CARD_COLUMNS:
        value = get_single_value(frame, column)
        if value is None:
            continue
        cards.append(
            {
                "name": column,
                "value": value,
                "description": descriptions.get(column, REFERENCE_MISSING_DESCRIPTION),
            }
        )
    return cards


def _short_fingerprint(value: str) -> str:
    """Return a readable fingerprint label while preserving the full value in card details."""
    return f"{value[:12]}..." if value else "none"


def _covered_stock_ticker_count(frame: pd.DataFrame, position_set: PositionSet) -> int:
    """Return how many stock tickers have rows in the selected dataset."""
    if "underlying_symbol" not in frame.columns:
        return 0
    dataset_tickers = set(frame["underlying_symbol"].dropna().astype(str).str.upper())
    return len(position_set.stock_tickers & dataset_tickers)


def _covered_option_contract_count(frame: pd.DataFrame, position_set: PositionSet) -> int:
    """Return how many held option contracts are present in the selected dataset."""
    required_columns = {"underlying_symbol", "expiration_date", "option_type", "strike"}
    if not required_columns.issubset(frame.columns):
        return 0

    underlying = frame["underlying_symbol"].astype(str).str.upper()
    expirations = frame["expiration_date"].astype(str)
    option_types = frame["option_type"].astype(str).str.lower()
    strikes = pd.to_numeric(frame["strike"], errors="coerce")

    covered = 0
    for key in position_set.option_keys:
        mask = (
            (underlying == key.ticker)
            & (expirations == key.expiration_date)
            & (option_types == key.option_type)
            & ((strikes - key.strike).abs() < STRIKE_MATCH_TOLERANCE)
        )
        if bool(mask.any()):
            covered += 1
    return covered


def build_positions_dataset_cards(frame: pd.DataFrame, dataset_path: Path) -> list[DatasetCard]:
    """Build lightweight positions metadata cards for the selected dataset."""
    positions_path = _positions_sidecar_for_dataset(dataset_path)
    if positions_path is None:
        return [
            {
                "name": "Positions",
                "value": "not captured",
                "description": (
                    "No run positions sidecar was found for this dataset. "
                    "opx-chain intentionally does not browse portfolio rows."
                ),
            },
            {
                "name": "Position Fingerprint",
                "value": "none",
                "description": "No parsed-position fingerprint is available without a run sidecar.",
            },
            {
                "name": "Position Coverage",
                "value": "not available",
                "description": "Coverage cannot be computed without a run positions sidecar.",
            },
        ]

    position_set = load_positions(positions_path)
    stock_count = len(position_set.stock_tickers)
    option_count = len(position_set.option_keys)
    fingerprint = positions_fingerprint(positions_path, position_set)
    stock_covered = _covered_stock_ticker_count(frame, position_set)
    option_covered = _covered_option_contract_count(frame, position_set)
    source_label = _positions_source_label(positions_path)
    return [
        {
            "name": "Positions",
            "value": f"{stock_count} stocks / {option_count} options",
            "description": (
                f"Parsed from {source_label}. Rich positions browsing belongs in opx-strategy."
            ),
        },
        {
            "name": "Position Fingerprint",
            "value": _short_fingerprint(fingerprint),
            "description": f"Full parsed-position fingerprint: {fingerprint or 'none'}.",
        },
        {
            "name": "Position Coverage",
            "value": (
                f"{stock_covered}/{stock_count} stocks / "
                f"{option_covered}/{option_count} options"
            ),
            "description": (
                "Coverage is limited to whether the selected chain artifact includes "
                "rows for parsed stock tickers and exact held option contracts."
            ),
        },
    ]


def format_percent(value: float | None) -> float | None:
    """Convert a ratio into a percentage rounded for frontend display."""
    return None if value is None else round(value * 100, 1)


def normalize_opportunity(row: dict[str, Any] | None) -> OpportunitySummary | None:
    """Convert a row dict into the compact opportunity-summary schema."""
    if row is None:
        return None
    return {
        "contract_symbol": row.get("contract_symbol"),
        "option_type": row.get("option_type"),
        "expiration_date": row.get("expiration_date"),
        "strike": coerce_scalar_number(row.get("strike")),
        "premium_reference_price": coerce_scalar_number(row.get("premium_reference_price")),
        "return_on_margin_annualized_pct": format_percent(
            coerce_scalar_number(row.get("return_on_margin_annualized"))
        ),
        "probability_itm_pct": format_percent(coerce_scalar_number(row.get("probability_itm"))),
        "delta_abs": coerce_scalar_number(row.get("delta_abs")),
        "strike_distance_pct": format_percent(coerce_scalar_number(row.get("strike_distance_pct"))),
        "risk_level": row.get("risk_level"),
        "spread_score": coerce_scalar_number(row.get("spread_score")),
        "dte_score": coerce_scalar_number(row.get("dte_score")),
        "theta_efficiency": coerce_scalar_number(row.get("theta_efficiency")),
        "quote_quality_score": coerce_scalar_number(row.get("quote_quality_score")),
        "option_score": coerce_scalar_number(row.get("option_score")),
        "final_score": coerce_scalar_number(row.get("final_score")),
        "bid_ask_spread_pct_of_mid": format_percent(
            coerce_scalar_number(row.get("bid_ask_spread_pct_of_mid"))
        ),
        "event_risk_score": coerce_scalar_number(row.get("event_risk_score")),
        "summary": row.get("_summary"),
    }


def attach_opportunity_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """Attach a one-line summary string used in summary highlight cards."""
    frame = frame.copy()
    empty_metric = pd.Series(index=frame.index, dtype="float64")
    rom = (
        coerce_number(frame.get("return_on_margin_annualized", empty_metric))
        .mul(100)
        .round(1)
        .astype("string")
        .fillna("—")
    )
    itm = (
        coerce_number(frame.get("probability_itm", empty_metric))
        .mul(100)
        .round(1)
        .astype("string")
        .fillna("—")
    )
    spread = (
        coerce_number(frame.get("bid_ask_spread_pct_of_mid", empty_metric))
        .mul(100)
        .round(1)
        .astype("string")
        .fillna("—")
    )
    frame["_summary"] = "ROM " + rom + "% · ITM " + itm + "% · spread " + spread + "%"
    return frame


def screen_primary_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    """Prefer rows passing the primary screen when that flag exists."""
    candidates = frame.copy()
    if "passes_primary_screen" not in candidates.columns:
        return candidates
    return candidates[candidates["passes_primary_screen"].map(is_truthy)]


def pick_profitable_opportunity(frame: pd.DataFrame) -> OpportunitySummary | None:
    """Select the highest-ROM opportunity after primary-screen filtering."""
    if frame.empty:
        return None
    candidates = screen_primary_candidates(frame)
    candidates = attach_opportunity_summary(candidates)
    empty_metric = pd.Series(index=candidates.index, dtype="float64")
    candidates["_rom"] = coerce_number(candidates.get("return_on_margin_annualized", empty_metric))
    candidates["_score"] = coerce_number(candidates.get("option_score", empty_metric)).fillna(0)
    candidates["_final_score"] = coerce_number(candidates.get("final_score", candidates["_score"]))
    candidates["_final_score"] = candidates["_final_score"].fillna(candidates["_score"])
    candidates["_quality"] = coerce_number(
        candidates.get("quote_quality_score", empty_metric)
    ).fillna(0)
    candidates = candidates.sort_values(
        by=["_rom", "_final_score", "_quality"],
        ascending=[False, False, False],
        na_position="last",
    )
    return normalize_opportunity(candidates.iloc[0].to_dict()) if not candidates.empty else None


def pick_moderate_risk_opportunity(frame: pd.DataFrame) -> OpportunitySummary | None:
    """Select a lower-delta, primary-screen candidate when possible."""
    if frame.empty:
        return None
    config = get_runtime_config()
    candidates = screen_primary_candidates(frame)
    empty_metric = pd.Series(index=candidates.index, dtype="float64")
    candidates["_delta"] = coerce_number(candidates.get("delta_abs", empty_metric))
    candidates["_rom"] = coerce_number(candidates.get("return_on_margin_annualized", empty_metric))
    candidates["_score"] = coerce_number(candidates.get("option_score", empty_metric)).fillna(0)
    candidates["_final_score"] = coerce_number(candidates.get("final_score", candidates["_score"]))
    candidates["_final_score"] = candidates["_final_score"].fillna(candidates["_score"])
    candidates["_spread"] = coerce_number(
        candidates.get("bid_ask_spread_pct_of_mid", empty_metric)
    )
    moderate = candidates[
        (candidates["_delta"].notna()) & (candidates["_delta"] <= 0.40)
        & (candidates["_spread"].notna())
        & (candidates["_spread"] <= config.max_spread_pct_of_mid)
    ]
    if moderate.empty:
        moderate = candidates[(candidates["_delta"].notna()) & (candidates["_delta"] <= 0.45)]
    moderate = attach_opportunity_summary(moderate)
    moderate = moderate.sort_values(
        by=["_final_score", "_rom", "_delta"],
        ascending=[False, False, True],
        na_position="last",
    )
    return normalize_opportunity(moderate.iloc[0].to_dict()) if not moderate.empty else None


def _compute_direction_alignment(day_change_pct: Any, option_type: str) -> pd.Series:
    """Return signed alignment so opposite-direction momentum is penalized."""
    changes = coerce_number(day_change_pct).fillna(0.0)
    if option_type == OPTION_TYPE_CALL:
        return changes
    return -changes


def pick_high_conviction_opportunity(
    frame: pd.DataFrame,
    option_type: str,
) -> OpportunitySummary | None:
    """Select the strongest directional idea for one option side."""
    if frame.empty:
        return None
    candidates = screen_primary_candidates(frame)
    if "option_type" not in candidates.columns:
        return None
    candidates = candidates[candidates["option_type"].astype(str) == option_type].copy()
    if candidates.empty:
        return None

    candidates = attach_opportunity_summary(candidates)
    empty_metric = pd.Series(index=candidates.index, dtype="float64")
    candidates["_rom"] = coerce_number(
        candidates.get("return_on_margin_annualized", empty_metric)
    ).fillna(0.0)
    candidates["_final_score"] = coerce_number(
        candidates.get("final_score", candidates.get("option_score", empty_metric))
    ).fillna(0.0)
    candidates["_quality"] = coerce_number(
        candidates.get("quote_quality_score", empty_metric)
    ).fillna(0.0)
    candidates["_spread_score"] = coerce_number(
        candidates.get("spread_score", empty_metric)
    ).fillna(0.0)
    candidates["_strike_distance_pct"] = coerce_number(
        candidates.get("strike_distance_pct", empty_metric)
    )
    candidates["_delta_abs"] = coerce_number(candidates.get("delta_abs", empty_metric))
    candidates["_direction_alignment"] = _compute_direction_alignment(
        candidates.get("underlying_day_change_pct"),
        option_type,
    )
    direction_alignment_weight = 300.0
    delta_target = 0.40 if option_type == OPTION_TYPE_CALL else 0.35
    candidates["_distance_penalty"] = candidates["_strike_distance_pct"].fillna(1.0)
    candidates["_delta_penalty"] = (candidates["_delta_abs"] - delta_target).abs().fillna(1.0)
    candidates["_conviction_score"] = (
        candidates["_final_score"]
        + (candidates["_quality"] * 2.0)
        + (candidates["_spread_score"] * 0.5)
        + (candidates["_direction_alignment"] * direction_alignment_weight)
        - (candidates["_distance_penalty"] * 100.0)
        - (candidates["_delta_penalty"] * 40.0)
        + (candidates["_rom"] * 5.0)
    )
    candidates = candidates.sort_values(
        by=[
            "_conviction_score",
            "_final_score",
            "_quality",
            "_spread_score",
            "_strike_distance_pct",
        ],
        ascending=[False, False, False, False, True],
        na_position="last",
    )
    return normalize_opportunity(candidates.iloc[0].to_dict()) if not candidates.empty else None


def build_market_context(
    ticker: str,
    underlying_price: float | None,
    day_change_pct: float | None,
) -> str:
    """Summarize the latest underlying snapshot in plain language."""
    underlying_price = coerce_scalar_number(underlying_price)
    day_change_pct = coerce_scalar_number(day_change_pct)
    if underlying_price is None and day_change_pct is None:
        return f"{ticker} has no recent underlying snapshot in this file."
    if underlying_price is None:
        return f"{ticker} last underlying price was unavailable."
    if day_change_pct is None:
        return f"{ticker} last underlying price was {underlying_price:.2f}."
    direction = "up" if day_change_pct >= 0 else "down"
    return (
        f"{ticker} last underlying price was {underlying_price:.2f}, "
        f"{direction} {abs(day_change_pct) * 100:.1f}% versus previous close."
    )


def build_latest_status(
    day_change_pct: float | None,
    median_iv_pct: float | None,
    historical_volatility_pct: float | None,
) -> str:
    """Build a short status label summarizing move and volatility context."""
    day_change_pct = coerce_scalar_number(day_change_pct)
    median_iv_pct = coerce_scalar_number(median_iv_pct)
    historical_volatility_pct = coerce_scalar_number(historical_volatility_pct)
    if (
        day_change_pct is None
        and median_iv_pct is None
        and historical_volatility_pct is None
    ):
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

    if (
        median_iv_pct is not None
        and historical_volatility_pct is not None
        and historical_volatility_pct > 0
    ):
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


def extract_ticker_event_fields(
    frame: pd.DataFrame,
) -> tuple[str | None, float | None, bool | None]:
    """Pull the per-ticker event summary fields from a ticker frame."""
    earnings_dates = (
        frame["next_earnings_date"].dropna().astype(str).unique().tolist()
        if "next_earnings_date" in frame.columns
        else []
    )
    next_earnings_date_value = earnings_dates[0] if earnings_dates else None
    earnings_estimated_values = (
        frame["next_earnings_date_is_estimated"].dropna().tolist()
        if "next_earnings_date_is_estimated" in frame.columns
        else []
    )
    next_earnings_date_is_estimated = (
        is_truthy(earnings_estimated_values[0]) if earnings_estimated_values else None
    )
    event_risk_nums = coerce_number(frame.get("event_risk_score")).dropna()
    event_risk_value = None if event_risk_nums.empty else float(event_risk_nums.iloc[0])
    return next_earnings_date_value, event_risk_value, next_earnings_date_is_estimated


def _count_matching_values(frame: pd.DataFrame, column: str, expected: str) -> int:
    """Return a safe equality count for optional string columns."""
    if column not in frame.columns:
        return 0
    return int(frame[column].eq(expected).sum())


def _nunique_column(frame: pd.DataFrame, column: str) -> int:
    """Return the number of unique non-null values for an optional column."""
    if column not in frame.columns:
        return 0
    return int(frame[column].nunique())


def build_ticker_summary(  # pylint: disable=too-many-locals
    ticker: str, frame: pd.DataFrame,
) -> TickerSummary:
    """Aggregate one ticker's rows into the Summary tab record shape."""
    underlying_price = coerce_number(frame.get("underlying_price")).dropna()
    day_change = coerce_number(frame.get("underlying_day_change_pct")).dropna()
    implied_volatility = coerce_number(frame.get("implied_volatility")).dropna()
    hv = coerce_number(frame.get("historical_volatility")).dropna()
    profitable = pick_profitable_opportunity(frame)
    moderate = pick_moderate_risk_opportunity(frame)
    high_conviction_call = pick_high_conviction_opportunity(frame, OPTION_TYPE_CALL)
    high_conviction_put = pick_high_conviction_opportunity(frame, OPTION_TYPE_PUT)
    underlying_price_value = None if underlying_price.empty else float(underlying_price.iloc[0])
    day_change_value = None if day_change.empty else float(day_change.iloc[0])
    median_iv_value = (
        None if implied_volatility.empty else round(float(implied_volatility.median()) * 100, 1)
    )
    hv_value = None if hv.empty else round(float(hv.iloc[0]) * 100, 1)
    (
        next_earnings_date_value,
        event_risk_value,
        next_earnings_date_is_estimated,
    ) = extract_ticker_event_fields(frame)
    return {
        "ticker": ticker,
        "row_count": int(len(frame.index)),
        "call_count": _count_matching_values(frame, "option_type", OPTION_TYPE_CALL),
        "put_count": _count_matching_values(frame, "option_type", OPTION_TYPE_PUT),
        "expiration_count": _nunique_column(frame, "expiration_date"),
        "underlying_price": underlying_price_value,
        "underlying_day_change_pct": format_percent(day_change_value),
        "median_implied_volatility_pct": median_iv_value,
        "historical_volatility_pct": hv_value,
        "iv_hv_ratio": (
            None
            if median_iv_value is None or hv_value in (None, 0)
            else round(median_iv_value / hv_value, 2)
        ),
        "next_earnings_date": next_earnings_date_value,
        "next_earnings_date_is_estimated": next_earnings_date_is_estimated,
        "event_risk_score": event_risk_value,
        "latest_status": build_latest_status(day_change_value, median_iv_value, hv_value),
        "market_context": build_market_context(ticker, underlying_price_value, day_change_value),
        "profitable_opportunity": profitable,
        "moderate_risk_opportunity": moderate,
        "high_conviction_call": high_conviction_call,
        "high_conviction_put": high_conviction_put,
    }


def sort_ticker_candidates(
    items: list[TickerSummary],
    opportunity_key: str,
) -> list[TickerSummary]:
    """Sort ticker summaries by the chosen opportunity ROM value descending."""
    def _rom_value(item: TickerSummary) -> float:
        opportunity = item[opportunity_key]
        if opportunity is None:
            return -(10**9)
        value = opportunity.get("return_on_margin_annualized_pct")
        return -(10**9) if value is None else float(value)

    return sorted(
        items,
        key=_rom_value,
        reverse=True,
    )


def build_summary_payload(csv_name: str | None = None) -> SummaryPayload:
    """Build the compact per-ticker summary payload used by the Summary tab."""
    csv_path = resolve_csv_path(csv_name)
    frame = read_dataset_file(csv_path)
    if "underlying_symbol" not in frame.columns:
        return {
            "selected_file": csv_path.name,
            "tickers": [],
            "highlights": {"most_profitable": None, "moderate_risk": None},
        }
    tickers = sorted(frame["underlying_symbol"].dropna().astype(str).unique())

    ticker_summaries: list[TickerSummary] = []
    for ticker in tickers:
        ticker_frame = frame[frame["underlying_symbol"].astype(str) == ticker].copy()
        ticker_summaries.append(build_ticker_summary(ticker, ticker_frame))

    profitable_candidates = [
        item for item in ticker_summaries if item["profitable_opportunity"]
    ]
    moderate_candidates = [
        item for item in ticker_summaries if item["moderate_risk_opportunity"]
    ]
    profitable_candidates = sort_ticker_candidates(
        profitable_candidates, "profitable_opportunity"
    )
    moderate_candidates = sort_ticker_candidates(
        moderate_candidates, "moderate_risk_opportunity"
    )
    return {
        "selected_file": csv_path.name,
        "tickers": ticker_summaries,
        "highlights": {
            "most_profitable": profitable_candidates[0] if profitable_candidates else None,
            "moderate_risk": moderate_candidates[0] if moderate_candidates else None,
        },
    }


def build_column_definitions(
    frame: pd.DataFrame,
    descriptions: dict[str, str],
) -> list[ColumnDefinition]:
    """Build frontend column metadata including descriptions and numeric flags."""
    return [
        {
            "name": column,
            "description": descriptions.get(column, REFERENCE_MISSING_DESCRIPTION),
            "is_numeric": bool(
                is_numeric_dtype(frame[column]) and not is_bool_dtype(frame[column])
            ),
        }
        for column in frame.columns
    ]


def load_csv_payload(csv_name: str | None = None) -> CsvPayload:
    """Load the current dataset and serialize the table payload consumed by the browser."""
    csv_path = resolve_csv_path(csv_name)
    frame = read_dataset_file(csv_path)
    freshness_summary = build_freshness_summary(frame, csv_path)
    descriptions = extract_field_descriptions()
    dataset_cards = [
        *_integrity_dataset_cards(csv_path),
        *build_dataset_cards(frame, descriptions),
        *build_positions_dataset_cards(frame, csv_path),
    ]
    rows = [
        {column: normalize_row_value(column, value) for column, value in record.items()}
        for record in frame.to_dict(orient="records")
    ]
    columns = build_column_definitions(frame, descriptions)
    return {
        "selected_file": csv_path.name,
        "row_count": len(rows),
        "columns": columns,
        "rows": rows,
        "freshness_summary": freshness_summary,
        "dataset_cards": dataset_cards,
    }


def make_file_listing() -> list[dict[str, Any]]:
    """Return available dataset files with size and modified timestamps."""
    files = discover_dataset_paths()
    listings = []
    for path in files:
        stat_result = path.stat()
        listings.append({
            "name": path.name,
            "size_bytes": stat_result.st_size,
            "modified_at": stat_result.st_mtime,
            **_dataset_integrity_metadata(path),
        })
    return listings


class ViewerRequestHandler(SimpleHTTPRequestHandler):
    """Static-file and JSON API handler for the local CSV viewer."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_ROOT), **kwargs)

    def end_headers(self):
        """Disable caching so the viewer always serves fresh local data."""
        self.send_header(
            "Cache-Control",
            "no-store, no-cache, must-revalidate, max-age=0",
        )
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def _respond_payload(
        self,
        payload_factory,
        csv_name: str | None = None,
        *,
        error_label: str = "dataset",
    ) -> None:
        """Run a payload factory and translate errors into JSON error responses."""
        try:
            payload = payload_factory(csv_name)
        except FileNotFoundError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
            return
        except Exception as exc:  # pylint: disable=broad-except
            self.respond_json(
                {"error": f"Failed to load {error_label}: {exc}"},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        self.respond_json(payload)

    def do_GET(self) -> None:
        """Serve viewer JSON endpoints or fall back to static assets."""
        parsed = urlparse(self.path)
        if parsed.path == "/api/files":
            self._respond_payload(
                lambda _csv_name: {"files": make_file_listing()},
                error_label="file listing",
            )
            return
        if parsed.path == "/api/data":
            query = parse_qs(parsed.query)
            csv_name = query.get("file", [None])[0]
            self._respond_payload(load_csv_payload, csv_name)
            return
        if parsed.path == "/api/reference":
            self._respond_payload(
                lambda _csv_name: {"markdown": load_field_reference_markdown()},
                error_label="field reference",
            )
            return
        if parsed.path == "/api/summary":
            query = parse_qs(parsed.query)
            csv_name = query.get("file", [None])[0]
            self._respond_payload(build_summary_payload, csv_name, error_label="summary")
            return
        if parsed.path == "/":
            self.path = "/index.html"  # pylint: disable=attribute-defined-outside-init
        super().do_GET()

    def respond_json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        """Serialize and send a JSON response for one of the API endpoints."""
        encoded = dumps_sanitized_json(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:  # pylint: disable=redefined-builtin
        """Optionally suppress request logs when running screenshot automation."""
        if os.environ.get("OPX_VIEWER_QUIET") == "1":
            return
        super().log_message(format, *args)


def _display_host_for_bind(host: str) -> str:
    """Return a browser-safe destination host for a viewer bind host."""
    stripped_host = host.strip()
    try:
        bind_ip = ipaddress.ip_address(stripped_host.strip("[]"))
    except ValueError:
        return stripped_host
    if bind_ip.is_unspecified:
        return "127.0.0.1" if bind_ip.version == 4 else "[::1]"
    if bind_ip.version == 6:
        return f"[{bind_ip.compressed}]"
    return bind_ip.compressed


def _viewer_url(host: str, port: int) -> str:
    """Build the user-facing viewer URL for banners and browser launches."""
    return f"http://{_display_host_for_bind(host)}:{port}"


def serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    """Run the local viewer HTTP server."""
    server = ThreadingHTTPServer((host, port), ViewerRequestHandler)
    print(f"Options Screener running at {_viewer_url(host, port)}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def parse_args(argv=None):
    """Parse viewer CLI arguments."""
    if argv is None and "PYTEST_CURRENT_TEST" in os.environ:
        argv = []
    parser = argparse.ArgumentParser(
        prog="opx-view",
        description="Serve the local Options Screener UI.",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help="Open the viewer URL in the default browser after startup.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help=(
            "Directory to scan for dataset files (.csv, .parquet). "
            "Overrides the storage backend and the default XDG data-dir runs directory."
        ),
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help=(
            "Skip the storage backend and read timestamped CSV exports "
            "(options_engine_output_*.csv) from the XDG data-dir runs directory directly."
        ),
    )
    return parser.parse_args(argv)


def open_viewer_in_browser(host: str, port: int) -> None:
    """Open the viewer URL in the default browser."""
    webbrowser.open(_viewer_url(host, port), new=2)


def _resolve_viewer_port(config_port: int) -> int:
    """Return a validated viewer port from environment or config."""
    raw_port = os.environ.get("OPX_VIEWER_PORT", str(config_port))
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise ValueError(
            f"Invalid OPX_VIEWER_PORT={raw_port!r}; expected an integer port 1-65535."
        ) from exc
    if not 1 <= port <= 65535:
        raise ValueError(
            f"Invalid OPX_VIEWER_PORT={raw_port!r}; expected an integer port 1-65535."
        )
    return port


def _is_valid_hostname(host: str) -> bool:
    """Return whether host is syntactically valid for local bind resolution."""
    if len(host) > 253 or any(char.isspace() for char in host):
        return False
    labels = host.rstrip(".").split(".")
    if not labels or any(not label for label in labels):
        return False
    if len(labels) > 1 and all(label.isdigit() for label in labels):
        return False
    return all(_HOST_LABEL_RE.fullmatch(label) for label in labels)


def _resolve_viewer_host(config_host: str) -> str:
    """Return a validated viewer bind host from environment or config."""
    source = "OPX_VIEWER_HOST" if "OPX_VIEWER_HOST" in os.environ else "settings.viewer_host"
    raw_host = os.environ.get("OPX_VIEWER_HOST", config_host)
    host = raw_host.strip()
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1].strip()
    if not host:
        raise ValueError(
            f"Invalid {source}={raw_host!r}; expected an IP address or hostname."
        )
    try:
        return ipaddress.ip_address(host).compressed
    except ValueError:
        if ":" in host or not _is_valid_hostname(host):
            raise ValueError(
                f"Invalid {source}={raw_host!r}; expected an IP address or hostname."
            ) from None
    return host


def main(argv=None) -> None:
    """Start the local viewer using runtime config with optional env overrides."""
    global _DATA_DIR_OVERRIDE, _CSV_MODE  # pylint: disable=global-statement
    args = parse_args(argv)
    if args.data_dir is not None:
        _DATA_DIR_OVERRIDE = args.data_dir.expanduser().resolve()
    else:
        _DATA_DIR_OVERRIDE = None
    _CSV_MODE = args.csv
    config = get_runtime_config()
    host = _resolve_viewer_host(config.viewer_host)
    port = _resolve_viewer_port(config.viewer_port)
    if args.open:
        threading.Timer(0.2, open_viewer_in_browser, args=(host, port)).start()
    serve(host=host, port=port)


if __name__ == "__main__":
    main()
