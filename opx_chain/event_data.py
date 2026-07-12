"""Event-data snapshots and overlays for canonical option-chain rows."""
# pylint: disable=line-too-long,too-many-instance-attributes,too-many-return-statements
# pylint: disable=too-many-arguments,too-many-locals,too-many-nested-blocks,duplicate-code
# pylint: disable=too-many-lines

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
import uuid

import numpy as np
import pandas as pd

from opx_chain.config import (
    SUPPORTED_PROVIDERS,
    get_runtime_config,
    get_runtime_config_override,
    set_runtime_config_override,
)
from opx_chain.fetch import append_ticker_event_fields
from opx_chain.json_utils import dumps_sanitized_json, loads_strict_json
from opx_chain.metrics import add_event_risk_flags
from opx_chain.paths import get_data_dir
from opx_chain.providers import get_data_provider_by_name
from opx_chain.providers.base import date_arg
from opx_chain.runtime_args import strict_bool_arg, timezone_aware_datetime_arg
from opx_chain.storage.atomic import atomic_write_text
from opx_chain.tickers import is_valid_ticker
from opx_chain.timestamps import format_utc_z_seconds, parse_iso_datetime

EVENT_DATA_SCHEMA_VERSION = 1
EVENT_DATA_SUPPORTED_PROVIDERS = frozenset({"yfinance", "marketdata"})
EVENT_DATA_PROVIDER_CHOICES = frozenset({"same_as_chain", *EVENT_DATA_SUPPORTED_PROVIDERS})
EVENT_DATA_FETCH_MODES = frozenset({"auto", "fetch_latest"})
EVENT_DATA_FRESHNESS_POLICY = "trading_day"
EVENT_DATA_TICKER_UNIVERSE_SOURCE_DEFAULT = "caller_supplied_tickers"
EVENT_SNAPSHOT_LATEST_FILENAME = "event_snapshot_latest.json"
EVENT_SNAPSHOT_DIRNAME = "event_snapshots"
_UNUSABLE_RETAINED_STATUSES = frozenset({"provider_error", "invalid_payload"})
_INSPECTABLE_RETAINED_STATUSES = frozenset({"ready", "partial", "missing"})
_REUSABLE_RETAINED_STATUSES = frozenset({"ready", "partial"})
_USABLE_RECORD_STATUSES = frozenset({"ready", "no_known_event"})

_EVENT_FIELD_DEFAULTS: dict[str, Any] = {
    "next_earnings_date": None,
    "next_earnings_date_is_estimated": None,
    "next_earnings_date_source": None,
    "next_earnings_date_confidence": None,
    "next_ex_div_date": None,
    "next_ex_div_date_source": None,
    "next_ex_div_date_confidence": None,
    "dividend_amount": np.nan,
}
EVENT_OVERLAY_COLUMNS = (
    *tuple(_EVENT_FIELD_DEFAULTS),
    "days_to_earnings",
    "earnings_within_5d",
    "earnings_within_10d",
    "days_to_ex_div",
    "ex_div_within_3d",
    "event_risk_score",
    "event_data_provider",
    "event_data_snapshot_id",
    "event_data_fetched_at",
    "event_data_status",
)


@dataclass(frozen=True)
class EventDataSnapshotResult:
    """Resolved event snapshot plus source-health metadata."""

    status: str
    provider: str | None
    resolved_provider: str | None
    fetch_mode: str
    reused: bool
    snapshot_id: str | None
    fetched_at: str | None
    path: Path | None
    payload: dict[str, Any]


def normalize_event_data_provider(
    provider: str | None,
    *,
    chain_provider: str | None = None,
    default: str = "yfinance",
) -> tuple[str, str]:
    """Return `(selected_provider, resolved_provider)` for event data."""
    if provider is not None and not isinstance(provider, str):
        raise ValueError("event_data_provider must be a string")
    selected = (provider or default).strip().lower()
    if selected not in EVENT_DATA_PROVIDER_CHOICES:
        allowed = ", ".join(sorted(EVENT_DATA_PROVIDER_CHOICES))
        raise ValueError(f"event_data_provider must be one of: {allowed}")
    if selected == "same_as_chain":
        resolved = str(chain_provider or "").strip().lower()
        if not resolved:
            raise ValueError("event_data_provider='same_as_chain' requires chain_provider")
    else:
        resolved = selected
    if resolved not in SUPPORTED_PROVIDERS:
        allowed = ", ".join(sorted(SUPPORTED_PROVIDERS))
        raise ValueError(f"resolved event data provider must be one of: {allowed}")
    return selected, resolved


def normalize_event_data_fetch_mode(value: str | None, *, default: str = "auto") -> str:
    """Return a supported event-data fetch mode."""
    if value is not None and not isinstance(value, str):
        raise ValueError("event_data_fetch_mode must be a string")
    mode = (value or default).strip().lower()
    if mode not in EVENT_DATA_FETCH_MODES:
        allowed = ", ".join(sorted(EVENT_DATA_FETCH_MODES))
        raise ValueError(f"event_data_fetch_mode must be one of: {allowed}")
    return mode


def event_data_snapshot_dir(base_dir: Path | None = None) -> Path:
    """Return the durable event-snapshot directory."""
    return (base_dir or get_data_dir()) / EVENT_SNAPSHOT_DIRNAME


def event_data_latest_path(base_dir: Path | None = None) -> Path:
    """Return the latest event-snapshot alias path."""
    return (base_dir or get_data_dir()) / EVENT_SNAPSHOT_LATEST_FILENAME


def _normalize_tickers(tickers: tuple[str, ...] | list[str] | set[str]) -> tuple[str, ...]:
    if not isinstance(tickers, (list, tuple, set)):
        raise ValueError("event-data tickers must be a list, tuple, or set")
    normalized: list[str] = []
    for raw in tickers:
        if not isinstance(raw, str):
            raise ValueError("event-data ticker members must be strings")
        ticker = raw.strip().upper()
        if not ticker:
            continue
        if not is_valid_ticker(ticker):
            raise ValueError(f"invalid event-data ticker: {ticker!r}")
        normalized.append(ticker)
    return tuple(dict.fromkeys(sorted(normalized)))


def _normalize_ticker_universe_source(value: str | None) -> str:
    source = str(value or EVENT_DATA_TICKER_UNIVERSE_SOURCE_DEFAULT).strip()
    return source or EVENT_DATA_TICKER_UNIVERSE_SOURCE_DEFAULT


def _parse_date(value: Any) -> date | None:
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed.date()


def _canonical_date_string(value: Any) -> str | None:
    parsed = _parse_date(value)
    return parsed.isoformat() if parsed is not None else None


def _sanitize_event_fields(fields: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Normalize provider date fields and report malformed truthy values."""
    sanitized = dict(fields)
    invalid_fields: list[str] = []
    for key in ("next_earnings_date", "next_ex_div_date"):
        value = sanitized.get(key)
        if value is None or value is pd.NaT or value == "":
            sanitized[key] = None
            continue
        canonical = _canonical_date_string(value)
        if canonical is None:
            invalid_fields.append(key)
            sanitized[key] = None
        else:
            sanitized[key] = canonical
    return sanitized, invalid_fields


def _event_fields_from_snapshot(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    records = payload.get("records")
    if not isinstance(records, list):
        return {}
    by_ticker: dict[str, dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        ticker = str(record.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        fields = dict(_EVENT_FIELD_DEFAULTS)
        raw_fields = record.get("event_fields")
        if isinstance(raw_fields, dict):
            for key in _EVENT_FIELD_DEFAULTS:
                if key in raw_fields:
                    fields[key] = raw_fields[key]
        by_ticker[ticker] = fields
    return by_ticker


def _event_status_from_snapshot(payload: dict[str, Any]) -> dict[str, str]:
    records = payload.get("records")
    if not isinstance(records, list):
        return {}
    result: dict[str, str] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        ticker = str(record.get("ticker") or "").strip().upper()
        status = str(record.get("provider_status") or "").strip().lower()
        if ticker and status:
            result[ticker] = status
    return result


def _snapshot_file_paths(base_dir: Path | None = None) -> list[Path]:
    root = event_data_snapshot_dir(base_dir)
    if not root.exists():
        return []
    return sorted(root.glob("*.json"))


def _load_snapshot(path: Path) -> dict[str, Any] | None:
    try:
        payload = loads_strict_json(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _snapshot_path_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _snapshot_fetched_at_epoch(payload: dict[str, Any]) -> float | None:
    raw_value = payload.get("fetched_at")
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    try:
        return parse_iso_datetime(raw_value).timestamp()
    except ValueError:
        return None


def _snapshot_candidate_sort_key(candidate: tuple[Path, dict[str, Any]]) -> tuple[int, float, float, str]:
    path, payload = candidate
    fetched_at_epoch = _snapshot_fetched_at_epoch(payload)
    mtime = _snapshot_path_mtime(path)
    if fetched_at_epoch is not None:
        return (1, fetched_at_epoch, mtime, path.name)
    return (0, mtime, 0.0, path.name)


def _snapshot_candidates(base_dir: Path | None = None) -> list[tuple[Path, dict[str, Any]]]:
    candidates: list[tuple[Path, dict[str, Any]]] = []
    for path in _snapshot_file_paths(base_dir):
        payload = _load_snapshot(path)
        if payload is not None:
            candidates.append((path, payload))
    return sorted(candidates, key=_snapshot_candidate_sort_key, reverse=True)


def _snapshot_resolved_provider(payload: dict[str, Any]) -> str:
    return str(payload.get("resolved_provider") or payload.get("provider") or "").lower()


def _snapshot_selected_provider(payload: dict[str, Any]) -> str:
    return str(payload.get("provider") or "").strip().lower()


def _snapshot_status(payload: dict[str, Any]) -> str:
    return str(payload.get("status") or "").strip().lower()


def _snapshot_base_valid(payload: dict[str, Any]) -> bool:
    return (
        payload.get("artifact_type") == "event_data_snapshot"
        and payload.get("schema_version") == EVENT_DATA_SCHEMA_VERSION
    )


def _snapshot_providers_match(
    payload: dict[str, Any],
    *,
    selected_provider: str,
    resolved_provider: str,
) -> bool:
    if _snapshot_resolved_provider(payload) != resolved_provider:
        return False
    snapshot_selected = _snapshot_selected_provider(payload)
    return not snapshot_selected or snapshot_selected == selected_provider


def _snapshot_requested_tickers(payload: dict[str, Any]) -> tuple[str, ...] | None:
    try:
        return _normalize_tickers(payload.get("tickers_requested") or ())
    except ValueError:
        return None


def _snapshot_usable_tickers(payload: dict[str, Any]) -> tuple[str, ...] | None:
    records = payload.get("records")
    usable: list[str] = []
    if isinstance(records, list):
        for record in records:
            if not isinstance(record, dict):
                continue
            status = str(record.get("provider_status") or "").strip().lower()
            if status not in _USABLE_RECORD_STATUSES:
                continue
            ticker = str(record.get("ticker") or "").strip().upper()
            if ticker:
                usable.append(ticker)
        try:
            return _normalize_tickers(usable)
        except ValueError:
            return None
    try:
        return _normalize_tickers(payload.get("tickers_succeeded") or ())
    except ValueError:
        return None


def _snapshot_same_trading_day(payload: dict[str, Any], trading_date: date) -> bool:
    iso_date = trading_date.isoformat()
    return (
        str(payload.get("trading_date") or "") == iso_date
        and str(payload.get("fresh_through_trading_date") or "") == iso_date
    )


def _snapshot_matches(
    payload: dict[str, Any],
    *,
    selected_provider: str,
    resolved_provider: str,
    trading_date: date,
    tickers: tuple[str, ...],
) -> bool:
    if not _snapshot_base_valid(payload):
        return False
    if _snapshot_status(payload) not in _REUSABLE_RETAINED_STATUSES:
        return False
    if not _snapshot_providers_match(
        payload,
        selected_provider=selected_provider,
        resolved_provider=resolved_provider,
    ):
        return False
    usable = _snapshot_usable_tickers(payload)
    if usable is None or not set(tickers).issubset(set(usable)):
        return False
    if not _snapshot_same_trading_day(payload, trading_date):
        return False
    return True


def _snapshot_covers_provider_tickers(
    payload: dict[str, Any],
    *,
    selected_provider: str,
    resolved_provider: str,
    tickers: tuple[str, ...],
) -> bool:
    if not _snapshot_base_valid(payload):
        return False
    if _snapshot_status(payload) not in _REUSABLE_RETAINED_STATUSES:
        return False
    if not _snapshot_providers_match(
        payload,
        selected_provider=selected_provider,
        resolved_provider=resolved_provider,
    ):
        return False
    usable = _snapshot_usable_tickers(payload)
    if usable is None:
        return False
    return set(tickers).issubset(set(usable))


def _snapshot_source_health_matches(
    payload: dict[str, Any],
    *,
    selected_provider: str,
    resolved_provider: str,
    trading_date: date,
    tickers: tuple[str, ...],
) -> bool:
    if not _snapshot_base_valid(payload):
        return False
    if _snapshot_status(payload) not in _INSPECTABLE_RETAINED_STATUSES:
        return False
    if not _snapshot_providers_match(
        payload,
        selected_provider=selected_provider,
        resolved_provider=resolved_provider,
    ):
        return False
    requested = _snapshot_requested_tickers(payload)
    if requested is None or not set(tickers).issubset(set(requested)):
        return False
    return _snapshot_same_trading_day(payload, trading_date)


def _snapshot_provider_mismatch_matches(
    payload: dict[str, Any],
    *,
    selected_provider: str,
    resolved_provider: str,
    tickers: tuple[str, ...],
) -> bool:
    if not _snapshot_base_valid(payload):
        return False
    if _snapshot_status(payload) not in _REUSABLE_RETAINED_STATUSES:
        return False
    if _snapshot_providers_match(
        payload,
        selected_provider=selected_provider,
        resolved_provider=resolved_provider,
    ):
        return False
    usable = _snapshot_usable_tickers(payload)
    if usable is None:
        return False
    return set(tickers).issubset(set(usable))


def latest_event_data_snapshot(
    *,
    provider: str,
    selected_provider: str | None = None,
    trading_date: date | None = None,
    tickers: tuple[str, ...] | list[str] | set[str] = (),
    base_dir: Path | None = None,
) -> tuple[dict[str, Any] | None, Path | None]:
    """Return latest reusable same-trading-day event snapshot, if present."""
    resolved_date = (
        date_arg(trading_date, name="trading_date")
        if trading_date is not None
        else get_runtime_config().today
    )
    selected = selected_provider or provider
    normalized_tickers = _normalize_tickers(tickers)
    for path, payload in _snapshot_candidates(base_dir):
        if _snapshot_matches(
            payload,
            selected_provider=selected,
            resolved_provider=provider,
            trading_date=resolved_date,
            tickers=normalized_tickers,
        ):
            return payload, path
    return None, None


def _latest_same_day_event_data_snapshot_for_source_health(
    *,
    selected_provider: str,
    resolved_provider: str,
    trading_date: date,
    tickers: tuple[str, ...],
    base_dir: Path | None = None,
) -> tuple[dict[str, Any] | None, Path | None]:
    for path, payload in _snapshot_candidates(base_dir):
        if _snapshot_source_health_matches(
            payload,
            selected_provider=selected_provider,
            resolved_provider=resolved_provider,
            trading_date=trading_date,
            tickers=tickers,
        ):
            return payload, path
    return None, None


def latest_retained_event_data_snapshot(
    *,
    provider: str,
    selected_provider: str | None = None,
    tickers: tuple[str, ...] | list[str] | set[str] = (),
    base_dir: Path | None = None,
) -> tuple[dict[str, Any] | None, Path | None]:
    """Return latest usable retained event snapshot regardless of trading date."""
    selected = selected_provider or provider
    normalized_tickers = _normalize_tickers(tickers)
    for path, payload in _snapshot_candidates(base_dir):
        if _snapshot_covers_provider_tickers(
            payload,
            selected_provider=selected,
            resolved_provider=provider,
            tickers=normalized_tickers,
        ):
            return payload, path
    return None, None


def _latest_provider_mismatch_event_data_snapshot(
    *,
    selected_provider: str,
    resolved_provider: str,
    tickers: tuple[str, ...],
    base_dir: Path | None = None,
) -> tuple[dict[str, Any] | None, Path | None]:
    for path, payload in _snapshot_candidates(base_dir):
        if _snapshot_provider_mismatch_matches(
            payload,
            selected_provider=selected_provider,
            resolved_provider=resolved_provider,
            tickers=tickers,
        ):
            return payload, path
    return None, None


def _record_status(fields: dict[str, Any], error: str | None, invalid_fields: list[str] | None = None) -> str:
    if error:
        return "provider_error"
    if invalid_fields:
        return "invalid_payload"
    if fields.get("next_earnings_date") or fields.get("next_ex_div_date"):
        return "ready"
    return "no_known_event"


def _canonical_event_records(
    *,
    ticker: str,
    fields: dict[str, Any],
    provider: str,
    snapshot_id: str,
    fetched_at: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    earnings_date = fields.get("next_earnings_date")
    if earnings_date:
        records.append(
            {
                "canonical_event_key": f"{ticker}|earnings|next",
                "ticker": ticker,
                "event_type": "earnings",
                "event_period_key": "next",
                "event_date": str(earnings_date),
                "event_date_is_estimated": fields.get("next_earnings_date_is_estimated"),
                "provider": provider,
                "source": fields.get("next_earnings_date_source") or provider,
                "confidence": fields.get("next_earnings_date_confidence") or "unknown",
                "event_snapshot_id": snapshot_id,
                "event_data_fetched_at": fetched_at,
                "provider_event_id": None,
                "raw_source_ref": fields.get("next_earnings_date_source") or provider,
            }
        )
    ex_div_date = fields.get("next_ex_div_date")
    if ex_div_date:
        records.append(
            {
                "canonical_event_key": f"{ticker}|ex_dividend|next",
                "ticker": ticker,
                "event_type": "ex_dividend",
                "event_period_key": "next",
                "event_date": str(ex_div_date),
                "event_date_is_estimated": False,
                "provider": provider,
                "source": fields.get("next_ex_div_date_source") or provider,
                "confidence": fields.get("next_ex_div_date_confidence") or "unknown",
                "event_snapshot_id": snapshot_id,
                "event_data_fetched_at": fetched_at,
                "provider_event_id": None,
                "raw_source_ref": fields.get("next_ex_div_date_source") or provider,
                "dividend_amount": fields.get("dividend_amount"),
            }
        )
    return records


def _payload_status(status_counts: dict[str, int]) -> str:
    if not status_counts:
        return "missing"
    usable_count = status_counts.get("ready", 0) + status_counts.get("no_known_event", 0)
    if status_counts.get("provider_error") or status_counts.get("invalid_payload"):
        if usable_count:
            return "partial"
        if status_counts.get("invalid_payload"):
            return "invalid_payload"
        return "provider_error"
    return "ready"


def _write_snapshot(payload: dict[str, Any], *, base_dir: Path | None = None) -> Path:
    root = event_data_snapshot_dir(base_dir)
    path = root / f"{payload['event_snapshot_id']}.json"
    text = dumps_sanitized_json(payload, indent=2, sort_keys=True) + "\n"
    atomic_write_text(path, text, encoding="utf-8")
    atomic_write_text(event_data_latest_path(base_dir), text, encoding="utf-8")
    return path


def _fetch_event_payload(
    *,
    provider_name: str,
    selected_provider_name: str | None = None,
    trading_date: date,
    tickers: tuple[str, ...],
    ticker_universe_source: str,
    now: datetime,
    base_dir: Path | None,
) -> tuple[dict[str, Any], Path]:
    provider = get_data_provider_by_name(provider_name)
    snapshot_id = str(uuid.uuid4())
    fetched_at = format_utc_z_seconds(now)
    ticker_records: list[dict[str, Any]] = []
    canonical_events: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}

    # Provider methods read RuntimeConfig.today; align one-off fetches with the
    # requested trading date without changing the caller's long-lived override.
    previous_override = get_runtime_config_override()
    base_config = get_runtime_config()
    try:
        set_runtime_config_override(replace(base_config, today=trading_date))
        for ticker in tickers:
            error = None
            fields = dict(_EVENT_FIELD_DEFAULTS)
            try:
                provider.prepare_ticker_fetch(ticker)
                loaded = provider.load_ticker_events(ticker)
                if isinstance(loaded, dict):
                    for key in _EVENT_FIELD_DEFAULTS:
                        if key in loaded:
                            fields[key] = loaded[key]
            except Exception as exc:  # pylint: disable=broad-exception-caught
                error = f"{type(exc).__name__}: {exc}"
            fields, invalid_fields = _sanitize_event_fields(fields)
            provider_message = error
            if invalid_fields and not provider_message:
                provider_message = (
                    "invalid_payload: malformed event date field(s): "
                    + ", ".join(invalid_fields)
                )
            status = _record_status(fields, error, invalid_fields)
            status_counts[status] = status_counts.get(status, 0) + 1
            canonical_events.extend(
                _canonical_event_records(
                    ticker=ticker,
                    fields=fields,
                    provider=provider_name,
                    snapshot_id=snapshot_id,
                    fetched_at=fetched_at,
                )
            )
            ticker_records.append(
                {
                    "ticker": ticker,
                    "provider_status": status,
                    "provider_message": provider_message,
                    "event_fields": fields,
                    "canonical_event_keys": [
                        event["canonical_event_key"]
                        for event in canonical_events
                        if event["ticker"] == ticker
                    ],
                }
            )
    finally:
        set_runtime_config_override(previous_override)

    payload = {
        "artifact_type": "event_data_snapshot",
        "schema_version": EVENT_DATA_SCHEMA_VERSION,
        "event_snapshot_id": snapshot_id,
        "provider": selected_provider_name or provider_name,
        "resolved_provider": provider_name,
        "fetched_at": fetched_at,
        "provider_version": getattr(provider, "name", provider_name),
        "trading_date": trading_date.isoformat(),
        "freshness_policy": EVENT_DATA_FRESHNESS_POLICY,
        "fresh_through_trading_date": trading_date.isoformat(),
        "ticker_universe_source": ticker_universe_source,
        "tickers_requested": list(tickers),
        "tickers_succeeded": [
            row["ticker"] for row in ticker_records
            if row["provider_status"] in {"ready", "no_known_event"}
        ],
        "tickers_failed": [
            row["ticker"] for row in ticker_records
            if row["provider_status"] in {"provider_error", "invalid_payload"}
        ],
        "tickers_no_known_event": [
            row["ticker"] for row in ticker_records
            if row["provider_status"] == "no_known_event"
        ],
        "status": _payload_status(status_counts),
        "status_counts": status_counts,
        "records": ticker_records,
        "canonical_events": canonical_events,
        "summary": (
            f"Event data snapshot fetched from {provider_name} for {len(tickers)} ticker(s)."
        ),
    }
    path = _write_snapshot(payload, base_dir=base_dir)
    payload["path"] = str(path)
    return payload, path


def run_event_fetch(
    *,
    enabled: bool = True,
    provider: str | None = "yfinance",
    chain_provider: str | None = None,
    fetch_mode: str | None = "auto",
    trading_date: date | None = None,
    tickers: tuple[str, ...] | list[str] | set[str] = (),
    ticker_universe_source: str | None = None,
    base_dir: Path | None = None,
    now: datetime | None = None,
) -> EventDataSnapshotResult:
    """Resolve or fetch a durable event snapshot for a run."""
    resolved_enabled = strict_bool_arg(enabled, name="enabled")
    mode = normalize_event_data_fetch_mode(fetch_mode)
    resolved_date = (
        date_arg(trading_date, name="trading_date")
        if trading_date is not None
        else get_runtime_config().today
    )
    normalized_tickers = _normalize_tickers(tickers)
    universe_source = _normalize_ticker_universe_source(ticker_universe_source)
    current_time = (
        timezone_aware_datetime_arg(now, name="now")
        if now is not None
        else datetime.now(timezone.utc)
    )
    if not resolved_enabled:
        payload = {
            "artifact_type": "event_data_snapshot",
            "schema_version": EVENT_DATA_SCHEMA_VERSION,
            "status": "disabled",
            "provider": None,
            "resolved_provider": None,
            "fetch_mode": mode,
            "trading_date": resolved_date.isoformat(),
            "freshness_policy": EVENT_DATA_FRESHNESS_POLICY,
            "fresh_through_trading_date": resolved_date.isoformat(),
            "ticker_universe_source": universe_source,
            "tickers_requested": list(normalized_tickers),
            "tickers_succeeded": [],
            "tickers_failed": [],
            "tickers_no_known_event": [],
            "records": [],
            "canonical_events": [],
            "summary": "Event data is disabled for this run.",
        }
        return EventDataSnapshotResult(
            status="disabled",
            provider=None,
            resolved_provider=None,
            fetch_mode=mode,
            reused=False,
            snapshot_id=None,
            fetched_at=None,
            path=None,
            payload=payload,
        )
    selected_provider, resolved_provider = normalize_event_data_provider(
        provider,
        chain_provider=chain_provider,
    )
    if resolved_provider not in EVENT_DATA_SUPPORTED_PROVIDERS:
        payload = {
            "artifact_type": "event_data_snapshot",
            "schema_version": EVENT_DATA_SCHEMA_VERSION,
            "status": "not_supported",
            "provider": selected_provider,
            "resolved_provider": resolved_provider,
            "fetch_mode": mode,
            "trading_date": resolved_date.isoformat(),
            "freshness_policy": EVENT_DATA_FRESHNESS_POLICY,
            "fresh_through_trading_date": resolved_date.isoformat(),
            "ticker_universe_source": universe_source,
            "tickers_requested": list(normalized_tickers),
            "tickers_succeeded": [],
            "tickers_failed": [],
            "tickers_no_known_event": [],
            "records": [],
            "canonical_events": [],
            "summary": f"Event provider {resolved_provider!r} is not supported.",
        }
        return EventDataSnapshotResult(
            status="not_supported",
            provider=selected_provider,
            resolved_provider=resolved_provider,
            fetch_mode=mode,
            reused=False,
            snapshot_id=None,
            fetched_at=None,
            path=None,
            payload=payload,
        )
    if mode == "auto":
        cached, cached_path = latest_event_data_snapshot(
            provider=resolved_provider,
            selected_provider=selected_provider,
            trading_date=resolved_date,
            tickers=normalized_tickers,
            base_dir=base_dir,
        )
        if cached is not None:
            return EventDataSnapshotResult(
                status=str(cached.get("status") or "ready"),
                provider=selected_provider,
                resolved_provider=resolved_provider,
                fetch_mode=mode,
                reused=True,
                snapshot_id=str(cached.get("event_snapshot_id") or ""),
                fetched_at=str(cached.get("fetched_at") or ""),
                path=cached_path,
                payload={**cached, "path": str(cached_path) if cached_path else None},
            )
    payload, path = _fetch_event_payload(
        provider_name=resolved_provider,
        selected_provider_name=selected_provider,
        trading_date=resolved_date,
        tickers=normalized_tickers,
        ticker_universe_source=universe_source,
        now=current_time,
        base_dir=base_dir,
    )
    return EventDataSnapshotResult(
        status=str(payload.get("status") or "ready"),
        provider=selected_provider,
        resolved_provider=resolved_provider,
        fetch_mode=mode,
        reused=False,
        snapshot_id=str(payload.get("event_snapshot_id") or ""),
        fetched_at=str(payload.get("fetched_at") or ""),
        path=path,
        payload=payload,
    )


def overlay_event_snapshot(
    df: pd.DataFrame,
    snapshot: dict[str, Any] | EventDataSnapshotResult | None,
    *,
    trading_date: date | None = None,
    disabled: bool = False,
) -> pd.DataFrame:
    """Overlay snapshot event fields onto canonical option-chain rows."""
    resolved_disabled = strict_bool_arg(disabled, name="disabled")
    today = (
        date_arg(trading_date, name="trading_date")
        if trading_date is not None
        else get_runtime_config().today
    )
    result = df.copy()
    if result.empty or "underlying_symbol" not in result.columns:
        return result
    if isinstance(snapshot, EventDataSnapshotResult):
        payload = snapshot.payload
    else:
        payload = snapshot if isinstance(snapshot, dict) else {}
    provider = payload.get("provider")
    snapshot_id = payload.get("event_snapshot_id")
    fetched_at = payload.get("fetched_at")
    status = payload.get("status") or ("disabled" if resolved_disabled else "missing")
    by_ticker = {} if resolved_disabled else _event_fields_from_snapshot(payload)
    status_by_ticker = {} if resolved_disabled else _event_status_from_snapshot(payload)

    parts: list[pd.DataFrame] = []
    for ticker, ticker_frame in result.groupby(result["underlying_symbol"].astype(str).str.upper()):
        events = by_ticker.get(ticker, dict(_EVENT_FIELD_DEFAULTS))
        enriched = append_ticker_event_fields(ticker_frame.copy(), events, today)
        enriched = add_event_risk_flags(enriched)
        enriched["event_data_provider"] = provider
        enriched["event_data_snapshot_id"] = snapshot_id
        enriched["event_data_fetched_at"] = fetched_at
        enriched["event_data_status"] = status_by_ticker.get(ticker, status)
        parts.append(enriched)
    return pd.concat(parts).sort_index() if parts else result


def clear_event_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Clear strategy-facing event fields while preserving chain quote data."""
    result = df.copy()
    for column in EVENT_OVERLAY_COLUMNS:
        if column in {"event_data_status"}:
            result[column] = "disabled"
        elif column in {"event_data_provider", "event_data_snapshot_id", "event_data_fetched_at"}:
            result[column] = None
        elif column == "dividend_amount":
            result[column] = np.nan
        else:
            result[column] = None
    return result


def summarize_latest_event_data(
    *,
    provider: str,
    chain_provider: str | None = None,
    tickers: tuple[str, ...] | list[str] | set[str] = (),
    trading_date: date | None = None,
    base_dir: Path | None = None,
) -> dict[str, Any]:
    """Return read-only freshness metadata for New Run source health."""
    selected, resolved = normalize_event_data_provider(provider, chain_provider=chain_provider)
    resolved_date = (
        date_arg(trading_date, name="trading_date")
        if trading_date is not None
        else get_runtime_config().today
    )
    requested = _normalize_tickers(tickers)
    if resolved not in EVENT_DATA_SUPPORTED_PROVIDERS:
        return {
            "available": False,
            "reusable": False,
            "status": "not_supported",
            "freshness_label": "NOT_SUPPORTED",
            "provider": selected,
            "resolved_provider": resolved,
            "trading_date": resolved_date.isoformat(),
            "required_tickers": list(requested),
            "covered_required_tickers": [],
            "missing_tickers": list(requested),
            "record_count": 0,
            "status_counts": {},
            "auto_would_reuse": False,
            "provider_api_call_expected": False,
            "summary": f"Event provider {resolved!r} is not supported.",
        }
    payload, path = latest_event_data_snapshot(
        provider=resolved,
        selected_provider=selected,
        trading_date=resolved_date,
        tickers=requested,
        base_dir=base_dir,
    )
    if payload is None:
        same_day_payload, same_day_path = _latest_same_day_event_data_snapshot_for_source_health(
            selected_provider=selected,
            resolved_provider=resolved,
            trading_date=resolved_date,
            tickers=requested,
            base_dir=base_dir,
        )
        if same_day_payload is not None:
            status = str(same_day_payload.get("status") or "missing")
            records = (
                same_day_payload.get("records")
                if isinstance(same_day_payload.get("records"), list)
                else []
            )
            status_counts = (
                same_day_payload.get("status_counts")
                if isinstance(same_day_payload.get("status_counts"), dict)
                else {}
            )
            covered = _snapshot_usable_tickers(same_day_payload) or ()
            missing = sorted(set(requested) - set(covered))
            reusable = not missing and status in _REUSABLE_RETAINED_STATUSES
            freshness_label = (
                "CURRENT_TRADING_DAY"
                if reusable
                else "MISSING"
                if status == "missing" or missing
                else "STALE"
            )
            return {
                "available": True,
                "reusable": reusable,
                "status": status,
                "freshness_label": freshness_label,
                "provider": selected,
                "resolved_provider": resolved,
                "event_snapshot_id": same_day_payload.get("event_snapshot_id"),
                "fetched_at": same_day_payload.get("fetched_at"),
                "path": str(same_day_path) if same_day_path else None,
                "trading_date": resolved_date.isoformat(),
                "snapshot_trading_date": same_day_payload.get("trading_date"),
                "fresh_through_trading_date": same_day_payload.get("fresh_through_trading_date"),
                "snapshot_age_days": 0,
                "ticker_universe_source": same_day_payload.get("ticker_universe_source"),
                "required_tickers": list(requested),
                "covered_required_tickers": sorted(set(requested) & set(covered)),
                "missing_tickers": missing,
                "record_count": len(records),
                "status_counts": status_counts,
                "auto_would_reuse": reusable,
                "provider_api_call_expected": not reusable,
                "summary": (
                    f"Retained event snapshot {same_day_payload.get('event_snapshot_id')} "
                    f"covers {len(set(requested) & set(covered))}/{len(requested)} "
                    "requested ticker(s) with usable event rows."
                ),
            }
        stale_payload, stale_path = latest_retained_event_data_snapshot(
            provider=resolved,
            selected_provider=selected,
            tickers=requested,
            base_dir=base_dir,
        )
        if stale_payload is not None:
            records = (
                stale_payload.get("records")
                if isinstance(stale_payload.get("records"), list)
                else []
            )
            status_counts = (
                stale_payload.get("status_counts")
                if isinstance(stale_payload.get("status_counts"), dict)
                else {}
            )
            covered = _snapshot_usable_tickers(stale_payload) or ()
            snapshot_trading_date = _parse_date(stale_payload.get("trading_date"))
            snapshot_age_days = (
                (resolved_date - snapshot_trading_date).days
                if snapshot_trading_date is not None
                else None
            )
            return {
                "available": True,
                "reusable": False,
                "status": "stale",
                "freshness_label": "STALE",
                "provider": selected,
                "resolved_provider": resolved,
                "event_snapshot_id": stale_payload.get("event_snapshot_id"),
                "fetched_at": stale_payload.get("fetched_at"),
                "path": str(stale_path) if stale_path else None,
                "trading_date": resolved_date.isoformat(),
                "snapshot_trading_date": stale_payload.get("trading_date"),
                "fresh_through_trading_date": stale_payload.get("fresh_through_trading_date"),
                "snapshot_age_days": snapshot_age_days,
                "ticker_universe_source": stale_payload.get("ticker_universe_source"),
                "required_tickers": list(requested),
                "covered_required_tickers": sorted(set(requested) & set(covered)),
                "missing_tickers": sorted(set(requested) - set(covered)),
                "record_count": len(records),
                "status_counts": status_counts,
                "auto_would_reuse": False,
                "provider_api_call_expected": True,
                "summary": (
                    f"Retained event snapshot {stale_payload.get('event_snapshot_id')} "
                    f"is stale for {resolved_date.isoformat()}."
                ),
            }
        mismatch_payload, mismatch_path = _latest_provider_mismatch_event_data_snapshot(
            selected_provider=selected,
            resolved_provider=resolved,
            tickers=requested,
            base_dir=base_dir,
        )
        if mismatch_payload is not None:
            records = (
                mismatch_payload.get("records")
                if isinstance(mismatch_payload.get("records"), list)
                else []
            )
            status_counts = (
                mismatch_payload.get("status_counts")
                if isinstance(mismatch_payload.get("status_counts"), dict)
                else {}
            )
            covered = _snapshot_usable_tickers(mismatch_payload) or ()
            snapshot_trading_date = _parse_date(mismatch_payload.get("trading_date"))
            snapshot_age_days = (
                (resolved_date - snapshot_trading_date).days
                if snapshot_trading_date is not None
                else None
            )
            retained_provider = _snapshot_selected_provider(mismatch_payload) or None
            retained_resolved_provider = _snapshot_resolved_provider(mismatch_payload) or None
            return {
                "available": True,
                "reusable": False,
                "status": "provider_mismatch",
                "freshness_label": "PROVIDER_MISMATCH",
                "provider": selected,
                "resolved_provider": resolved,
                "retained_provider": retained_provider,
                "retained_resolved_provider": retained_resolved_provider,
                "event_snapshot_id": mismatch_payload.get("event_snapshot_id"),
                "fetched_at": mismatch_payload.get("fetched_at"),
                "path": str(mismatch_path) if mismatch_path else None,
                "trading_date": resolved_date.isoformat(),
                "snapshot_trading_date": mismatch_payload.get("trading_date"),
                "fresh_through_trading_date": mismatch_payload.get("fresh_through_trading_date"),
                "snapshot_age_days": snapshot_age_days,
                "ticker_universe_source": mismatch_payload.get("ticker_universe_source"),
                "required_tickers": list(requested),
                "covered_required_tickers": sorted(set(requested) & set(covered)),
                "missing_tickers": sorted(set(requested) - set(covered)),
                "record_count": len(records),
                "status_counts": status_counts,
                "auto_would_reuse": False,
                "provider_api_call_expected": True,
                "summary": (
                    f"Retained event snapshot {mismatch_payload.get('event_snapshot_id')} "
                    f"was produced by {retained_provider or retained_resolved_provider}, "
                    f"not selected provider {selected}."
                ),
            }
        return {
            "available": False,
            "reusable": False,
            "status": "missing",
            "freshness_label": "MISSING",
            "provider": selected,
            "resolved_provider": resolved,
            "trading_date": resolved_date.isoformat(),
            "required_tickers": list(requested),
            "covered_required_tickers": [],
            "missing_tickers": list(requested),
            "record_count": 0,
            "status_counts": {},
            "auto_would_reuse": False,
            "provider_api_call_expected": True,
            "summary": f"No same-trading-day event snapshot is available for {resolved}.",
        }
    status = str(payload.get("status") or "ready")
    records = payload.get("records") if isinstance(payload.get("records"), list) else []
    status_counts = payload.get("status_counts") if isinstance(payload.get("status_counts"), dict) else {}
    covered = _snapshot_usable_tickers(payload) or ()
    missing = sorted(set(requested) - set(covered))
    reusable = not missing and status in {"ready", "partial"}
    return {
        "available": True,
        "reusable": reusable,
        "status": status,
        "freshness_label": "CURRENT_TRADING_DAY" if reusable else "STALE",
        "provider": selected,
        "resolved_provider": resolved,
        "event_snapshot_id": payload.get("event_snapshot_id"),
        "fetched_at": payload.get("fetched_at"),
        "path": str(path) if path else None,
        "trading_date": resolved_date.isoformat(),
        "snapshot_trading_date": payload.get("trading_date"),
        "fresh_through_trading_date": payload.get("fresh_through_trading_date"),
        "snapshot_age_days": 0 if str(payload.get("trading_date") or "") == resolved_date.isoformat() else None,
        "ticker_universe_source": payload.get("ticker_universe_source"),
        "required_tickers": list(requested),
        "covered_required_tickers": sorted(set(requested) & set(covered)),
        "missing_tickers": missing,
        "record_count": len(records),
        "status_counts": status_counts,
        "auto_would_reuse": reusable,
        "provider_api_call_expected": not reusable,
        "summary": (
            f"Retained event snapshot {payload.get('event_snapshot_id')} covers "
            f"{len(set(requested) & set(covered))}/{len(requested)} requested ticker(s)."
        ),
    }
