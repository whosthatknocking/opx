"""Provider-scoped daily price-history backfill command."""

# pylint: disable=too-many-instance-attributes,duplicate-code

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import date
import re
from pathlib import Path
from typing import Callable, Iterable, Sequence

from opx_chain.config import (
    SUPPORTED_PROVIDERS,
    RuntimeConfig,
    get_runtime_config,
    get_runtime_config_override,
    set_runtime_config_override,
)
from opx_chain.locks import acquire_nonblocking_file_lock, release_file_lock
from opx_chain.paths import get_data_dir
from opx_chain.price_history import (
    PriceHistoryReconcileResult,
    PriceHistoryStore,
    get_price_history_store,
    reconcile_price_history,
)
from opx_chain.providers import get_data_provider

PRICE_HISTORY_BACKFILL_PROVIDERS = frozenset({"marketdata", "yfinance"})
_VALID_TICKER_RE = re.compile(r"^[A-Z](?:[A-Z.]{0,9})$")


@dataclass(frozen=True)
class PriceHistoryBackfillRow:
    """One provider/ticker backfill outcome."""

    provider: str
    ticker: str
    status: str
    fetched: bool
    requested_lookback_days: int | None
    fetched_rows: int
    stored_rows: int
    stored_row_count: int
    latest_trading_date: date | None
    error_summary: str | None = None


@dataclass(frozen=True)
class PriceHistoryBackfillResult:
    """Summary returned by a provider-scoped price-history backfill."""

    providers: tuple[str, ...]
    tickers: tuple[str, ...]
    lookback_days: int
    refresh: bool
    dry_run: bool
    rows: tuple[PriceHistoryBackfillRow, ...]


def _normalize_csv_values(values: Iterable[str] | str | None, *, name: str) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, str):
        values = (values,)
    normalized: list[str] = []
    for raw_value in values:
        if not isinstance(raw_value, str):
            raise ValueError(f"{name} must be a string or iterable of strings")
        for item in raw_value.split(","):
            value = item.strip()
            if value:
                normalized.append(value)
    return tuple(dict.fromkeys(normalized))


def _normalize_providers(
    values: Iterable[str] | str | None,
    default_provider: str,
) -> tuple[str, ...]:
    providers = tuple(
        provider.lower()
        for provider in _normalize_csv_values(values, name="providers")
    )
    if not providers:
        providers = (default_provider.lower(),)
    unsupported = sorted(set(providers) - SUPPORTED_PROVIDERS)
    if unsupported:
        supported = ", ".join(sorted(SUPPORTED_PROVIDERS))
        raise ValueError(
            f"unsupported provider(s): {', '.join(unsupported)}; expected one of: {supported}"
        )
    unsupported_price_history = sorted(set(providers) - PRICE_HISTORY_BACKFILL_PROVIDERS)
    if unsupported_price_history:
        supported = ", ".join(sorted(PRICE_HISTORY_BACKFILL_PROVIDERS))
        raise ValueError(
            "price-history backfill is only available for provider(s): "
            f"{supported}; got: {', '.join(unsupported_price_history)}"
        )
    return providers


def _normalize_tickers(
    values: Iterable[str] | str | None,
    default_tickers: Sequence[str],
) -> tuple[str, ...]:
    tickers = tuple(
        _normalize_ticker(ticker)
        for ticker in _normalize_csv_values(values, name="tickers")
    )
    if not tickers:
        tickers = tuple(
            _normalize_ticker(ticker)
            for ticker in default_tickers
            if str(ticker).strip()
        )
    if not tickers:
        raise ValueError("at least one ticker is required")
    return tickers


def _normalize_ticker(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("ticker must be a non-empty string")
    text = value.upper().strip()
    if not text:
        raise ValueError("ticker must be a non-empty string")
    if not _VALID_TICKER_RE.fullmatch(text):
        raise ValueError("ticker must be a valid stock ticker symbol")
    return text


def _strict_bool(value: bool, *, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _positive_int(value: int, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be a positive integer")
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _lock_path(config: RuntimeConfig) -> Path:
    base = Path(config.storage_dir) if config.storage_dir else get_data_dir()
    return base / "fetcher.lock"


def _provider_config(
    config: RuntimeConfig,
    *,
    provider: str,
    lookback_days: int,
    refresh: bool,
) -> RuntimeConfig:
    return replace(
        config,
        data_provider=provider,
        price_context_enable=True,
        price_context_lookback_days=lookback_days,
        provider_price_context_ttl=0 if refresh else config.provider_price_context_ttl,
    )


def _status_for_result(result: PriceHistoryReconcileResult) -> str:
    if result.error_summary:
        return "ERROR"
    if result.fetched:
        return "FETCHED"
    return "CACHED"


def run_price_history_backfill(  # pylint: disable=too-many-arguments,too-many-locals
    *,
    providers: Iterable[str] | str | None = None,
    tickers: Iterable[str] | str | None = None,
    lookback_days: int | None = None,
    refresh: bool = False,
    dry_run: bool = False,
    config: RuntimeConfig | None = None,
    store: PriceHistoryStore | None = None,
    provider_factory: Callable[[str], object] | None = None,
) -> PriceHistoryBackfillResult:
    """Backfill local daily OHLCV bars for provider/ticker pairs."""
    base_config = config or get_runtime_config()
    resolved_providers = _normalize_providers(providers, base_config.data_provider)
    resolved_tickers = _normalize_tickers(tickers, base_config.tickers)
    resolved_lookback = _positive_int(
        base_config.price_context_lookback_days if lookback_days is None else lookback_days,
        name="lookback_days",
    )
    resolved_refresh = _strict_bool(refresh, name="refresh")
    resolved_dry_run = _strict_bool(dry_run, name="dry_run")

    owns_store = store is None
    history_store = store or get_price_history_store(base_config)
    provider_factory = provider_factory or (lambda _provider_name: get_data_provider())
    rows: list[PriceHistoryBackfillRow] = []
    previous_override = get_runtime_config_override()

    try:
        for provider_name in resolved_providers:
            provider_config = _provider_config(
                base_config,
                provider=provider_name,
                lookback_days=resolved_lookback,
                refresh=resolved_refresh,
            )
            provider = None
            if not resolved_dry_run:
                set_runtime_config_override(provider_config)
                provider = provider_factory(provider_name)
                actual_provider_name = str(getattr(provider, "name", "") or "").lower()
                if actual_provider_name != provider_name:
                    raise ValueError(
                        "provider_factory returned provider "
                        f"{actual_provider_name or '<missing>'!r} for requested "
                        f"provider {provider_name!r}"
                    )
            for ticker in resolved_tickers:
                if resolved_dry_run:
                    stats = history_store.stats(provider=provider_name, ticker=ticker)
                    rows.append(
                        PriceHistoryBackfillRow(
                            provider=provider_name,
                            ticker=ticker,
                            status="DRY_RUN",
                            fetched=False,
                            requested_lookback_days=None,
                            fetched_rows=0,
                            stored_rows=0,
                            stored_row_count=stats.row_count,
                            latest_trading_date=stats.latest_date,
                        )
                    )
                    continue
                if provider is None:
                    raise RuntimeError("price-history provider was not initialized")
                prepare_ticker_fetch = getattr(provider, "prepare_ticker_fetch", None)
                if prepare_ticker_fetch is not None:
                    prepare_ticker_fetch(ticker)
                result = reconcile_price_history(
                    ticker=ticker,
                    provider=provider,
                    config=provider_config,
                    store=history_store,
                )
                stats = history_store.stats(provider=provider_name, ticker=ticker)
                rows.append(
                    PriceHistoryBackfillRow(
                        provider=provider_name,
                        ticker=ticker,
                        status=_status_for_result(result),
                        fetched=result.fetched,
                        requested_lookback_days=result.requested_lookback_days,
                        fetched_rows=result.fetched_rows,
                        stored_rows=result.stored_rows,
                        stored_row_count=stats.row_count,
                        latest_trading_date=stats.latest_date,
                        error_summary=result.error_summary,
                    )
                )
    finally:
        set_runtime_config_override(previous_override)
        if owns_store:
            history_store.close()

    return PriceHistoryBackfillResult(
        providers=resolved_providers,
        tickers=resolved_tickers,
        lookback_days=resolved_lookback,
        refresh=resolved_refresh,
        dry_run=resolved_dry_run,
        rows=tuple(rows),
    )


def _format_date(value: date | None) -> str:
    return value.isoformat() if value is not None else "-"


def _format_int(value: int | None) -> str:
    return "-" if value is None else str(value)


def format_backfill_result(result: PriceHistoryBackfillResult) -> str:
    """Return a compact operator-readable backfill summary."""
    lines = [
        "Price history backfill",
        f"providers: {', '.join(result.providers)}",
        f"tickers: {', '.join(result.tickers)}",
        f"lookback_days: {result.lookback_days}",
        f"mode: {'dry-run' if result.dry_run else 'write'}",
        f"refresh: {'yes' if result.refresh else 'no'}",
        "",
        "provider    ticker  status   fetched  requested  fetched_rows  "
        "stored_rows  total_rows  latest",
    ]
    for row in result.rows:
        lines.append(
            f"{row.provider:<11} {row.ticker:<7} {row.status:<8} "
            f"{str(row.fetched).lower():<7} "
            f"{_format_int(row.requested_lookback_days):<9} "
            f"{row.fetched_rows:<12} {row.stored_rows:<11} "
            f"{row.stored_row_count:<10} {_format_date(row.latest_trading_date)}"
        )
        if row.error_summary:
            lines.append(f"  error: {row.error_summary}")
    return "\n".join(lines)


def parse_args(argv=None):
    """Parse price-history backfill CLI arguments."""
    parser = argparse.ArgumentParser(
        prog="opx-price-history-backfill",
        description="Backfill provider-scoped daily OHLCV history for volatility context.",
    )
    parser.add_argument(
        "--providers",
        action="append",
        default=None,
        help=(
            "Comma-separated provider list. Supported: marketdata,yfinance. "
            "Defaults to the configured provider."
        ),
    )
    parser.add_argument(
        "--tickers",
        action="append",
        default=None,
        help="Comma-separated ticker list. Defaults to settings.tickers.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Daily-bar lookback to reconcile. Defaults to price_context.lookback_days.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Bypass the price-history sync TTL and retry provider reconciliation now.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report current local coverage without provider API calls or writes.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    """CLI entrypoint for provider-scoped price-history backfill."""
    args = parse_args(argv)
    config = get_runtime_config()
    lock_handle = None
    lock_path = _lock_path(config)
    if not args.dry_run:
        lock_handle = acquire_nonblocking_file_lock(lock_path)
        if lock_handle is None:
            print(f"Another fetcher/backfill run is already active: {lock_path}")
            return 1
    try:
        result = run_price_history_backfill(
            providers=args.providers,
            tickers=args.tickers,
            lookback_days=args.lookback_days,
            refresh=args.refresh,
            dry_run=args.dry_run,
            config=config,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        print(f"ERROR: {exc}")
        return 1
    finally:
        if lock_handle is not None:
            release_file_lock(lock_handle)
    print(format_backfill_result(result))
    return 0
