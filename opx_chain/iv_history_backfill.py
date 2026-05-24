"""Backfill durable IV history from retained option-chain datasets."""

# pylint: disable=duplicate-code,too-many-instance-attributes

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Sequence

import pandas as pd

from opx_chain.config import (
    SUPPORTED_PROVIDERS,
    RuntimeConfig,
    get_runtime_config,
    set_runtime_config_override,
)
from opx_chain.iv_history import (
    IVHistoryStore,
    build_iv_observation_frame,
    get_iv_history_store,
)
from opx_chain.locks import acquire_nonblocking_file_lock, release_file_lock
from opx_chain.paths import get_data_dir
from opx_chain.providers import get_data_provider
from opx_chain.storage.factory import get_storage_backend
from opx_chain.storage.models import DatasetHandle, DatasetRecord
from opx_chain.utils import read_dataset_file

_IV_HISTORY_COLUMNS = (
    "underlying_symbol",
    "ticker",
    "symbol",
    "implied_volatility",
    "option_type",
    "days_to_expiration",
    "expiration_date",
    "delta_abs",
    "delta",
    "option_quote_time",
)
BACKFILL_STATUS_ERROR = "ERROR"
HISTORICAL_IV_FETCH_PROVIDERS = frozenset({"marketdata"})
HISTORICAL_STATUS_WOULD_FETCH = "WOULD_FETCH"


@dataclass(frozen=True)
class IVHistoryBackfillRow:
    """One option-chain dataset IV-history ingestion outcome."""

    provider: str
    dataset_id: str
    run_id: str | None
    status: str
    observation_date: str | None
    source_rows: int
    stored_rows: int
    tickers: tuple[str, ...]
    error_summary: str | None = None


@dataclass(frozen=True)
class IVHistoryBackfillResult:
    """Summary returned by a durable IV-history backfill."""

    providers: tuple[str, ...]
    tickers: tuple[str, ...]
    lookback_days: int
    limit: int
    refresh: bool
    dry_run: bool
    rows: tuple[IVHistoryBackfillRow, ...]
    fetch_historical: bool = False
    sessions: int | None = None
    end_date: str | None = None
    estimated_requests: int = 0


def _normalize_csv_values(values: Iterable[str] | None) -> tuple[str, ...]:
    if values is None:
        return ()
    normalized: list[str] = []
    for raw_value in values:
        for item in str(raw_value).split(","):
            value = item.strip()
            if value:
                normalized.append(value)
    return tuple(dict.fromkeys(normalized))


def _normalize_providers(
    values: Iterable[str] | None,
    default_provider: str,
) -> tuple[str, ...]:
    providers = tuple(provider.lower() for provider in _normalize_csv_values(values))
    if not providers:
        providers = (default_provider.lower(),)
    unsupported = sorted(set(providers) - SUPPORTED_PROVIDERS)
    if unsupported:
        supported = ", ".join(sorted(SUPPORTED_PROVIDERS))
        raise ValueError(
            f"unsupported provider(s): {', '.join(unsupported)}; expected one of: {supported}"
        )
    return providers


def _validate_historical_providers(providers: tuple[str, ...]) -> None:
    unsupported = sorted(set(providers) - HISTORICAL_IV_FETCH_PROVIDERS)
    if unsupported:
        supported = ", ".join(sorted(HISTORICAL_IV_FETCH_PROVIDERS))
        raise ValueError(
            "historical IV fetch is only available for provider(s): "
            f"{supported}; got: {', '.join(unsupported)}"
        )


def _normalize_tickers(
    values: Iterable[str] | None,
    default_tickers: Sequence[str],
) -> tuple[str, ...]:
    tickers = tuple(ticker.upper() for ticker in _normalize_csv_values(values))
    if tickers:
        return tickers
    return tuple(
        str(ticker).upper().strip()
        for ticker in default_tickers
        if str(ticker).strip()
    )


def _parse_end_date(value: str | date | None, default_end_date: date) -> date:
    if value is None:
        return default_end_date
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError("end_date must be YYYY-MM-DD") from exc


def _historical_observation_dates(*, end_date: date, sessions: int) -> tuple[date, ...]:
    resolved_sessions = int(sessions)
    if resolved_sessions <= 0:
        raise ValueError("sessions must be positive")
    business_end = pd.Timestamp(end_date)
    while business_end.dayofweek >= 5:
        business_end -= pd.Timedelta(days=1)
    return tuple(
        item.date()
        for item in pd.bdate_range(end=business_end, periods=resolved_sessions)
    )


def _lock_path(config: RuntimeConfig) -> Path:
    base = Path(config.storage_dir) if config.storage_dir else get_data_dir()
    return base / "fetcher.lock"


def _since_datetime(config: RuntimeConfig, lookback_days: int) -> datetime:
    start_day = config.today - timedelta(days=max(int(lookback_days), 0))
    return datetime.combine(start_day, time.min, tzinfo=timezone.utc)


def _date_text(value) -> str | None:
    return value.isoformat() if value is not None else None


def _dataset_tickers(frame: pd.DataFrame) -> pd.Series:
    for column in ("underlying_symbol", "ticker", "symbol"):
        if column in frame.columns:
            return frame[column].astype(str).str.upper().str.strip()
    return pd.Series([""] * len(frame), index=frame.index)


def _filter_frame_tickers(frame: pd.DataFrame, tickers: tuple[str, ...]) -> pd.DataFrame:
    if frame.empty or not tickers:
        return frame
    ticker_values = _dataset_tickers(frame)
    return frame.loc[ticker_values.isin(set(tickers))].copy()


def _listed_datasets(
    storage,
    *,
    providers: tuple[str, ...],
    limit: int,
    since: datetime,
    dataset_ids: tuple[str, ...],
) -> list[DatasetRecord | DatasetHandle]:
    if dataset_ids:
        return [storage.get_dataset(dataset_id) for dataset_id in dataset_ids]
    records: list[DatasetRecord | DatasetHandle] = []
    for provider in providers:
        records.extend(storage.list_datasets(limit=limit, provider=provider, since=since))
    deduped: dict[str, DatasetRecord | DatasetHandle] = {}
    for record in records:
        deduped[record.dataset_id] = record
    return sorted(deduped.values(), key=lambda record: record.created_at, reverse=True)


def _read_chain_dataset(record: DatasetRecord | DatasetHandle) -> pd.DataFrame:
    return read_dataset_file(Path(record.location), columns=_IV_HISTORY_COLUMNS)


def _observed_at_for_dataset(
    frame: pd.DataFrame,
    record: DatasetRecord | DatasetHandle,
) -> datetime | None:
    if "option_quote_time" not in frame.columns:
        return record.created_at
    quote_times = pd.to_datetime(frame["option_quote_time"], utc=True, errors="coerce")
    if quote_times.dropna().empty:
        return record.created_at
    return None


def _row_tickers(observations: pd.DataFrame) -> tuple[str, ...]:
    if observations.empty or "ticker" not in observations.columns:
        return ()
    return tuple(sorted(set(observations["ticker"].astype(str).str.upper().str.strip())))


def _historical_dataset_id(provider: str, ticker: str, observation_date: date) -> str:
    return f"historical:{provider}:{ticker.upper()}:{observation_date.isoformat()}"


def _historical_provider_config(config: RuntimeConfig, provider: str) -> RuntimeConfig:
    return replace(config, data_provider=provider)


def _historical_fetch_rows(  # pylint: disable=too-many-arguments,too-many-locals
    *,
    providers: tuple[str, ...],
    tickers: tuple[str, ...],
    observation_dates: tuple[date, ...],
    refresh: bool,
    dry_run: bool,
    config: RuntimeConfig,
    iv_store: IVHistoryStore,
    provider_factory: Callable[[str], object] | None,
) -> list[IVHistoryBackfillRow]:
    rows: list[IVHistoryBackfillRow] = []
    if dry_run:
        for provider_name in providers:
            for ticker in tickers:
                for observation_date in observation_dates:
                    exists = iv_store.has_observation_date(
                        provider=provider_name,
                        ticker=ticker,
                        observation_date=observation_date,
                    )
                    rows.append(
                        IVHistoryBackfillRow(
                            provider=provider_name,
                            dataset_id=_historical_dataset_id(
                                provider_name,
                                ticker,
                                observation_date,
                            ),
                            run_id=None,
                            status=(
                                "SKIPPED"
                                if exists and not refresh
                                else HISTORICAL_STATUS_WOULD_FETCH
                            ),
                            observation_date=observation_date.isoformat(),
                            source_rows=0,
                            stored_rows=0,
                            tickers=(ticker,),
                        )
                    )
        return rows

    provider_factory = provider_factory or (lambda _provider_name: get_data_provider())
    try:
        for provider_name in providers:
            set_runtime_config_override(
                _historical_provider_config(config, provider_name)
            )
            provider = provider_factory(provider_name)
            for ticker in tickers:
                prepare_ticker_fetch = getattr(provider, "prepare_ticker_fetch", None)
                if callable(prepare_ticker_fetch):
                    prepare_ticker_fetch(ticker)
                load_historical = getattr(provider, "load_historical_option_chain_frame")
                for observation_date in observation_dates:
                    dataset_id = _historical_dataset_id(
                        provider_name,
                        ticker,
                        observation_date,
                    )
                    if not refresh and iv_store.has_observation_date(
                        provider=provider_name,
                        ticker=ticker,
                        observation_date=observation_date,
                    ):
                        rows.append(
                            IVHistoryBackfillRow(
                                provider=provider_name,
                                dataset_id=dataset_id,
                                run_id=None,
                                status="SKIPPED",
                                observation_date=observation_date.isoformat(),
                                source_rows=0,
                                stored_rows=0,
                                tickers=(ticker,),
                            )
                        )
                        continue
                    try:
                        chain = load_historical(
                            ticker,
                            observation_date=observation_date,
                        )
                        observations = build_iv_observation_frame(
                            chain,
                            provider=provider_name,
                            dataset_id=dataset_id,
                            run_id=None,
                            observed_at=observation_date,
                            source_created_at=datetime.combine(
                                observation_date,
                                time.min,
                                tzinfo=timezone.utc,
                            ),
                        )
                        stored_rows = iv_store.upsert_observations(observations)
                        status = "INGESTED" if stored_rows else "EMPTY"
                        iv_store.record_sync(
                            dataset_id=dataset_id,
                            provider=provider_name,
                            run_id=None,
                            status=status,
                            observation_date=observation_date,
                            source_rows=len(chain),
                            stored_rows=stored_rows,
                        )
                        rows.append(
                            IVHistoryBackfillRow(
                                provider=provider_name,
                                dataset_id=dataset_id,
                                run_id=None,
                                status=status,
                                observation_date=observation_date.isoformat(),
                                source_rows=len(chain),
                                stored_rows=stored_rows,
                                tickers=_row_tickers(observations) or (ticker,),
                            )
                        )
                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        error_summary = str(exc).splitlines()[0]
                        iv_store.record_sync(
                            dataset_id=dataset_id,
                            provider=provider_name,
                            run_id=None,
                            status=BACKFILL_STATUS_ERROR,
                            observation_date=observation_date,
                            source_rows=0,
                            stored_rows=0,
                            error_summary=error_summary,
                        )
                        rows.append(
                            IVHistoryBackfillRow(
                                provider=provider_name,
                                dataset_id=dataset_id,
                                run_id=None,
                                status=BACKFILL_STATUS_ERROR,
                                observation_date=observation_date.isoformat(),
                                source_rows=0,
                                stored_rows=0,
                                tickers=(ticker,),
                                error_summary=error_summary,
                            )
                        )
    finally:
        set_runtime_config_override(None)
    return rows


# pylint: disable-next=too-many-arguments,too-many-locals,too-many-branches,too-many-statements
def run_iv_history_backfill(
    *,
    providers: Iterable[str] | None = None,
    tickers: Iterable[str] | None = None,
    lookback_days: int = 365,
    limit: int = 200,
    refresh: bool = False,
    dry_run: bool = False,
    dataset_ids: Iterable[str] | None = None,
    fetch_historical: bool = False,
    sessions: int = 20,
    end_date: str | date | None = None,
    config: RuntimeConfig | None = None,
    store: IVHistoryStore | None = None,
    storage=None,
    provider_factory: Callable[[str], object] | None = None,
) -> IVHistoryBackfillResult:
    """Backfill local IV history from retained option-chain datasets."""
    base_config = config or get_runtime_config()
    resolved_providers = _normalize_providers(providers, base_config.data_provider)
    resolved_tickers = _normalize_tickers(tickers, base_config.tickers)
    resolved_dataset_ids = _normalize_csv_values(dataset_ids)
    if fetch_historical and resolved_dataset_ids:
        raise ValueError("dataset_ids cannot be used with fetch_historical")
    resolved_lookback = int(lookback_days)
    resolved_limit = int(limit)
    if resolved_lookback <= 0:
        raise ValueError("lookback_days must be positive")
    if resolved_limit <= 0:
        raise ValueError("limit must be positive")
    resolved_sessions = int(sessions)
    resolved_end_date = _parse_end_date(end_date, base_config.today)
    historical_dates: tuple[date, ...] = ()
    if fetch_historical:
        _validate_historical_providers(resolved_providers)
        if not resolved_tickers:
            raise ValueError("at least one ticker is required for fetch_historical")
        historical_dates = _historical_observation_dates(
            end_date=resolved_end_date,
            sessions=resolved_sessions,
        )

    owns_store = store is None
    iv_store = store or get_iv_history_store(base_config)
    rows: list[IVHistoryBackfillRow] = []

    try:
        if fetch_historical:
            rows = _historical_fetch_rows(
                providers=resolved_providers,
                tickers=resolved_tickers,
                observation_dates=historical_dates,
                refresh=refresh,
                dry_run=dry_run,
                config=base_config,
                iv_store=iv_store,
                provider_factory=provider_factory,
            )
            return IVHistoryBackfillResult(
                providers=resolved_providers,
                tickers=resolved_tickers,
                lookback_days=resolved_lookback,
                limit=resolved_limit,
                refresh=refresh,
                dry_run=dry_run,
                rows=tuple(rows),
                fetch_historical=True,
                sessions=resolved_sessions,
                end_date=resolved_end_date.isoformat(),
                estimated_requests=sum(
                    1
                    for row in rows
                    if row.status != "SKIPPED"
                ),
            )

        storage_backend = storage or get_storage_backend(base_config)
        if storage_backend is None:
            raise RuntimeError("opx-chain storage is disabled or unavailable")
        records = _listed_datasets(
            storage_backend,
            providers=resolved_providers,
            limit=resolved_limit,
            since=_since_datetime(base_config, resolved_lookback),
            dataset_ids=resolved_dataset_ids,
        )
        for record in records:
            sync = iv_store.get_sync(dataset_id=record.dataset_id)
            if sync is not None and not refresh:
                rows.append(
                    IVHistoryBackfillRow(
                        provider=record.provider,
                        dataset_id=record.dataset_id,
                        run_id=record.run_id,
                        status="SKIPPED",
                        observation_date=_date_text(sync.observation_date),
                        source_rows=sync.source_rows,
                        stored_rows=sync.stored_rows,
                        tickers=(),
                    )
                )
                continue
            try:
                chain = _filter_frame_tickers(_read_chain_dataset(record), resolved_tickers)
                observations = build_iv_observation_frame(
                    chain,
                    provider=record.provider,
                    dataset_id=record.dataset_id,
                    run_id=record.run_id,
                    observed_at=_observed_at_for_dataset(chain, record),
                    source_created_at=record.created_at,
                )
                observation_date = (
                    str(observations["observation_date"].iloc[0])
                    if not observations.empty
                    else None
                )
                stored_rows = (
                    len(observations)
                    if dry_run
                    else iv_store.upsert_observations(observations)
                )
                status = "DRY_RUN" if dry_run else ("INGESTED" if stored_rows else "EMPTY")
                if not dry_run:
                    iv_store.record_sync(
                        dataset_id=record.dataset_id,
                        provider=record.provider,
                        run_id=record.run_id,
                        status=status,
                        observation_date=(
                            datetime.fromisoformat(observation_date).date()
                            if observation_date is not None
                            else None
                        ),
                        source_rows=len(chain),
                        stored_rows=stored_rows,
                    )
                rows.append(
                    IVHistoryBackfillRow(
                        provider=record.provider,
                        dataset_id=record.dataset_id,
                        run_id=record.run_id,
                        status=status,
                        observation_date=observation_date,
                        source_rows=len(chain),
                        stored_rows=stored_rows,
                        tickers=_row_tickers(observations),
                    )
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                error_summary = str(exc).splitlines()[0]
                if not dry_run:
                    iv_store.record_sync(
                        dataset_id=record.dataset_id,
                        provider=record.provider,
                        run_id=record.run_id,
                        status=BACKFILL_STATUS_ERROR,
                        observation_date=None,
                        source_rows=0,
                        stored_rows=0,
                        error_summary=error_summary,
                    )
                rows.append(
                    IVHistoryBackfillRow(
                        provider=record.provider,
                        dataset_id=record.dataset_id,
                        run_id=record.run_id,
                        status=BACKFILL_STATUS_ERROR,
                        observation_date=None,
                        source_rows=0,
                        stored_rows=0,
                        tickers=(),
                        error_summary=error_summary,
                    )
                )
    finally:
        if owns_store:
            iv_store.close()

    return IVHistoryBackfillResult(
        providers=resolved_providers,
        tickers=resolved_tickers,
        lookback_days=resolved_lookback,
        limit=resolved_limit,
        refresh=refresh,
        dry_run=dry_run,
        rows=tuple(rows),
        fetch_historical=False,
    )


def _format_tickers(tickers: tuple[str, ...]) -> str:
    return ",".join(tickers) if tickers else "-"


def format_backfill_result(result: IVHistoryBackfillResult) -> str:
    """Return a compact operator-readable IV-history backfill summary."""
    source = "historical provider fetch" if result.fetch_historical else "retained datasets"
    lines = [
        "IV history backfill",
        f"source: {source}",
        f"providers: {', '.join(result.providers)}",
        f"tickers: {', '.join(result.tickers) if result.tickers else 'all dataset tickers'}",
        f"lookback_days: {result.lookback_days}",
        f"dataset_limit: {result.limit}",
        f"mode: {'dry-run' if result.dry_run else 'write'}",
        f"refresh: {'yes' if result.refresh else 'no'}",
    ]
    if result.fetch_historical:
        lines.extend(
            [
                f"sessions: {result.sessions}",
                f"end_date: {result.end_date}",
                f"estimated_provider_requests: {result.estimated_requests}",
            ]
        )
    lines.extend(
        [
            "",
            "provider    dataset   status       date        source_rows  stored_rows  tickers",
        ]
    )
    for row in result.rows:
        lines.append(
            f"{row.provider:<11} {row.dataset_id[:8]:<9} {row.status:<12} "
            f"{row.observation_date or '-':<10} {row.source_rows:<12} "
            f"{row.stored_rows:<11} {_format_tickers(row.tickers)}"
        )
        if row.error_summary:
            lines.append(f"  error: {row.error_summary}")
    return "\n".join(lines)


def parse_args(argv=None):
    """Parse IV-history backfill CLI arguments."""
    parser = argparse.ArgumentParser(
        prog="opx-iv-history-backfill",
        description=(
            "Backfill provider-scoped implied-volatility history from retained "
            "option-chain datasets, or explicitly seed it from provider "
            "historical option-chain snapshots."
        ),
    )
    parser.add_argument(
        "--providers",
        action="append",
        default=None,
        help="Comma-separated provider list. Defaults to the configured provider.",
    )
    parser.add_argument(
        "--tickers",
        action="append",
        default=None,
        help="Comma-separated ticker list. Defaults to settings.tickers; empty means all rows.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=365,
        help="Dataset created_at lookback window to ingest. Defaults to 365.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum datasets to inspect per provider. Defaults to 200.",
    )
    parser.add_argument(
        "--dataset-id",
        action="append",
        default=None,
        help="Specific dataset id to ingest. Can be repeated or comma-separated.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Reingest datasets even when a sync record already exists.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report datasets and derived aggregate rows without writing iv-history.db.",
    )
    parser.add_argument(
        "--fetch-historical",
        action="store_true",
        help=(
            "Fetch historical option-chain snapshots from supported providers and "
            "write only iv-history.db. Use --dry-run first to estimate requests."
        ),
    )
    parser.add_argument(
        "--sessions",
        type=int,
        default=20,
        help="Historical business sessions to fetch when --fetch-historical is set.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help=(
            "Last historical observation date for --fetch-historical, YYYY-MM-DD. "
            "Defaults to the configured market date."
        ),
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    """CLI entrypoint for durable IV-history backfill."""
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
        result = run_iv_history_backfill(
            providers=args.providers,
            tickers=args.tickers,
            lookback_days=args.lookback_days,
            limit=args.limit,
            dataset_ids=args.dataset_id,
            fetch_historical=args.fetch_historical,
            sessions=args.sessions,
            end_date=args.end_date,
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
