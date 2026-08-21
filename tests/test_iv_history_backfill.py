"""Tests for durable IV-history backfill from retained and historical data."""

# pylint: disable=too-many-lines

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pandas as pd
import pytest

from conftest import make_runtime_config
import opx_chain.iv_history_backfill as backfill_module
from opx_chain.config import get_runtime_config, set_runtime_config_override
from opx_chain.iv_history import (
    IVHistoryStore,
    check_iv_history_integrity,
)
from opx_chain.iv_history_backfill import (
    IVHistoryRecoveryBusyError,
    format_backfill_result,
    format_recovery_result,
    main,
    recover_iv_history_store,
    run_iv_history_backfill,
)
from opx_chain.storage.models import DatasetRecord


def _dataset(path, *, ticker: str = "TSLA", quote_time: str | None = None) -> None:
    rows = []
    for dte, expiration, iv in (
        (14, "2026-06-05", 0.24),
        (14, "2026-06-05", 0.28),
        (28, "2026-06-19", 0.31),
    ):
        rows.append(
            {
                "underlying_symbol": ticker,
                "expiration_date": expiration,
                "days_to_expiration": dte,
                "option_type": "PUT",
                "delta": -0.2,
                "implied_volatility": iv,
                "option_quote_time": quote_time,
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)


class FakeStorage:
    """Storage stub that exposes retained option-chain datasets."""

    def __init__(self, records, *, runs_dir=None):
        self.records = list(records)
        self._runs_dir = runs_dir

    def list_datasets(self, **kwargs):  # pylint: disable=unused-argument
        """Return configured records."""
        return self.records

    def get_dataset(self, dataset_id: str):
        """Return one configured record by dataset id."""
        for record in self.records:
            if record.dataset_id == dataset_id:
                return record
        raise KeyError(dataset_id)

    def load_validated_option_chain_dataset(self, dataset_id: str):
        """Return a fresh frame for the selected fake retained record."""
        record = self.get_dataset(dataset_id)
        location = Path(record.location).resolve()
        if self._runs_dir is not None and not location.is_relative_to(
            Path(self._runs_dir).resolve()
        ):
            raise ValueError(
                f"dataset location is outside managed storage roots: {dataset_id}"
            )
        return SimpleNamespace(frame=pd.read_csv(record.location))


class HistoricalProvider:
    """Provider stub that exposes historical option-chain snapshots."""

    name = "marketdata"

    def __init__(self, *, response_tickers: tuple[str, ...] | None = None) -> None:
        self.prepared: list[str] = []
        self.calls: list[tuple[str, date]] = []
        self.response_tickers = response_tickers

    def prepare_ticker_fetch(self, ticker: str) -> None:
        """Record ticker preparation."""
        self.prepared.append(ticker)

    def load_historical_option_chain_frame(
        self,
        ticker: str,
        *,
        observation_date: date,
    ) -> pd.DataFrame:
        """Return a minimal historical option-chain frame."""
        self.calls.append((ticker, observation_date))
        expiration = (observation_date + timedelta(days=21)).isoformat()
        response_tickers = self.response_tickers or (ticker, ticker)
        rows = []
        for index, response_ticker in enumerate(response_tickers):
            rows.append(
                {
                    "underlying_symbol": response_ticker,
                    "expiration_date": expiration,
                    "option_type": "PUT" if index % 2 == 0 else "CALL",
                    "delta": -0.20 if index % 2 == 0 else 0.20,
                    "implied_volatility": 0.30 + (index * 0.02),
                }
            )
        return pd.DataFrame(rows)


class WrongHistoricalProvider(HistoricalProvider):
    """Provider stub with mismatched public identity."""

    name = "yfinance"


class BlankHistoricalError(Exception):
    """Historical provider error whose string representation is blank."""

    def __str__(self) -> str:
        """Return a deliberately blank message."""
        return ""


class BlankFailingHistoricalProvider(HistoricalProvider):
    """Historical provider stub that raises a blank-message exception."""

    def load_historical_option_chain_frame(
        self,
        ticker: str,
        *,
        observation_date: date,
    ) -> pd.DataFrame:
        """Raise a blank-message provider error after recording the request."""
        self.calls.append((ticker, observation_date))
        raise BlankHistoricalError()


def _record(path, *, dataset_id: str = "dataset-1") -> DatasetRecord:
    return DatasetRecord(
        dataset_id=dataset_id,
        run_id="run-1",
        created_at=datetime(2026, 5, 22, 20, 0, tzinfo=timezone.utc),
        provider="marketdata",
        schema_version=2,
        row_count=3,
        format="csv",
        location=str(path),
        content_hash="hash",
    )


def _record_for_provider(
    path,
    *,
    provider: str,
    dataset_id: str,
) -> DatasetRecord:
    record = _record(path, dataset_id=dataset_id)
    return DatasetRecord(
        dataset_id=record.dataset_id,
        run_id=record.run_id,
        created_at=record.created_at,
        provider=provider,
        schema_version=record.schema_version,
        row_count=record.row_count,
        format=record.format,
        location=record.location,
        content_hash=record.content_hash,
    )


def test_iv_history_backfill_ingests_retained_dataset(tmp_path):
    """Backfill should derive IV aggregates without provider API calls."""
    path = tmp_path / "chain.csv"
    _dataset(path)
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=datetime(2026, 5, 22, tzinfo=timezone.utc).date(),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        config=config,
        store=store,
        storage=FakeStorage([_record(path)]),
    )

    assert result.rows[0].status == "INGESTED"
    assert result.rows[0].source_rows == 3
    assert result.rows[0].stored_rows > 0
    assert store.stats(provider="marketdata", ticker="TSLA").observation_dates == 1


def test_iv_history_backfill_accepts_scalar_csv_strings(tmp_path):
    """Programmatic callers should not have scalar provider/ticker strings split."""
    path = tmp_path / "chain.csv"
    _dataset(path)
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=datetime(2026, 5, 22, tzinfo=timezone.utc).date(),
    )

    result = run_iv_history_backfill(
        providers="marketdata",
        tickers="TSLA",
        dataset_ids="dataset-1",
        config=config,
        store=store,
        storage=FakeStorage([_record(path)]),
    )

    assert result.providers == ("marketdata",)
    assert result.tickers == ("TSLA",)
    assert result.rows[0].status == "INGESTED"
    assert store.stats(provider="marketdata", ticker="TSLA").observation_dates == 1


@pytest.mark.parametrize(
    "bad_ticker",
    ["BAD/TICKER", "...", "TSLA1", "A.", "BRK..B", "ABCDEFGHIJK"],
)
def test_iv_history_backfill_rejects_malformed_tickers(tmp_path, bad_ticker):
    """Backfill should validate direct ticker scope before provider/store work."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(data_provider="marketdata", tickers=("TSLA",))

    with pytest.raises(ValueError, match="valid stock ticker"):
        run_iv_history_backfill(
            providers=("marketdata",),
            tickers=(bad_ticker,),
            config=config,
            store=store,
            storage=FakeStorage([]),
        )


@pytest.mark.parametrize("bad_ticker", [True, None, False])
def test_iv_history_backfill_rejects_non_string_ticker_scope(tmp_path, bad_ticker):
    """Direct ticker filter members should not be stringified into symbols."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(data_provider="marketdata", tickers=("TSLA",))

    with pytest.raises(ValueError, match="tickers must be a string or iterable of strings"):
        run_iv_history_backfill(
            providers=("marketdata",),
            tickers=(bad_ticker,),
            config=config,
            store=store,
            storage=FakeStorage([]),
        )


@pytest.mark.parametrize("bad_ticker", [True, None, False, float("nan")])
def test_iv_history_backfill_rejects_non_string_default_config_tickers(
    tmp_path,
    bad_ticker,
):
    """Default config ticker members should not be stringified into symbols."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(data_provider="marketdata", tickers=(bad_ticker,))

    with pytest.raises(ValueError, match="ticker must be a non-empty string"):
        run_iv_history_backfill(
            providers=("marketdata",),
            tickers=None,
            fetch_historical=True,
            dry_run=True,
            sessions=1,
            config=config,
            store=store,
            storage=FakeStorage([]),
        )


def test_iv_history_backfill_rejects_malformed_default_provider(tmp_path) -> None:
    """Direct config provider values should fail at a stable backfill boundary."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(data_provider=True, tickers=("TSLA",))

    with pytest.raises(ValueError, match="data_provider must be a non-empty string"):
        run_iv_history_backfill(
            config=config,
            store=store,
            storage=FakeStorage([]),
        )


def test_iv_history_backfill_filters_explicit_dataset_ids_by_provider(tmp_path):
    """Explicit retained datasets should still respect requested provider scope."""
    marketdata_path = tmp_path / "marketdata.csv"
    yfinance_path = tmp_path / "yfinance.csv"
    _dataset(marketdata_path)
    _dataset(yfinance_path)
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(data_provider="marketdata", tickers=("TSLA",))
    storage = FakeStorage(
        [
            _record_for_provider(
                marketdata_path,
                provider="marketdata",
                dataset_id="marketdata-dataset",
            ),
            _record_for_provider(
                yfinance_path,
                provider="yfinance",
                dataset_id="yfinance-dataset",
            ),
        ]
    )

    with pytest.raises(ValueError, match="outside requested provider scope"):
        run_iv_history_backfill(
            providers=("marketdata",),
            dataset_ids=("yfinance-dataset",),
            config=config,
            store=store,
            storage=storage,
        )

    assert store.stats(provider="marketdata", ticker="TSLA").row_count == 0
    assert store.stats(provider="yfinance", ticker="TSLA").row_count == 0


def test_iv_history_backfill_prefers_quote_date_over_dataset_created_at(tmp_path):
    """Weekend backfills should store IV under the quote date when available."""
    path = tmp_path / "chain.csv"
    _dataset(path, quote_time="2026-05-22T20:00:00Z")
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=datetime(2026, 5, 24, tzinfo=timezone.utc).date(),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        config=config,
        store=store,
        storage=FakeStorage([_record(path)]),
    )

    assert result.rows[0].observation_date == "2026-05-22"
    assert store.stats(provider="marketdata", ticker="TSLA").latest_date.isoformat() == "2026-05-22"


def test_iv_history_backfill_dry_run_does_not_write(tmp_path):
    """Dry run should compute aggregate rows but leave the durable store untouched."""
    path = tmp_path / "chain.csv"
    _dataset(path)
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=datetime(2026, 5, 22, tzinfo=timezone.utc).date(),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        config=config,
        dry_run=True,
        store=store,
        storage=FakeStorage([_record(path)]),
    )

    assert result.rows[0].status == "DRY_RUN"
    assert result.rows[0].stored_rows > 0
    assert store.stats(provider="marketdata", ticker="TSLA").row_count == 0
    assert "mode: dry-run" in format_backfill_result(result)


def test_iv_history_backfill_skips_already_ingested_dataset(tmp_path):
    """Existing sync records should prevent duplicate ingest unless refreshed."""
    path = tmp_path / "chain.csv"
    _dataset(path)
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(data_provider="marketdata", tickers=("TSLA",))
    storage = FakeStorage([_record(path)])

    first = run_iv_history_backfill(
        providers=("marketdata",),
        config=config,
        store=store,
        storage=storage,
    )
    second = run_iv_history_backfill(
        providers=("marketdata",),
        config=config,
        store=store,
        storage=storage,
    )

    assert first.rows[0].status == "INGESTED"
    assert second.rows[0].status == "SKIPPED"


def test_iv_history_backfill_rejects_outside_dataset_location(tmp_path):
    """Retained replay should not ingest artifacts outside the storage runs root."""
    outside_path = tmp_path / "outside.csv"
    _dataset(outside_path)
    store = IVHistoryStore(tmp_path / "iv-history.db")
    record = _record(outside_path)
    storage = FakeStorage([record], runs_dir=tmp_path / "runs")
    config = make_runtime_config(data_provider="marketdata", tickers=("TSLA",))

    result = run_iv_history_backfill(
        providers=("marketdata",),
        config=config,
        store=store,
        storage=storage,
    )

    assert result.rows[0].status == "ERROR"
    assert "outside managed storage roots" in (result.rows[0].error_summary or "")
    assert store.stats(provider="marketdata", ticker="TSLA").row_count == 0


def test_iv_history_backfill_preserves_ingested_sync_when_artifact_missing(tmp_path):
    """Missing retained artifacts must not downgrade a successful durable sync row."""
    runs_dir = tmp_path / "runs"
    path = runs_dir / "run-1" / "output" / "chain.csv"
    path.parent.mkdir(parents=True)
    _dataset(path, quote_time="2026-05-22T20:00:00Z")
    store = IVHistoryStore(tmp_path / "iv-history.db")
    record = _record(path)
    storage = FakeStorage([record], runs_dir=runs_dir)
    config = make_runtime_config(data_provider="marketdata", tickers=("TSLA",))

    first = run_iv_history_backfill(
        providers=("marketdata",),
        config=config,
        store=store,
        storage=storage,
    )
    path.unlink()
    second = run_iv_history_backfill(
        providers=("marketdata",),
        config=config,
        store=store,
        storage=storage,
    )
    sync = store.get_sync(dataset_id=record.dataset_id)

    assert first.rows[0].status == "INGESTED"
    assert second.rows[0].status == "SKIPPED"
    assert second.rows[0].error_summary
    assert sync is not None
    assert sync.status == "INGESTED"
    assert sync.stored_rows == first.rows[0].stored_rows


def test_iv_history_backfill_ticker_filtered_empty_does_not_downgrade_sync(tmp_path):
    """A later ticker-filter miss should not overwrite a complete dataset sync."""
    path = tmp_path / "chain.csv"
    _dataset(path, ticker="TSLA", quote_time="2026-05-22T20:00:00Z")
    store = IVHistoryStore(tmp_path / "iv-history.db")
    record = _record(path)
    storage = FakeStorage([record])
    config = make_runtime_config(data_provider="marketdata", tickers=("TSLA",))

    first = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        config=config,
        store=store,
        storage=storage,
    )
    second = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("NVDA",),
        config=config,
        store=store,
        storage=storage,
    )
    sync = store.get_sync(dataset_id=record.dataset_id)

    assert first.rows[0].status == "INGESTED"
    assert second.rows[0].status == "SKIPPED"
    assert sync is not None
    assert sync.status == "INGESTED"
    assert sync.stored_rows == first.rows[0].stored_rows
    assert store.stats(provider="marketdata", ticker="TSLA").observation_dates == 1
    assert store.stats(provider="marketdata", ticker="NVDA").row_count == 0


@pytest.mark.parametrize("status", ["ERROR", "EMPTY"])
def test_iv_history_backfill_retries_failed_or_empty_sync_without_refresh(
    tmp_path,
    status,
):
    """Failed or empty retained-dataset sync rows should not block repair replay."""
    path = tmp_path / "chain.csv"
    _dataset(path)
    store = IVHistoryStore(tmp_path / "iv-history.db")
    record = _record(path)
    store.record_sync(
        dataset_id=record.dataset_id,
        provider=record.provider,
        run_id=record.run_id,
        status=status,
        observation_date=None,
        source_rows=0,
        stored_rows=0,
        error_summary="prior failure" if status == "ERROR" else None,
    )
    config = make_runtime_config(data_provider="marketdata", tickers=("TSLA",))

    result = run_iv_history_backfill(
        providers=("marketdata",),
        config=config,
        store=store,
        storage=FakeStorage([record]),
    )

    assert result.rows[0].status == "INGESTED"
    assert result.rows[0].source_rows == 3
    assert result.rows[0].stored_rows > 0
    assert store.stats(provider="marketdata", ticker="TSLA").observation_dates == 1


def test_iv_history_backfill_retries_malformed_sync_timestamp(tmp_path):
    """Malformed retained sync metadata should not abort retained replay."""
    path = tmp_path / "chain.csv"
    _dataset(path)
    store = IVHistoryStore(tmp_path / "iv-history.db")
    record = _record(path)
    conn = store._connection_for_use()  # pylint: disable=protected-access
    conn.execute(
        """
        INSERT INTO iv_history_syncs
            (dataset_id, provider, run_id, checked_at, status,
             observation_date, source_rows, stored_rows)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record.dataset_id,
            record.provider,
            record.run_id,
            "not-a-timestamp",
            "INGESTED",
            "2026-05-22",
            3,
            8,
        ),
    )
    conn.commit()
    config = make_runtime_config(data_provider="marketdata", tickers=("TSLA",))

    result = run_iv_history_backfill(
        providers=("marketdata",),
        config=config,
        store=store,
        storage=FakeStorage([record]),
    )

    assert result.rows[0].status == "INGESTED"
    assert result.rows[0].stored_rows > 0


def test_iv_history_backfill_retries_ingested_sync_without_observations(tmp_path):
    """Successful sync metadata alone should not suppress a retained replay."""
    path = tmp_path / "chain.csv"
    _dataset(path, quote_time="2026-05-22T20:00:00Z")
    store = IVHistoryStore(tmp_path / "iv-history.db")
    record = _record(path)
    store.record_sync(
        dataset_id=record.dataset_id,
        provider=record.provider,
        run_id=record.run_id,
        status="INGESTED",
        observation_date=date(2026, 5, 22),
        source_rows=3,
        stored_rows=8,
    )
    config = make_runtime_config(data_provider="marketdata", tickers=("TSLA",))

    result = run_iv_history_backfill(
        providers=("marketdata",),
        config=config,
        store=store,
        storage=FakeStorage([record]),
    )

    assert result.rows[0].status == "INGESTED"
    assert result.rows[0].source_rows == 3
    assert result.rows[0].stored_rows > 0
    assert store.stats(provider="marketdata", ticker="TSLA").observation_dates == 1


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("refresh", "false", "refresh must be a boolean"),
        ("dry_run", "false", "dry_run must be a boolean"),
        ("fetch_historical", "false", "fetch_historical must be a boolean"),
    ],
)
def test_iv_history_backfill_rejects_false_like_boolean_strings(
    tmp_path,
    field,
    value,
    message,
):
    """Programmatic callers must pass real booleans for mutation controls."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(data_provider="marketdata", tickers=("TSLA",))
    kwargs = {field: value}

    with pytest.raises(ValueError, match=message):
        run_iv_history_backfill(config=config, store=store, **kwargs)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("lookback_days", "365", "lookback_days must be a positive integer"),
        ("limit", 1.5, "limit must be a positive integer"),
        ("sessions", True, "sessions must be a positive integer"),
    ],
)
def test_iv_history_backfill_rejects_loosely_typed_numeric_windows(
    tmp_path,
    field,
    value,
    message,
):
    """Programmatic callers must pass integer windows, not coercible values."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(data_provider="marketdata", tickers=("TSLA",))
    kwargs = {field: value}

    with pytest.raises(ValueError, match=message):
        run_iv_history_backfill(config=config, store=store, **kwargs)


def test_iv_history_historical_dry_run_estimates_requests_without_fetch(tmp_path):
    """Historical dry-run should show provider calls without consuming API quota."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    provider = HistoricalProvider()
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA", "NVDA"),
        today=date(2026, 5, 22),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        fetch_historical=True,
        sessions=2,
        config=config,
        store=store,
        dry_run=True,
        provider_factory=lambda _provider_name: provider,
    )

    assert not provider.calls
    assert result.fetch_historical is True
    assert result.estimated_requests == 4
    assert [row.status for row in result.rows] == ["WOULD_FETCH"] * 4
    assert "source: historical provider fetch" in format_backfill_result(result)
    assert "estimated_provider_requests: 4" in format_backfill_result(result)


def test_iv_history_historical_sessions_roll_weekend_end_date(tmp_path):
    """Historical session planning should count back from the prior business day."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=date(2026, 5, 24),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        fetch_historical=True,
        sessions=2,
        config=config,
        store=store,
        dry_run=True,
    )

    assert [row.observation_date for row in result.rows] == [
        "2026-05-21",
        "2026-05-22",
    ]
    assert result.estimated_requests == 2


def test_iv_history_historical_backfill_rejects_datetime_end_date(tmp_path):
    """Historical request boundaries are date-only and must not truncate datetimes."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=date(2026, 5, 24),
    )

    with pytest.raises(ValueError, match="end_date must be YYYY-MM-DD"):
        run_iv_history_backfill(
            providers=("marketdata",),
            fetch_historical=True,
            sessions=2,
            end_date=datetime(2026, 5, 22, 15, 30, tzinfo=timezone.utc),
            config=config,
            store=store,
            dry_run=True,
        )


def test_iv_history_historical_fetch_ingests_marketdata_snapshots(tmp_path):
    """Historical fetch should write provider/ticker/date IV aggregates."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    provider = HistoricalProvider()
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=date(2026, 5, 22),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        fetch_historical=True,
        sessions=2,
        config=config,
        store=store,
        provider_factory=lambda _provider_name: provider,
    )

    assert [row.status for row in result.rows] == ["INGESTED", "INGESTED"]
    assert provider.prepared == ["TSLA"]
    assert provider.calls == [
        ("TSLA", date(2026, 5, 21)),
        ("TSLA", date(2026, 5, 22)),
    ]
    stats = store.stats(provider="marketdata", ticker="TSLA")
    assert stats.observation_dates == 2
    assert stats.latest_date == date(2026, 5, 22)


def test_iv_history_historical_fetch_restores_existing_runtime_override(tmp_path):
    """Historical provider overrides should not clear an embedding caller override."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    provider = HistoricalProvider()
    outer_config = make_runtime_config(data_provider="yfinance", tickers=("OUTER",))
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=date(2026, 5, 22),
    )
    set_runtime_config_override(outer_config)

    run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        fetch_historical=True,
        sessions=1,
        config=config,
        store=store,
        provider_factory=lambda _provider_name: provider,
    )

    assert get_runtime_config() is outer_config


def test_iv_history_historical_fetch_rejects_provider_identity_mismatch(tmp_path):
    """Historical fetch should not label rows with a different requested provider."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    provider = WrongHistoricalProvider()
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=date(2026, 5, 22),
    )

    with pytest.raises(ValueError, match="provider_factory returned provider"):
        run_iv_history_backfill(
            providers=("marketdata",),
            tickers=("TSLA",),
            fetch_historical=True,
            sessions=1,
            config=config,
            store=store,
            provider_factory=lambda _provider_name: provider,
        )

    assert store.stats(provider="marketdata", ticker="TSLA").row_count == 0


def test_iv_history_historical_fetch_filters_mixed_provider_tickers(tmp_path):
    """Historical fetch should write only rows matching the requested ticker."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    provider = HistoricalProvider(response_tickers=("TSLA", "NVDA"))
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=date(2026, 5, 22),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        fetch_historical=True,
        sessions=1,
        config=config,
        store=store,
        provider_factory=lambda _provider_name: provider,
    )

    assert result.rows[0].status == "INGESTED"
    assert result.rows[0].tickers == ("TSLA",)
    assert store.stats(provider="marketdata", ticker="TSLA").observation_dates == 1
    assert store.stats(provider="marketdata", ticker="NVDA").row_count == 0


def test_iv_history_historical_fetch_rejects_wrong_ticker_only_response(tmp_path):
    """Historical fetch should not pollute IV history when the provider returns another ticker."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    provider = HistoricalProvider(response_tickers=("NVDA", "NVDA"))
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=date(2026, 5, 22),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        fetch_historical=True,
        sessions=1,
        config=config,
        store=store,
        provider_factory=lambda _provider_name: provider,
    )

    assert result.rows[0].status == "ERROR"
    assert "requested ticker TSLA" in (result.rows[0].error_summary or "")
    assert store.stats(provider="marketdata", ticker="TSLA").row_count == 0
    assert store.stats(provider="marketdata", ticker="NVDA").row_count == 0


def test_iv_history_historical_fetch_blank_provider_error_records_summary(tmp_path):
    """Blank historical provider errors should produce stable ERROR rows."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    provider = BlankFailingHistoricalProvider()
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=date(2026, 5, 22),
    )

    result = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        fetch_historical=True,
        sessions=1,
        config=config,
        store=store,
        provider_factory=lambda _provider_name: provider,
    )

    assert result.rows[0].status == "ERROR"
    assert result.rows[0].error_summary == "BlankHistoricalError"
    sync = store.get_sync(dataset_id=result.rows[0].dataset_id)
    assert sync is not None
    assert sync.status == "ERROR"
    assert sync.error_summary == "BlankHistoricalError"


def test_iv_history_historical_fetch_skips_existing_date_without_refresh(tmp_path):
    """Existing provider/ticker/date coverage should not be refetched by default."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    provider = HistoricalProvider()
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        today=date(2026, 5, 22),
    )

    first = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        fetch_historical=True,
        sessions=1,
        config=config,
        store=store,
        provider_factory=lambda _provider_name: provider,
    )
    second = run_iv_history_backfill(
        providers=("marketdata",),
        tickers=("TSLA",),
        fetch_historical=True,
        sessions=1,
        config=config,
        store=store,
        provider_factory=lambda _provider_name: provider,
    )

    assert first.rows[0].status == "INGESTED"
    assert second.rows[0].status == "SKIPPED"
    assert provider.calls == [("TSLA", date(2026, 5, 22))]


def test_iv_history_historical_fetch_rejects_unsupported_provider(tmp_path):
    """Only providers with historical option-chain snapshots should be accepted."""
    store = IVHistoryStore(tmp_path / "iv-history.db")
    config = make_runtime_config(data_provider="yfinance", tickers=("TSLA",))

    with pytest.raises(ValueError, match="historical IV fetch is only available"):
        run_iv_history_backfill(
            providers=("yfinance",),
            fetch_historical=True,
            config=config,
            store=store,
            dry_run=True,
        )


def _corrupt_recovery_fixture(tmp_path):
    chain_path = tmp_path / "chain.csv"
    _dataset(chain_path)
    database_path = tmp_path / "iv-history.db"
    store = IVHistoryStore(database_path)
    store.close()
    valid_payload = database_path.read_bytes()
    encoded_page_size = int.from_bytes(valid_payload[16:18], "big")
    page_size = 65536 if encoded_page_size == 1 else encoded_page_size
    declared_pages = int.from_bytes(valid_payload[28:32], "big")
    assert declared_pages > 1
    assert len(valid_payload) == page_size * declared_pages
    corrupt_payload = valid_payload[:-page_size]
    database_path.write_bytes(corrupt_payload)
    config = make_runtime_config(
        data_provider="marketdata",
        tickers=("TSLA",),
        storage_dir=tmp_path,
        today=date(2026, 5, 22),
    )
    storage = FakeStorage([_record(chain_path)], runs_dir=tmp_path)
    return database_path, corrupt_payload, config, storage


def test_iv_history_truncated_page_fails_integrity_and_constructor(tmp_path):
    """A valid-header database missing a declared page must fail closed."""
    database_path, corrupt_payload, _config, _storage = _corrupt_recovery_fixture(
        tmp_path
    )
    page_size = int.from_bytes(corrupt_payload[16:18], "big")
    if page_size == 1:
        page_size = 65536
    declared_pages = int.from_bytes(corrupt_payload[28:32], "big")

    assert corrupt_payload.startswith(b"SQLite format 3\x00")
    assert len(corrupt_payload) == page_size * (declared_pages - 1)
    integrity = check_iv_history_integrity(database_path)
    assert integrity.status == "ERROR"
    assert "malformed" in (integrity.error_summary or "").lower()
    with pytest.raises(sqlite3.DatabaseError, match="malformed"):
        IVHistoryStore(database_path)


def test_iv_history_recovery_dry_run_never_changes_corrupt_store(tmp_path):
    """Recovery planning should derive rows in a disposable database only."""
    database_path, corrupt_payload, config, storage = _corrupt_recovery_fixture(
        tmp_path
    )

    result = recover_iv_history_store(
        providers=("marketdata",),
        tickers=("TSLA",),
        dry_run=True,
        config=config,
        storage=storage,
    )

    assert result.original_status == "ERROR"
    assert result.candidate_rows > 0
    assert result.recovered is False
    assert database_path.read_bytes() == corrupt_payload
    assert not list(tmp_path.glob("iv-history.corrupt-*.db"))
    assert "provider requests: 0" in format_recovery_result(result)


def test_iv_history_recovery_quarantines_and_atomically_rebuilds(tmp_path):
    """A usable retained replay should replace, not mutate, the corrupt store."""
    database_path, corrupt_payload, config, storage = _corrupt_recovery_fixture(
        tmp_path
    )

    result = recover_iv_history_store(
        providers=("marketdata",),
        tickers=("TSLA",),
        config=config,
        storage=storage,
    )

    assert result.recovered is True
    assert result.quarantine_path is not None
    assert result.quarantine_path.read_bytes() == corrupt_payload
    assert check_iv_history_integrity(database_path).healthy
    store = IVHistoryStore(database_path)
    assert store.stats(provider="marketdata", ticker="TSLA").observation_dates == 1
    store.close()


def test_iv_history_recovery_preserves_active_store_when_replay_is_empty(tmp_path):
    """An empty retained replay must not displace the original database."""
    database_path, corrupt_payload, config, _storage = _corrupt_recovery_fixture(
        tmp_path
    )

    with pytest.raises(RuntimeError, match="no usable IV history rows"):
        recover_iv_history_store(
            providers=("marketdata",),
            tickers=("TSLA",),
            config=config,
            storage=FakeStorage([], runs_dir=tmp_path),
        )

    assert database_path.read_bytes() == corrupt_payload
    assert not list(tmp_path.glob("iv-history.corrupt-*.db"))


def test_iv_history_recovery_rolls_back_failed_atomic_install(tmp_path, monkeypatch):
    """An install failure should restore the quarantined database in place."""
    database_path, corrupt_payload, config, storage = _corrupt_recovery_fixture(
        tmp_path
    )
    real_replace = Path.replace
    calls = 0

    def fail_candidate_install(source, destination):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated install failure")
        real_replace(source, destination)

    monkeypatch.setattr(backfill_module, "_atomic_replace", fail_candidate_install)

    with pytest.raises(OSError, match="simulated install failure"):
        recover_iv_history_store(
            providers=("marketdata",),
            tickers=("TSLA",),
            config=config,
            storage=storage,
        )

    assert calls == 3
    assert database_path.read_bytes() == corrupt_payload
    assert not list(tmp_path.glob("iv-history.corrupt-*.db"))


def test_iv_history_recovery_cli_honors_shared_writer_lock(
    tmp_path,
    monkeypatch,
    capsys,
):
    """Write-mode recovery must stop before work when the shared lock is busy."""
    config = make_runtime_config(storage_dir=tmp_path)
    monkeypatch.setattr(backfill_module, "get_runtime_config", lambda: config)
    monkeypatch.setattr(
        backfill_module,
        "acquire_nonblocking_file_lock",
        lambda _path: None,
    )
    monkeypatch.setattr(
        backfill_module,
        "_recover_iv_history_store_unlocked",
        lambda **_kwargs: pytest.fail("recovery must not start without the lock"),
    )

    assert main(["--recover-corrupt"]) == 1
    assert "Another fetcher/backfill run is already active" in capsys.readouterr().out


def test_iv_history_recovery_public_api_honors_shared_writer_lock(
    tmp_path,
    monkeypatch,
):
    """Programmatic write-mode recovery must own the shared writer lock."""
    config = make_runtime_config(storage_dir=tmp_path)
    monkeypatch.setattr(
        backfill_module,
        "acquire_nonblocking_file_lock",
        lambda _path: None,
    )
    monkeypatch.setattr(
        backfill_module,
        "_recover_iv_history_store_unlocked",
        lambda **_kwargs: pytest.fail("recovery must not start without the lock"),
    )

    with pytest.raises(IVHistoryRecoveryBusyError, match="already active"):
        recover_iv_history_store(config=config)


def test_iv_history_recovery_cli_rejects_historical_provider_fetch(
    tmp_path,
    monkeypatch,
    capsys,
):
    """Corrupt-store recovery must remain separate from provider-backed seeding."""
    config = make_runtime_config(storage_dir=tmp_path)
    monkeypatch.setattr(backfill_module, "get_runtime_config", lambda: config)
    monkeypatch.setattr(
        backfill_module,
        "recover_iv_history_store",
        lambda **_kwargs: pytest.fail("invalid recovery mode must not execute"),
    )

    assert main(["--recover-corrupt", "--fetch-historical", "--dry-run"]) == 1
    output = capsys.readouterr().out
    assert "cannot be combined with --fetch-historical" in output
    assert "retained datasets only" in output
