"""Tests for the public backup-inventory dependency surface."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from opx_chain.backup_inventory import build_backup_inventory
from opx_chain.paths import get_data_dir


def _sqlite_file(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE sample (value TEXT)")
        conn.commit()
    finally:
        conn.close()


def test_backup_inventory_reports_execution_dependencies(tmp_path: Path) -> None:
    """Inventory should include durable DBs, price context, and retained chains."""
    data_dir = tmp_path / "data"
    runs_dir = data_dir / "runs"
    chain_path = runs_dir / "run-1" / "output" / "dataset.parquet"
    chain_path.parent.mkdir(parents=True)
    chain_path.write_bytes(b"chain")
    _sqlite_file(data_dir / "price-history.db")
    _sqlite_file(data_dir / "iv-history.db")
    (runs_dir / "price_context_latest.json").write_text(
        json.dumps(
            {
                "artifact_type": "price_context",
                "provider": "marketdata",
                "fetched_at": "2026-05-31T15:00:00Z",
                "records": [
                    {
                        "ticker": "TSLA",
                        "price_context_staleness_status": "FRESH",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (data_dir / "event_snapshot_latest.json").write_text(
        json.dumps({
            "artifact_type": "event_data_snapshot",
            "provider": "yfinance",
            "status": "ready",
            "fetched_at": "2026-05-31T15:05:00Z",
            "trading_date": "2026-05-31",
            "records": [{"ticker": "TSLA"}],
        }),
        encoding="utf-8",
    )
    event_snapshot = data_dir / "event_snapshots" / "event-1.json"
    event_snapshot.parent.mkdir()
    event_snapshot.write_text(
        json.dumps({
            "artifact_type": "event_data_snapshot",
            "provider": "yfinance",
            "status": "ready",
            "records": [{"ticker": "TSLA"}],
        }),
        encoding="utf-8",
    )

    inventory = build_backup_inventory(
        data_dir=data_dir,
        runs_dir=runs_dir,
        chain_locations=[chain_path],
    )
    records = {record.archive_path: record for record in inventory.records}

    assert set(records) == {
        "dependencies/opx-chain/price-history.db",
        "dependencies/opx-chain/iv-history.db",
        "dependencies/opx-chain/event_snapshot_latest.json",
        "dependencies/opx-chain/event_snapshots/event-1.json",
        "dependencies/opx-chain/runs/price_context_latest.json",
        "dependencies/opx-chain/runs/run-1/output/dataset.parquet",
    }
    price_context = records["dependencies/opx-chain/runs/price_context_latest.json"]
    assert price_context.logical_kind == "price_context_artifact"
    assert price_context.required_for_execution is True
    assert price_context.provider == "marketdata"
    assert price_context.freshness_status == "FRESH"
    assert price_context.metadata["freshness_statuses"] == ["FRESH"]
    event_context = records["dependencies/opx-chain/event_snapshot_latest.json"]
    assert event_context.logical_kind == "event_data_snapshot_artifact"
    assert event_context.required_for_execution is True
    assert event_context.provider == "yfinance"
    assert event_context.freshness_status == "ready"
    assert event_context.metadata["record_count"] == 1
    dataset = records["dependencies/opx-chain/runs/run-1/output/dataset.parquet"]
    assert dataset.logical_kind == "chain_dataset_artifact"
    assert dataset.dataset_history is True
    assert dataset.required_for_execution is False


def test_backup_inventory_derives_runs_dir_from_data_dir_override(tmp_path: Path) -> None:
    """A custom data_dir should imply the matching data_dir/runs root."""
    data_dir = tmp_path / "custom-storage"
    runs_dir = data_dir / "runs"
    chain_path = runs_dir / "run-1" / "output" / "dataset.parquet"
    chain_path.parent.mkdir(parents=True)
    chain_path.write_bytes(b"chain")
    _sqlite_file(data_dir / "price-history.db")
    (runs_dir / "price_context_latest.json").write_text(
        json.dumps(
            {
                "artifact_type": "price_context",
                "provider": "marketdata",
                "records": [
                    {
                        "ticker": "TSLA",
                        "price_context_staleness_status": "FRESH",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    inventory = build_backup_inventory(
        data_dir=data_dir,
        chain_locations=[chain_path],
    )
    records = {record.archive_path: record for record in inventory.records}

    assert inventory.data_dir == data_dir
    assert inventory.runs_dir == runs_dir
    assert set(records) == {
        "dependencies/opx-chain/price-history.db",
        "dependencies/opx-chain/runs/price_context_latest.json",
        "dependencies/opx-chain/runs/run-1/output/dataset.parquet",
    }


def test_backup_inventory_accepts_absolute_chain_location_under_relative_data_dir(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Relative custom roots must still include absolute retained chain paths."""
    monkeypatch.chdir(tmp_path)
    data_dir = Path("custom-storage")
    runs_dir = data_dir / "runs"
    chain_path = runs_dir / "run-1" / "output" / "dataset.parquet"
    chain_path.parent.mkdir(parents=True)
    chain_path.write_bytes(b"chain")

    inventory = build_backup_inventory(
        data_dir=data_dir,
        chain_locations=[chain_path.resolve()],
    )
    records = {record.archive_path: record for record in inventory.records}

    assert inventory.data_dir == data_dir
    assert inventory.runs_dir == runs_dir
    assert set(records) == {
        "dependencies/opx-chain/runs/run-1/output/dataset.parquet",
    }


def test_backup_inventory_uses_runtime_storage_dir_when_overrides_omitted(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """The active runtime storage.dir should drive both inventory roots."""
    storage_dir = tmp_path / "configured-storage"
    runs_dir = storage_dir / "runs"
    runs_dir.mkdir(parents=True)
    (runs_dir / "price_context_latest.json").write_text(
        json.dumps(
            {
                "artifact_type": "price_context",
                "provider": "marketdata",
                "records": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "opx_chain.backup_inventory.get_runtime_config_override",
        lambda: SimpleNamespace(storage_dir=storage_dir),
    )

    inventory = build_backup_inventory()

    assert inventory.data_dir == storage_dir
    assert inventory.runs_dir == runs_dir
    assert [record.archive_path for record in inventory.records] == [
        "dependencies/opx-chain/runs/price_context_latest.json"
    ]


def test_backup_inventory_storage_dir_does_not_require_provider_credentials(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Storage-only discovery should not require fetch-provider credentials."""
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[settings]
data_provider = "marketdata"
auto_fallback_to_yfinance = false

[storage]
dir = "store"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr("opx_chain.config.DEFAULT_CONFIG_PATH_OVERRIDE", config_path)
    storage_dir = get_data_dir() / "store"
    runs_dir = storage_dir / "runs"
    runs_dir.mkdir(parents=True)
    (runs_dir / "price_context_latest.json").write_text(
        json.dumps(
            {
                "artifact_type": "price_context",
                "provider": "marketdata",
                "records": [],
            }
        ),
        encoding="utf-8",
    )

    inventory = build_backup_inventory()

    assert inventory.data_dir == storage_dir
    assert inventory.runs_dir == runs_dir
    assert [record.archive_path for record in inventory.records] == [
        "dependencies/opx-chain/runs/price_context_latest.json"
    ]


def test_backup_inventory_ignores_chain_locations_outside_runs_dir(tmp_path: Path) -> None:
    """Retained chain locations outside the runs root must be ignored."""
    data_dir = tmp_path / "data"
    runs_dir = data_dir / "runs"
    runs_dir.mkdir(parents=True)
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(b"outside")

    inventory = build_backup_inventory(
        data_dir=data_dir,
        runs_dir=runs_dir,
        chain_locations=[outside],
    )

    assert not inventory.records


def test_backup_inventory_ignores_symlinked_storage_root(tmp_path: Path) -> None:
    """Inventory must not follow a declared storage root through a symlink."""
    outside_storage = tmp_path / "outside-storage"
    outside_runs = outside_storage / "runs"
    outside_runs.mkdir(parents=True)
    _sqlite_file(outside_storage / "price-history.db")
    (outside_runs / "price_context_latest.json").write_text(
        json.dumps({"artifact_type": "price_context", "provider": "marketdata", "records": []}),
        encoding="utf-8",
    )
    storage_link = tmp_path / "storage-link"
    storage_link.symlink_to(outside_storage, target_is_directory=True)

    inventory = build_backup_inventory(data_dir=storage_link)

    assert inventory.data_dir == storage_link
    assert inventory.runs_dir == storage_link / "runs"
    assert not inventory.records


def test_backup_inventory_ignores_explicit_runs_dir_under_symlinked_storage_root(
    tmp_path: Path,
) -> None:
    """Explicit runs_dir must not bypass symlinked storage-root protection."""
    outside_storage = tmp_path / "outside-storage"
    outside_runs = outside_storage / "runs"
    outside_runs.mkdir(parents=True)
    _sqlite_file(outside_storage / "price-history.db")
    (outside_runs / "price_context_latest.json").write_text(
        json.dumps({"artifact_type": "price_context", "provider": "marketdata", "records": []}),
        encoding="utf-8",
    )
    storage_link = tmp_path / "storage-link"
    storage_link.symlink_to(outside_storage, target_is_directory=True)

    inventory = build_backup_inventory(
        data_dir=storage_link,
        runs_dir=storage_link / "runs",
    )

    assert inventory.data_dir == storage_link
    assert inventory.runs_dir == storage_link / "runs"
    assert not inventory.records


def test_backup_inventory_reports_mixed_price_context_freshness(tmp_path: Path) -> None:
    """Mixed per-record freshness should be represented explicitly."""
    data_dir = tmp_path / "data"
    runs_dir = data_dir / "runs"
    runs_dir.mkdir(parents=True)
    (runs_dir / "price_context_latest.json").write_text(
        json.dumps(
            {
                "artifact_type": "price_context",
                "provider": "marketdata",
                "fetched_at": "2026-05-31T15:00:00Z",
                "records": [
                    {
                        "ticker": "AAPL",
                        "price_context_staleness_status": "FRESH",
                    },
                    {
                        "ticker": "MSFT",
                        "price_context_staleness_status": "STALE",
                    },
                    {
                        "ticker": "TSLA",
                        "price_context_source": "marketdata",
                        "price_context_staleness_status": "ERROR",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    inventory = build_backup_inventory(data_dir=data_dir, runs_dir=runs_dir)
    record = inventory.records[0]

    assert record.logical_kind == "price_context_artifact"
    assert record.provider == "marketdata"
    assert record.freshness_status == "MIXED:ERROR,FRESH,STALE"
    assert record.metadata["freshness_statuses"] == ["ERROR", "FRESH", "STALE"]


def test_backup_inventory_reports_empty_price_context_freshness_as_missing(
    tmp_path: Path,
) -> None:
    """A readable artifact with no records should not invent freshness."""
    data_dir = tmp_path / "data"
    runs_dir = data_dir / "runs"
    runs_dir.mkdir(parents=True)
    (runs_dir / "price_context_latest.json").write_text(
        json.dumps(
            {
                "artifact_type": "price_context",
                "provider": "marketdata",
                "records": [],
            }
        ),
        encoding="utf-8",
    )

    inventory = build_backup_inventory(data_dir=data_dir, runs_dir=runs_dir)
    record = inventory.records[0]

    assert record.provider == "marketdata"
    assert record.freshness_status is None
    assert record.metadata["freshness_statuses"] == []
