"""Public backup inventory for opx-chain execution dependencies."""

from __future__ import annotations

import json
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

from opx_chain.config import get_runtime_config_override, load_storage_dir_config
from opx_chain.event_data import EVENT_SNAPSHOT_DIRNAME, EVENT_SNAPSHOT_LATEST_FILENAME
from opx_chain.paths import get_data_dir, get_runs_dir


@dataclass(frozen=True)
# pylint: disable=too-many-instance-attributes
class BackupDependencyRecord:
    """One producer-owned dependency that downstream backups may archive."""

    logical_kind: str
    source_path: Path
    archive_path: str
    required_for_execution: bool
    dataset_history: bool = False
    provider: str | None = None
    freshness_status: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BackupInventory:
    """Stable backup inventory for opx-chain market-data dependencies."""

    data_dir: Path
    runs_dir: Path
    records: tuple[BackupDependencyRecord, ...]


# pylint: disable=too-many-locals
def build_backup_inventory(
    *,
    chain_locations: Iterable[str | Path] = (),
    data_dir: str | Path | None = None,
    runs_dir: str | Path | None = None,
) -> BackupInventory:
    """Return producer-owned dependency records for downstream backup archives."""
    resolved_data_dir, resolved_runs_dir = _resolve_storage_roots(
        data_dir=data_dir,
        runs_dir=runs_dir,
    )
    records: list[BackupDependencyRecord] = []
    seen: set[str] = set()

    def append(record: BackupDependencyRecord) -> None:
        if record.archive_path in seen:
            return
        seen.add(record.archive_path)
        records.append(record)

    data_root_safe = _is_dependency_root(resolved_data_dir)
    runs_root_safe = _is_dependency_root(resolved_runs_dir) and (
        runs_dir is not None or data_root_safe
    )

    for db_name, kind in (
        ("price-history.db", "price_history_db"),
        ("iv-history.db", "iv_history_db"),
    ):
        source_path = resolved_data_dir / db_name
        if data_root_safe and _is_regular_dependency(source_path, root=resolved_data_dir):
            append(
                BackupDependencyRecord(
                    logical_kind=kind,
                    source_path=source_path,
                    archive_path=f"dependencies/opx-chain/{db_name}",
                    required_for_execution=True,
                    metadata={"filename": db_name},
                )
            )

    if data_root_safe:
        latest_event_snapshot = resolved_data_dir / EVENT_SNAPSHOT_LATEST_FILENAME
        if _is_regular_dependency(latest_event_snapshot, root=resolved_data_dir):
            metadata = _event_snapshot_metadata(latest_event_snapshot)
            append(
                BackupDependencyRecord(
                    logical_kind="event_data_snapshot_artifact",
                    source_path=latest_event_snapshot,
                    archive_path=f"dependencies/opx-chain/{EVENT_SNAPSHOT_LATEST_FILENAME}",
                    required_for_execution=True,
                    provider=_text_or_none(metadata.get("provider")),
                    freshness_status=_text_or_none(metadata.get("status")),
                    metadata=metadata,
                )
            )

        event_snapshot_dir = resolved_data_dir / EVENT_SNAPSHOT_DIRNAME
        event_snapshot_root = event_snapshot_dir.resolve(strict=False)
        if _is_dependency_root(event_snapshot_dir):
            for path in sorted(event_snapshot_dir.glob("*.json")):
                if (
                    not _path_stays_under(path, event_snapshot_root)
                    or not _is_regular_dependency(path, root=resolved_data_dir)
                ):
                    continue
                relative = path.relative_to(event_snapshot_dir)
                metadata = _event_snapshot_metadata(path)
                append(
                    BackupDependencyRecord(
                        logical_kind="event_data_snapshot_artifact",
                        source_path=path,
                        archive_path=(
                            f"dependencies/opx-chain/{EVENT_SNAPSHOT_DIRNAME}/"
                            f"{relative.as_posix()}"
                        ),
                        required_for_execution=True,
                        provider=_text_or_none(metadata.get("provider")),
                        freshness_status=_text_or_none(metadata.get("status")),
                        metadata=metadata,
                    )
                )

    runs_root = resolved_runs_dir.resolve(strict=False)
    if runs_root_safe:
        for path in sorted(resolved_runs_dir.glob("price_context*.json")):
            if (
                not _path_stays_under(path, runs_root)
                or not _is_regular_dependency(path, root=resolved_runs_dir)
            ):
                continue
            metadata = _price_context_metadata(path)
            append(
                BackupDependencyRecord(
                    logical_kind="price_context_artifact",
                    source_path=path,
                    archive_path=f"dependencies/opx-chain/runs/{path.name}",
                    required_for_execution=True,
                    provider=_text_or_none(metadata.get("provider")),
                    freshness_status=_text_or_none(metadata.get("freshness_status")),
                    metadata=metadata,
                )
            )

        for raw_location in chain_locations:
            path = Path(raw_location).expanduser()
            if (
                not _path_stays_under(path, runs_root)
                or not _is_regular_dependency(path, root=resolved_runs_dir)
            ):
                continue
            relative = path.resolve(strict=False).relative_to(runs_root)
            append(
                BackupDependencyRecord(
                    logical_kind="chain_dataset_artifact",
                    source_path=path,
                    archive_path=f"dependencies/opx-chain/runs/{relative.as_posix()}",
                    required_for_execution=False,
                    dataset_history=True,
                    metadata={"relative_path": relative.as_posix()},
                )
            )

    return BackupInventory(
        data_dir=resolved_data_dir,
        runs_dir=resolved_runs_dir,
        records=tuple(records),
    )


def _resolve_storage_roots(
    *,
    data_dir: str | Path | None,
    runs_dir: str | Path | None,
) -> tuple[Path, Path]:
    if data_dir is not None:
        resolved_data_dir = Path(data_dir).expanduser()
    else:
        config = get_runtime_config_override()
        storage_dir = config.storage_dir if config is not None else load_storage_dir_config()
        resolved_data_dir = (
            Path(storage_dir).expanduser()
            if storage_dir
            else get_data_dir()
        )

    if runs_dir is not None:
        resolved_runs_dir = Path(runs_dir).expanduser()
    else:
        resolved_runs_dir = get_runs_dir(resolved_data_dir)

    return resolved_data_dir, resolved_runs_dir


def _is_dependency_root(path: Path) -> bool:
    try:
        mode = path.lstat().st_mode
    except OSError:
        return False
    return (
        stat.S_ISDIR(mode)
        and not stat.S_ISLNK(mode)
        and _has_real_directory_components(path)
    )


def _is_regular_dependency(path: Path, *, root: Path | None = None) -> bool:
    try:
        mode = path.lstat().st_mode
    except OSError:
        return False
    if not stat.S_ISREG(mode) or stat.S_ISLNK(mode):
        return False
    if root is not None and not _has_real_parent_dirs(path, root):
        return False
    return True


def _has_real_parent_dirs(path: Path, root: Path) -> bool:
    full_path = path if path.is_absolute() else Path.cwd() / path
    full_root = root if root.is_absolute() else Path.cwd() / root
    try:
        relative_parent = full_path.parent.relative_to(full_root)
    except ValueError:
        return False
    current = full_root
    for part in relative_parent.parts:
        current = current / part
        try:
            mode = current.lstat().st_mode
        except OSError:
            return False
        if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
            return False
    return True


def _has_real_directory_components(path: Path) -> bool:
    full_path = path if path.is_absolute() else Path.cwd() / path
    current = Path(full_path.anchor) if full_path.anchor else Path()
    parts = full_path.parts[1:] if full_path.anchor else full_path.parts
    for part in parts:
        current = current / part
        try:
            mode = current.lstat().st_mode
        except OSError:
            return False
        if stat.S_ISLNK(mode):
            return False
        if current == full_path and not stat.S_ISDIR(mode):
            return False
    return True


def _path_stays_under(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root)
    except (OSError, ValueError):
        return False
    return True


def _price_context_metadata(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {"artifact_readable": False}
    if not isinstance(payload, dict):
        return {"artifact_readable": False}
    provider = _text_or_none(payload.get("provider"))
    record_statuses: list[str] = []
    records = payload.get("records")
    if isinstance(records, list) and records:
        for record in records:
            if not isinstance(record, dict):
                continue
            provider = provider or _text_or_none(record.get("price_context_source"))
            status = _text_or_none(record.get("price_context_staleness_status"))
            if status:
                record_statuses.append(status)
    freshness = _aggregate_freshness_status(record_statuses)
    return {
        "artifact_readable": True,
        "provider": provider,
        "freshness_status": freshness,
        "freshness_statuses": sorted(set(record_statuses)),
        "fetched_at": _text_or_none(payload.get("fetched_at")),
        "record_count": len(records) if isinstance(records, list) else None,
    }


def _event_snapshot_metadata(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {"artifact_readable": False}
    if not isinstance(payload, dict):
        return {"artifact_readable": False}
    records = payload.get("records")
    return {
        "artifact_readable": True,
        "provider": _text_or_none(payload.get("provider")),
        "status": _text_or_none(payload.get("status")),
        "fetched_at": _text_or_none(payload.get("fetched_at")),
        "trading_date": _text_or_none(payload.get("trading_date")),
        "record_count": len(records) if isinstance(records, list) else None,
    }


def _aggregate_freshness_status(statuses: list[str]) -> str | None:
    unique = sorted(set(statuses))
    if not unique:
        return None
    if len(unique) == 1:
        return unique[0]
    return "MIXED:" + ",".join(unique)


def _text_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
