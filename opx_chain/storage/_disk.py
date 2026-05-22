"""Shared disk-write utilities for filesystem-backed storage implementations."""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from opx_chain.storage.atomic import atomic_write_bytes
from opx_chain.storage.serializers import DatasetSerializer


def content_hash_for_bytes(content: bytes) -> str:
    """Return the stable SHA-256 hex digest for stored artifact bytes."""
    return hashlib.sha256(content).hexdigest()


def validate_path_component(value: str, *, label: str = "path component") -> str:
    """Return a single safe path component or raise ValueError."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"invalid {label}: {value!r}")
    if "\x00" in value:
        raise ValueError(f"invalid {label}: {value!r}")
    path = Path(value)
    if (
        path.is_absolute()
        or path.name != value
        or value in {".", ".."}
        or "/" in value
        or "\\" in value
    ):
        raise ValueError(f"invalid {label}: {value!r}")
    return value


def resolve_child_path(base_dir: Path, *components: str) -> Path:
    """Resolve a child path and ensure it stays under base_dir."""
    resolved_base = base_dir.resolve()
    dest = resolved_base
    for component in components:
        dest /= validate_path_component(component)
    resolved_dest = dest.resolve()
    if not resolved_dest.is_relative_to(resolved_base):
        raise ValueError(f"path escapes base directory: {resolved_dest}")
    return resolved_dest


def retained_path_under_roots(location: str, roots: Iterable[Path]) -> Path | None:
    """Return a retained metadata path only when it resolves under an allowed root."""
    try:
        resolved = Path(location).expanduser().resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        return None
    for root in roots:
        try:
            resolved_root = root.expanduser().resolve(strict=False)
        except (OSError, RuntimeError, ValueError):
            continue
        if resolved.is_relative_to(resolved_root):
            return resolved
    return None


def write_dataset_artifact(
    data: pd.DataFrame,
    output_dir: Path,
    dataset_format: str,
    serializer: DatasetSerializer,
) -> tuple[str, Path, str]:
    """Write a DataFrame artifact to disk. Returns (dataset_id, artifact_path, content_hash)."""
    dataset_id = str(uuid.uuid4())
    artifact_path = (output_dir / f"{dataset_id}.{dataset_format}").resolve()
    content = serializer.serialize_bytes(data)
    atomic_write_bytes(artifact_path, content)
    content_hash = content_hash_for_bytes(content)
    return dataset_id, artifact_path, content_hash


def write_artifact_bytes(
    content: bytes,
    debug_dir: Path,
    filename: str,
) -> tuple[str, Path, str]:
    """Write raw artifact bytes to disk. Returns (artifact_id, dest_path, content_hash)."""
    artifact_id = str(uuid.uuid4())
    dest = resolve_child_path(
        debug_dir,
        artifact_id,
        validate_path_component(filename, label="filename"),
    )
    atomic_write_bytes(dest, content)
    content_hash = content_hash_for_bytes(content)
    return artifact_id, dest, content_hash
