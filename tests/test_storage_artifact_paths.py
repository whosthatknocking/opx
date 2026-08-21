"""Storage artifact path containment tests."""
# pylint: disable=duplicate-code

import json
import sqlite3
from collections.abc import Callable
from pathlib import Path

import pytest
from conftest import make_option_chain_frame

from opx_chain import SCHEMA_VERSION
from opx_chain.storage._disk import write_artifact_bytes
from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage.models import (
    ArtifactWrite,
    DatasetRecord,
    DatasetWrite,
    RunContext,
    RunSummary,
)
from opx_chain.storage.sqlite_indexed import SqliteIndexedBackend


def _make_context(**kwargs) -> RunContext:
    defaults = {
        "provider": "yfinance",
        "tickers": ("TSLA",),
        "config_fingerprint": "abc123",
        "positions_fingerprint": "",
    }
    return RunContext(**{**defaults, **kwargs})


def _filesystem_backend(tmp_path: Path):
    return FilesystemBackend(
        runs_dir=tmp_path / "runs",
        debug_dir=tmp_path / "debug",
    )


def _sqlite_backend(tmp_path: Path):
    return SqliteIndexedBackend(
        db_path=tmp_path / "opx-chain.db",
        runs_dir=tmp_path / "runs",
        debug_dir=tmp_path / "debug",
    )


def _write_dataset(backend, run_id: str) -> DatasetRecord:
    record = backend.write_dataset(
        run_id,
        DatasetWrite(
            data=make_option_chain_frame(rows=1),
            provider="yfinance",
            schema_version=SCHEMA_VERSION,
        ),
    )
    backend.finalize_run(run_id, RunSummary(status="complete"))
    return record


def _tamper_artifact_location(
    tmp_path: Path,
    backend: object,
    run_id: str,
    outside_path: Path,
) -> None:
    if isinstance(backend, FilesystemBackend):
        run_path = tmp_path / "runs" / run_id / "run.json"
        data = json.loads(run_path.read_text(encoding="utf-8"))
        data["artifacts"][0]["location"] = str(outside_path)
        run_path.write_text(json.dumps(data), encoding="utf-8")
        return

    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE artifacts SET location = ? WHERE run_id = ?",
            (str(outside_path), run_id),
        )
        conn.commit()
    finally:
        conn.close()


def _tamper_dataset_location(
    tmp_path: Path,
    backend: object,
    record: DatasetRecord,
    outside_path: Path,
) -> None:
    if isinstance(backend, FilesystemBackend):
        meta_path = Path(record.location).with_suffix(".meta.json")
        data = json.loads(meta_path.read_text(encoding="utf-8"))
        data["location"] = str(outside_path)
        meta_path.write_text(json.dumps(data), encoding="utf-8")
        return

    conn = sqlite3.connect(tmp_path / "opx-chain.db")
    try:
        conn.execute(
            "UPDATE datasets SET location = ? WHERE dataset_id = ?",
            (str(outside_path), record.dataset_id),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.mark.parametrize(
    "filename",
    ["", ".", "..", "../escape.txt", "nested/file.txt", "/tmp/escape.txt", "nested\\file.txt"],
)
def test_write_artifact_bytes_rejects_unsafe_filename(tmp_path: Path, filename: str):
    """Debug artifact filenames must be single path components."""
    with pytest.raises(ValueError, match="invalid filename"):
        write_artifact_bytes(b"x", tmp_path / "debug", filename)


def test_write_artifact_bytes_rejects_nul_byte_filename(tmp_path: Path):
    """NUL bytes must be rejected by the path-component validator itself."""
    with pytest.raises(ValueError, match="invalid filename"):
        write_artifact_bytes(b"x", tmp_path / "debug", "bad\x00name.json")


@pytest.mark.parametrize("backend_factory", [_filesystem_backend, _sqlite_backend])
@pytest.mark.parametrize("filename", ["../escape.txt", "nested/file.txt", "/tmp/escape.txt"])
def test_sidecar_artifacts_reject_unsafe_filename(
    tmp_path: Path,
    backend_factory: Callable[[Path], object],
    filename: str,
):
    """Sidecar artifact filenames must not escape their run directory."""
    backend = backend_factory(tmp_path)
    run_id = backend.create_run(_make_context())

    with pytest.raises(ValueError, match="invalid path component"):
        backend.write_artifact(
            run_id,
            ArtifactWrite(artifact_type="sidecar", content=b"x", filename=filename),
        )


@pytest.mark.parametrize("backend_factory", [_filesystem_backend, _sqlite_backend])
def test_sidecar_artifacts_reject_unsafe_run_id(
    tmp_path: Path,
    backend_factory: Callable[[Path], object],
):
    """Sidecar writes must validate the run-id path component before writing."""
    backend = backend_factory(tmp_path)

    with pytest.raises(ValueError, match="invalid path component"):
        backend.write_artifact(
            "../escape",
            ArtifactWrite(artifact_type="sidecar", content=b"x", filename="positions.csv"),
        )

    assert not (tmp_path / "escape" / "positions.csv").exists()


@pytest.mark.parametrize("backend_factory", [_filesystem_backend, _sqlite_backend])
def test_delete_run_artifacts_skips_outside_retained_artifact_location(
    tmp_path: Path,
    backend_factory: Callable[[Path], object],
):
    """Corrupt retained artifact locations must not delete outside files."""
    backend = backend_factory(tmp_path)
    run_id = backend.create_run(_make_context())
    artifact = backend.write_artifact(
        run_id,
        ArtifactWrite(artifact_type="sidecar", content=b"positions", filename="positions.csv"),
    )
    outside_path = tmp_path / "outside_artifact.txt"
    outside_path.write_text("keep", encoding="utf-8")
    _tamper_artifact_location(tmp_path, backend, run_id, outside_path)

    backend.delete_run_artifacts(run_id)

    assert outside_path.read_text(encoding="utf-8") == "keep"
    assert not Path(artifact.location).exists()


@pytest.mark.parametrize("backend_factory", [_filesystem_backend, _sqlite_backend])
def test_retention_pruning_skips_outside_retained_dataset_location(
    tmp_path: Path,
    backend_factory: Callable[[Path], object],
):
    """Corrupt retained dataset locations must not delete outside files."""
    backend = backend_factory(tmp_path)
    backend._max_runs_retained = 1  # pylint: disable=protected-access
    old_run_id = backend.create_run(_make_context())
    old_record = _write_dataset(backend, old_run_id)
    outside_path = tmp_path / "outside_dataset.csv"
    outside_path.write_text("keep", encoding="utf-8")
    _tamper_dataset_location(tmp_path, backend, old_record, outside_path)

    new_run_id = backend.create_run(_make_context(provider="marketdata"))
    _write_dataset(backend, new_run_id)

    assert outside_path.read_text(encoding="utf-8") == "keep"
    assert not Path(old_record.location).exists()
