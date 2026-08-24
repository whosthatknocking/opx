"""Atomic storage-write helper tests."""

import os
from pathlib import Path
import warnings

import pandas as pd
import pytest

from opx_chain.storage.atomic import atomic_file_write, atomic_write_bytes
from opx_chain.storage.serializers import CsvSerializer


def _temp_files_for(path: Path) -> list[Path]:
    return list(path.parent.glob(f".{path.name}.*.tmp"))


def test_atomic_write_bytes_fsyncs_temp_and_parent_dir(tmp_path: Path, monkeypatch):
    """Successful byte writes should fsync content before rename and the parent after."""
    dest = tmp_path / "artifact.bin"
    fsync_targets = []
    original_fsync = os.fsync

    def spy_fsync(fd: int) -> None:
        fsync_targets.append(os.fstat(fd).st_mode)
        original_fsync(fd)

    monkeypatch.setattr(os, "fsync", spy_fsync)

    atomic_write_bytes(dest, b"new")

    assert dest.read_bytes() == b"new"
    assert len(fsync_targets) >= 2


def test_atomic_write_bytes_replaces_existing_file_and_cleans_temp(tmp_path: Path):
    """Successful writes should replace existing content without temp leftovers."""
    dest = tmp_path / "artifact.bin"
    dest.write_bytes(b"old")

    atomic_write_bytes(dest, b"new")

    assert dest.read_bytes() == b"new"
    assert not _temp_files_for(dest)


def test_atomic_file_write_preserves_existing_file_when_writer_fails(tmp_path: Path):
    """Failed temp writes must leave the existing destination intact."""
    dest = tmp_path / "run.json"
    dest.write_text("old", encoding="utf-8")

    def failing_writer(tmp_path: Path) -> None:
        tmp_path.write_text("partial", encoding="utf-8")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        atomic_file_write(dest, failing_writer)

    assert dest.read_text(encoding="utf-8") == "old"
    assert not _temp_files_for(dest)


def test_atomic_file_write_fsyncs_temp_and_parent_dir(tmp_path: Path, monkeypatch):
    """Successful callback writes should fsync the temp file and parent directory."""
    dest = tmp_path / "run.json"
    fsync_targets = []
    original_fsync = os.fsync

    def spy_fsync(fd: int) -> None:
        fsync_targets.append(os.fstat(fd).st_mode)
        original_fsync(fd)

    monkeypatch.setattr(os, "fsync", spy_fsync)

    atomic_file_write(dest, lambda tmp_path: tmp_path.write_text("new", encoding="utf-8"))

    assert dest.read_text(encoding="utf-8") == "new"
    assert len(fsync_targets) >= 2


def test_csv_serializer_writes_without_temp_leftovers(tmp_path: Path):
    """Dataset serializers should write through the shared atomic file helper."""
    dest = tmp_path / "dataset.csv"
    serializer = CsvSerializer()

    bytes_written = serializer.serialize(pd.DataFrame({"ticker": ["TSLA"]}), str(dest))

    assert bytes_written == dest.stat().st_size
    assert "TSLA" in dest.read_text(encoding="utf-8")
    assert not _temp_files_for(dest)


def test_csv_serializer_reparses_wide_nullable_boolean_without_dtype_warning():
    """Canonical nullable booleans must not trigger chunk-inference warnings."""
    row_count = 10_128
    frame = pd.DataFrame(
        {
            "risk_model_inconsistent": pd.Series(
                [pd.NA] * 6_000 + [False] * 3_000 + [True] * 1_128,
                dtype="boolean",
            ),
            **{f"metric_{index}": [float(index)] * row_count for index in range(96)},
        }
    )
    serializer = CsvSerializer()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        reparsed = serializer.deserialize_bytes(serializer.serialize_bytes(frame))

    assert not [item for item in caught if issubclass(item.category, pd.errors.DtypeWarning)]
    assert reparsed["risk_model_inconsistent"].dropna().map(type).eq(bool).all()
    assert reparsed["risk_model_inconsistent"].isna().sum() == 6_000
