"""Cross-backend missing-run contract tests."""

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage.memory import MemoryBackend
from opx_chain.storage.models import (
    ArtifactWrite,
    DatasetWrite,
    RunContext,
    RunSummary,
    TickerFetchResult,
    ValidationRecord,
)
from opx_chain.storage.sqlite_indexed import SqliteIndexedBackend

BackendFactory = Callable[[Path], object]


def _filesystem_backend(tmp_path: Path) -> FilesystemBackend:
    return FilesystemBackend(
        runs_dir=tmp_path / "filesystem-runs",
        debug_dir=tmp_path / "filesystem-debug",
    )


def _memory_backend(_tmp_path: Path) -> MemoryBackend:
    return MemoryBackend()


def _sqlite_backend(tmp_path: Path) -> SqliteIndexedBackend:
    return SqliteIndexedBackend(
        db_path=tmp_path / "opx-chain.db",
        runs_dir=tmp_path / "sqlite-runs",
        debug_dir=tmp_path / "sqlite-debug",
    )


BACKENDS: tuple[BackendFactory, ...] = (
    _filesystem_backend,
    _memory_backend,
    _sqlite_backend,
)


def _close_backend(backend: object) -> None:
    close = getattr(backend, "close", None)
    if close is not None:
        close()


def _make_context(**kwargs) -> RunContext:
    defaults = {"provider": "yfinance", "tickers": ("TSLA",)}
    defaults.update(config_fingerprint="abc123", positions_fingerprint="")
    return RunContext(**{**defaults, **kwargs})


def _dataset_write(**kwargs) -> DatasetWrite:
    defaults = {
        "data": pd.DataFrame({"underlying_symbol": ["TSLA"], "strike": [100.0]}),
        "provider": "yfinance",
        "schema_version": 1,
    }
    return DatasetWrite(**{**defaults, **kwargs})


def _ticker_result(**kwargs) -> TickerFetchResult:
    defaults = {
        "ticker": "TSLA",
        "raw_row_count": 50,
        "normalized_row_count": 48,
        "kept_row_count": 40,
        "filtered_row_count": 8,
        "expiration_count": 4,
        "status": "ok",
    }
    return TickerFetchResult(**{**defaults, **kwargs})


@pytest.mark.parametrize("backend_factory", BACKENDS)
def test_get_run_raises_key_error_for_unknown_run(
    backend_factory: BackendFactory,
    tmp_path: Path,
):
    """get_run must raise the same error type for unknown run IDs."""
    backend = backend_factory(tmp_path)

    try:
        with pytest.raises(KeyError, match="run not found"):
            backend.get_run("missing-run")
    finally:
        _close_backend(backend)


@pytest.mark.parametrize("backend_factory", BACKENDS)
@pytest.mark.parametrize(
    "method_name,args",
    [
        ("get_run", ([],)),
        ("get_ticker_results", ({},)),
        ("get_dataset", ([],)),
        ("delete_run_artifacts", ([],)),
    ],
)
def test_read_and_delete_boundaries_reject_malformed_ids(
    backend_factory: BackendFactory,
    tmp_path: Path,
    method_name: str,
    args: tuple,
):
    """Storage read/delete boundaries must reject malformed IDs before backend plumbing."""
    backend = backend_factory(tmp_path)

    try:
        with pytest.raises(ValueError):
            getattr(backend, method_name)(*args)
    finally:
        _close_backend(backend)


@pytest.mark.parametrize("backend_factory", BACKENDS)
@pytest.mark.parametrize(
    "method_name,args",
    [
        ("write_dataset", ("missing-run", _dataset_write())),
        ("record_ticker_result", ("missing-run", _ticker_result())),
        (
            "record_validation",
            (ValidationRecord("missing-run", "warning", "MISSING_GREEKS", 1, None),),
        ),
        (
            "write_artifact",
            ("missing-run", ArtifactWrite("sidecar", b"positions", "positions.csv")),
        ),
        ("finalize_run", ("missing-run", RunSummary(status="complete"))),
        ("fail_run", ("missing-run", "provider timeout")),
    ],
)
def test_write_boundaries_raise_key_error_for_unknown_runs(
    backend_factory: BackendFactory,
    tmp_path: Path,
    method_name: str,
    args: tuple,
):
    """Write and lifecycle APIs must not create impossible missing-run state."""
    backend = backend_factory(tmp_path)

    try:
        with pytest.raises(KeyError, match="run not found"):
            getattr(backend, method_name)(*args)
    finally:
        _close_backend(backend)


@pytest.mark.parametrize("backend_factory", BACKENDS)
@pytest.mark.parametrize(
    "context",
    [
        _make_context(tickers="TSLA"),
        _make_context(tickers=("TSLA", True)),
        _make_context(tickers=("A.",)),
    ],
)
def test_create_run_rejects_malformed_ticker_metadata(
    backend_factory: BackendFactory,
    tmp_path: Path,
    context: RunContext,
):
    """RunContext.tickers must follow the shared ticker policy across backends."""
    backend = backend_factory(tmp_path)

    try:
        with pytest.raises(ValueError, match="ticker"):
            backend.create_run(context)
    finally:
        _close_backend(backend)


@pytest.mark.parametrize("backend_factory", BACKENDS)
@pytest.mark.parametrize(
    "result",
    [
        _ticker_result(raw_row_count=-1),
        _ticker_result(kept_row_count=True),
        _ticker_result(status="maybe"),
    ],
)
def test_record_ticker_result_rejects_malformed_metadata(
    backend_factory: BackendFactory,
    tmp_path: Path,
    result: TickerFetchResult,
):
    """Ticker result metadata must stay in the documented status/count domain."""
    backend = backend_factory(tmp_path)
    run_id = backend.create_run(_make_context())

    try:
        with pytest.raises(ValueError):
            backend.record_ticker_result(run_id, result)
    finally:
        _close_backend(backend)


@pytest.mark.parametrize("backend_factory", BACKENDS)
@pytest.mark.parametrize(
    "record",
    [
        ValidationRecord("placeholder", "fatal", "CODE", 1, None),
        ValidationRecord("placeholder", "warning", "", 1, None),
        ValidationRecord("placeholder", "warning", "CODE", True, None),
        ValidationRecord("placeholder", "warning", "CODE", 1, {"bad": "shape"}),
    ],
)
def test_record_validation_rejects_malformed_metadata(
    backend_factory: BackendFactory,
    tmp_path: Path,
    record: ValidationRecord,
):
    """Validation summary metadata must stay stable across storage backends."""
    backend = backend_factory(tmp_path)
    run_id = backend.create_run(_make_context())
    record.run_id = run_id

    try:
        with pytest.raises(ValueError):
            backend.record_validation(record)
    finally:
        _close_backend(backend)


@pytest.mark.parametrize("backend_factory", BACKENDS)
@pytest.mark.parametrize(
    "dataset",
    [
        _dataset_write(provider=""),
        _dataset_write(schema_version=0),
        _dataset_write(schema_version=True),
        _dataset_write(script_version=[]),
    ],
)
def test_write_dataset_rejects_malformed_metadata(
    backend_factory: BackendFactory,
    tmp_path: Path,
    dataset: DatasetWrite,
):
    """DatasetWrite metadata must be validated before serialization or persistence."""
    backend = backend_factory(tmp_path)
    run_id = backend.create_run(_make_context())

    try:
        with pytest.raises(ValueError):
            backend.write_dataset(run_id, dataset)
    finally:
        _close_backend(backend)


@pytest.mark.parametrize("backend_factory", BACKENDS)
@pytest.mark.parametrize(
    "artifact",
    [
        ArtifactWrite("", b"x", "payload.json"),
        ArtifactWrite("unknown", b"x", "payload.json"),
        ArtifactWrite("sidecar", "not bytes", "positions.csv"),
        ArtifactWrite("sidecar", b"x", "../escape.txt"),
    ],
)
def test_write_artifact_rejects_malformed_metadata(
    backend_factory: BackendFactory,
    tmp_path: Path,
    artifact: ArtifactWrite,
):
    """Artifact type, bytes, and filename validation must be backend-independent."""
    backend = backend_factory(tmp_path)
    run_id = backend.create_run(_make_context())

    try:
        with pytest.raises(ValueError):
            backend.write_artifact(run_id, artifact)
    finally:
        _close_backend(backend)


@pytest.mark.parametrize("backend_factory", BACKENDS)
@pytest.mark.parametrize(
    "summary",
    [
        RunSummary(status="done"),
        RunSummary(status=True),
        RunSummary(status="interrupted", error_summary=[]),
    ],
)
def test_finalize_run_rejects_malformed_summary(
    backend_factory: BackendFactory,
    tmp_path: Path,
    summary: RunSummary,
):
    """RunSummary status and error fields must be validated before lifecycle writes."""
    backend = backend_factory(tmp_path)
    run_id = backend.create_run(_make_context())

    try:
        with pytest.raises(ValueError):
            backend.finalize_run(run_id, summary)
    finally:
        _close_backend(backend)


@pytest.mark.parametrize("backend_factory", BACKENDS)
@pytest.mark.parametrize("error", [None, True, [], {}])
def test_fail_run_rejects_malformed_error(
    backend_factory: BackendFactory,
    tmp_path: Path,
    error,
):
    """fail_run error summaries must be text across storage backends."""
    backend = backend_factory(tmp_path)
    run_id = backend.create_run(_make_context())

    try:
        with pytest.raises(ValueError, match="error"):
            backend.fail_run(run_id, error)
    finally:
        _close_backend(backend)


@pytest.mark.parametrize("backend_factory", BACKENDS)
@pytest.mark.parametrize(
    "args",
    [
        ("not-a-datetime", "interrupted"),
        (True, "interrupted"),
        (datetime.now(), []),
    ],
)
def test_interrupt_stale_runs_rejects_malformed_inputs(
    backend_factory: BackendFactory,
    tmp_path: Path,
    args: tuple,
):
    """interrupt_stale_runs must reject malformed cutoff and error metadata."""
    backend = backend_factory(tmp_path)

    try:
        with pytest.raises(ValueError):
            backend.interrupt_stale_runs(*args)
    finally:
        _close_backend(backend)


@pytest.mark.parametrize("backend_factory", BACKENDS)
def test_get_ticker_results_raises_key_error_for_unknown_run(
    backend_factory: BackendFactory,
    tmp_path: Path,
):
    """get_ticker_results must match get_run semantics for unknown run IDs."""
    backend = backend_factory(tmp_path)

    try:
        with pytest.raises(KeyError, match="run not found"):
            backend.get_ticker_results("missing-run")
    finally:
        _close_backend(backend)
