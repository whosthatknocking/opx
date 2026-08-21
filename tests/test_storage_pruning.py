"""Cross-backend storage pruning cleanup tests."""
# pylint: disable=duplicate-code

from collections.abc import Callable
from pathlib import Path

import pytest
from conftest import make_option_chain_frame

from opx_chain import SCHEMA_VERSION
from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage.models import DatasetWrite, RunContext, RunSummary
from opx_chain.storage.sqlite_indexed import SqliteIndexedBackend


def _make_context(**kwargs) -> RunContext:
    defaults = {
        "provider": "yfinance",
        "tickers": ("TSLA",),
        "config_fingerprint": "abc123",
        "positions_fingerprint": "",
    }
    return RunContext(**{**defaults, **kwargs})


def _write_dataset(backend, run_id: str, provider: str = "yfinance") -> None:
    backend.write_dataset(
        run_id,
        DatasetWrite(
            data=make_option_chain_frame(rows=1, provider=provider),
            provider=provider,
            schema_version=SCHEMA_VERSION,
        ),
    )
    backend.finalize_run(run_id, RunSummary(status="complete"))


def _filesystem_backend(tmp_path: Path):
    return FilesystemBackend(
        runs_dir=tmp_path / "runs",
        debug_dir=tmp_path / "debug",
        max_runs_retained=1,
    )


def _sqlite_backend(tmp_path: Path):
    return SqliteIndexedBackend(
        db_path=tmp_path / "opx-chain.db",
        runs_dir=tmp_path / "runs",
        debug_dir=tmp_path / "debug",
        max_runs_retained=1,
    )


@pytest.mark.parametrize("backend_factory", [_filesystem_backend, _sqlite_backend])
def test_pruning_removes_untracked_run_payload_dir(
    tmp_path: Path,
    backend_factory: Callable[[Path], object],
):
    """Pruning must remove residual run payload directories once no datasets remain."""
    backend = backend_factory(tmp_path)
    run_id = backend.create_run(_make_context())
    stale_dir = tmp_path / "runs" / run_id / "scratch"
    stale_dir.mkdir(parents=True)
    stale_file = stale_dir / "payload.json"
    stale_file.write_text("stale", encoding="utf-8")
    _write_dataset(backend, run_id)

    next_run_id = backend.create_run(_make_context(provider="marketdata"))
    _write_dataset(backend, next_run_id, provider="marketdata")

    assert not stale_dir.exists()
    assert not stale_file.exists()
    assert backend.get_run(run_id).run_id == run_id
