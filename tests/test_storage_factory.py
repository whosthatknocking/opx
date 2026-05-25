"""Tests for storage backend factory wiring."""

import threading
import time
from pathlib import Path

import pytest

from conftest import make_runtime_config
from opx_chain.config_coercion import ConfigError
import opx_chain.storage.factory as factory_mod
from opx_chain.storage.filesystem import FilesystemBackend


def test_factory_constructs_one_filesystem_backend_under_concurrency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Concurrent callers must share the first cached backend instance."""
    config = make_runtime_config(
        storage_enabled=True,
        storage_backend="filesystem",
        storage_dir=tmp_path,
        debug_dump_dir=tmp_path / "debug",
    )
    constructed = 0
    count_lock = threading.Lock()

    class SlowFilesystemBackend(FilesystemBackend):
        """Filesystem backend with a widened construction race window."""

        def __init__(self, *args, **kwargs):
            nonlocal constructed
            with count_lock:
                constructed += 1
            time.sleep(0.02)
            super().__init__(*args, **kwargs)

    factory_mod.clear_storage_backend_cache()
    monkeypatch.setattr(factory_mod, "FilesystemBackend", SlowFilesystemBackend)
    results = []

    def call_factory() -> None:
        results.append(factory_mod.get_storage_backend(config))

    threads = [threading.Thread(target=call_factory) for _ in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == len(threads)
    assert len({id(result) for result in results}) == 1
    assert constructed == 1


@pytest.mark.parametrize("raw_enabled", [False, "false", "0", "off", 0])
def test_factory_treats_false_like_storage_enabled_as_disabled(
    tmp_path: Path,
    raw_enabled,
):
    """Direct config callers should get the same false-like storage boundary."""
    config = make_runtime_config(
        storage_enabled=raw_enabled,
        storage_backend="filesystem",
        storage_dir=tmp_path,
        debug_dump_dir=tmp_path / "debug",
    )

    assert factory_mod.get_storage_backend(config) is None


@pytest.mark.parametrize("raw_enabled", ["maybe", 2, [], {}])
def test_factory_rejects_malformed_storage_enabled(tmp_path: Path, raw_enabled):
    """Malformed direct storage enablement should fail before backend selection."""
    config = make_runtime_config(
        storage_enabled=raw_enabled,
        storage_backend="filesystem",
        storage_dir=tmp_path,
        debug_dump_dir=tmp_path / "debug",
    )

    with pytest.raises(ConfigError, match="storage.enable"):
        factory_mod.get_storage_backend(config)


@pytest.mark.parametrize("backend", ["", "massive", "FILESYSTEM", [], {}])
def test_factory_rejects_malformed_storage_backend(tmp_path: Path, backend):
    """Direct storage backend selectors should not silently become filesystem."""
    config = make_runtime_config(
        storage_enabled=True,
        storage_backend=backend,
        storage_dir=tmp_path,
        debug_dump_dir=tmp_path / "debug",
    )

    with pytest.raises(ConfigError, match="storage.backend"):
        factory_mod.get_storage_backend(config)


@pytest.mark.parametrize("max_runs_retained", [-1, True, "1", 1.5, [], {}])
def test_factory_rejects_malformed_storage_retention_limit(
    tmp_path: Path,
    max_runs_retained,
):
    """Direct retention limits should be validated before pruning can run."""
    config = make_runtime_config(
        storage_enabled=True,
        storage_backend="filesystem",
        storage_max_runs_retained=max_runs_retained,
        storage_dir=tmp_path,
        debug_dump_dir=tmp_path / "debug",
    )

    with pytest.raises(ConfigError, match="storage.max_runs_retained"):
        factory_mod.get_storage_backend(config)
