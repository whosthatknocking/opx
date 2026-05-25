"""Config-driven storage backend factory."""

from __future__ import annotations

import threading
from pathlib import Path

from opx_chain.config_coercion import ConfigError, coerce_bool, coerce_int, coerce_str
from opx_chain.paths import get_data_dir as get_xdg_data_dir
from opx_chain.storage.filesystem import FilesystemBackend


_BackendCacheKey = tuple[str, Path, Path, int, str]
_BACKEND_CACHE: dict[_BackendCacheKey, object] = {}
_BACKEND_LOCK = threading.Lock()
_SUPPORTED_STORAGE_BACKENDS = frozenset({"filesystem", "sqlite"})


def get_data_dir() -> Path:
    """Return the XDG-compliant base data directory for opx-chain."""
    return _default_data_dir()


def _default_data_dir() -> Path:
    return get_xdg_data_dir()


def clear_storage_backend_cache() -> None:
    """Clear cached backend instances.

    This is primarily useful for tests or long-lived embedding processes that
    deliberately change storage configuration at runtime.
    """
    with _BACKEND_LOCK:
        _BACKEND_CACHE.clear()


def _cache_key(config) -> _BackendCacheKey:
    """Return the process-cache key for the configured storage backend."""
    base = config.storage_dir if config.storage_dir else _default_data_dir()
    return (
        _validate_storage_backend(config.storage_backend),
        Path(base),
        Path(config.debug_dump_dir),
        _validate_storage_max_runs_retained(config.storage_max_runs_retained),
        config.storage_dataset_format,
    )


def _storage_enabled(config) -> bool:
    """Return validated storage enablement for direct or loaded config objects."""
    enabled = coerce_bool(config.storage_enabled, field_name="storage.enable")
    return False if enabled is None else enabled


def _validate_storage_backend(value) -> str:
    """Validate direct storage backend selectors before cache lookup."""
    backend = coerce_str(value, field_name="storage.backend")
    if backend not in _SUPPORTED_STORAGE_BACKENDS:
        raise ConfigError("Config field 'storage.backend' must be one of ['filesystem', 'sqlite'].")
    return backend


def _validate_storage_max_runs_retained(value) -> int:
    """Validate direct retention limits before backend construction."""
    retained = coerce_int(value, field_name="storage.max_runs_retained")
    resolved = 0 if retained is None else retained
    if resolved < 0:
        raise ConfigError("Config field 'storage.max_runs_retained' must be >= 0.")
    return resolved


def get_storage_backend(config=None):
    """Return a configured StorageBackend, or None when storage is disabled.

    When config is None, the process runtime config is loaded automatically.
    Returns None when storage.enable = false (the default).
    Returns a FilesystemBackend or SqliteIndexedBackend when enabled.
    """
    if config is None:
        from opx_chain.config import get_runtime_config  # pylint: disable=import-outside-toplevel
        config = get_runtime_config()

    if not _storage_enabled(config):
        return None

    key = _cache_key(config)
    with _BACKEND_LOCK:
        cached = _BACKEND_CACHE.get(key)
        if cached is not None:
            return cached

        _, base, debug_dir, max_runs_retained, dataset_format = key
        kwargs = {
            "runs_dir": base / "runs",
            "debug_dir": debug_dir,
            "max_runs_retained": max_runs_retained,
            "dataset_format": dataset_format,
        }

        if key[0] == "sqlite":
            from opx_chain.storage.sqlite_indexed import SqliteIndexedBackend  # pylint: disable=import-outside-toplevel,no-name-in-module
            backend = SqliteIndexedBackend(db_path=base / "opx-chain.db", **kwargs)
        else:
            backend = FilesystemBackend(**kwargs)

        _BACKEND_CACHE[key] = backend
        return backend
