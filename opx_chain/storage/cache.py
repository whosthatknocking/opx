"""ProviderCache implementations: NullCache (no-op) and FilesystemCache (disk-backed).

Use get_provider_cache(config) to obtain the cache configured by [storage] settings.
"""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
from datetime import datetime, timedelta, timezone
from os import PathLike
from pathlib import Path
from threading import Lock
from typing import ClassVar, Iterator

from opx_chain.config_coercion import ConfigError, coerce_str
from opx_chain.json_utils import dumps_strict_json, loads_strict_json
from opx_chain.locks import acquire_blocking_file_lock, release_file_lock
from opx_chain.paths import get_cache_dir, get_default_provider_cache_dir, resolve_relative_path
from opx_chain.storage.atomic import atomic_write_bytes, atomic_write_text
from opx_chain.timestamps import parse_iso_datetime

_SUPPORTED_PROVIDER_CACHE_BACKENDS = frozenset({"none", "filesystem"})


def _validate_cache_key(key: str) -> str:
    """Return a validated nonblank cache key."""
    if not isinstance(key, str) or not key.strip():
        raise ValueError("cache key must be a nonblank string")
    return key


def _validate_cache_value(value: bytes) -> bytes:
    """Return validated cache payload bytes."""
    if not isinstance(value, bytes):
        raise ValueError("cache value must be bytes")
    return value


def _validate_cache_ttl_seconds(ttl_seconds: int) -> int:
    """Return a validated positive cache TTL."""
    if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be a positive integer")
    return ttl_seconds


def _validate_provider_cache_backend(value) -> str:
    """Validate direct provider-cache backend selectors."""
    backend = coerce_str(value, field_name="storage.cache_backend")
    if backend not in _SUPPORTED_PROVIDER_CACHE_BACKENDS:
        raise ConfigError(
            "Config field 'storage.cache_backend' must be one of ['filesystem', 'none']."
        )
    return backend


def _validate_provider_cache_dir(value) -> Path:
    """Validate and XDG-anchor direct provider-cache directories."""
    if value is None:
        return get_default_provider_cache_dir()
    if isinstance(value, PathLike):
        path = Path(value).expanduser()
        if not str(path):
            raise ConfigError("Config field 'storage.cache_dir' must not be blank.")
    elif isinstance(value, str):
        path = Path(coerce_str(value, field_name="storage.cache_dir")).expanduser()
    else:
        raise ConfigError("Config field 'storage.cache_dir' must be a string path.")
    return resolve_relative_path(path, base_dir=get_cache_dir())


class NullCache:  # pylint: disable=too-few-public-methods
    """No-op cache that never stores anything. Default when cache is disabled."""

    def get(self, key: str) -> bytes | None:  # pylint: disable=unused-argument
        """Always return None."""
        _validate_cache_key(key)

    def put(self, key: str, value: bytes, ttl_seconds: int) -> None:  # pylint: disable=unused-argument
        """Discard the value."""
        _validate_cache_key(key)
        _validate_cache_value(value)
        _validate_cache_ttl_seconds(ttl_seconds)

    def invalidate(self, key: str) -> None:  # pylint: disable=unused-argument
        """No-op."""
        _validate_cache_key(key)


class FilesystemCache:
    """Disk-backed cache with per-entry TTL."""

    _pruned_dirs: ClassVar[set[Path]] = set()
    _prune_lock: ClassVar[Lock] = Lock()
    _io_lock: ClassVar[Lock] = Lock()

    def __init__(self, cache_dir: Path) -> None:
        self._dir = Path(cache_dir)
        self._prune_expired_once()

    def _prune_expired_once(self) -> None:
        """Run startup pruning once per cache directory in this process."""
        cache_dir = self._dir.expanduser().resolve()
        with self._prune_lock:
            if cache_dir in self._pruned_dirs:
                return
            self.prune_expired()
            self._pruned_dirs.add(cache_dir)

    def _key_paths(self, key: str) -> tuple[Path, Path]:
        key = _validate_cache_key(key)
        digest = hashlib.sha256(key.encode()).hexdigest()
        return self._dir / f"{digest}.bin", self._dir / f"{digest}.meta.json"

    @staticmethod
    def _unlink_entry(bin_path: Path, meta_path: Path) -> None:
        bin_path.unlink(missing_ok=True)
        meta_path.unlink(missing_ok=True)

    @contextmanager
    def _locked_cache(self, *, create: bool = False) -> Iterator[None]:
        if create:
            self._dir.mkdir(parents=True, exist_ok=True)
        with self._io_lock:
            if create:
                self._dir.mkdir(parents=True, exist_ok=True)
            lock_file = acquire_blocking_file_lock(self._dir / ".cache.lock")
            try:
                yield
            finally:
                release_file_lock(lock_file)

    def get(self, key: str) -> bytes | None:
        """Return cached bytes if present and unexpired, else None."""
        _validate_cache_key(key)
        if not self._dir.exists():
            return None
        bin_path, meta_path = self._key_paths(key)
        with self._locked_cache():
            if not bin_path.exists() or not meta_path.exists():
                if bin_path.exists() != meta_path.exists():
                    bin_path.unlink(missing_ok=True)
                    meta_path.unlink(missing_ok=True)
                return None
            try:
                meta = loads_strict_json(meta_path.read_text(encoding="utf-8"))
                expires_at = parse_iso_datetime(meta["expires_at"])
                if datetime.now(tz=timezone.utc) > expires_at:
                    self._unlink_entry(bin_path, meta_path)
                    return None
                return bin_path.read_bytes()
            except (OSError, KeyError, TypeError, ValueError):
                self._unlink_entry(bin_path, meta_path)
                return None

    def put(self, key: str, value: bytes, ttl_seconds: int) -> None:
        """Write bytes to disk with an expiry timestamp."""
        value = _validate_cache_value(value)
        ttl_seconds = _validate_cache_ttl_seconds(ttl_seconds)
        with self._locked_cache(create=True):
            bin_path, meta_path = self._key_paths(key)
            expires_at = datetime.now(tz=timezone.utc) + timedelta(seconds=ttl_seconds)
            atomic_write_bytes(bin_path, value)
            atomic_write_text(
                meta_path,
                dumps_strict_json({"key": key, "expires_at": expires_at.isoformat()}),
            )

    def invalidate(self, key: str) -> None:
        """Delete the cache entry for a key if it exists."""
        _validate_cache_key(key)
        if not self._dir.exists():
            return
        bin_path, meta_path = self._key_paths(key)
        with self._locked_cache():
            self._unlink_entry(bin_path, meta_path)

    def prune_expired(self) -> None:
        """Remove expired or unreadable cache entries from the cache directory."""
        if not self._dir.exists():
            return
        with self._locked_cache():
            now = datetime.now(tz=timezone.utc)
            meta_bins = {
                meta_path.with_name(meta_path.name.removesuffix(".meta.json") + ".bin")
                for meta_path in self._dir.glob("*.meta.json")
            }
            for meta_path in self._dir.glob("*.meta.json"):
                bin_path = meta_path.with_name(meta_path.name.removesuffix(".meta.json") + ".bin")
                if not bin_path.exists():
                    meta_path.unlink(missing_ok=True)
                    continue
                try:
                    meta = loads_strict_json(meta_path.read_text(encoding="utf-8"))
                    expires_at = parse_iso_datetime(meta["expires_at"])
                except (OSError, KeyError, TypeError, ValueError):
                    meta_path.unlink(missing_ok=True)
                    bin_path.unlink(missing_ok=True)
                    continue
                if now > expires_at:
                    meta_path.unlink(missing_ok=True)
                    bin_path.unlink(missing_ok=True)
            for bin_path in self._dir.glob("*.bin"):
                if bin_path not in meta_bins:
                    bin_path.unlink(missing_ok=True)


def get_provider_cache(config=None):
    """Return a ProviderCache instance based on config, or NullCache when disabled."""
    if config is None:
        from opx_chain.config import get_runtime_config  # pylint: disable=import-outside-toplevel
        config = get_runtime_config()
    backend = _validate_provider_cache_backend(config.provider_cache_backend)
    if backend == "filesystem":
        return FilesystemCache(_validate_provider_cache_dir(config.provider_cache_dir))
    return NullCache()
