"""Tests for NullCache, FilesystemCache, and get_provider_cache factory."""

import hashlib
import json
import pickle
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from conftest import make_runtime_config
from opx_chain import fetch
from opx_chain.config_coercion import ConfigError
from opx_chain.storage.cache import FilesystemCache, NullCache, get_provider_cache


def _cache_paths(cache_dir: Path, key: str) -> tuple[Path, Path]:
    digest = hashlib.sha256(key.encode()).hexdigest()
    return cache_dir / f"{digest}.bin", cache_dir / f"{digest}.meta.json"


# ---------------------------------------------------------------------------
# NullCache
# ---------------------------------------------------------------------------

def test_null_cache_get_always_returns_none():
    """NullCache.get must always return None regardless of prior puts."""
    cache = NullCache()
    cache.put("k", b"v", ttl_seconds=60)
    assert cache.get("k") is None


def test_null_cache_invalidate_is_no_op():
    """NullCache.invalidate must not raise."""
    NullCache().invalidate("k")


def _cache_instances(tmp_path: Path):
    """Return cache implementations with the same public input boundary."""
    return (NullCache(), FilesystemCache(tmp_path / "cache"))


@pytest.mark.parametrize("bad_key", ["", "   ", True, 7, [], {}])
def test_provider_caches_reject_malformed_keys(tmp_path: Path, bad_key):
    """Provider cache keys must have the same boundary when disabled or enabled."""
    for cache in _cache_instances(tmp_path):
        with pytest.raises(ValueError, match="cache key"):
            cache.get(bad_key)
        with pytest.raises(ValueError, match="cache key"):
            cache.invalidate(bad_key)
        with pytest.raises(ValueError, match="cache key"):
            cache.put(bad_key, b"value", ttl_seconds=60)


@pytest.mark.parametrize(
    "bad_value",
    [bytearray(b"value"), memoryview(b"value"), "value", True, 1, [], {}],
)
def test_provider_caches_reject_malformed_payloads(tmp_path: Path, bad_value):
    """Provider cache values must be bytes, not byte-like or arbitrary objects."""
    for cache in _cache_instances(tmp_path):
        with pytest.raises(ValueError, match="cache value"):
            cache.put("key", bad_value, ttl_seconds=60)


@pytest.mark.parametrize("bad_ttl", [0, -1, True, 1.5, "60", [], {}])
def test_provider_caches_reject_malformed_ttl_seconds(tmp_path: Path, bad_ttl):
    """Provider cache TTLs must be positive non-boolean integers."""
    for cache in _cache_instances(tmp_path):
        with pytest.raises(ValueError, match="ttl_seconds"):
            cache.put("key", b"value", ttl_seconds=bad_ttl)


# ---------------------------------------------------------------------------
# FilesystemCache
# ---------------------------------------------------------------------------

def test_filesystem_cache_roundtrip(tmp_path: Path):
    """put then get must return the same bytes when TTL has not expired."""
    cache = FilesystemCache(tmp_path / "cache")
    cache.put("mykey", b"hello", ttl_seconds=60)
    assert cache.get("mykey") == b"hello"


def test_filesystem_cache_reads_trailing_z_expiry_metadata(tmp_path: Path):
    """Legacy/API-style Zulu expiry timestamps should not invalidate cache entries."""
    cache_dir = tmp_path / "cache"
    cache = FilesystemCache(cache_dir)
    cache.put("zulu-key", b"hello", ttl_seconds=60)
    _, meta_path = _cache_paths(cache_dir, "zulu-key")
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    metadata["expires_at"] = metadata["expires_at"].replace("+00:00", "Z")
    meta_path.write_text(json.dumps(metadata), encoding="utf-8")

    assert cache.get("zulu-key") == b"hello"


def test_filesystem_cache_miss_returns_none(tmp_path: Path):
    """get must return None for keys that were never put."""
    cache = FilesystemCache(tmp_path / "cache")
    assert cache.get("no-such-key") is None


def test_filesystem_cache_invalidate_removes_entry(tmp_path: Path):
    """invalidate must cause subsequent get calls to return None."""
    cache = FilesystemCache(tmp_path / "cache")
    cache.put("k", b"data", ttl_seconds=60)
    cache.invalidate("k")
    assert cache.get("k") is None


def test_filesystem_cache_expired_returns_none(tmp_path: Path):
    """get must return None when the entry's TTL has elapsed."""
    cache_dir = tmp_path / "cache"
    cache = FilesystemCache(cache_dir)
    cache.put("k", b"x", ttl_seconds=1)
    time.sleep(1.1)

    assert cache.get("k") is None
    bin_path, meta_path = _cache_paths(cache_dir, "k")
    assert not bin_path.exists()
    assert not meta_path.exists()


def test_filesystem_cache_prunes_expired_entries_on_startup(tmp_path: Path):
    """Constructor should remove stale cache files left by prior runs."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    bin_path, meta_path = _cache_paths(cache_dir, "old-key")
    bin_path.write_bytes(b"stale")
    meta_path.write_text(
        json.dumps({
            "key": "old-key",
            "expires_at": (datetime.now(tz=timezone.utc) - timedelta(seconds=1)).isoformat(),
        }),
        encoding="utf-8",
    )

    FilesystemCache(cache_dir)

    assert not bin_path.exists()
    assert not meta_path.exists()


def test_filesystem_cache_prunes_unreadable_metadata_on_startup(tmp_path: Path):
    """Corrupt metadata should not keep orphaned cache payloads forever."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    bin_path, meta_path = _cache_paths(cache_dir, "bad-key")
    bin_path.write_bytes(b"bad")
    meta_path.write_text("{not json", encoding="utf-8")

    FilesystemCache(cache_dir)

    assert not bin_path.exists()
    assert not meta_path.exists()


def test_filesystem_cache_prunes_orphaned_payload_on_startup(tmp_path: Path):
    """Payload files with no metadata should not accumulate indefinitely."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    bin_path, meta_path = _cache_paths(cache_dir, "orphan-key")
    bin_path.write_bytes(b"orphan")

    FilesystemCache(cache_dir)

    assert not bin_path.exists()
    assert not meta_path.exists()


def test_filesystem_cache_prunes_orphaned_metadata_on_startup(tmp_path: Path):
    """Metadata files with no payload should not accumulate indefinitely."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    bin_path, meta_path = _cache_paths(cache_dir, "orphan-meta-key")
    meta_path.write_text(
        json.dumps({
            "key": "orphan-meta-key",
            "expires_at": (datetime.now(tz=timezone.utc) + timedelta(hours=1)).isoformat(),
        }),
        encoding="utf-8",
    )

    FilesystemCache(cache_dir)

    assert not bin_path.exists()
    assert not meta_path.exists()


def test_filesystem_cache_get_removes_orphaned_metadata(tmp_path: Path):
    """A missing payload should make get clean up the corresponding metadata."""
    cache_dir = tmp_path / "cache"
    cache = FilesystemCache(cache_dir)
    bin_path, meta_path = _cache_paths(cache_dir, "orphan-meta-key")
    cache_dir.mkdir(exist_ok=True)
    meta_path.write_text(
        json.dumps({
            "key": "orphan-meta-key",
            "expires_at": (datetime.now(tz=timezone.utc) + timedelta(hours=1)).isoformat(),
        }),
        encoding="utf-8",
    )

    assert cache.get("orphan-meta-key") is None

    assert not bin_path.exists()
    assert not meta_path.exists()


def test_filesystem_cache_rejects_non_standard_json_metadata(tmp_path: Path):
    """NaN/Infinity metadata should be treated as corrupt cache state."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    bin_path, meta_path = _cache_paths(cache_dir, "bad-json-key")
    bin_path.write_bytes(b"bad")
    meta_path.write_text(
        '{"key":"bad-json-key","expires_at": NaN}',
        encoding="utf-8",
    )

    FilesystemCache(cache_dir)

    assert not bin_path.exists()
    assert not meta_path.exists()


@pytest.mark.parametrize("payload", [b"[]", b'"not-a-dict"', b"123"])
def test_fetch_json_cache_rejects_non_object_payloads(tmp_path: Path, payload):
    """Provider JSON cache reads should only restore dict-shaped payloads."""
    cache = FilesystemCache(tmp_path)
    cache.put("snapshot:stub:BAD", payload, ttl_seconds=300)

    assert fetch._cache_get_json(cache, "snapshot:stub:BAD") is None  # pylint: disable=protected-access
    assert cache.get("snapshot:stub:BAD") is None


@pytest.mark.parametrize(
    "payload",
    [
        b'{"ts":{"__opx_pd_timestamp__":true}}',
        b'{"ts":{"__opx_pd_timestamp__":{}}}',
        b'{"ts":{"__opx_pd_nat__":false}}',
    ],
)
def test_fetch_json_cache_rejects_malformed_timestamp_markers(
    tmp_path: Path,
    payload,
):
    """Reserved pandas timestamp markers should be validated before restore."""
    cache = FilesystemCache(tmp_path)
    cache.put("snapshot:stub:BAD", payload, ttl_seconds=300)

    assert fetch._cache_get_json(cache, "snapshot:stub:BAD") is None  # pylint: disable=protected-access
    assert cache.get("snapshot:stub:BAD") is None


def test_fetch_chain_cache_rejects_wrong_typed_pickle(tmp_path: Path):
    """Wrong-typed pickle payloads are corrupt cache entries, not chains."""
    cache = FilesystemCache(tmp_path)
    key = "chain:stub:TSLA:2026-04-17"
    cache.put(key, pickle.dumps({"calls": [], "puts": []}), ttl_seconds=300)

    assert fetch._cache_get_chain(cache, key) is None  # pylint: disable=protected-access
    assert cache.get(key) is None


def test_fetch_chain_cache_invalidates_unpicklable_payload(tmp_path: Path):
    """Unpicklable chain cache bytes should be removed after the first miss."""
    cache = FilesystemCache(tmp_path)
    key = "chain:stub:TSLA:2026-04-17"
    cache.put(key, b"not a pickle", ttl_seconds=300)

    assert fetch._cache_get_chain(cache, key) is None  # pylint: disable=protected-access
    assert cache.get(key) is None


def test_filesystem_cache_prunes_each_directory_once_per_process(tmp_path: Path, monkeypatch):
    """Repeated cache construction must not rescan the same directory per ticker."""
    prune_calls = []
    original_prune = FilesystemCache.prune_expired

    def tracking_prune(self):
        prune_calls.append("called")
        original_prune(self)

    monkeypatch.setattr(FilesystemCache, "prune_expired", tracking_prune)

    FilesystemCache(tmp_path / "cache-a")
    FilesystemCache(tmp_path / "cache-a")
    FilesystemCache(tmp_path / "cache-b")

    assert prune_calls == ["called", "called"]


def test_filesystem_cache_creates_directory(tmp_path: Path):
    """FilesystemCache must create the cache directory on first put."""
    cache_dir = tmp_path / "nested" / "cache"
    cache = FilesystemCache(cache_dir)
    cache.put("k", b"v", ttl_seconds=10)
    assert cache_dir.exists()


def test_filesystem_cache_uses_shared_blocking_lock(tmp_path: Path, monkeypatch):
    """FilesystemCache locking must route through the cross-platform helper."""
    cache_dir = tmp_path / "cache"
    cache = FilesystemCache(cache_dir)
    lock_handle = object()
    acquired_paths = []
    released_handles = []

    def fake_acquire(path: Path):
        acquired_paths.append(path)
        return lock_handle

    def fake_release(handle):
        released_handles.append(handle)

    monkeypatch.setattr("opx_chain.storage.cache.acquire_blocking_file_lock", fake_acquire)
    monkeypatch.setattr("opx_chain.storage.cache.release_file_lock", fake_release)

    cache.put("k", b"v", ttl_seconds=10)

    assert acquired_paths == [cache_dir / ".cache.lock"]
    assert released_handles == [lock_handle]


def test_filesystem_cache_invalidate_nonexistent_is_safe(tmp_path: Path):
    """invalidate must not raise when the key does not exist."""
    FilesystemCache(tmp_path / "cache").invalidate("ghost")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def test_factory_returns_null_cache_when_disabled():
    """get_provider_cache must return NullCache when provider_cache_backend = 'none'."""
    config = make_runtime_config(provider_cache_backend="none")
    assert isinstance(get_provider_cache(config), NullCache)


def test_factory_returns_filesystem_cache_when_enabled(tmp_path: Path):
    """get_provider_cache must return FilesystemCache when backend = 'filesystem'."""
    config = make_runtime_config(
        provider_cache_backend="filesystem",
        provider_cache_dir=tmp_path / "cache",
    )
    assert isinstance(get_provider_cache(config), FilesystemCache)


@pytest.mark.parametrize("backend", ["", "bad", "FILESYSTEM", False, 0, [], {}])
def test_factory_rejects_malformed_provider_cache_backend(tmp_path: Path, backend):
    """Direct cache backend selectors should not silently disable caching."""
    config = make_runtime_config(
        provider_cache_backend=backend,
        provider_cache_dir=tmp_path / "cache",
    )

    with pytest.raises(ConfigError, match="storage.cache_backend"):
        get_provider_cache(config)


@pytest.mark.parametrize("cache_dir", ["", "   ", False, 0, [], {}])
def test_factory_rejects_malformed_provider_cache_dir(cache_dir):
    """Direct filesystem cache directories should share the config path boundary."""
    config = make_runtime_config(
        provider_cache_backend="filesystem",
        provider_cache_dir=cache_dir,
    )

    with pytest.raises(ConfigError, match="storage.cache_dir"):
        get_provider_cache(config)


def test_factory_anchors_relative_provider_cache_dir(tmp_path: Path, monkeypatch):
    """Direct relative cache directories should resolve under the XDG cache root."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg-cache"))
    config = make_runtime_config(
        provider_cache_backend="filesystem",
        provider_cache_dir="provider-cache",
    )

    cache = get_provider_cache(config)

    assert isinstance(cache, FilesystemCache)
    assert cache._dir == tmp_path / "xdg-cache" / "opx-chain" / "provider-cache"  # pylint: disable=protected-access
