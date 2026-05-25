"""Storage contract documentation tests."""

import re
from pathlib import Path

from opx_chain.storage.base import StorageBackend


ROOT = Path(__file__).resolve().parents[1]


def test_storage_spec_protocol_methods_match_runtime_protocol():
    """The canonical spec snippet should list every StorageBackend method."""
    spec = (ROOT / "docs" / "STORAGE_SPEC.md").read_text(encoding="utf-8")
    snippet = spec.split("class StorageBackend(Protocol):", maxsplit=1)[1]
    snippet = snippet.split("class ProviderCache(Protocol):", maxsplit=1)[0]
    documented = set(
        re.findall(
            r"^    def ([a-zA-Z_][a-zA-Z0-9_]*)\(",
            snippet,
            flags=re.MULTILINE,
        )
    )
    runtime = {
        name
        for name, value in vars(StorageBackend).items()
        if not name.startswith("_") and callable(value)
    }

    assert documented == runtime


def test_storage_spec_uses_current_package_paths():
    """Storage docs should not regress to the pre-rename `opx/` package paths."""
    spec = (ROOT / "docs" / "STORAGE_SPEC.md").read_text(encoding="utf-8")

    assert "`opx/" not in spec
    assert "`opx[" not in spec
    assert "`opx." not in spec


def test_storage_spec_documents_current_opx_check_lookup():
    """The opx-check storage contract should match the implemented lookup."""
    spec = (ROOT / "docs" / "STORAGE_SPEC.md").read_text(encoding="utf-8")

    assert "`opx-check` uses `list_datasets(limit=100)`" in spec
    assert "newest existing CSV artifact" in spec
    assert "falls back to the newest existing readable dataset artifact" in spec
    assert "including parquet" in spec
    assert "until the reader supports non-CSV datasets" not in spec
    assert "`opx-check` uses `list_datasets(limit=1)`" not in spec


def test_storage_spec_documents_latest_csv_copy_semantics():
    """The latest CSV pointer should be documented as a copy, not a symlink."""
    spec = (ROOT / "docs" / "STORAGE_SPEC.md").read_text(encoding="utf-8")

    assert "`options_engine_output_latest.csv`" in spec
    assert "not a symlink" in spec
    assert "remains readable even" in spec
    assert "original timestamped CSV artifact is later removed" in spec
    assert "latest symlink" not in spec.lower()


def test_storage_spec_documents_current_viewer_storage_discovery():
    """The viewer storage contract should match the implemented discovery path."""
    spec = (ROOT / "docs" / "STORAGE_SPEC.md").read_text(encoding="utf-8")

    assert "viewer discovers datasets through" in spec
    assert "`StorageBackend.list_datasets(limit=10000)`" in spec
    assert "showing only the backend's small default page" in spec
    assert "It falls back to filesystem" in spec
    assert "migrating it to the storage port should happen" not in spec


def test_storage_factory_direct_config_dataset_format_boundary_is_documented():
    """Public docs should cover direct factory dataset-format validation."""
    storage_spec = (ROOT / "docs" / "STORAGE_SPEC.md").read_text(encoding="utf-8")
    interface_spec = (ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md").read_text(
        encoding="utf-8"
    )

    for spec in (storage_spec, interface_spec):
        assert "get_storage_backend(config=...)" in spec
        assert "storage_dataset_format" in spec
        assert "storage.dataset_format" in spec
        assert "`csv` or `parquet`" in spec
