"""Architecture guards for the option-chain integrity boundary."""

from __future__ import annotations

import ast
from pathlib import Path


_PACKAGE_ROOT = Path("opx_chain")


def _source(relative_path: str) -> str:
    return (_PACKAGE_ROOT / relative_path).read_text(encoding="utf-8")


def test_chain_package_never_imports_strategy_policy() -> None:
    """The producer package must not depend on downstream strategy policy."""
    offenders = [
        str(path)
        for path in _PACKAGE_ROOT.rglob("*.py")
        if "opx_strategy" in path.read_text(encoding="utf-8")
    ]

    assert offenders == []


def test_generic_dataset_reader_is_limited_to_raw_inspection_surfaces() -> None:
    """Only explicitly raw inspection commands may import the generic reader."""
    importing_modules: set[str] = set()
    for path in _PACKAGE_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.module != "opx_chain.utils":
                continue
            if any(alias.name == "read_dataset_file" for alias in node.names):
                importing_modules.add(path.relative_to(_PACKAGE_ROOT).as_posix())

    assert importing_modules == {"check_positions.py", "viewer.py"}


def test_package_owned_semantic_replay_uses_validated_loader() -> None:
    """Package-owned calculations must enter through the semantic loader."""
    position_check = _source("check_positions.py")
    iv_replay = _source("iv_history_backfill.py")

    assert "storage.load_validated_option_chain_dataset(" in position_check
    assert "storage.load_validated_option_chain_dataset(" in iv_replay
    assert "read_dataset_file" not in iv_replay


def test_viewer_remains_a_raw_disclosure_surface() -> None:
    """Raw viewer access must disclose effective integrity and facts state."""
    viewer = _source("viewer.py")

    assert "read_dataset_file" in viewer
    assert "integrity_status" in viewer
    assert "dataset_facts_status" in viewer
