"""Pytest configuration ensuring the repository root is importable."""

from pathlib import Path
import sys

import pytest

from opx.config import reset_runtime_config


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(autouse=True)
def reset_config_cache():
    """Ensure tests do not share cached runtime config state."""
    reset_runtime_config()
    yield
    reset_runtime_config()
