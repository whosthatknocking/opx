"""XDG-aware path helpers for opx-chain runtime files."""

from __future__ import annotations

import os
from pathlib import Path


APP_NAME = "opx-chain"


def _xdg_base_dir(env_var: str, fallback: Path) -> Path:
    """Return an XDG base directory from the environment or a platform fallback."""
    value = os.environ.get(env_var)
    return Path(value).expanduser() if value else fallback


def get_config_dir() -> Path:
    """Return the app config directory."""
    return _xdg_base_dir("XDG_CONFIG_HOME", Path.home() / ".config") / APP_NAME


def get_data_dir() -> Path:
    """Return the app data directory."""
    return _xdg_base_dir("XDG_DATA_HOME", Path.home() / ".local" / "share") / APP_NAME


def get_cache_dir() -> Path:
    """Return the app cache directory."""
    return _xdg_base_dir("XDG_CACHE_HOME", Path.home() / ".cache") / APP_NAME


def get_default_config_path() -> Path:
    """Return the default runtime config path."""
    return get_config_dir() / "config.toml"


def get_default_viewer_prefs_path() -> Path:
    """Return the default viewer preferences path."""
    return get_config_dir() / "viewer_prefs.json"


def get_default_positions_path() -> Path:
    """Return the default user positions CSV path."""
    return get_data_dir() / "positions.csv"


def get_default_debug_dump_dir() -> Path:
    """Return the default directory for provider payload dumps."""
    return get_data_dir() / "debug"


def get_default_provider_cache_dir() -> Path:
    """Return the default provider-cache directory."""
    return get_cache_dir() / "cache"


def resolve_relative_path(path: Path, *, base_dir: Path) -> Path:
    """Resolve relative runtime paths against an XDG base directory."""
    expanded = path.expanduser()
    if expanded.is_absolute():
        return expanded
    return base_dir / expanded
