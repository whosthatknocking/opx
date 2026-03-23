"""Compatibility package forwarding legacy imports to `opx`."""

from __future__ import annotations

from importlib import import_module
import sys

_MODULE_ALIASES = (
    "__init__",
    "config",
    "export",
    "fetch",
    "greeks",
    "metrics",
    "normalize",
    "runlog",
    "utils",
    "viewer",
    "providers",
    "providers.base",
    "providers.yfinance",
)

for module_name in _MODULE_ALIASES:
    target = "opx" if module_name == "__init__" else f"opx.{module_name}"
    alias = "options_fetcher" if module_name == "__init__" else f"options_fetcher.{module_name}"
    sys.modules[alias] = import_module(target)

