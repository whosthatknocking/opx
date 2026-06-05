"""Provider selection helpers for the market-data layer."""

from importlib import import_module
from functools import lru_cache

from opx_chain.config import get_runtime_config
from opx_chain.providers.base import DataProvider


PROVIDER_FACTORIES = {
    "yfinance": "opx_chain.providers.yfinance:YFinanceProvider",
    "massive": "opx_chain.providers.massive:MassiveProvider",
    "marketdata": "opx_chain.providers.marketdata:MarketDataProvider",
}

_CLASS_EXPORTS = {
    "YFinanceProvider": PROVIDER_FACTORIES["yfinance"],
    "MassiveProvider": PROVIDER_FACTORIES["massive"],
    "MarketDataProvider": PROVIDER_FACTORIES["marketdata"],
}


def _load_symbol(target: str):
    module_name, class_name = target.split(":", maxsplit=1)
    module = import_module(module_name)
    return getattr(module, class_name)


@lru_cache(maxsize=None)
def _make_provider(provider_name: str) -> DataProvider:
    try:
        return _load_symbol(PROVIDER_FACTORIES[provider_name])()
    except KeyError as exc:
        raise ValueError(f"Unsupported data provider: {provider_name}") from exc


def get_data_provider() -> DataProvider:
    """Return the configured market-data provider implementation (cached per name)."""
    return _make_provider(get_runtime_config().data_provider)


def get_data_provider_by_name(provider_name: str) -> DataProvider:
    """Return one provider implementation by explicit provider id."""
    normalized = str(provider_name or "").strip().lower()
    return _make_provider(normalized)


def __getattr__(name: str):
    """Load provider classes only when callers explicitly request them."""
    if name in _CLASS_EXPORTS:
        return _load_symbol(_CLASS_EXPORTS[name])
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DataProvider",
    "PROVIDER_FACTORIES",
    "get_data_provider",
    "get_data_provider_by_name",
]
