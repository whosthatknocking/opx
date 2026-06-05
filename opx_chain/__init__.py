"""Options chain data pipeline."""

from opx_chain.analyst_forecast import (
    ANALYST_FORECAST_SCHEMA_VERSION,
    fetch_analyst_forecasts,
)
from opx_chain.version import __version__

SCHEMA_VERSION: int = 2

__all__ = [
    "__version__",
    "SCHEMA_VERSION",
    "ANALYST_FORECAST_SCHEMA_VERSION",
    "fetch_analyst_forecasts",
]
