"""Small scalar conversion helpers shared across fetch and normalization code."""

import pandas as pd
import numpy as np


def coerce_float(value):
    """Convert scalar inputs to float while keeping missing values as NaN."""
    return pd.to_numeric(value, errors="coerce")


def normalize_timestamp(value):
    """Convert vendor timestamps to timezone-aware UTC pandas timestamps."""
    if value is None or pd.isna(value):
        return pd.NaT

    if isinstance(value, (int, float, np.integer, np.floating)):
        numeric_value = float(value)
        absolute_value = abs(numeric_value)
        if absolute_value >= 1e17:
            unit = "ns"
        elif absolute_value >= 1e14:
            unit = "us"
        elif absolute_value >= 1e11:
            unit = "ms"
        else:
            unit = "s"
        return pd.to_datetime(value, unit=unit, utc=True, errors="coerce")

    return pd.to_datetime(value, utc=True, errors="coerce")
