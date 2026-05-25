"""Tests for shared scalar and timestamp utilities."""

import ast
from decimal import Decimal
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from opx_chain.coerce import coerce_bool_or_default
from opx_chain.storage.serializers import get_serializer
from opx_chain.utils import (
    coerce_float,
    finite_float,
    finite_float_or_none,
    finite_numeric_series,
    first_non_missing,
    is_finite_positive_number,
    is_missing_or_non_finite,
    normalize_timestamp,
    read_dataset_file,
)


@pytest.mark.parametrize(
    "value",
    [
        True,
        np.bool_(True),
        1,
        1.0,
        np.int64(1),
        np.float64(1.0),
        "1",
        "true",
        "yes",
        "y",
        "on",
        " TRUE ",
    ],
)
def test_coerce_bool_or_default_accepts_truthy_values(value):
    """Shared bool coercion should accept the canonical truthy set."""
    assert coerce_bool_or_default(value, default=None) is True


@pytest.mark.parametrize(
    "value",
    [
        False,
        np.bool_(False),
        0,
        0.0,
        np.int64(0),
        np.float64(0.0),
        "0",
        "false",
        "no",
        "n",
        "off",
        " FALSE ",
    ],
)
def test_coerce_bool_or_default_accepts_falsy_values(value):
    """Shared bool coercion should accept the canonical falsy set."""
    assert coerce_bool_or_default(value, default=None) is False


@pytest.mark.parametrize("value", [None, "", "random", 2, 2.0, math.inf, math.nan, pd.NA])
def test_coerce_bool_or_default_returns_default_for_unknown_values(value):
    """Unknown bool inputs should resolve through the caller-provided default."""
    assert coerce_bool_or_default(value, default=None) is None
    assert coerce_bool_or_default(value, default=False) is False


def test_normalize_timestamp_infers_numeric_epoch_units():
    """Numeric vendor timestamps should infer seconds, milliseconds, and nanoseconds."""
    assert str(normalize_timestamp(1710942000)) == "2024-03-20 13:40:00+00:00"
    assert str(normalize_timestamp(1710942000000)) == "2024-03-20 13:40:00+00:00"
    assert str(normalize_timestamp(1710942000000000000)) == "2024-03-20 13:40:00+00:00"


def test_read_dataset_file_normalizes_csv_and_parquet_dtypes(tmp_path):
    """Dataset reads should expose the same core dtypes for CSV and parquet artifacts."""
    frame = pd.DataFrame({
        "underlying_symbol": ["TSLA", "NVDA"],
        "days_to_expiration": pd.Series([21, pd.NA], dtype="Int64"),
        "is_stale_quote": pd.Series([False, pd.NA], dtype="boolean"),
        "earnings_within_5d": pd.Series([True, False], dtype="boolean"),
        "option_quote_time": pd.to_datetime(
            ["2026-05-01T13:00:00.123456Z", None],
            utc=True,
        ),
        "underlying_price_time": pd.to_datetime(
            ["2026-05-01T13:00:01.654321Z", "2026-05-01T13:00:02Z"],
            utc=True,
            format="mixed",
        ),
    })
    csv_path = tmp_path / "dataset.csv"
    parquet_path = tmp_path / "dataset.parquet"
    get_serializer("csv").serialize(frame, str(csv_path))
    get_serializer("parquet").serialize(frame, str(parquet_path))

    csv_result = read_dataset_file(csv_path)
    parquet_result = read_dataset_file(parquet_path)

    expected_dtypes = {
        "days_to_expiration": "Int64",
        "is_stale_quote": "boolean",
        "earnings_within_5d": "boolean",
        "option_quote_time": "datetime64[ns, UTC]",
        "underlying_price_time": "datetime64[ns, UTC]",
    }
    assert {
        column: str(csv_result[column].dtype)
        for column in expected_dtypes
    } == expected_dtypes
    assert {
        column: str(parquet_result[column].dtype)
        for column in expected_dtypes
    } == expected_dtypes
    assert not bool(csv_result.loc[0, "is_stale_quote"])
    assert not bool(parquet_result.loc[0, "is_stale_quote"])
    assert pd.isna(csv_result.loc[1, "is_stale_quote"])
    assert pd.isna(parquet_result.loc[1, "is_stale_quote"])


def test_read_dataset_file_normalizes_extended_boolean_strings(tmp_path):
    """CSV boolean normalization should share the config bool policy."""
    path = tmp_path / "dataset.csv"
    pd.DataFrame(
        {
            "underlying_symbol": ["A", "B", "C", "D", "E"],
            "is_stale_quote": ["on", "off", "y", "n", "garbage"],
        }
    ).to_csv(path, index=False)

    result = read_dataset_file(path)

    assert result["is_stale_quote"].tolist() == [True, False, True, False, pd.NA]


def test_read_dataset_file_projects_columns_for_csv_and_parquet(tmp_path):
    """Projected dataset reads should still apply canonical dtype normalization."""
    frame = pd.DataFrame({
        "underlying_symbol": ["TSLA", "NVDA"],
        "is_stale_quote": pd.Series([False, True], dtype="boolean"),
        "unused_column": [1, 2],
    })
    csv_path = tmp_path / "dataset.csv"
    parquet_path = tmp_path / "dataset.parquet"
    get_serializer("csv").serialize(frame, str(csv_path))
    get_serializer("parquet").serialize(frame, str(parquet_path))

    columns = ["is_stale_quote", "missing_column"]
    csv_result = read_dataset_file(csv_path, columns=columns)
    parquet_result = read_dataset_file(parquet_path, columns=columns)

    assert csv_result.columns.tolist() == ["is_stale_quote"]
    assert parquet_result.columns.tolist() == ["is_stale_quote"]
    assert str(csv_result["is_stale_quote"].dtype) == "boolean"
    assert str(parquet_result["is_stale_quote"].dtype) == "boolean"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1, 1.0),
        ("2.5", 2.5),
        (np.float64(3.5), 3.5),
        (None, None),
        (True, None),
        (np.bool_(True), None),
        ("bad", None),
        (Decimal("1e1000000000"), None),
        (math.inf, None),
        (math.nan, None),
    ],
)
def test_finite_float_or_none_shares_finite_float_policy(value, expected):
    """None-returning callers should share the package finite-float policy."""
    result = finite_float_or_none(value)

    if expected is None:
        assert result is None
        assert math.isnan(finite_float(value))
    else:
        assert result == pytest.approx(expected)
        assert finite_float(value) == pytest.approx(expected)


def test_coerce_float_uses_strict_finite_float_policy():
    """Legacy provider coercion should not treat booleans as numeric prices."""
    assert math.isnan(coerce_float(True))
    assert math.isnan(coerce_float(np.bool_(False)))
    assert coerce_float("2.5") == pytest.approx(2.5)


def test_finite_numeric_series_masks_booleans_and_non_finite_values():
    """Vector numeric coercion should share scalar finite-float boolean handling."""
    result = finite_numeric_series(pd.Series([True, False, "2.5", math.inf, "bad"]))

    assert result.isna().tolist() == [True, True, False, True, True]
    assert result.loc[2] == pytest.approx(2.5)


def test_first_non_missing_rejects_non_finite_numeric_values():
    """Provider coalescing should not treat inf as a valid market value."""
    assert is_missing_or_non_finite(math.inf) is True
    assert is_missing_or_non_finite(pd.NA) is True
    assert first_non_missing(None, math.inf, np.nan, "fallback") == "fallback"
    assert first_non_missing(False, "fallback") is False


@pytest.mark.parametrize(
    "value",
    [True, np.bool_(True), 0, 0.0, -1, "0", "bad", math.inf, math.nan],
)
def test_is_finite_positive_number_rejects_non_positive_and_invalid(value):
    """Positive finite checks should not treat booleans as numeric price values."""
    assert is_finite_positive_number(value) is False


@pytest.mark.parametrize("value", [1, 1.0, "2.5", np.float64(3.5)])
def test_is_finite_positive_number_accepts_positive_values(value):
    """Positive finite checks should accept scalar positive numeric values."""
    assert is_finite_positive_number(value) is True


def test_finite_float_or_none_is_canonical_missing_value_variant():
    """Production modules should not grow private finite-float clones."""
    project_root = Path(__file__).resolve().parents[1]
    package_root = project_root / "opx_chain"
    modules = [
        path for path in package_root.rglob("*.py") if path.name != "utils.py"
    ]
    offenders = [
        f"{path.relative_to(project_root)}:{node.lineno} defines _finite_float"
        for path in modules
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.FunctionDef) and node.name == "_finite_float"
    ]

    assert not offenders
