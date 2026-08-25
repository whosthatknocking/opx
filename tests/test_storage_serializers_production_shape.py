"""Production-shaped canonical dataset serializer regressions."""

from __future__ import annotations

import warnings

import pandas as pd
import pytest

from opx_chain.export import CANONICAL_EXPORT_COLUMNS
from opx_chain.schema import BOOLEAN_FIELDS, TIMESTAMP_FIELDS
from opx_chain.storage.serializers import get_serializer
from opx_chain.validate import NUMERIC_FIELDS


ROW_COUNT = 12_288
LANDMARKS = (0, 4_095, 4_096, 8_191, 8_192, 10_127, 12_287)
WARNING_CLASSES = (
    pd.errors.DtypeWarning,
    pd.errors.ParserWarning,
    RuntimeWarning,
    ResourceWarning,
)


def _nullable_pattern(
    *,
    distribution: int,
    values: tuple[object, object, object],
) -> list[object]:
    result = [values[(index + distribution) % 3] for index in range(ROW_COUNT)]
    for ordinal, row_index in enumerate(LANDMARKS):
        result[row_index] = values[(ordinal + distribution) % 3]
    return result


def _canonical_frame(distribution: int) -> pd.DataFrame:
    columns: dict[str, pd.Series] = {}
    boolean_fields = set(BOOLEAN_FIELDS)
    timestamp_fields = set(TIMESTAMP_FIELDS)
    numeric_fields = set(NUMERIC_FIELDS)
    assert boolean_fields | timestamp_fields | numeric_fields <= set(
        CANONICAL_EXPORT_COLUMNS
    )
    for ordinal, column in enumerate(CANONICAL_EXPORT_COLUMNS):
        offset = (distribution + ordinal) % 3
        if column in boolean_fields:
            columns[column] = pd.Series(
                _nullable_pattern(
                    distribution=offset,
                    values=(pd.NA, False, True),
                ),
                dtype="boolean",
            )
        elif column in numeric_fields:
            columns[column] = pd.Series(
                _nullable_pattern(
                    distribution=offset,
                    values=(pd.NA, float(ordinal + 1), float(ordinal + 2.5)),
                ),
                dtype="Float64",
            )
        elif column in timestamp_fields:
            columns[column] = pd.Series(
                _nullable_pattern(
                    distribution=offset,
                    values=(
                        pd.NaT,
                        pd.Timestamp("2026-08-24T12:00:00Z"),
                        pd.Timestamp("2026-08-25T12:00:00Z"),
                    ),
                ),
                dtype="datetime64[ns, UTC]",
            )
        else:
            columns[column] = pd.Series(
                _nullable_pattern(
                    distribution=offset,
                    values=(pd.NA, f"{column}-a", f"{column}-b"),
                ),
                dtype="string",
            )
    frame = pd.DataFrame(columns)
    assert tuple(frame.columns) == CANONICAL_EXPORT_COLUMNS
    assert len(frame) == ROW_COUNT
    return frame


def _logical_values(frame: pd.DataFrame, column: str) -> tuple[object, ...]:
    values: list[object] = []
    for value in frame[column].tolist():
        if pd.isna(value):
            values.append(None)
        elif column in BOOLEAN_FIELDS:
            values.append(bool(value))
        elif column in NUMERIC_FIELDS:
            values.append(float(value))
        elif column in TIMESTAMP_FIELDS:
            values.append(pd.Timestamp(value).isoformat())
        else:
            values.append(str(value))
    return tuple(values)


@pytest.mark.parametrize("serializer_name", ("csv", "parquet"))
@pytest.mark.parametrize("distribution", (0, 1))
def test_canonical_serializers_round_trip_production_shape_without_warnings(
    serializer_name: str,
    distribution: int,
) -> None:
    """Every canonical field must survive large CSV/Parquet round trips."""
    frame = _canonical_frame(distribution)
    serializer = get_serializer(serializer_name)

    with warnings.catch_warnings():
        for warning_class in WARNING_CLASSES:
            warnings.simplefilter("error", warning_class)
        content = serializer.serialize_bytes(frame)
        restored = serializer.deserialize_bytes(content)

    assert tuple(restored.columns) == CANONICAL_EXPORT_COLUMNS
    assert restored.shape == frame.shape
    for column in CANONICAL_EXPORT_COLUMNS:
        assert _logical_values(restored, column) == _logical_values(frame, column)

    for column in BOOLEAN_FIELDS:
        values_before = _logical_values(frame, column)
        values_after = _logical_values(restored, column)
        assert set(values_before) == {None, False, True}
        assert values_after == values_before
