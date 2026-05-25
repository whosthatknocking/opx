"""Validation tests for shared row-level and file-level checks."""

import pandas as pd

import opx_chain.validate as validate_mod
from opx_chain.export import CANONICAL_EXPORT_COLUMNS
from opx_chain.schema import BOOLEAN_FIELDS, INTEGER_DATASET_FIELDS, TIMESTAMP_FIELDS
from opx_chain.validate import (
    NUMERIC_FIELDS,
    emit_validation_report,
    summarize_validation_findings,
    validate_export_frame,
    validate_option_rows,
)


DERIVED_BOOLEAN_FIELDS = {
    "earnings_within_5d",
    "earnings_within_10d",
    "ex_div_within_3d",
    "has_negative_extrinsic_mid",
    "theta_efficiency_below_p25",
    "risk_model_inconsistent",
}


def make_valid_row(**overrides):
    """Build one canonical row that satisfies the shared validation contract."""
    row = {
        "data_source": "stub",
        "underlying_symbol": "TEST",
        "contract_symbol": "TEST260417C00100000",
        "option_type": "call",
        "expiration_date": "2026-04-17",
        "strike": 100.0,
        "underlying_price": 101.0,
        "bid": 1.0,
        "ask": 1.2,
        "last_trade_price": 1.1,
        "volume": 10,
        "open_interest": 20,
        "implied_volatility": 0.3,
        "option_quote_time": pd.Timestamp("2026-03-20T13:40:00Z"),
        "underlying_price_time": pd.Timestamp("2026-03-20T13:45:00Z"),
        "is_in_the_money": False,
        "has_valid_quote": True,
        "passes_primary_screen": True,
    }
    row.update(overrides)
    return row


def test_validate_option_rows_flags_missing_required_bid_and_ask():
    """Missing shared required quote fields should be row-level errors."""
    frame = pd.DataFrame([make_valid_row(bid=None, ask=None)])

    findings = validate_option_rows(frame)

    assert any(f.code == "missing_required_field" and f.field == "bid" for f in findings)
    assert any(f.code == "missing_required_field" and f.field == "ask" for f in findings)


def test_validate_option_rows_flags_invalid_types_and_quote_order():
    """Malformed shared field values should surface as validation findings."""
    frame = pd.DataFrame(
        [
            make_valid_row(
                option_type="CALL",
                expiration_date="04/17/2026",
                bid=2.0,
                ask=1.0,
                has_valid_quote="yes",
            )
        ]
    )

    findings = validate_option_rows(frame)

    assert any(f.code == "invalid_option_type" for f in findings)
    assert any(f.code == "invalid_expiration_date" for f in findings)
    assert any(f.code == "invalid_quote_order" for f in findings)
    assert any(f.code == "invalid_boolean_field" for f in findings)


def test_validate_option_rows_flags_non_finite_numeric_fields():
    """NaN and Infinity must be rejected at the validation boundary."""
    rows = []
    for field in NUMERIC_FIELDS:
        rows.append(make_valid_row(contract_symbol=f"INF{field}", **{field: "Infinity"}))
        rows.append(make_valid_row(contract_symbol=f"NEG_INF{field}", **{field: -float("inf")}))

    findings = validate_option_rows(pd.DataFrame(rows))

    invalid_fields = [
        finding.field
        for finding in findings
        if finding.code == "invalid_numeric_field"
    ]
    assert invalid_fields.count("strike") == 2
    assert invalid_fields.count("underlying_price") == 2
    assert invalid_fields.count("bid") == 2
    assert invalid_fields.count("ask") == 2
    assert invalid_fields.count("last_trade_price") == 2
    assert invalid_fields.count("volume") == 2
    assert invalid_fields.count("open_interest") == 2
    assert invalid_fields.count("implied_volatility") == 2


def test_validate_option_rows_flags_boolean_numeric_fields():
    """Booleans in numeric fields should not pass as finite one/zero values."""
    rows = [
        make_valid_row(contract_symbol=f"BOOL_{field}", **{field: True})
        for field in NUMERIC_FIELDS
    ]

    findings = validate_option_rows(pd.DataFrame(rows))
    invalid_fields = {
        finding.field
        for finding in findings
        if finding.code == "invalid_numeric_field"
    }

    assert invalid_fields == set(NUMERIC_FIELDS)


def test_exported_boolean_fields_are_validated():
    """Every exported boolean-like field should be covered by row validation."""
    assert DERIVED_BOOLEAN_FIELDS.issubset(BOOLEAN_FIELDS)

    for field in BOOLEAN_FIELDS:
        assert field in CANONICAL_EXPORT_COLUMNS


def test_schema_field_groups_cover_dataset_dtypes_and_validation():
    """Dataset normalization and validation should share canonical schema fields."""
    assert INTEGER_DATASET_FIELDS == ("days_to_expiration",)
    assert TIMESTAMP_FIELDS == ("option_quote_time", "underlying_price_time")
    assert validate_mod.TIMESTAMP_FIELDS is TIMESTAMP_FIELDS


def test_validate_option_rows_flags_derived_boolean_fields():
    """Derived boolean fields should not bypass invalid_boolean_field checks."""
    frame = pd.DataFrame(
        [
            make_valid_row(
                earnings_within_5d="yes",
                earnings_within_10d="no",
                ex_div_within_3d=1,
                has_negative_extrinsic_mid="false",
                theta_efficiency_below_p25="true",
                risk_model_inconsistent=0,
            )
        ]
    )

    findings = validate_option_rows(frame)
    fields = {finding.field for finding in findings if finding.code == "invalid_boolean_field"}

    assert DERIVED_BOOLEAN_FIELDS.issubset(fields)


def test_validate_option_rows_skips_python_boolean_map_for_bool_dtype(monkeypatch):
    """Native bool columns should use the dtype fast path."""
    frame = pd.DataFrame(
        [
            {
                **make_valid_row(),
                **{field: True for field in BOOLEAN_FIELDS},
            }
        ]
    )

    def fail_boolean_like(_value):
        raise AssertionError("bool dtype fields should not call _is_boolean_like")

    monkeypatch.setattr(validate_mod, "_is_boolean_like", fail_boolean_like)

    findings = validate_option_rows(frame)

    assert not any(f.code == "invalid_boolean_field" for f in findings)


def test_validate_option_rows_does_not_use_iterrows(monkeypatch):
    """Row validation should be vectorized instead of materializing row Series."""

    def fail_iterrows(_self):
        raise AssertionError("iterrows should not be used by validate_option_rows")

    monkeypatch.setattr(pd.DataFrame, "iterrows", fail_iterrows)
    frame = pd.DataFrame(
        [
            make_valid_row(contract_symbol="TEST260417C00100000"),
            make_valid_row(contract_symbol="TEST260417P00100000", bid=2.0, ask=1.0),
        ],
        index=[10, 20],
    )

    findings = validate_option_rows(frame)

    assert any(
        f.code == "invalid_quote_order"
        and f.row_index == 20
        and f.contract_symbol == "TEST260417P00100000"
        for f in findings
    )


def test_validate_export_frame_flags_missing_columns_and_duplicates():
    """Combined export validation should catch file-level schema and duplicate issues."""
    frame = pd.DataFrame(
        [
            make_valid_row(),
            make_valid_row(),
        ]
    ).drop(columns=["bid"])
    frame.loc[1, "contract_symbol"] = frame.loc[0, "contract_symbol"]

    findings = validate_export_frame(frame)

    assert any(f.code == "missing_required_column" and f.field == "bid" for f in findings)
    assert any(f.code == "duplicate_contract_row" for f in findings)


def test_validate_export_frame_does_not_use_iterrows(monkeypatch):
    """Export validation should avoid materializing duplicate rows."""

    def fail_iterrows(_self):
        raise AssertionError("iterrows should not be used by validate_export_frame")

    monkeypatch.setattr(pd.DataFrame, "iterrows", fail_iterrows)
    frame = pd.DataFrame(
        [
            make_valid_row(contract_symbol="TEST260417C00100000"),
            make_valid_row(contract_symbol="TEST260417C00100000"),
        ],
        index=[100, 200],
    )

    findings = validate_export_frame(frame)

    assert any(
        f.code == "duplicate_contract_row"
        and f.row_index == 200
        and f.contract_symbol == "TEST260417C00100000"
        for f in findings
    )


def test_emit_validation_report_prints_counts(capsys):
    """Validation reporting should print a stable summary even when findings are empty."""
    emit_validation_report([])

    stdout = capsys.readouterr().out
    assert "Validation summary:" in stdout
    assert "warnings: 0" in stdout
    assert "errors: 0" in stdout


def test_summarize_validation_findings_counts_warning_and_error():
    """Severity counts should be easy to aggregate for the run summary."""
    findings = validate_option_rows(
        pd.DataFrame([make_valid_row(option_type="CALL", has_valid_quote="yes")])
    )

    warnings, errors = summarize_validation_findings(findings)

    assert warnings == 1
    assert errors == 1
