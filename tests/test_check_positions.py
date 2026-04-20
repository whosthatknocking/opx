"""Tests for opx.check_positions."""

import csv
import io
from pathlib import Path

import pandas as pd
import pytest

from opx.check_positions import check_positions, find_latest_output, main


def _write_positions(tmp_path, rows):
    path = tmp_path / "positions.csv"
    fieldnames = ["Account Number", "Account Name", "Symbol", "Description",
                  "Quantity", "Last Price", "Last Price Change", "Current Value",
                  "Today's Gain/Loss Dollar", "Today's Gain/Loss Percent",
                  "Total Gain/Loss Dollar", "Total Gain/Loss Percent",
                  "Percent Of Account", "Cost Basis Total", "Average Cost Basis", "Type"]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            full_row = {k: "" for k in fieldnames}
            full_row.update(row)
            writer.writerow(full_row)
    return path


def _write_output(tmp_path, name, rows):
    path = tmp_path / name
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_find_latest_output_returns_none_when_empty(tmp_path):
    assert find_latest_output(tmp_path) is None


def test_find_latest_output_returns_most_recent(tmp_path):
    older = tmp_path / "options_engine_output_20260101_120000.csv"
    newer = tmp_path / "options_engine_output_20260102_120000.csv"
    older.write_text("x")
    import time; time.sleep(0.01)
    newer.write_text("x")
    assert find_latest_output(tmp_path) == newer


def test_check_positions_found(tmp_path):
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200", "Description": "AAPL JUN 20 2026 $200 CALL"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {"underlying_symbol": "AAPL", "expiration_date": "2026-06-20",
         "option_type": "call", "strike": 200.0, "bid": 5.0, "ask": 5.5,
         "passes_primary_screen": True},
    ])
    found, missing = check_positions(pos_path, out_path)
    assert len(found) == 1
    assert len(missing) == 0
    key, row = found[0]
    assert key.ticker == "AAPL"
    assert key.strike == 200.0


def test_check_positions_missing(tmp_path):
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200", "Description": "AAPL JUN 20 2026 $200 CALL"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {"underlying_symbol": "MSFT", "expiration_date": "2026-06-20",
         "option_type": "call", "strike": 200.0, "bid": 5.0, "ask": 5.5,
         "passes_primary_screen": True},
    ])
    found, missing = check_positions(pos_path, out_path)
    assert len(found) == 0
    assert len(missing) == 1
    assert missing[0].ticker == "AAPL"


def test_check_positions_no_output_returns_all_missing(tmp_path):
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    found, missing = check_positions(pos_path, tmp_path / "nonexistent.csv")
    assert len(found) == 0
    assert len(missing) == 1


def test_check_positions_empty_positions_returns_empty(tmp_path):
    pos_path = _write_positions(tmp_path, [])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [])
    found, missing = check_positions(pos_path, out_path)
    assert found == []
    assert missing == []


def test_main_exits_0_all_found(tmp_path, monkeypatch):
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {"underlying_symbol": "AAPL", "expiration_date": "2026-06-20",
         "option_type": "call", "strike": 200.0, "bid": 5.0, "ask": 5.5,
         "passes_primary_screen": True},
    ])
    result = main(["--positions", str(pos_path), "--output", str(out_path)])
    assert result == 0


def test_main_exits_1_some_missing(tmp_path, monkeypatch):
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {"underlying_symbol": "MSFT", "expiration_date": "2026-06-20",
         "option_type": "call", "strike": 200.0, "bid": 5.0, "ask": 5.5},
    ])
    result = main(["--positions", str(pos_path), "--output", str(out_path)])
    assert result == 1
