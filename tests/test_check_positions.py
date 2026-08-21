"""Tests for opx.check_positions."""

import ast
import csv
from datetime import datetime, timezone
import os
from pathlib import Path
import time
from types import SimpleNamespace

import pandas as pd

from opx_chain.storage.models import DatasetRecord
from opx_chain.check_positions import (
    _format_filter_value,  # pylint: disable=protected-access
    _format_iso_timestamp,  # pylint: disable=protected-access
    _format_quote_value,  # pylint: disable=protected-access
    _is_true_like,  # pylint: disable=protected-access
    check_positions,
    find_latest_output,
    format_freshness_summary_lines,
    main,
)


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
    """Returns None when no output CSVs exist."""
    assert find_latest_output(tmp_path) is None


def test_find_latest_output_returns_most_recent(tmp_path):
    """Returns the most recently modified output CSV."""
    older = tmp_path / "options_engine_output_20260101_120000.csv"
    newer = tmp_path / "options_engine_output_20260102_120000.csv"
    older.write_text("x")
    time.sleep(0.01)
    newer.write_text("x")
    assert find_latest_output(tmp_path) == newer


def test_find_latest_output_uses_runtime_storage_dir(tmp_path, monkeypatch):
    """Default fallback scans should honor storage.dir from runtime config."""
    storage_dir = tmp_path / "custom-data"
    output_dir = storage_dir / "runs" / "run-1" / "output"
    output_dir.mkdir(parents=True)
    dataset = output_dir / "options_engine_output_20260102_120000.csv"
    dataset.write_text("underlying_symbol\nAAPL\n", encoding="utf-8")
    config = type("Config", (), {"storage_dir": storage_dir})()

    monkeypatch.setattr("opx_chain.check_positions.get_runtime_config", lambda: config)

    assert find_latest_output() == dataset


def test_check_positions_found(tmp_path):
    """A position present in the output CSV appears in the found list."""
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
    assert not missing
    key, _row = found[0]
    assert key.ticker == "AAPL"
    assert key.strike == 200.0


def test_check_positions_direct_uses_storage_latest_dataset(tmp_path, monkeypatch):
    """Direct callers should resolve the same storage-backed latest dataset as the CLI."""
    runs_dir = tmp_path / "runs"
    artifact = runs_dir / "run-1" / "output" / "ds.csv"
    artifact.parent.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "underlying_symbol": "AAPL",
                "expiration_date": "2026-06-20",
                "option_type": "call",
                "strike": 200.0,
            }
        ]
    ).to_csv(artifact, index=False)
    record = DatasetRecord(
        dataset_id="ds-id",
        run_id="run-1",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        provider="yfinance",
        schema_version=1,
        row_count=1,
        format="csv",
        location=str(artifact),
        content_hash="a" * 64,
    )

    class Storage:  # pylint: disable=too-few-public-methods
        """Storage stub exposing a retained dataset."""

        _runs_dir = runs_dir

        def list_datasets(self, *, limit):
            """Return retained dataset records."""
            assert limit == 100
            return [record]

        def load_validated_option_chain_dataset(self, dataset_id):
            """Return the configured fake dataset through the validated surface."""
            assert dataset_id == record.dataset_id
            return SimpleNamespace(
                handle=SimpleNamespace(location=record.location),
                frame=pd.read_csv(record.location),
            )

    pos_path = _write_positions(
        tmp_path,
        [{"Symbol": " -AAPL260620C200", "Description": "AAPL JUN 20 2026 $200 CALL"}],
    )
    monkeypatch.setattr("opx_chain.check_positions.get_storage_backend", Storage)

    found, missing = check_positions(pos_path)

    assert len(found) == 1
    assert not missing


def test_check_positions_missing(tmp_path):
    """A position absent from the output CSV appears in the missing list."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200", "Description": "AAPL JUN 20 2026 $200 CALL"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {"underlying_symbol": "MSFT", "expiration_date": "2026-06-20",
         "option_type": "call", "strike": 200.0, "bid": 5.0, "ask": 5.5,
         "passes_primary_screen": True},
    ])
    found, missing = check_positions(pos_path, out_path)
    assert not found
    assert len(missing) == 1
    assert missing[0].ticker == "AAPL"


def test_check_positions_missing_identity_columns_reports_missing(tmp_path):
    """Malformed outputs without option identity columns should not crash coverage checks."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200", "Description": "AAPL JUN 20 2026 $200 CALL"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {"bid": 5.0, "ask": 5.5},
    ])

    found, missing = check_positions(pos_path, out_path)

    assert not found
    assert len(missing) == 1
    assert missing[0].ticker == "AAPL"


def test_check_positions_no_output_returns_all_missing(tmp_path):
    """All positions are reported missing when the output file does not exist."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    found, missing = check_positions(pos_path, tmp_path / "nonexistent.csv")
    assert not found
    assert len(missing) == 1


def test_check_positions_empty_positions_returns_empty(tmp_path):
    """Returns empty lists when the positions file has no option positions."""
    pos_path = _write_positions(tmp_path, [])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [])
    found, missing = check_positions(pos_path, out_path)
    assert not found
    assert not missing


def test_check_positions_true_like_uses_canonical_boolean_coercion():
    """opx-check CSV boolean parsing should share the package vocabulary."""
    assert _is_true_like("on") is True
    assert _is_true_like("y") is True
    assert _is_true_like(1.0) is True
    assert _is_true_like("off") is False
    assert _is_true_like("n") is False
    assert _is_true_like(0.0) is False
    assert _is_true_like("garbage") is False


def test_check_positions_formatters_treat_non_finite_as_missing():
    """CLI formatting should not print inf as if it were a valid value."""
    assert _format_filter_value(float("inf")) == "missing"
    assert _format_filter_value(float("-inf")) == "missing"
    assert _format_quote_value(float("inf")) == "—"
    assert _format_quote_value(float("-inf")) == "—"


def test_check_positions_true_like_delegates_to_canonical_coercer():
    """Avoid reintroducing the old 3-string truthy set in opx-check."""
    module_path = Path(__file__).resolve().parents[1] / "opx_chain" / "check_positions.py"
    tree = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))
    true_like = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_is_true_like"
    )

    def call_name(node):
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        return None

    def is_false_constant(node):
        return isinstance(node, ast.Constant) and node.value is False

    canonical_calls = [
        node
        for node in ast.walk(true_like)
        if isinstance(node, ast.Call) and call_name(node.func) == "coerce_bool_or_default"
    ]
    assert len(canonical_calls) == 1
    assert any(
        keyword.arg == "default" and is_false_constant(keyword.value)
        for keyword in canonical_calls[0].keywords
    )

    legacy_truthy = {"true", "1", "yes"}

    def literal_strings(node):
        if isinstance(node, (ast.Set, ast.Tuple, ast.List)):
            return {
                elt.value
                for elt in node.elts
                if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
            }
        if (
            isinstance(node, ast.Call)
            and call_name(node.func) in {"set", "frozenset", "tuple", "list"}
            and len(node.args) == 1
        ):
            return literal_strings(node.args[0])
        return set()

    violations = [
        f"{module_path.relative_to(module_path.parents[1])}:{node.lineno}"
        for node in ast.walk(tree)
        if legacy_truthy.issubset(literal_strings(node))
    ]
    assert violations == []


def test_main_exits_0_all_found(tmp_path):
    """main() returns 0 when every position is present in the output."""
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


def test_main_ignores_pytest_argv_when_called_without_args(monkeypatch, tmp_path):
    """Bare main() under pytest should not parse pytest's own process argv."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {"underlying_symbol": "AAPL", "expiration_date": "2026-06-20",
         "option_type": "call", "strike": 200.0, "bid": 5.0, "ask": 5.5,
         "passes_primary_screen": True},
    ])
    monkeypatch.setattr("opx_chain.check_positions.DEFAULT_POSITIONS_PATH", pos_path)
    monkeypatch.setattr("opx_chain.check_positions.find_latest_output", lambda: out_path)

    assert main() == 0


def test_main_storage_default_fails_closed_on_outside_dataset_location(
    tmp_path,
    monkeypatch,
    capsys,
):
    """Storage-backed selection must not fall back around corrupt latest metadata."""

    class FakeStorage:  # pylint: disable=too-few-public-methods
        """Storage stub exposing retained dataset records and a runs root."""

        def __init__(self, records, runs_dir):
            self.records = records
            self._runs_dir = runs_dir

        def list_datasets(self, limit=100):
            """Return retained dataset records."""
            return self.records[:limit]

        def load_validated_option_chain_dataset(self, dataset_id):
            """Enforce the same path-containment behavior as a real backend."""
            record = next(item for item in self.records if item.dataset_id == dataset_id)
            path = Path(record.location).resolve()
            if not path.is_relative_to(Path(self._runs_dir).resolve()):
                raise ValueError(
                    f"dataset location escapes storage root: {dataset_id}"
                )
            return SimpleNamespace(
                handle=SimpleNamespace(location=record.location),
                frame=pd.read_csv(record.location),
            )

    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    runs_dir = tmp_path / "runs"
    inside_dir = runs_dir / "run-1" / "output"
    inside_dir.mkdir(parents=True)
    inside = _write_output(inside_dir, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-06-20",
            "option_type": "call",
            "strike": 200.0,
            "bid": 5.0,
            "ask": 5.5,
            "passes_primary_screen": True,
        },
    ])
    outside = _write_output(tmp_path, "outside.csv", [
        {
            "underlying_symbol": "MSFT",
            "expiration_date": "2026-06-20",
            "option_type": "call",
            "strike": 200.0,
            "bid": 1.0,
            "ask": 1.5,
            "passes_primary_screen": True,
        },
    ])

    def retained_record(dataset_id, run_id, created_at, path):
        return DatasetRecord(
            dataset_id=dataset_id,
            run_id=run_id,
            created_at=created_at,
            provider="yfinance",
            schema_version=1,
            row_count=1,
            format="csv",
            location=str(path),
            content_hash=f"{dataset_id}-hash",
        )

    records = [
        retained_record(
            "outside",
            "run-outside",
            datetime(2026, 1, 2, 12, 1, tzinfo=timezone.utc),
            outside,
        ),
        retained_record(
            "inside",
            "run-inside",
            datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc),
            inside,
        ),
    ]

    monkeypatch.setattr(
        "opx_chain.check_positions.get_storage_backend",
        lambda: FakeStorage(records, runs_dir),
    )
    monkeypatch.setattr("opx_chain.check_positions.find_latest_output", lambda: None)

    result = main(["--positions", str(pos_path)])

    captured = capsys.readouterr()
    assert result == 1
    assert "Stored output dataset is unusable" in captured.out
    assert "dataset location escapes storage root: outside" in captured.out
    assert f"Output:    {inside}" not in captured.out


def test_main_exits_1_some_missing(tmp_path):
    """main() returns 1 when any position is missing from the output."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {"underlying_symbol": "MSFT", "expiration_date": "2026-06-20",
         "option_type": "call", "strike": 200.0, "bid": 5.0, "ask": 5.5},
    ])
    result = main(["--positions", str(pos_path), "--output", str(out_path)])
    assert result == 1


def test_main_prints_passes_primary_screen_true_for_passing_row(tmp_path, capsys):
    """Found rows should use the canonical passes_primary_screen naming."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-06-20",
            "option_type": "call",
            "strike": 200.0,
            "bid": 5.0,
            "ask": 5.5,
            "bid_ask_spread_pct_of_mid": 0.08,
            "open_interest": 500,
            "volume": 25,
            "passes_primary_screen": True,
        },
    ])
    os.utime(out_path, (1_776_000_000, 1_776_000_000))

    result = main(["--positions", str(pos_path), "--output", str(out_path)])

    captured = capsys.readouterr()
    assert result == 0
    assert f"Positions: {pos_path}" in captured.out
    assert f"Output:    {out_path}" in captured.out
    assert "(fetched 2026-04-12T13:20:00Z)" in captured.out
    assert "passes_primary_screen=true" in captured.out
    assert "failed_filters:" not in captured.out


def test_main_prints_failed_primary_screen_filters_for_non_passing_row(tmp_path, capsys):
    """Found rows should show which configured primary-screen filters failed."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-06-20",
            "option_type": "call",
            "strike": 200.0,
            "bid": 5.0,
            "ask": 5.5,
            "bid_ask_spread_pct_of_mid": 0.30,
            "open_interest": 40,
            "volume": 5,
            "passes_primary_screen": False,
        },
    ])

    result = main(["--positions", str(pos_path), "--output", str(out_path)])

    captured = capsys.readouterr()
    assert result == 0
    assert "passes_primary_screen=false" in captured.out
    assert "failed_filters:" in captured.out
    assert "\n             - filters_max_spread_pct_of_mid(0.3000>0.2500)" in captured.out
    assert "\n             - filters_min_open_interest(40.0000<100.0000)" in captured.out
    assert "\n             - filters_min_volume(5.0000<10.0000)" in captured.out
    assert "filters_max_spread_pct_of_mid(0.3000>0.2500)" in captured.out
    assert "filters_min_open_interest(40.0000<100.0000)" in captured.out
    assert "filters_min_volume(5.0000<10.0000)" in captured.out


def test_main_formats_quotes_to_two_decimals_and_wraps_failed_filters(tmp_path, capsys):
    """Found rows should render bid/ask consistently and wrap long filter summaries."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-06-20",
            "option_type": "call",
            "strike": 200.0,
            "bid": 5.0,
            "ask": 5.5,
            "bid_ask_spread_pct_of_mid": 0.30,
            "open_interest": 40,
            "volume": 5,
            "passes_primary_screen": False,
        },
    ])

    result = main(["--positions", str(pos_path), "--output", str(out_path)])

    captured = capsys.readouterr()
    assert result == 0
    assert "bid=  5.00  ask=  5.50" in captured.out
    assert "\n           failed_filters:" in captured.out
    assert "\n             - filters_max_spread_pct_of_mid(0.3000>0.2500)" in captured.out
    assert "\n             - filters_min_open_interest(40.0000<100.0000)" in captured.out
    assert "\n             - filters_min_volume(5.0000<10.0000)" in captured.out


def test_main_found_position_missing_bid_ask_renders_missing_quotes(tmp_path, capsys):
    """Found rows without bid/ask columns should render missing quotes instead of crashing."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-06-20",
            "option_type": "call",
            "strike": 200.0,
            "passes_primary_screen": True,
        },
    ])

    result = main(["--positions", str(pos_path), "--output", str(out_path)])

    captured = capsys.readouterr()
    assert result == 0
    assert "bid=     —  ask=     —" in captured.out


def test_format_freshness_summary_lines_recomputes_current_age_from_saved_timestamps(tmp_path):
    """Freshness summary should reflect read-time age, not just stored fetch-time flags."""
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "GOOGL",
            "option_quote_time": "2026-04-10T13:40:56Z",
            "underlying_price_time": "2026-04-10T13:50:56Z",
            "is_stale_quote": False,
            "is_stale_underlying_price": False,
        },
        {
            "underlying_symbol": "GOOGL",
            "option_quote_time": "2026-04-10T13:40:56Z",
            "underlying_price_time": "2026-04-10T13:50:56Z",
            "is_stale_quote": False,
            "is_stale_underlying_price": False,
        },
    ])
    file_time = pd.Timestamp("2026-04-21T12:50:56Z").timestamp()
    os.utime(out_path, (file_time, file_time))

    lines = format_freshness_summary_lines(
        out_path,
        now=pd.Timestamp("2026-04-21T13:50:56Z"),
    )
    rendered = "\n".join(lines)

    assert "Freshness now:" in rendered
    assert "file_age_now=1h 00m 00s" in rendered
    assert (
        "option_quotes_now: rows_with_timestamp=2  stale_now_rows=2  stale_at_fetch_rows=0"
        in rendered
    )
    assert (
        "underlying_quotes_now: rows_with_timestamp=2  stale_now_rows=2  "
        "stale_at_fetch_rows=0" in rendered
    )
    assert "stale_underlyings_now:" in rendered
    assert "GOOGL" in rendered
    assert "time=2026-04-10T13:50:56Z" in rendered
    assert "newest_age=11d 00h 00m" in rendered


def test_format_freshness_summary_lines_handles_missing_timestamp_columns(tmp_path):
    """Freshness summary should not crash when an output lacks timestamp fields."""
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "GOOGL",
            "expiration_date": "2026-06-20",
            "option_type": "call",
            "strike": 200.0,
        },
    ])

    lines = format_freshness_summary_lines(
        out_path,
        now=pd.Timestamp("2026-04-21T13:50:56Z"),
    )
    rendered = "\n".join(lines)

    assert (
        "option_quotes_now: rows_with_timestamp=0  stale_now_rows=0  stale_at_fetch_rows=0"
        in rendered
    )
    assert (
        "underlying_quotes_now: rows_with_timestamp=0  stale_now_rows=0  "
        "stale_at_fetch_rows=0" in rendered
    )


def test_format_freshness_summary_lines_treats_future_timestamps_as_stale(tmp_path):
    """Future quote timestamps are suspect and should not pass freshness checks."""
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "GOOGL",
            "option_quote_time": "2026-04-21T14:05:56Z",
            "underlying_price_time": "2026-04-21T14:10:56Z",
            "is_stale_quote": False,
            "is_stale_underlying_price": False,
        },
    ])

    lines = format_freshness_summary_lines(
        out_path,
        now=pd.Timestamp("2026-04-21T13:50:56Z"),
    )
    rendered = "\n".join(lines)

    assert (
        "option_quotes_now: rows_with_timestamp=1  stale_now_rows=1  stale_at_fetch_rows=0"
        in rendered
    )
    assert (
        "underlying_quotes_now: rows_with_timestamp=1  stale_now_rows=1  "
        "stale_at_fetch_rows=0" in rendered
    )
    assert "stale_underlyings_now:" in rendered
    assert "GOOGL" in rendered


def test_format_iso_timestamp_formats_naive_timestamps_as_utc():
    """Naive timestamp values should be treated as UTC instead of crashing."""
    assert (
        _format_iso_timestamp(pd.Timestamp("2026-04-10T13:50:56"))
        == "2026-04-10T13:50:56Z"
    )


def test_main_prints_freshness_summary_when_requested(tmp_path, capsys, monkeypatch):
    """--freshness should print a runtime freshness section alongside position coverage."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-06-20",
            "option_type": "call",
            "strike": 200.0,
            "bid": 5.0,
            "ask": 5.5,
            "passes_primary_screen": True,
            "option_quote_time": "2026-04-21T16:40:00Z",
            "underlying_price_time": "2026-04-10T13:50:56Z",
            "is_stale_quote": False,
            "is_stale_underlying_price": False,
        },
    ])
    monkeypatch.setattr(
        "opx_chain.check_positions.utc_now_timestamp",
        lambda: pd.Timestamp("2026-04-21T17:00:00Z"),
    )

    result = main([
        "--positions", str(pos_path), "--output", str(out_path), "--freshness",
    ])

    captured = capsys.readouterr()
    assert result == 0
    assert "Freshness now:" in captured.out
    assert (
        "underlying_quotes_now: rows_with_timestamp=1  stale_now_rows=1  "
        "stale_at_fetch_rows=0" in captured.out
    )
    assert "stale_underlyings_now:" in captured.out
    assert "AAPL" in captured.out
    assert "passes_primary_screen=true" in captured.out
