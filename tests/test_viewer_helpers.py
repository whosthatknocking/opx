"""Viewer helper tests for field descriptions, cards, and freshness metadata."""
# pylint: disable=too-many-lines
from datetime import datetime, timezone
import io
from http import HTTPStatus
from importlib import resources
import json
import os
from pathlib import Path

import pandas as pd
import pytest

from conftest import make_option_chain_frame
from opx_chain import SCHEMA_VERSION
from opx_chain import viewer
from opx_chain.export import CANONICAL_EXPORT_COLUMNS
from opx_chain.storage.filesystem import FilesystemBackend
from opx_chain.storage.models import (
    DatasetRecord,
    DatasetWrite,
    RunContext,
    RunSummary,
)


def build_config(viewer_host: str, viewer_port: int):
    """Create a lightweight runtime-config stub for viewer tests."""
    return type(
        "Config",
        (),
        {"viewer_host": viewer_host, "viewer_port": viewer_port},
    )()


def test_extract_field_descriptions_reads_current_field_reference_entries():
    """Field-reference descriptions should stay discoverable for the viewer."""
    descriptions = viewer.extract_field_descriptions()

    assert "underlying_symbol" in descriptions
    assert "delta_safety_pct" in descriptions
    assert "Use it to group rows by underlying." in descriptions["underlying_symbol"]


def test_extract_field_descriptions_returns_only_canonical_columns():
    """Provider mapping legends should not leak into viewer field descriptions."""
    descriptions = viewer.extract_field_descriptions()

    assert set(descriptions) <= set(CANONICAL_EXPORT_COLUMNS)
    assert {"Blank", "Derived", "Direct", "Transformed"}.isdisjoint(descriptions)


def test_viewer_is_truthy_uses_canonical_boolean_coercion():
    """Viewer CSV boolean parsing should share the package-level vocabulary."""
    assert viewer.is_truthy("on") is True
    assert viewer.is_truthy(1.0) is True
    assert viewer.is_truthy("off") is False


def test_viewer_numeric_coercion_treats_booleans_as_missing():
    """Boolean cells should not be accepted as numeric market-data values."""
    series = viewer.coerce_number(pd.Series([True, False]))

    assert series.dropna().empty
    assert viewer.coerce_scalar_number(True) is None


def test_viewer_packaged_docs_match_canonical_docs():
    """Wheel-installed viewer docs should stay synced with source docs."""
    for filename in ("FIELD_REFERENCE.md", "USER_GUIDE.md"):
        canonical = (Path(__file__).resolve().parents[1] / "docs" / filename).read_text(
            encoding="utf-8"
        )
        packaged = (
            resources.files("opx_chain")
            .joinpath("docs", filename)
            .read_text(encoding="utf-8")
        )

        assert packaged == canonical


def test_field_reference_yfinance_event_booleans_match_runtime_support():
    """YFinance event docs should not claim derived event flags are unsupported."""
    field_reference_path = Path(__file__).resolve().parents[1] / "docs" / "FIELD_REFERENCE.md"
    field_reference = field_reference_path.read_text(encoding="utf-8")
    table = field_reference.split("### Corporate Event Mapping", maxsplit=1)[1]
    table = table.split("### Run Metadata Mapping", maxsplit=1)[0]

    for field in ("earnings_within_5d", "earnings_within_10d", "ex_div_within_3d"):
        row = next(line for line in table.splitlines() if line.startswith(f"| `{field}`"))
        yfinance_cell = row.split("|")[2].strip()

        assert yfinance_cell.startswith("Derived:")
        assert "event fetching is not implemented" not in yfinance_cell


def test_field_reference_documents_viewer_only_positions_metadata():
    """Viewer-only positions cards should be documented outside the CSV schema."""
    field_reference_path = Path(__file__).resolve().parents[1] / "docs" / "FIELD_REFERENCE.md"
    field_reference = field_reference_path.read_text(encoding="utf-8")

    assert "## Viewer-Only Dataset Metadata Cards" in field_reference
    assert "`Positions`" in field_reference
    assert "`Position Fingerprint`" in field_reference
    assert "`Position Coverage`" in field_reference
    assert "not exported CSV columns" in field_reference
    assert "portfolio row browsing belongs in\nopx-strategy" in field_reference


def test_viewer_markdown_loader_falls_back_to_packaged_docs(tmp_path: Path):
    """Non-editable installs should not require a sibling source-tree docs directory."""
    missing_source_doc = tmp_path / "missing" / "FIELD_REFERENCE.md"

    markdown = viewer.load_viewer_markdown("FIELD_REFERENCE.md", missing_source_doc)

    assert "underlying_symbol" in markdown


def test_viewer_has_no_dead_user_guide_loader():
    """The viewer runtime should depend on the field-reference document only."""
    assert not hasattr(viewer, "load_user_guide_text")
    assert not hasattr(viewer, "USER_GUIDE_PATH")


def test_viewer_has_no_unused_or_out_of_scope_api_scaffold():
    """The viewer should not expose endpoints without an in-scope UI consumer."""
    source = (Path(__file__).resolve().parents[1] / "opx_chain" / "viewer.py").read_text(
        encoding="utf-8"
    )

    assert "VIEWER_PREFS_PATH" not in source
    assert "load_viewer_prefs" not in source
    assert "save_viewer_prefs" not in source
    assert '"/api/prefs"' not in source
    assert '"/api/readme"' not in source
    assert '"/api/positions"' not in source


def test_build_dataset_cards_only_promotes_dataset_wide_constant_values():
    """Only dataset-wide constant values should be promoted into header cards."""
    frame = pd.DataFrame(
        [
            {
                "premium_reference_method": "mid",
                "risk_free_rate_used": 0.045,
                "data_source": "yfinance",
            },
            {
                "premium_reference_method": "bid",
                "risk_free_rate_used": 0.045,
                "data_source": "yfinance",
            },
        ]
    )

    cards = viewer.build_dataset_cards(frame, descriptions={"data_source": "Source label."})
    card_names = [card["name"] for card in cards]

    assert "risk_free_rate_used" in card_names
    assert "data_source" in card_names
    assert "premium_reference_method" not in card_names


def test_build_column_definitions_marks_numeric_but_not_boolean_columns():
    """Boolean columns should not be classified as numeric in the viewer schema."""
    frame = pd.DataFrame(
        {
            "strike": [100.0, 105.0],
            "underlying_symbol": ["TSLA", "TSLA"],
            "passes_primary_screen": [True, False],
        }
    )

    definitions = viewer.build_column_definitions(frame, descriptions={})
    by_name = {column["name"]: column for column in definitions}

    assert by_name["strike"]["is_numeric"] is True
    assert by_name["underlying_symbol"]["is_numeric"] is False
    assert by_name["passes_primary_screen"]["is_numeric"] is False


def test_build_freshness_summary_reports_file_and_quote_ages(tmp_path: Path):
    """Freshness summary should report both file age and quote age statistics."""
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("placeholder", encoding="utf-8")
    os.utime(csv_path, (1_776_000_000, 1_776_000_000))
    frame = pd.DataFrame(
        {
            "quote_age_seconds": [10, 30, 50],
            "underlying_price_age_seconds": [5, 15, 25],
        }
    )

    summary = viewer.build_freshness_summary(frame, csv_path)

    assert summary["option_quote_age_median_seconds"] == 30.0
    assert summary["option_quote_age_max_seconds"] == 50.0
    assert summary["underlying_quote_age_median_seconds"] == 15.0
    assert summary["underlying_quote_age_max_seconds"] == 25.0
    assert summary["file_age_seconds"] >= 0
    assert summary["file_modified_at"] == "2026-04-12T13:20:00Z"


def test_normalize_row_value_keeps_days_to_expiration_as_integer():
    """Viewer payload serialization should keep days_to_expiration whole."""
    assert viewer.normalize_row_value("days_to_expiration", 14.0) == 14
    assert viewer.normalize_row_value("time_to_expiration_years", 14.0) == 14.0


def test_respond_json_serializes_non_finite_values_as_null():
    """Viewer JSON responses must stay parseable by strict JSON clients."""
    handler = object.__new__(viewer.ViewerRequestHandler)
    handler.wfile = io.BytesIO()
    headers: list[tuple[str, str]] = []
    handler.send_response = lambda _status: None
    handler.send_header = lambda name, value: headers.append((name, value))
    handler.end_headers = lambda: None

    handler.respond_json({
        "nan_value": float("nan"),
        "infinite_value": float("inf"),
        "nested": [{"negative_infinite_value": float("-inf")}],
    })

    body = handler.wfile.getvalue().decode("utf-8")
    assert "NaN" not in body
    assert "Infinity" not in body
    assert json.loads(body) == {
        "nan_value": None,
        "infinite_value": None,
        "nested": [{"negative_infinite_value": None}],
    }
    assert ("Content-Type", "application/json; charset=utf-8") in headers


def test_market_context_ignores_non_finite_values():
    """Market text helpers should not render inf or inf%."""
    assert (
        viewer.build_market_context("TSLA", float("inf"), 0.01)
        == "TSLA last underlying price was unavailable."
    )
    assert viewer.build_latest_status(float("inf"), float("inf"), 0.2) == "Snapshot available"


def _capture_api_response(handler):
    """Capture JSON endpoint responses from a handler built without a socket."""
    captured: dict[str, object] = {}
    handler.respond_json = lambda payload, status=HTTPStatus.OK: captured.update(
        {"payload": payload, "status": status}
    )
    return captured


def test_api_files_returns_structured_error_json(monkeypatch):
    """File listing failures should return JSON 500 responses, not uncaught errors."""
    handler = object.__new__(viewer.ViewerRequestHandler)
    handler.path = "/api/files"
    captured = _capture_api_response(handler)

    def fail_listing():
        raise RuntimeError("disk unavailable")

    monkeypatch.setattr(viewer, "make_file_listing", fail_listing)

    handler.do_GET()

    assert captured == {
        "payload": {"error": "Failed to load file listing: disk unavailable"},
        "status": HTTPStatus.INTERNAL_SERVER_ERROR,
    }


def test_api_reference_returns_structured_not_found_json(monkeypatch):
    """Reference markdown failures should use the same FileNotFound JSON contract."""
    handler = object.__new__(viewer.ViewerRequestHandler)
    handler.path = "/api/reference"
    captured = _capture_api_response(handler)

    def missing_reference():
        raise FileNotFoundError("FIELD_REFERENCE.md missing")

    monkeypatch.setattr(viewer, "load_field_reference_markdown", missing_reference)

    handler.do_GET()

    assert captured == {
        "payload": {"error": "FIELD_REFERENCE.md missing"},
        "status": HTTPStatus.NOT_FOUND,
    }


def test_build_positions_dataset_cards_reports_counts_fingerprint_and_coverage(
    tmp_path: Path,
):
    """The viewer should expose positions metadata without browsing positions rows."""
    run_dir = tmp_path / "runs" / "run-123"
    output_dir = run_dir / "output"
    output_dir.mkdir(parents=True)
    dataset_path = output_dir / "options_engine_output_20260421_120000.csv"
    frame = pd.DataFrame(
        [
            {
                "underlying_symbol": "TSLA",
                "expiration_date": "2026-08-21",
                "option_type": "put",
                "strike": 360.0,
            },
            {
                "underlying_symbol": "NVDA",
                "expiration_date": "2026-06-05",
                "option_type": "put",
                "strike": 200.0,
            },
        ]
    )
    frame.to_csv(dataset_path, index=False)
    (run_dir / "positions.csv").write_text(
        "Symbol,Description,Quantity\n"
        "TSLA,TESLA INC,100\n"
        "-NVDA260605P200,NVDA JUN 05 2026 $200 PUT,-1\n"
        "-ORCL260605P120,ORCL JUN 05 2026 $120 PUT,-1\n",
        encoding="utf-8",
    )

    cards = viewer.build_positions_dataset_cards(frame, dataset_path)
    by_name = {card["name"]: card for card in cards}

    assert by_name["Positions"]["value"] == "1 stocks / 2 options"
    assert by_name["Position Fingerprint"]["value"] != "none"
    assert "Full parsed-position fingerprint" in by_name["Position Fingerprint"]["description"]
    assert by_name["Position Coverage"]["value"] == "1/1 stocks / 1/2 options"


def test_load_csv_payload_includes_run_positions_summary_cards(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Dataset payloads should include lightweight positions metadata cards."""
    run_dir = tmp_path / "runs" / "run-456"
    output_dir = run_dir / "output"
    output_dir.mkdir(parents=True)
    dataset_path = output_dir / "options_engine_output_20260422_120000.csv"
    dataset_path.write_text(
        "underlying_symbol,expiration_date,option_type,strike\n"
        "NVDA,2026-06-05,put,200\n",
        encoding="utf-8",
    )
    (run_dir / "positions.csv").write_text(
        "Symbol,Description,Quantity\nNVDA,NVIDIA CORP,200\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(viewer, "discover_dataset_paths", lambda: [dataset_path])

    payload = viewer.load_csv_payload(dataset_path.name)
    card_names = {card["name"] for card in payload["dataset_cards"]}

    assert {
        "Integrity Status",
        "Dataset Facts Status",
        "Positions",
        "Position Fingerprint",
        "Position Coverage",
    } <= card_names


def test_viewer_discloses_valid_registered_integrity_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Raw viewer inspection labels the effective registered metadata state."""
    backend = FilesystemBackend(tmp_path / "runs", tmp_path / "debug")
    run_id = backend.create_run(RunContext("synthetic", ("SYNTH",), "cfg", "pos"))
    record = backend.write_dataset(
        run_id,
        DatasetWrite(
            make_option_chain_frame(
                rows=1,
                ticker="SYNTH",
                provider="synthetic",
            ),
            "synthetic",
            SCHEMA_VERSION,
        ),
    )
    backend.finalize_run(run_id, RunSummary("complete"))
    dataset_path = Path(record.location)
    monkeypatch.setattr(viewer, "_DATA_DIR_OVERRIDE", None)
    monkeypatch.setattr(viewer, "_CSV_MODE", False)
    monkeypatch.setattr(viewer, "get_storage_backend", lambda: backend)
    monkeypatch.setattr(viewer, "discover_dataset_paths", lambda: [dataset_path])

    payload = viewer.load_csv_payload(dataset_path.name)
    cards = {card["name"]: card["value"] for card in payload["dataset_cards"]}
    listing = viewer.make_file_listing()[0]

    assert cards["Integrity Status"] == "valid"
    assert cards["Dataset Facts Status"] == "available"
    assert listing["dataset_id"] == record.dataset_id
    assert listing["integrity_status"] == "valid"


def test_load_csv_payload_treats_malformed_integer_fields_as_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Malformed integer-like viewer fields should not crash JSON serialization."""
    dataset_path = tmp_path / "options_engine_output_20260422_120000.csv"
    dataset_path.write_text(
        "underlying_symbol,days_to_earnings,days_to_ex_div,event_risk_score\n"
        "NVDA,soon,later,bad\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(viewer, "discover_dataset_paths", lambda: [dataset_path])

    payload = viewer.load_csv_payload(dataset_path.name)

    assert payload["rows"][0]["days_to_earnings"] is None
    assert payload["rows"][0]["days_to_ex_div"] is None
    assert payload["rows"][0]["event_risk_score"] is None


def test_resolve_csv_path_rejects_undiscovered_dataset_names(tmp_path: Path, monkeypatch):
    """Viewer dataset selection should only accept discovered dataset basenames."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    dataset_path = output_dir / "options_engine_output_20260421_120000.csv"
    dataset_path.write_text("underlying_symbol\nTSLA\n", encoding="utf-8")
    outside_path = tmp_path / "secret.csv"
    outside_path.write_text("do not read\n", encoding="utf-8")

    monkeypatch.setattr(viewer, "_DATA_DIR_OVERRIDE", output_dir)

    assert viewer.resolve_csv_path(dataset_path.name) == dataset_path
    for invalid_name in ("../secret.csv", "missing.csv", str(outside_path)):
        try:
            viewer.resolve_csv_path(invalid_name)
        except FileNotFoundError as exc:
            assert str(exc) == f"Dataset file not found: {invalid_name}"
        else:
            raise AssertionError(f"Expected FileNotFoundError for {invalid_name}")


def test_discover_dataset_paths_uses_runtime_storage_dir_fallback(
    tmp_path: Path,
    monkeypatch,
):
    """Viewer fallback discovery should honor storage.dir from runtime config."""
    dataset = tmp_path.joinpath(
        "custom-data",
        "runs",
        "run-1",
        "output",
        "options_engine_output_20260102_120000.csv",
    )
    dataset.parent.mkdir(parents=True)
    dataset.write_text("underlying_symbol\nAAPL\n", encoding="utf-8")
    config = type("Config", (), {"storage_dir": tmp_path / "custom-data"})()

    monkeypatch.setattr(viewer, "_DATA_DIR_OVERRIDE", None)
    monkeypatch.setattr(viewer, "_CSV_MODE", False)
    monkeypatch.setattr(viewer, "get_storage_backend", lambda: None)
    monkeypatch.setattr(viewer, "get_runtime_config", lambda: config)

    assert viewer.discover_dataset_paths() == [dataset]


def test_discover_dataset_paths_requests_uncapped_storage_listing(tmp_path: Path, monkeypatch):
    """Storage-backed viewer discovery should not inherit the backend's small default cap."""

    class FakeStorage:  # pylint: disable=too-few-public-methods
        """Storage stub that honors the requested limit like real backends do."""

        def __init__(self, records: list[DatasetRecord]) -> None:
            self.records = records
            self.requested_limit = None
            self._runs_dir = tmp_path

        def list_datasets(self, limit=50, **_kwargs):
            """Return records up to the supplied limit."""
            self.requested_limit = limit
            return self.records[:limit]

    records = []
    for index in range(60):
        dataset = tmp_path / f"options_engine_output_20260102_12{index:04d}.csv"
        dataset.write_text("underlying_symbol\nAAPL\n", encoding="utf-8")
        records.append(
            DatasetRecord(
                dataset_id=f"dataset-{index}",
                run_id=f"run-{index}",
                created_at=datetime(2026, 1, 2, 12, index % 60, tzinfo=timezone.utc),
                provider="yfinance",
                schema_version=1,
                row_count=1,
                format="csv",
                location=str(dataset),
                content_hash=f"hash-{index}",
            )
        )
    storage = FakeStorage(records)

    monkeypatch.setattr(viewer, "_DATA_DIR_OVERRIDE", None)
    monkeypatch.setattr(viewer, "_CSV_MODE", False)
    monkeypatch.setattr(viewer, "get_storage_backend", lambda: storage)

    discovered = viewer.discover_dataset_paths()

    assert storage.requested_limit == viewer.VIEWER_DATASET_DISCOVERY_LIMIT
    assert len(discovered) == len(records)


def test_discover_dataset_paths_skips_storage_records_outside_runs_root(
    tmp_path: Path,
    monkeypatch,
):
    """Storage-backed discovery should not trust retained locations outside runs."""

    class FakeStorage:  # pylint: disable=too-few-public-methods
        """Storage stub with an explicit runs root."""

        def __init__(self, records: list[DatasetRecord], runs_dir: Path) -> None:
            self.records = records
            self._runs_dir = runs_dir

        def list_datasets(self, limit=50, **_kwargs):
            """Return all records without applying extra filtering."""
            return self.records[:limit]

    runs_dir = tmp_path / "runs"
    inside = runs_dir / "run-1" / "output" / "options_engine_output_inside.csv"
    inside.parent.mkdir(parents=True)
    inside.write_text("underlying_symbol\nAAPL\n", encoding="utf-8")
    outside = tmp_path / "outside.csv"
    outside.write_text("underlying_symbol\nMSFT\n", encoding="utf-8")
    records = [
        DatasetRecord(
            dataset_id="outside",
            run_id="run-outside",
            created_at=datetime(2026, 1, 2, 12, 1, tzinfo=timezone.utc),
            provider="yfinance",
            schema_version=1,
            row_count=1,
            format="csv",
            location=str(outside),
            content_hash="outside-hash",
        ),
        DatasetRecord(
            dataset_id="inside",
            run_id="run-inside",
            created_at=datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc),
            provider="yfinance",
            schema_version=1,
            row_count=1,
            format="csv",
            location=str(inside),
            content_hash="inside-hash",
        ),
    ]
    storage = FakeStorage(records, runs_dir)

    monkeypatch.setattr(viewer, "_DATA_DIR_OVERRIDE", None)
    monkeypatch.setattr(viewer, "_CSV_MODE", False)
    monkeypatch.setattr(viewer, "get_storage_backend", lambda: storage)

    assert viewer.discover_dataset_paths() == [inside]


def test_make_file_listing_stats_each_file_once(tmp_path: Path, monkeypatch):
    """File listings should reuse a single stat result for size and modified time."""
    dataset = tmp_path / "options_engine_output_20260102_120000.csv"
    dataset.write_text("underlying_symbol\nAAPL\n", encoding="utf-8")
    expected_stat = dataset.stat()
    stat_count = 0
    original_stat = Path.stat

    def counting_stat(path: Path, *args, **kwargs):
        nonlocal stat_count
        stat_count += 1
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(viewer, "discover_dataset_paths", lambda: [dataset])
    monkeypatch.setattr(
        viewer,
        "_dataset_integrity_metadata",
        lambda _path: {
            "dataset_id": None,
            "integrity_status": "unknown",
            "dataset_facts_status": "unknown",
        },
    )
    monkeypatch.setattr(Path, "stat", counting_stat)

    listing = viewer.make_file_listing()

    assert listing == [{
        "name": dataset.name,
        "size_bytes": expected_stat.st_size,
        "modified_at": expected_stat.st_mtime,
        "dataset_id": None,
        "integrity_status": "unknown",
        "dataset_facts_status": "unknown",
    }]
    assert stat_count == 1


def test_build_ticker_summary_marks_estimated_marketdata_earnings_dates():
    """Summary payload should preserve whether the next earnings date is estimated."""
    frame = pd.DataFrame(
        [
            {
                "underlying_price": 100.0,
                "underlying_day_change_pct": 0.01,
                "implied_volatility": 0.25,
                "historical_volatility": 0.20,
                "option_type": "call",
                "expiration_date": "2026-04-17",
                "next_earnings_date": "2026-04-30",
                "next_earnings_date_is_estimated": "True",
                "event_risk_score": 60.0,
            }
        ]
    )

    summary = viewer.build_ticker_summary("TSLA", frame)

    assert summary["next_earnings_date"] == "2026-04-30"
    assert summary["next_earnings_date_is_estimated"] is True
    assert summary["event_risk_score"] == 60.0


def test_build_ticker_summary_handles_missing_identity_columns():
    """Summary cards should not crash on partial or malformed retained datasets."""
    frame = pd.DataFrame([{"underlying_price": 100.0}])

    summary = viewer.build_ticker_summary("TSLA", frame)

    assert summary["call_count"] == 0
    assert summary["put_count"] == 0
    assert summary["expiration_count"] == 0


def test_pick_profitable_opportunity_prefers_higher_final_score_when_rom_matches():
    """Summary highlights should use final score as a tie-breaker ahead of quote quality."""
    frame = pd.DataFrame(
        [
            {
                "contract_symbol": "TSLA260417C00100000",
                "option_type": "call",
                "strike": 100.0,
                "expiration_date": "2026-04-17",
                "probability_itm": 0.22,
                "risk_level": "LOW",
                "spread_score": 100.0,
                "dte_score": 100.0,
                "theta_efficiency": 10.0,
                "bid_ask_spread_pct_of_mid": 0.08,
                "return_on_margin_annualized": 1.5,
                "option_score": 90.0,
                "final_score": 80.0,
                "quote_quality_score": 7,
                "passes_primary_screen": True,
            },
            {
                "contract_symbol": "TSLA260417C00105000",
                "option_type": "call",
                "strike": 105.0,
                "expiration_date": "2026-04-17",
                "probability_itm": 0.24,
                "risk_level": "LOW",
                "spread_score": 85.0,
                "dte_score": 85.0,
                "theta_efficiency": 8.0,
                "bid_ask_spread_pct_of_mid": 0.09,
                "return_on_margin_annualized": 1.5,
                "option_score": 88.0,
                "final_score": 92.0,
                "quote_quality_score": 5,
                "passes_primary_screen": True,
            },
        ]
    )

    summary = viewer.pick_profitable_opportunity(frame)

    assert summary is not None
    assert summary["contract_symbol"] == "TSLA260417C00105000"
    assert summary["option_score"] == 88.0
    assert summary["final_score"] == 92.0


def test_viewer_numeric_coercion_drops_non_finite_values():
    """Viewer numeric helpers should not preserve inf values as sortable metrics."""
    numbers = viewer.coerce_number(pd.Series([1.25, "inf", float("-inf"), "nan", "bad"]))

    assert numbers.iloc[0] == 1.25
    assert numbers.iloc[1:].isna().all()
    assert viewer.coerce_scalar_number("inf") is None
    assert viewer.coerce_scalar_number(float("-inf")) is None
    assert viewer.coerce_scalar_number("nan") is None


def test_pick_profitable_opportunity_does_not_promote_inf_rom():
    """A corrupted infinite ROM should sort behind finite opportunity rows."""
    frame = pd.DataFrame(
        [
            {
                "contract_symbol": "INF_BAD",
                "option_type": "call",
                "strike": 100.0,
                "expiration_date": "2026-04-17",
                "probability_itm": 0.22,
                "risk_level": "LOW",
                "spread_score": 100.0,
                "dte_score": 100.0,
                "theta_efficiency": 10.0,
                "bid_ask_spread_pct_of_mid": 0.08,
                "return_on_margin_annualized": float("inf"),
                "option_score": 99.0,
                "final_score": 99.0,
                "quote_quality_score": 9,
                "passes_primary_screen": True,
            },
            {
                "contract_symbol": "FINITE_GOOD",
                "option_type": "call",
                "strike": 105.0,
                "expiration_date": "2026-04-17",
                "probability_itm": 0.24,
                "risk_level": "LOW",
                "spread_score": 85.0,
                "dte_score": 85.0,
                "theta_efficiency": 8.0,
                "bid_ask_spread_pct_of_mid": 0.09,
                "return_on_margin_annualized": 1.0,
                "option_score": 70.0,
                "final_score": 70.0,
                "quote_quality_score": 5,
                "passes_primary_screen": True,
            },
        ]
    )

    summary = viewer.pick_profitable_opportunity(frame)

    assert summary is not None
    assert summary["contract_symbol"] == "FINITE_GOOD"
    assert summary["return_on_margin_annualized_pct"] == 100.0


def test_sort_ticker_candidates_preserves_zero_rom_as_real_value():
    """A real zero ROM should rank above missing ROM instead of being treated as absent."""
    items = [
        {
            "ticker": "ZERO",
            "row_count": 1,
            "call_count": 1,
            "put_count": 0,
            "expiration_count": 1,
            "underlying_price": 100.0,
            "underlying_day_change_pct": None,
            "median_implied_volatility_pct": None,
            "historical_volatility_pct": None,
            "iv_hv_ratio": None,
            "next_earnings_date": None,
            "next_earnings_date_is_estimated": None,
            "event_risk_score": None,
            "latest_status": "Snapshot unavailable",
            "market_context": "",
            "profitable_opportunity": {"return_on_margin_annualized_pct": 0.0},
            "moderate_risk_opportunity": None,
            "high_conviction_call": None,
            "high_conviction_put": None,
        },
        {
            "ticker": "MISSING",
            "row_count": 1,
            "call_count": 1,
            "put_count": 0,
            "expiration_count": 1,
            "underlying_price": 100.0,
            "underlying_day_change_pct": None,
            "median_implied_volatility_pct": None,
            "historical_volatility_pct": None,
            "iv_hv_ratio": None,
            "next_earnings_date": None,
            "next_earnings_date_is_estimated": None,
            "event_risk_score": None,
            "latest_status": "Snapshot unavailable",
            "market_context": "",
            "profitable_opportunity": {"return_on_margin_annualized_pct": None},
            "moderate_risk_opportunity": None,
            "high_conviction_call": None,
            "high_conviction_put": None,
        },
    ]

    sorted_items = viewer.sort_ticker_candidates(items, "profitable_opportunity")

    assert [item["ticker"] for item in sorted_items] == ["ZERO", "MISSING"]


def test_pick_moderate_risk_opportunity_accepts_spread_at_config_cutoff(monkeypatch):
    """Moderate-risk selection should keep candidates whose spread equals the configured limit."""
    def make_config():
        return type("Config", (), {"max_spread_pct_of_mid": 0.25})()

    monkeypatch.setattr("opx_chain.viewer.get_runtime_config", make_config)
    frame = pd.DataFrame(
        [
            {
                "contract_symbol": "EDGE",
                "option_type": "put",
                "strike": 95.0,
                "expiration_date": "2026-04-17",
                "probability_itm": 0.30,
                "delta_abs": 0.35,
                "strike_distance_pct": 0.04,
                "bid_ask_spread_pct_of_mid": 0.25,
                "return_on_margin_annualized": 1.2,
                "option_score": 82.0,
                "final_score": 87.0,
                "quote_quality_score": 7,
                "passes_primary_screen": True,
            }
        ]
    )

    summary = viewer.pick_moderate_risk_opportunity(frame)

    assert summary is not None
    assert summary["contract_symbol"] == "EDGE"


def test_pick_high_conviction_call_prefers_bullish_aligned_liquid_candidate():
    """Call conviction should prefer cleaner bullish alignment over raw ROM alone."""
    frame = pd.DataFrame(
        [
            {
                "contract_symbol": "CALL_ROM",
                "option_type": "call",
                "strike": 110.0,
                "expiration_date": "2026-04-17",
                "underlying_day_change_pct": -0.03,
                "strike_distance_pct": 0.12,
                "delta_abs": 0.18,
                "spread_score": 70.0,
                "quote_quality_score": 4.0,
                "return_on_margin_annualized": 2.2,
                "option_score": 70.0,
                "final_score": 72.0,
                "passes_primary_screen": True,
            },
            {
                "contract_symbol": "CALL_CONVICTION",
                "option_type": "call",
                "strike": 102.0,
                "expiration_date": "2026-04-17",
                "underlying_day_change_pct": 0.025,
                "strike_distance_pct": 0.03,
                "delta_abs": 0.39,
                "spread_score": 92.0,
                "quote_quality_score": 8.0,
                "return_on_margin_annualized": 1.4,
                "option_score": 88.0,
                "final_score": 90.0,
                "passes_primary_screen": True,
            },
        ]
    )

    summary = viewer.pick_high_conviction_opportunity(frame, "call")

    assert summary is not None
    assert summary["contract_symbol"] == "CALL_CONVICTION"


def test_pick_high_conviction_put_prefers_bearish_aligned_candidate():
    """Put conviction should stay side-specific and prefer downside alignment."""
    frame = pd.DataFrame(
        [
            {
                "contract_symbol": "PUT_BULLISH",
                "option_type": "put",
                "strike": 95.0,
                "expiration_date": "2026-04-17",
                "underlying_day_change_pct": 0.03,
                "strike_distance_pct": 0.02,
                "delta_abs": 0.34,
                "spread_score": 95.0,
                "quote_quality_score": 8.0,
                "return_on_margin_annualized": 1.3,
                "option_score": 89.0,
                "final_score": 91.0,
                "passes_primary_screen": True,
            },
            {
                "contract_symbol": "PUT_CONVICTION",
                "option_type": "put",
                "strike": 98.0,
                "expiration_date": "2026-04-17",
                "underlying_day_change_pct": -0.025,
                "strike_distance_pct": 0.04,
                "delta_abs": 0.36,
                "spread_score": 90.0,
                "quote_quality_score": 7.0,
                "return_on_margin_annualized": 1.2,
                "option_score": 86.0,
                "final_score": 89.0,
                "passes_primary_screen": True,
            },
        ]
    )

    summary = viewer.pick_high_conviction_opportunity(frame, "put")

    assert summary is not None
    assert summary["contract_symbol"] == "PUT_CONVICTION"


def test_viewer_main_uses_runtime_config_host_and_port(monkeypatch):
    """Viewer startup should default to the resolved runtime config values."""
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "opx_chain.viewer.get_runtime_config",
        lambda: build_config("0.0.0.0", 9001),
    )
    monkeypatch.setattr("opx_chain.viewer.serve", captured.update)

    monkeypatch.delenv("OPX_VIEWER_HOST", raising=False)
    monkeypatch.delenv("OPX_VIEWER_PORT", raising=False)

    viewer.main()

    assert captured == {"host": "0.0.0.0", "port": 9001}


def test_viewer_main_env_overrides_runtime_config(monkeypatch):
    """Explicit viewer environment variables should override file config values."""
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "opx_chain.viewer.get_runtime_config",
        lambda: build_config("127.0.0.1", 8000),
    )
    monkeypatch.setattr("opx_chain.viewer.serve", captured.update)
    monkeypatch.setenv("OPX_VIEWER_HOST", "0.0.0.0")
    monkeypatch.setenv("OPX_VIEWER_PORT", "9100")

    viewer.main()

    assert captured == {"host": "0.0.0.0", "port": 9100}


def test_viewer_main_accepts_bracketed_ipv6_env_host(monkeypatch):
    """Bracketed IPv6 env hosts should be normalized for socket binding."""
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "opx_chain.viewer.get_runtime_config",
        lambda: build_config("127.0.0.1", 8000),
    )
    monkeypatch.setattr("opx_chain.viewer.serve", captured.update)
    monkeypatch.setenv("OPX_VIEWER_HOST", "[::1]")

    viewer.main()

    assert captured == {"host": "::1", "port": 8000}


@pytest.mark.parametrize(
    ("host", "expected_url"),
    [
        ("0.0.0.0", "http://127.0.0.1:8000"),
        ("::", "http://[::1]:8000"),
        ("::1", "http://[::1]:8000"),
        ("127.0.0.1", "http://127.0.0.1:8000"),
        ("localhost", "http://localhost:8000"),
    ],
)
def test_viewer_url_uses_browser_safe_display_host(host, expected_url):
    """User-facing viewer URLs should never use wildcard bind destinations."""
    assert viewer._viewer_url(host, 8000) == expected_url  # pylint: disable=protected-access


def test_viewer_serve_prints_display_url_without_changing_bind(monkeypatch, capsys):
    """The banner should show loopback for wildcard binds while binding wildcard."""
    captured: dict[str, object] = {}

    class FakeServer:  # pylint: disable=too-few-public-methods
        """Capture server construction and return immediately."""

        def __init__(self, server_address, request_handler):
            captured["server_address"] = server_address
            captured["request_handler"] = request_handler
            captured["closed"] = False

        def serve_forever(self):
            """Return immediately instead of starting a real server."""

        def server_close(self):
            """Record that serve() closes the server."""
            captured["closed"] = True

    monkeypatch.setattr("opx_chain.viewer.ThreadingHTTPServer", FakeServer)

    viewer.serve(host="0.0.0.0", port=8000)

    assert captured["server_address"] == ("0.0.0.0", 8000)
    assert captured["request_handler"] is viewer.ViewerRequestHandler
    assert captured["closed"] is True
    assert "http://127.0.0.1:8000" in capsys.readouterr().out


def test_open_viewer_in_browser_uses_display_url(monkeypatch):
    """The --open browser destination should map wildcard binds to loopback."""
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "opx_chain.viewer.webbrowser.open",
        lambda url, new: captured.update({"url": url, "new": new}),
    )

    viewer.open_viewer_in_browser("::", 8000)

    assert captured == {"url": "http://[::1]:8000", "new": 2}


def test_viewer_main_rejects_invalid_env_port(monkeypatch):
    """Invalid OPX_VIEWER_PORT values should fail with a clear message."""
    monkeypatch.setattr(
        "opx_chain.viewer.get_runtime_config",
        lambda: build_config("127.0.0.1", 8000),
    )
    monkeypatch.setenv("OPX_VIEWER_PORT", "abc")

    with pytest.raises(ValueError, match="Invalid OPX_VIEWER_PORT='abc'"):
        viewer.main()


@pytest.mark.parametrize("host", ["   ", "invalid host", "999.999.999.999", "localhost:8000"])
def test_viewer_main_rejects_invalid_env_host(monkeypatch, host: str):
    """Invalid OPX_VIEWER_HOST values should fail before socket bind."""
    monkeypatch.setattr(
        "opx_chain.viewer.get_runtime_config",
        lambda: build_config("127.0.0.1", 8000),
    )
    monkeypatch.setenv("OPX_VIEWER_HOST", host)

    with pytest.raises(ValueError, match="Invalid OPX_VIEWER_HOST="):
        viewer.main()


def test_viewer_main_rejects_invalid_config_host(monkeypatch):
    """Invalid configured viewer hosts should fail before socket bind."""
    monkeypatch.setattr(
        "opx_chain.viewer.get_runtime_config",
        lambda: build_config("invalid host", 8000),
    )
    monkeypatch.delenv("OPX_VIEWER_HOST", raising=False)

    with pytest.raises(ValueError, match="Invalid settings.viewer_host="):
        viewer.main()


def test_viewer_main_rejects_out_of_range_env_port(monkeypatch):
    """Out-of-range OPX_VIEWER_PORT values should fail before socket bind."""
    monkeypatch.setattr(
        "opx_chain.viewer.get_runtime_config",
        lambda: build_config("127.0.0.1", 8000),
    )
    monkeypatch.setenv("OPX_VIEWER_PORT", "70000")

    with pytest.raises(ValueError, match="Invalid OPX_VIEWER_PORT='70000'"):
        viewer.main()


def test_viewer_main_can_open_browser(monkeypatch):
    """The --open flag should launch the resolved viewer URL in a browser."""
    captured: dict[str, object] = {}

    class ImmediateTimer:  # pylint: disable=too-few-public-methods
        """Run the scheduled browser open immediately during tests."""

        def __init__(self, _delay, callback, args=None, kwargs=None):
            self._callback = callback
            self._args = args or ()
            self._kwargs = kwargs or {}

        def start(self):
            """Execute the scheduled callback immediately."""
            self._callback(*self._args, **self._kwargs)

    monkeypatch.setattr(
        "opx_chain.viewer.get_runtime_config",
        lambda: build_config("127.0.0.1", 8000),
    )
    monkeypatch.setattr(
        "opx_chain.viewer.serve", lambda **kwargs: captured.update({"serve": kwargs})
    )
    monkeypatch.setattr(
        "opx_chain.viewer.open_viewer_in_browser",
        lambda host, port: captured.update({"open": (host, port)}),
    )
    monkeypatch.setattr("opx_chain.viewer.threading.Timer", ImmediateTimer)

    viewer.main(["--open"])

    assert captured == {
        "open": ("127.0.0.1", 8000),
        "serve": {"host": "127.0.0.1", "port": 8000},
    }


def test_viewer_main_does_not_open_browser_without_flag(monkeypatch):
    """Browser launch should remain opt-in."""
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "opx_chain.viewer.get_runtime_config",
        lambda: build_config("127.0.0.1", 8000),
    )
    monkeypatch.setattr(
        "opx_chain.viewer.serve", lambda **kwargs: captured.update({"serve": kwargs})
    )
    monkeypatch.setattr(
        "opx_chain.viewer.open_viewer_in_browser",
        lambda host, port: captured.update({"open": (host, port)}),
    )

    viewer.main([])

    assert captured == {"serve": {"host": "127.0.0.1", "port": 8000}}


def test_viewer_main_resets_data_dir_override_between_runs(monkeypatch, tmp_path: Path):
    """A prior --data-dir run must not leak into later viewer invocations."""
    first_dir = tmp_path / "first"
    first_dir.mkdir()
    captured: list[Path | None] = []

    monkeypatch.setattr(
        "opx_chain.viewer.get_runtime_config",
        lambda: build_config("127.0.0.1", 8000),
    )

    def capture_serve(**_kwargs):
        captured.append(viewer._DATA_DIR_OVERRIDE)  # pylint: disable=protected-access

    monkeypatch.setattr("opx_chain.viewer.serve", capture_serve)

    viewer.main(["--data-dir", str(first_dir)])
    viewer.main([])

    assert captured == [first_dir.resolve(), None]
