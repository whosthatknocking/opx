"""Documentation coverage tests for CLI contracts."""

import inspect
from pathlib import Path

from opx_chain.config import (
    DEFAULT_ENABLE_FILTERS,
    DEFAULT_MAX_SPREAD_PCT_OF_MID,
    DEFAULT_MAX_STRIKE_DISTANCE_PCT,
    DEFAULT_MIN_BID,
    DEFAULT_MIN_OPEN_INTEREST,
    DEFAULT_MIN_VOLUME,
)
from opx_chain.check_positions import find_latest_output
from opx_chain.fetcher import run_fetch


ROOT = Path(__file__).resolve().parents[1]


def test_dry_run_cli_flag_is_documented():
    """The zero-call fetch preflight flag should stay visible in user docs."""
    docs = {
        "README.md": ROOT / "README.md",
        "USER_GUIDE.md": ROOT / "docs" / "USER_GUIDE.md",
        "EXTERNAL_INTERFACE_SPEC.md": ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md",
    }

    for name, path in docs.items():
        assert "--dry-run" in path.read_text(encoding="utf-8"), name


def test_opx_check_latest_docstring_does_not_claim_symlink():
    """The latest CSV lookup docstring should match copy-based runtime behavior."""
    assert "latest copy" in inspect.getdoc(find_latest_output)
    assert "symlink" not in inspect.getdoc(find_latest_output).lower()


def test_project_spec_lists_builtin_filter_defaults():
    """The canonical defaults list should cover every shared filter knob."""
    spec = (ROOT / "docs" / "PROJECT_SPEC.md").read_text(encoding="utf-8")
    min_bid_value = "disabled" if DEFAULT_MIN_BID is None else str(DEFAULT_MIN_BID)
    enable_filters_value = str(DEFAULT_ENABLE_FILTERS).lower()

    expected_lines = (
        f"`filters_max_spread_pct_of_mid = {DEFAULT_MAX_SPREAD_PCT_OF_MID}`",
        f"`filters_max_strike_distance_pct = {DEFAULT_MAX_STRIKE_DISTANCE_PCT}`",
        f"`filters_min_bid = {min_bid_value}`",
        f"`filters_min_open_interest = {DEFAULT_MIN_OPEN_INTEREST}`",
        f"`filters_min_volume = {DEFAULT_MIN_VOLUME}`",
        f"`filters_enable = {enable_filters_value}`",
    )

    for line in expected_lines:
        assert line in spec


def test_min_bid_docs_describe_screen_not_export_filter():
    """filters_min_bid should be documented as a screen, not a row removal filter."""
    guide = (ROOT / "docs" / "USER_GUIDE.md").read_text(encoding="utf-8")
    project_spec = (ROOT / "docs" / "PROJECT_SPEC.md").read_text(encoding="utf-8")

    assert "it does not remove those rows from the exported dataset" in guide
    assert "rows are not removed solely by this threshold" in project_spec
    assert "exclude contracts below that premium threshold" not in guide


def test_recommended_dataset_reader_is_stable_public_surface():
    """The recommended artifact reader must not live outside the public API list."""
    spec = (ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md").read_text(encoding="utf-8")
    public_surface = spec.split("### 3.1 Public surface", maxsplit=1)[1]
    public_surface = public_surface.split("### 3.2", maxsplit=1)[0]
    reader_section = spec.split("### 3.7 Reading the chain artifact", maxsplit=1)[1]

    assert "from opx_chain.utils import read_dataset_file" in public_surface
    assert "ValidatedOptionChainDataset" in public_surface
    assert "load_validated_option_chain_dataset(handle.dataset_id)" in reader_section
    assert "`read_dataset_file` remains a stable raw inspection/export helper" in reader_section
    assert "only stable public import from `opx_chain.utils`" in public_surface


def test_positions_parser_is_stable_public_surface():
    """Downstream positions parsing should be covered by the public API contract."""
    spec = (ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md").read_text(encoding="utf-8")
    public_surface = spec.split("### 3.1 Public surface", maxsplit=1)[1]
    public_surface = public_surface.split("### 3.2", maxsplit=1)[0]
    positions_section = spec.split("### 3.8 Parsing positions consistently", maxsplit=1)[1]
    for name in (
        "OptionPositionKey",
        "PositionSet",
        "load_positions",
        "positions_fingerprint",
    ):
        assert name in public_surface
        assert name in positions_section
    assert "positions.option_keys" in positions_section
    assert "positions_fingerprint(Path" in positions_section


def test_domain_helpers_are_stable_public_surface():
    """Domain-owned helpers consumed downstream should be public contracts."""
    spec = (ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md").read_text(encoding="utf-8")
    public_surface = spec.split("### 3.1 Public surface", maxsplit=1)[1]
    public_surface = public_surface.split("### 3.2", maxsplit=1)[0]
    price_context_section = spec.split("### 5.4 `PRICE_CONTEXT_SCHEMA_VERSION`", maxsplit=1)[1]
    for name in (
        "BackupDependencyRecord",
        "BackupInventory",
        "build_backup_inventory",
        "PRICE_CONTEXT_RECORD_FIELDS",
        "PRICE_CONTEXT_SCHEMA_VERSION",
        "PriceContextStatus",
        "blank_price_context",
        "OPTION_TYPE_CALL",
        "OPTION_TYPE_PUT",
        "OPTION_TYPES",
        "normalize_option_type",
        "option_type_label",
        "get_runs_dir",
        "DEFAULT_PRICE_CONTEXT_MAX_AGE_DAYS",
        "US_MARKET_TIMEZONE",
    ):
        assert name in public_surface
    for status in ("FRESH", "STALE", "MISSING", "ERROR"):
        assert status in price_context_section
    assert "backup-inventory surface" in public_surface


def test_external_interface_notes_are_present_tense():
    """Implemented interface notes should not read like pending TODOs."""
    spec = (ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md").read_text(encoding="utf-8")
    section = spec.split("## 7. Implemented Interface Notes", maxsplit=1)[1]
    section = section.split("## 8.", maxsplit=1)[0]

    assert "## 7. Changes Required" not in spec
    assert "Required addition" not in section
    assert "must also implement" not in section
    assert "Add `get_run(" not in section
    assert "### 7.6" in section
    assert "### 7.7" in section
    assert "### 7.8" in section
    assert section.index("### 7.6") < section.index("### 7.7")
    assert section.index("### 7.7") < section.index("### 7.8")
    headings = [line for line in section.splitlines() if line.startswith("### ")]
    assert len(headings) == len(set(headings))


def test_project_spec_numeric_headings_are_unique():
    """Specification section numbers should not be duplicated."""
    spec = (ROOT / "docs" / "PROJECT_SPEC.md").read_text(encoding="utf-8")
    headings = [
        line
        for line in spec.splitlines()
        if line.startswith("### ") and line[4:7].replace(".", "").isdigit()
    ]

    heading_numbers = [heading.split(maxsplit=2)[1] for heading in headings]
    assert len(heading_numbers) == len(set(heading_numbers))


def test_run_fetch_public_params_are_documented():
    """The in-process fetch contract should document every public parameter."""
    spec = (ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md").read_text(encoding="utf-8")
    section = spec.split("### 3.2 Triggering a fresh fetch programmatically", maxsplit=1)[1]
    section = section.split("### 3.3", maxsplit=1)[0]

    for param in inspect.signature(run_fetch).parameters:
        assert f"**`{param}`" in section


def test_external_interface_documents_special_fetch_modes():
    """The external contract should match dry-run and price-context-only behavior."""
    spec = (ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md").read_text(encoding="utf-8")
    cli = spec.split("### 2.1 `opx-fetch`", maxsplit=1)[1]
    cli = cli.split("### 2.2", maxsplit=1)[0]
    run_fetch_heading = "### 3.2 Triggering a fresh fetch programmatically"
    run_fetch_section = spec.split(run_fetch_heading, maxsplit=1)[1]
    run_fetch_section = run_fetch_section.split("### 3.3", maxsplit=1)[0]
    normalized_run_fetch = " ".join(run_fetch_section.split())

    assert "--enable-price-context" in cli
    assert "--disable-price-context" in cli
    assert "--price-context-only" in cli
    assert "--enable-filters" not in cli
    assert "--disable-filters" not in cli
    assert "Dry runs do not acquire the fetcher lock" in normalized_run_fetch
    assert "dry_run=True` writes no result" in normalized_run_fetch
    assert (
        "price_context_only=True` writes only the standalone price-context artifact"
        in normalized_run_fetch
    )
    assert "storage dataset or run record" in normalized_run_fetch


def test_external_interface_documents_price_history_backfill_cli():
    """The external contract should document provider-scoped price-history backfill."""
    spec = (ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md").read_text(encoding="utf-8")
    section = spec.split("### 2.2 `opx-price-history-backfill`", maxsplit=1)[1]
    section = section.split("### 2.3", maxsplit=1)[0]

    assert "--providers" in section
    assert "--tickers" in section
    assert "--lookback-days" in section
    assert "--refresh" in section
    assert "--dry-run" in section
    assert "(provider, ticker, trading_date)" in section
    assert "does not write an option-chain dataset" in section


def test_external_interface_documents_iv_history_backfill_cli():
    """The external contract should document durable IV-history backfill."""
    spec = (ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md").read_text(encoding="utf-8")
    section = spec.split("### 2.3 `opx-iv-history-backfill`", maxsplit=1)[1]
    section = section.split("### 2.4", maxsplit=1)[0]

    assert "--providers" in section
    assert "--tickers" in section
    assert "--lookback-days" in section
    assert "--limit" in section
    assert "--dataset-id" in section
    assert "--refresh" in section
    assert "--recover-corrupt" in section
    assert "--dry-run" in section
    assert "(provider, ticker, observation_date, option_type, dte_bucket, delta_bucket)" in section
    assert "does not call provider APIs" in section
    assert "provider requests: 0" in section
    assert "iv-history.corrupt-<UTC timestamp>.db" in section


def test_agents_architecture_map_lists_load_bearing_modules():
    """Agent guidance should keep the architecture map aligned with core modules."""
    agents_doc = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    required_entries = (
        "`opx-check`",
        "`opx-price-history-backfill`",
        "`opx-iv-history-backfill`",
        "`opx_chain/check_positions.py`",
        "`opx_chain/paths.py`",
        "`opx_chain/positions.py`",
        "`opx_chain/price_history.py`",
        "`opx_chain/price_history_backfill.py`",
        "`opx_chain/iv_history.py`",
        "`opx_chain/iv_history_backfill.py`",
        "`opx_chain/volatility_features.py`",
        "`opx_chain/runlog.py`",
        "`opx_chain/schema.py`",
        "`opx_chain/storage/`",
        "`opx_chain/utils.py`",
        "`opx_chain/version.py`",
    )

    for entry in required_entries:
        assert entry in agents_doc


def test_canonical_doc_indexes_list_source_of_truth_docs():
    """Canonical doc indexes should surface source-of-truth docs."""
    agents_doc = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    source_of_truth = agents_doc.split("## Source of Truth", maxsplit=1)[1]
    source_of_truth = source_of_truth.split("## Architecture Map", maxsplit=1)[0]

    project_spec = (ROOT / "docs" / "PROJECT_SPEC.md").read_text(encoding="utf-8")
    doc_layout = project_spec.split("### 8.1 Documentation Layout", maxsplit=1)[1]
    doc_layout = doc_layout.split("### 8.2", maxsplit=1)[0]

    required_docs = (
        "AGENTS.md",
        "docs/STORAGE_SPEC.md",
        "docs/METADATA_SPEC.md",
    )
    for doc_path in required_docs:
        assert f"`{doc_path}`" in source_of_truth
        assert f"`{doc_path}`" in doc_layout


def test_provider_cache_default_path_is_documented_precisely():
    """User-facing docs should point to the actual default filesystem cache path."""
    expected_path = "$XDG_CACHE_HOME/opx-chain/cache/"
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    development = (ROOT / "docs" / "DEVELOPMENT.md").read_text(encoding="utf-8")
    agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")

    assert expected_path in readme
    assert expected_path in development
    assert expected_path in agents


def test_development_structure_lists_load_bearing_docs_and_modules():
    """Development onboarding should point to current source-of-truth surfaces."""
    development = (ROOT / "docs" / "DEVELOPMENT.md").read_text(encoding="utf-8")
    required_entries = (
        "docs/EXTERNAL_INTERFACE_SPEC.md",
        "docs/STORAGE_SPEC.md",
        "docs/METADATA_SPEC.md",
        "opx_chain/positions.py",
        "opx_chain/price_context.py",
        "opx_chain/price_history.py",
        "opx_chain/iv_history.py",
        "opx_chain/volatility_features.py",
        "opx_chain/storage/",
        "scripts/run_local_quality_checks.sh",
    )

    for entry in required_entries:
        assert f"`{entry}`" in development

    assert "file-level" in development
    assert "inventories drift quickly" in development


def test_user_guide_shared_settings_use_lowercase_toml_keys():
    """Shared settings examples should be copyable into TOML config files."""
    guide = (ROOT / "docs" / "USER_GUIDE.md").read_text(encoding="utf-8")
    shared_settings = guide.split("### Shared Settings", maxsplit=1)[1]
    shared_settings = shared_settings.split("### Provider Settings", maxsplit=1)[0]
    uppercase_keys = (
        "TICKERS",
        "FILTERS_MIN_BID",
        "FILTERS_MIN_OPEN_INTEREST",
        "FILTERS_MIN_VOLUME",
        "FILTERS_MAX_SPREAD_PCT_OF_MID",
        "FILTERS_MAX_STRIKE_DISTANCE_PCT",
        "RISK_FREE_RATE",
        "HV_LOOKBACK_DAYS",
        "TRADING_DAYS_PER_YEAR",
        "STALE_QUOTE_SECONDS",
        "MAX_EXPIRATION_WEEKS",
        "VIEWER_HOST",
        "VIEWER_PORT",
        "OPTION_SCORE_INCOME_WEIGHT",
        "OPTION_SCORE_LIQUIDITY_WEIGHT",
        "OPTION_SCORE_RISK_WEIGHT",
        "OPTION_SCORE_EFFICIENCY_WEIGHT",
        "FILTERS_ENABLE",
        "ENABLE_VALIDATION",
        "DEBUG_DUMP_PROVIDER_PAYLOAD",
        "DEBUG_DUMP_DIR",
    )

    for key in uppercase_keys:
        assert f"`{key}" not in shared_settings

    expected_tickers = (
        '`tickers = ["TSLA", "NVDA", "UBER", "MSFT", "GOOGL", "ORCL", "PLTR"]`'
    )
    assert expected_tickers in shared_settings
    assert "`filters_min_open_interest = 100`" in shared_settings
    assert "`enable_validation = true`" in shared_settings


def test_user_guide_documents_valid_max_expiration_disable_value():
    """The max-expiration disable guidance should be valid TOML."""
    guide = (ROOT / "docs" / "USER_GUIDE.md").read_text(encoding="utf-8")

    assert "set it to `0` to disable the expiration cap entirely" in guide
    assert "set it to `0` to disable the max-expiration cutoff entirely" in guide
    assert "set to `null`" not in guide
    assert "TOML has no null literal" in guide


def test_storage_dataset_format_default_is_documented_consistently():
    """User-facing docs and examples should agree that CSV is the default."""
    docs = (
        ROOT / "docs" / "USER_GUIDE.md",
        ROOT / "opx_chain" / "docs" / "USER_GUIDE.md",
        ROOT / "config" / "example.toml",
    )

    for path in docs:
        text = path.read_text(encoding="utf-8")
        assert 'dataset_format = "csv"' in text, path.name
        assert 'dataset_format = "parquet" (default)' not in text, path.name


def test_user_guide_storage_format_examples_match_dataset_contract():
    """High-visibility guide examples should not imply parquet or CSV-only behavior."""
    guides = (
        ROOT / "docs" / "USER_GUIDE.md",
        ROOT / "opx_chain" / "docs" / "USER_GUIDE.md",
    )

    for path in guides:
        text = path.read_text(encoding="utf-8")
        quick_check = text.split("Check that every option position", 1)[1]
        quick_check = quick_check.split("Use `--freshness`", 1)[0]
        assert "latest output dataset" in quick_check, path
        assert "output/<uuid>.csv" in quick_check, path
        assert "output/<uuid>.parquet" not in quick_check, path
        assert "--output /path/to/artifact.csv" in quick_check, path
        assert "file selector for available dataset exports" in text, path
        assert "file selector for available CSV exports" not in text, path
        assert "sortable table for the selected dataset artifact" in text, path
        assert "sortable table for the exported CSV" not in text, path


def test_project_docs_use_dataset_neutral_viewer_language():
    """Viewer/check-position docs should not describe dataset artifacts as CSV-only."""
    project_spec = (ROOT / "docs" / "PROJECT_SPEC.md").read_text(encoding="utf-8")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    check_positions_source = (ROOT / "opx_chain" / "check_positions.py").read_text(
        encoding="utf-8"
    )

    assert "local viewer for exported dataset artifacts" in project_spec
    assert "selected dataset rows" in project_spec
    assert "local viewer for exported CSV files" not in project_spec
    assert "exported CSV rows" not in project_spec
    assert "latest output dataset" in readme
    assert "latest output CSV" not in readme
    assert "latest output dataset" in check_positions_source
    assert "No output dataset found" in check_positions_source
