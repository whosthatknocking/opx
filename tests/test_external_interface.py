"""Public external-interface contract checks."""
# pylint: disable=duplicate-code

from __future__ import annotations

from importlib import import_module
import inspect
from pathlib import Path

from opx_chain.fetcher import run_fetch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXTERNAL_INTERFACE_SPEC = PROJECT_ROOT / "docs" / "EXTERNAL_INTERFACE_SPEC.md"

PUBLIC_NAMES: dict[str, set[str]] = {
    "opx_chain.event_data": {
        "EVENT_DATA_FETCH_MODES",
        "EVENT_DATA_FRESHNESS_POLICY",
        "EVENT_DATA_PROVIDER_CHOICES",
        "EVENT_DATA_SCHEMA_VERSION",
        "EVENT_DATA_SUPPORTED_PROVIDERS",
        "clear_event_columns",
        "normalize_event_data_fetch_mode",
        "normalize_event_data_provider",
        "overlay_event_snapshot",
        "run_event_fetch",
        "summarize_latest_event_data",
    },
    "opx_chain.fetcher": {
        "TickerFetchProgress",
        "fetch_ticker_option_chain",
        "run_fetch",
    },
    "opx_chain.iv_history_backfill": {
        "IVHistoryRecoveryBusyError",
        "IVHistoryRecoveryResult",
        "recover_iv_history_store",
    },
    "opx_chain.option_types": {
        "OPTION_TYPE_CALL",
        "OPTION_TYPE_CALL_LABEL",
        "OPTION_TYPE_PUT",
        "OPTION_TYPE_PUT_LABEL",
        "OPTION_TYPES",
        "normalize_option_type",
        "option_type_label",
    },
    "opx_chain.paths": {
        "get_data_dir",
        "get_runs_dir",
    },
    "opx_chain.price_history": {
        "PRICE_HISTORY_INTEGRITY_ERROR",
        "PRICE_HISTORY_INTEGRITY_MISSING",
        "PRICE_HISTORY_INTEGRITY_OK",
        "PriceHistoryIntegrity",
        "check_price_history_integrity",
        "get_price_history_db_path",
        "get_price_history_store",
    },
    "opx_chain.volatility_features": {
        "DTE_BUCKETS",
        "MIN_IV_HISTORY_OBSERVATIONS",
        "SOURCE_ERROR",
        "SOURCE_INSUFFICIENT_HISTORY",
        "SOURCE_MISSING",
        "SOURCE_PARTIAL",
        "SOURCE_READY",
        "SOURCE_STALE",
        "VOLATILITY_FEATURE_METHOD",
        "VOLATILITY_FEATURE_SCHEMA_VERSION",
        "build_iv_features",
        "build_price_volatility_features",
        "build_ticker_volatility_features",
        "dte_bucket",
        "load_price_volatility_features",
    },
}


def test_public_external_interface_names_are_importable() -> None:
    """Documented downstream public names must exist at import time."""
    for module_name, names in sorted(PUBLIC_NAMES.items()):
        module = import_module(module_name)
        missing = sorted(name for name in names if not hasattr(module, name))
        assert missing == []


def test_external_interface_spec_lists_public_import_names() -> None:
    """The external-interface spec must list the documented public names."""
    text = EXTERNAL_INTERFACE_SPEC.read_text(encoding="utf-8")
    missing: list[str] = []
    for module_name, names in sorted(PUBLIC_NAMES.items()):
        if f"from {module_name} import" not in text:
            missing.append(module_name)
        missing.extend(sorted(name for name in names if name not in text))
    assert not missing


def test_run_fetch_public_signature_includes_progress_callback() -> None:
    """Downstream orchestrators require the structured progress callback."""
    parameter = inspect.signature(run_fetch).parameters["progress_callback"]

    assert parameter.default is None
    assert parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
