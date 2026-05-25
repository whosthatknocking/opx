"""Config-loader and provider-selection tests for Milestone 1."""
# pylint: disable=too-many-lines

import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 compatibility
    import tomli as tomllib

from opx_chain.config import (
    ConfigError,
    _coerce_bool,
    describe_runtime_config,
    get_runtime_config,
    load_runtime_config,
    reset_runtime_config,
)
from opx_chain.paths import (
    get_default_debug_dump_dir,
    get_default_provider_cache_dir,
    get_data_dir,
)
from opx_chain.providers import (
    PROVIDER_FACTORIES,
    get_data_provider,
)
from opx_chain.providers.massive import MassiveProvider
from opx_chain.providers.yfinance import YFinanceProvider


def test_load_runtime_config_uses_defaults_when_file_is_absent(tmp_path: Path):
    """Default config should keep yfinance usable without a user config file."""
    config = load_runtime_config(tmp_path / "missing.toml")

    assert config.data_provider == "yfinance"
    assert config.massive_api_key is None
    assert config.marketdata_api_token is None
    assert config.marketdata_max_retries == 3
    assert config.marketdata_request_interval_seconds == 0.0
    assert config.marketdata_backoff_seconds == 1.0
    assert config.yfinance_request_interval_seconds == 0.0
    assert config.yfinance_max_retries == 0
    assert config.yfinance_backoff_seconds == 1.0
    assert config.stale_quote_seconds == 10800
    assert config.enable_validation is True
    assert config.price_context_enable is False
    assert config.price_context_lookback_days == 260
    assert config.price_context_max_age_days == 7
    assert config.option_score_income_weight == 0.30
    assert config.option_score_liquidity_weight == 0.30
    assert config.option_score_risk_weight == 0.25
    assert config.option_score_efficiency_weight == 0.15
    assert config.massive_snapshot_page_limit == 250
    assert config.massive_request_interval_seconds == 12.0
    assert config.massive_max_retries == 3
    assert config.massive_backoff_seconds == 1.0
    assert config.debug_dump_provider_payload is False
    assert config.debug_dump_dir == get_default_debug_dump_dir()
    assert config.viewer_host == "127.0.0.1"
    assert config.viewer_port == 8000
    assert config.auto_fallback_to_yfinance is False
    assert config.enable_filters is True
    assert config.max_spread_pct_of_mid == 0.25
    assert config.max_expiration_weeks == 34
    assert config.max_expiration is not None
    assert config.tickers
    assert config.config_path == tmp_path / "missing.toml"


def test_example_config_is_valid_toml():
    """The tracked example should be directly copyable as a user config."""
    config_path = Path(__file__).resolve().parents[1] / "config" / "example.toml"

    config = load_runtime_config(config_path)

    assert config.config_path == config_path
    assert config.min_bid is None
    assert not any("could not be parsed" in warning for warning in config.config_warnings)


def test_example_config_does_not_force_marketdata_mode():
    """The copyable example should let Market Data use account defaults unless edited."""
    config_path = Path(__file__).resolve().parents[1] / "config" / "example.toml"

    raw_config = tomllib.loads(config_path.read_text(encoding="utf-8"))

    assert "mode" not in raw_config["providers"]["marketdata"]


def test_load_runtime_config_uses_eastern_market_calendar_for_today(tmp_path: Path, monkeypatch):
    """Runtime today should follow the U.S. market calendar instead of host-local midnight."""

    class FixedDatetime(datetime):
        """Minimal datetime stub that pins now() to a single instant."""

        @classmethod
        def now(cls, tz=None):
            instant = datetime(2026, 4, 19, 4, 30, tzinfo=timezone.utc)
            return instant if tz is None else instant.astimezone(tz)

    monkeypatch.setattr("opx_chain.config.datetime", FixedDatetime)

    config = load_runtime_config(tmp_path / "missing.toml")

    assert config.today == date(2026, 4, 19)


def test_get_runtime_config_refreshes_after_market_day_boundary(monkeypatch):
    """Long-running processes should not keep yesterday's cached config forever."""

    class AdvancingDatetime(datetime):
        """Datetime stub that advances across a U.S. Eastern date boundary."""

        calls = iter(
            [
                datetime(2026, 4, 20, 3, 55, tzinfo=timezone.utc),
                datetime(2026, 4, 20, 4, 5, tzinfo=timezone.utc),
            ]
        )

        @classmethod
        def now(cls, tz=None):
            instant = next(cls.calls)
            return instant if tz is None else instant.astimezone(tz)

    monkeypatch.setattr("opx_chain.config.datetime", AdvancingDatetime)

    first = get_runtime_config()
    second = get_runtime_config()

    assert first.today == date(2026, 4, 19)
    assert second.today == date(2026, 4, 20)
    assert first is not second


def test_config_bool_coercion_uses_shared_bool_policy():
    """Config fields should share the same truthy/falsy policy as dataset reads."""
    assert _coerce_bool(None, field_name="settings.enable_validation") is None
    assert _coerce_bool("on", field_name="settings.enable_validation") is True
    assert _coerce_bool("off", field_name="settings.enable_validation") is False
    assert _coerce_bool(1, field_name="settings.enable_validation") is True
    assert _coerce_bool(0, field_name="settings.enable_validation") is False

    with pytest.raises(ConfigError, match="settings.enable_validation"):
        _coerce_bool("garbage", field_name="settings.enable_validation")


def test_load_runtime_config_reads_user_config_file(tmp_path: Path):
    """Runtime settings should load from the XDG config file format."""
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[settings]
tickers = ["spy", "qqq"]
data_provider = "yfinance"
filters_min_bid = 1.25
option_score_income_weight = 0.40
option_score_liquidity_weight = 0.20
option_score_risk_weight = 0.25
option_score_efficiency_weight = 0.15
filters_enable = false
enable_validation = false
debug_dump_provider_payload = true
debug_dump_dir = "logs/provider_payloads"
viewer_host = "0.0.0.0"
viewer_port = 9001
max_expiration_weeks = 8

[providers.massive]
api_key = "secret"
snapshot_page_limit = 250
request_interval_seconds = 1.5
max_retries = 4
backoff_seconds = 2.5

[providers.marketdata]
api_token = "market-token"
mode = "delayed"
max_retries = 5
request_interval_seconds = 0.75
backoff_seconds = 0.25

[providers.yfinance]
request_interval_seconds = 0.25
max_retries = 2
backoff_seconds = 0.5

[price_context]
enable = true
lookback_days = 300
max_age_days = 5
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.tickers == ("SPY", "QQQ")
    assert config.data_provider == "yfinance"
    assert config.min_bid == 1.25
    assert config.option_score_income_weight == 0.40
    assert config.option_score_liquidity_weight == 0.20
    assert config.option_score_risk_weight == 0.25
    assert config.option_score_efficiency_weight == 0.15
    assert config.enable_filters is False
    assert config.enable_validation is False
    assert config.price_context_enable is True
    assert config.price_context_lookback_days == 300
    assert config.price_context_max_age_days == 5
    assert config.debug_dump_provider_payload is True
    assert (
        config.debug_dump_dir
        == get_default_debug_dump_dir().parent / "logs" / "provider_payloads"
    )
    assert config.viewer_host == "0.0.0.0"
    assert config.viewer_port == 9001
    assert config.max_expiration_weeks == 8
    assert config.max_expiration is not None
    assert config.massive_api_key == "secret"
    assert config.marketdata_api_token == "market-token"
    assert config.marketdata_mode == "delayed"
    assert config.marketdata_max_retries == 5
    assert config.marketdata_request_interval_seconds == 0.75
    assert config.marketdata_backoff_seconds == 0.25
    assert config.yfinance_request_interval_seconds == 0.25
    assert config.yfinance_max_retries == 2
    assert config.yfinance_backoff_seconds == 0.5
    assert config.massive_snapshot_page_limit == 250
    assert config.massive_request_interval_seconds == 1.5
    assert config.massive_max_retries == 4
    assert config.massive_backoff_seconds == 2.5
    assert not any("providers.massive" in warning for warning in config.config_warnings)
    assert not any("providers.marketdata" in warning for warning in config.config_warnings)


@pytest.mark.parametrize(
    "ticker",
    [
        "...",
        "bad/ticker",
        "123ABC",
        "TOO-LONG-TICKER",
        ".BAD",
        "BAD.",
    ],
)
def test_load_runtime_config_defaults_invalid_ticker_symbols(
    tmp_path: Path,
    ticker: str,
):
    """Malformed configured tickers should not enter the fetch universe."""
    config_path = tmp_path / "invalid-tickers.toml"
    config_path.write_text(
        f"""
[settings]
tickers = ["TSLA", "{ticker}"]
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.tickers == ("TSLA", "NVDA", "UBER", "MSFT", "GOOGL", "ORCL", "PLTR")
    warning = next(
        warning for warning in config.config_warnings if "settings.tickers" in warning
    )
    assert "must contain valid ticker symbols" in warning


def test_load_runtime_config_defaults_negative_stale_quote_seconds(tmp_path: Path):
    """Negative quote-staleness thresholds should fall back to the documented default."""
    config_path = tmp_path / "negative-stale.toml"
    config_path.write_text(
        """
[settings]
stale_quote_seconds = -1
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.stale_quote_seconds == 10800
    warning = next(
        warning
        for warning in config.config_warnings
        if "settings.stale_quote_seconds" in warning
    )
    assert "must be >= 0" in warning


def test_load_runtime_config_requires_massive_key_only_when_selected(tmp_path: Path):
    """Missing Massive credentials should fail unless fallback is explicit."""
    yfinance_config = tmp_path / "yfinance.toml"
    yfinance_config.write_text("[settings]\ndata_provider = 'yfinance'\n", encoding="utf-8")
    assert load_runtime_config(yfinance_config).data_provider == "yfinance"

    massive_config = tmp_path / "massive.toml"
    massive_config.write_text("[settings]\ndata_provider = 'massive'\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="providers.massive.api_key"):
        load_runtime_config(massive_config)

    massive_config.write_text(
        "[settings]\ndata_provider = 'massive'\nauto_fallback_to_yfinance = true\n",
        encoding="utf-8",
    )
    config = load_runtime_config(massive_config)
    assert config.data_provider == "yfinance"
    assert config.auto_fallback_to_yfinance is True
    assert any("falling back to 'yfinance'" in warning for warning in config.config_warnings)


def test_load_runtime_config_requires_marketdata_token_only_when_selected(tmp_path: Path):
    """Missing Market Data credentials should fail unless fallback is explicit."""
    marketdata_config = tmp_path / "marketdata.toml"
    marketdata_config.write_text(
        "[settings]\ndata_provider = 'marketdata'\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="providers.marketdata.api_token"):
        load_runtime_config(marketdata_config)

    marketdata_config.write_text(
        "[settings]\ndata_provider = 'marketdata'\nauto_fallback_to_yfinance = true\n",
        encoding="utf-8",
    )
    config = load_runtime_config(marketdata_config)
    assert config.data_provider == "yfinance"
    assert config.auto_fallback_to_yfinance is True
    assert any("providers.marketdata.api_token" in warning for warning in config.config_warnings)


def test_load_runtime_config_defaults_invalid_marketdata_mode(tmp_path: Path):
    """Invalid Market Data mode values should fall back to the default."""
    config_path = tmp_path / "marketdata-mode.toml"
    config_path.write_text(
        """
[settings]
data_provider = "marketdata"

[providers.marketdata]
api_token = "market-token"
mode = "fast"
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)
    assert config.marketdata_mode is None
    assert any("providers.marketdata.mode" in warning for warning in config.config_warnings)


def test_load_runtime_config_ignores_inactive_provider_fallback_warnings(tmp_path: Path):
    """Inactive provider sections should not emit fallback warnings."""
    config_path = tmp_path / "inactive-provider.toml"
    config_path.write_text(
        """
[settings]
data_provider = "yfinance"

[providers.massive]
api_key = 42
snapshot_page_limit = 1000

[providers.marketdata]
api_token = 42
mode = "fast"
max_retries = -1
request_interval_seconds = -0.5
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.data_provider == "yfinance"
    assert config.massive_api_key is None
    assert config.marketdata_api_token is None
    assert config.marketdata_mode is None
    assert config.marketdata_max_retries == 3
    assert config.marketdata_request_interval_seconds == 0.0
    assert config.marketdata_backoff_seconds == 1.0
    assert config.massive_snapshot_page_limit == 250
    assert not any("providers.massive" in warning for warning in config.config_warnings)
    assert not any("providers.marketdata" in warning for warning in config.config_warnings)


def test_load_runtime_config_defaults_invalid_marketdata_tuning(tmp_path: Path):
    """Invalid Market Data rate-limit settings should fall back to defaults."""
    negative_retries = tmp_path / "marketdata-retries.toml"
    negative_retries.write_text(
        """
[settings]
data_provider = "marketdata"

[providers.marketdata]
api_token = "market-token"
max_retries = -1
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(negative_retries)
    assert config.marketdata_max_retries == 3
    assert any("providers.marketdata.max_retries" in warning for warning in config.config_warnings)

    negative_interval = tmp_path / "marketdata-interval.toml"
    negative_interval.write_text(
        """
[settings]
data_provider = "marketdata"

[providers.marketdata]
api_token = "market-token"
request_interval_seconds = -0.5
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(negative_interval)
    assert config.marketdata_request_interval_seconds == 0.0
    assert any(
        "providers.marketdata.request_interval_seconds" in warning
        for warning in config.config_warnings
    )

    negative_backoff = tmp_path / "marketdata-backoff.toml"
    negative_backoff.write_text(
        """
[settings]
data_provider = "marketdata"

[providers.marketdata]
api_token = "market-token"
backoff_seconds = -0.5
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(negative_backoff)
    assert config.marketdata_backoff_seconds == 1.0
    assert any(
        "providers.marketdata.backoff_seconds" in warning
        for warning in config.config_warnings
    )


def test_load_runtime_config_defaults_invalid_yfinance_tuning(tmp_path: Path):
    """Invalid yfinance pacing settings should fall back to defaults."""
    config_path = tmp_path / "yfinance-invalid.toml"
    config_path.write_text(
        """
[settings]
data_provider = "yfinance"

[providers.yfinance]
request_interval_seconds = -0.5
max_retries = -1
backoff_seconds = -2
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.yfinance_request_interval_seconds == 0.0
    assert config.yfinance_max_retries == 0
    assert config.yfinance_backoff_seconds == 1.0
    assert any(
        "providers.yfinance.request_interval_seconds" in warning
        for warning in config.config_warnings
    )
    assert any("providers.yfinance.max_retries" in warning for warning in config.config_warnings)
    assert any(
        "providers.yfinance.backoff_seconds" in warning
        for warning in config.config_warnings
    )


def test_load_runtime_config_defaults_invalid_option_score_weights(tmp_path: Path):
    """Invalid option-score weights should fall back to defaults."""
    negative_weight = tmp_path / "negative-score-weight.toml"
    negative_weight.write_text(
        """
[settings]
option_score_income_weight = -1
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(negative_weight)
    assert config.option_score_income_weight == 0.30
    assert any("option_score_income_weight" in warning for warning in config.config_warnings)

    zero_total = tmp_path / "zero-total-score-weights.toml"
    zero_total.write_text(
        """
[settings]
option_score_income_weight = 0
option_score_liquidity_weight = 0
option_score_risk_weight = 0
option_score_efficiency_weight = 0
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(zero_total)
    assert config.option_score_income_weight == 0.30
    assert config.option_score_liquidity_weight == 0.30
    assert config.option_score_risk_weight == 0.25
    assert config.option_score_efficiency_weight == 0.15
    assert any("option_score_*_weight" in warning for warning in config.config_warnings)


@pytest.mark.parametrize(
    ("setting", "raw_value", "attribute", "default_value"),
    [
        ("filters_min_bid", "0", "min_bid", None),
        ("filters_min_open_interest", "-1", "min_open_interest", 100),
        ("filters_min_volume", "-1", "min_volume", 10),
        ("filters_max_spread_pct_of_mid", "0", "max_spread_pct_of_mid", 0.25),
        ("risk_free_rate", "-0.01", "risk_free_rate", 0.045),
        ("hv_lookback_days", "0", "hv_lookback_days", 30),
        ("trading_days_per_year", "0", "trading_days_per_year", 252),
        ("filters_max_strike_distance_pct", "0", "max_strike_distance_pct", 0.35),
    ],
)
def test_load_runtime_config_defaults_invalid_numeric_settings(
    tmp_path: Path,
    setting: str,
    raw_value: str,
    attribute: str,
    default_value,
):
    """Invalid numeric settings should warn and fall back to safe defaults."""
    config_path = tmp_path / f"{setting}.toml"
    config_path.write_text(
        f"""
[settings]
{setting} = {raw_value}
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert getattr(config, attribute) == default_value
    assert any(setting in warning for warning in config.config_warnings)


def test_invalid_numeric_warning_includes_rejected_value_and_constraint(tmp_path: Path):
    """Validation warnings should identify the rejected value and constraint."""
    config_path = tmp_path / "invalid-volume.toml"
    config_path.write_text(
        """
[settings]
filters_min_volume = -5
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    warning = next(
        warning
        for warning in config.config_warnings
        if "settings.filters_min_volume" in warning
    )
    assert "rejected value -5" in warning
    assert "must be >= 0" in warning
    assert "using default 10" in warning


def test_type_error_warning_includes_rejected_value_and_coercion_message(tmp_path: Path):
    """Coercion warnings should preserve the invalid input and parser reason."""
    config_path = tmp_path / "invalid-volume-type.toml"
    config_path.write_text(
        """
[settings]
filters_min_volume = "many"
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    warning = next(
        warning
        for warning in config.config_warnings
        if "settings.filters_min_volume" in warning
    )
    assert "rejected value 'many'" in warning
    assert "must be an integer" in warning
    assert "using default 10" in warning


def test_load_runtime_config_defaults_non_finite_float_settings(tmp_path: Path):
    """TOML inf/nan float literals should warn and fall back to safe defaults."""
    config_path = tmp_path / "non-finite-floats.toml"
    config_path.write_text(
        """
[settings]
data_provider = "marketdata"
filters_max_spread_pct_of_mid = inf
risk_free_rate = nan

[providers.marketdata]
api_token = "market-token"
request_interval_seconds = inf
backoff_seconds = nan
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.max_spread_pct_of_mid == 0.25
    assert config.risk_free_rate == 0.045
    assert config.marketdata_request_interval_seconds == 0.0
    assert config.marketdata_backoff_seconds == 1.0
    for field_name in (
        "settings.filters_max_spread_pct_of_mid",
        "settings.risk_free_rate",
        "providers.marketdata.request_interval_seconds",
        "providers.marketdata.backoff_seconds",
    ):
        warning = next(
            warning for warning in config.config_warnings if field_name in warning
        )
        assert "must be finite" in warning


def test_get_data_provider_returns_provider_from_runtime_config(monkeypatch, tmp_path: Path):
    """Provider factory should resolve yfinance and massive from config."""
    yfinance_config = tmp_path / "yfinance.toml"
    yfinance_config.write_text("[settings]\ndata_provider = 'yfinance'\n", encoding="utf-8")
    monkeypatch.setattr("opx_chain.config.DEFAULT_CONFIG_PATH_OVERRIDE", yfinance_config)
    assert isinstance(get_data_provider(), YFinanceProvider)

    massive_config = tmp_path / "massive.toml"
    massive_config.write_text(
        "[settings]\ndata_provider = 'massive'\n\n[providers.massive]\napi_key = 'secret'\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("opx_chain.config.DEFAULT_CONFIG_PATH_OVERRIDE", massive_config)

    reset_runtime_config()
    assert isinstance(get_data_provider(), MassiveProvider)


def test_load_runtime_config_defaults_unsupported_provider(tmp_path: Path):
    """Unsupported provider names should fall back to the default provider."""
    config_path = tmp_path / "bad.toml"
    config_path.write_text("[settings]\ndata_provider = 'invalid'\n", encoding="utf-8")

    config = load_runtime_config(config_path)
    assert config.data_provider == "yfinance"
    assert any("settings.data_provider" in warning for warning in config.config_warnings)


def test_load_runtime_config_defaults_invalid_massive_tuning(tmp_path: Path):
    """Invalid Massive request spacing and page-size settings should use defaults."""
    zero_limit = tmp_path / "zero-limit.toml"
    zero_limit.write_text(
        """
[settings]
data_provider = "massive"

[providers.massive]
api_key = "secret"
snapshot_page_limit = 0
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(zero_limit)
    assert config.massive_snapshot_page_limit == 250
    assert any("snapshot_page_limit" in warning for warning in config.config_warnings)

    too_large_limit = tmp_path / "too-large-limit.toml"
    too_large_limit.write_text(
        """
[settings]
data_provider = "massive"

[providers.massive]
api_key = "secret"
snapshot_page_limit = 1000
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(too_large_limit)
    assert config.massive_snapshot_page_limit == 250
    assert any("clamped to 250" in warning for warning in config.config_warnings)

    negative_interval = tmp_path / "negative-interval.toml"
    negative_interval.write_text(
        """
[settings]
data_provider = "massive"

[providers.massive]
api_key = "secret"
request_interval_seconds = -1
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(negative_interval)
    assert config.massive_request_interval_seconds == 12.0
    assert any("request_interval_seconds" in warning for warning in config.config_warnings)

    negative_retries = tmp_path / "negative-retries.toml"
    negative_retries.write_text(
        """
[settings]
data_provider = "massive"

[providers.massive]
api_key = "secret"
max_retries = -1
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(negative_retries)
    assert config.massive_max_retries == 3
    assert any("max_retries" in warning for warning in config.config_warnings)

    negative_backoff = tmp_path / "negative-backoff.toml"
    negative_backoff.write_text(
        """
[settings]
data_provider = "massive"

[providers.massive]
api_key = "secret"
backoff_seconds = -0.5
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(negative_backoff)
    assert config.massive_backoff_seconds == 1.0
    assert any("backoff_seconds" in warning for warning in config.config_warnings)

    bad_debug_toggle = tmp_path / "bad-debug-toggle.toml"
    bad_debug_toggle.write_text(
        """
[settings]
data_provider = "massive"
debug_dump_provider_payload = "maybe"

[providers.massive]
api_key = "secret"
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(bad_debug_toggle)
    assert config.debug_dump_provider_payload is False
    assert any("debug_dump_provider_payload" in warning for warning in config.config_warnings)

    bad_debug_dir = tmp_path / "bad-debug-dir.toml"
    bad_debug_dir.write_text(
        """
[settings]
debug_dump_dir = 42
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(bad_debug_dir)
    assert config.debug_dump_dir == get_default_debug_dump_dir()
    assert any("debug_dump_dir" in warning for warning in config.config_warnings)


def test_load_runtime_config_resolves_relative_cache_dir_under_xdg_cache_home(tmp_path: Path):
    """Relative cache_dir values should resolve under the app cache directory."""
    config_path = tmp_path / "cache-dir.toml"
    config_path.write_text(
        """
[storage]
cache_backend = "filesystem"
cache_dir = "provider-cache"
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.provider_cache_dir == get_default_provider_cache_dir().parent / "provider-cache"


def test_load_runtime_config_resolves_relative_storage_dir_under_xdg_data_home(tmp_path: Path):
    """Relative storage.dir values should resolve under the app data directory."""
    config_path = tmp_path / "storage-dir.toml"
    config_path.write_text(
        """
[storage]
dir = "custom-data"
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.storage_dir == get_data_dir() / "custom-data"


def test_load_runtime_config_preserves_absolute_storage_dir(tmp_path: Path):
    """Absolute storage.dir values should remain unchanged."""
    storage_dir = tmp_path / "absolute-storage"
    config_path = tmp_path / "absolute-storage-dir.toml"
    config_path.write_text(
        f"""
[storage]
dir = "{storage_dir}"
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.storage_dir == storage_dir


def test_load_runtime_config_defaults_invalid_filter_toggle(tmp_path: Path):
    """Invalid filter-toggle values should fall back to the default."""
    config_path = tmp_path / "bad-filter-toggle.toml"
    config_path.write_text(
        """
[settings]
filters_enable = "sometimes"
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.enable_filters is True
    assert any("filters_enable" in warning for warning in config.config_warnings)


def test_load_runtime_config_defaults_invalid_validation_toggle(tmp_path: Path):
    """Invalid validation-toggle values should fall back to the default."""
    config_path = tmp_path / "bad-validation-toggle.toml"
    config_path.write_text(
        """
[settings]
enable_validation = "sometimes"
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.enable_validation is True
    assert any("enable_validation" in warning for warning in config.config_warnings)


def test_load_runtime_config_defaults_invalid_viewer_settings(tmp_path: Path):
    """Invalid viewer host/port values should fall back to defaults."""
    blank_host = tmp_path / "blank-viewer-host.toml"
    blank_host.write_text(
        """
[settings]
viewer_host = "   "
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(blank_host)
    assert config.viewer_host == "127.0.0.1"
    assert any("viewer_host" in warning for warning in config.config_warnings)

    bad_port = tmp_path / "bad-viewer-port.toml"
    bad_port.write_text(
        """
[settings]
viewer_port = 70000
""".strip(),
        encoding="utf-8",
    )
    config = load_runtime_config(bad_port)
    assert config.viewer_port == 8000
    assert any("viewer_port" in warning for warning in config.config_warnings)


def test_load_runtime_config_supports_disabling_max_expiration(tmp_path: Path):
    """A zero-week max expiration should disable the expiration cap."""
    config_path = tmp_path / "no-expiration-cap.toml"
    config_path.write_text(
        """
[settings]
max_expiration_weeks = 0
""".strip(),
        encoding="utf-8",
    )

    config = load_runtime_config(config_path)

    assert config.max_expiration_weeks == 0
    assert config.max_expiration is None


def test_load_runtime_config_defaults_invalid_toml(tmp_path: Path):
    """Malformed config files should fall back to built-in defaults."""
    config_path = tmp_path / "broken.toml"
    config_path.write_text("[settings\n", encoding="utf-8")

    config = load_runtime_config(config_path)

    assert config.data_provider == "yfinance"
    assert config.tickers


def test_describe_runtime_config_masks_massive_key(tmp_path: Path):
    """Resolved config output should avoid printing secrets for the active provider."""
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[settings]
data_provider = "massive"

[providers.massive]
api_key = "secret"

[providers.marketdata]
api_token = "market-token"
""".strip(),
        encoding="utf-8",
    )

    lines = describe_runtime_config(load_runtime_config(config_path))

    assert any(line.endswith("set") for line in lines if "api_key" in line)
    assert all("secret" not in line for line in lines)
    assert all("market-token" not in line for line in lines)
    assert any("debug_dump_provider_payload" in line for line in lines)
    assert "Price context:" in lines
    assert "  enable: False" in lines
    assert "  lookback_days: 260" in lines
    assert "  max_age_days: 7" in lines
    assert not any("providers.marketdata" in line for line in lines)
    assert "General:" in lines
    assert "Filters:" in lines
    assert "Diagnostics:" in lines
    assert "Provider:" in lines
    assert "" not in lines


def test_describe_runtime_config_includes_marketdata_request_interval(tmp_path: Path):
    """Market Data provider output should show every pacing-related setting."""
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[settings]
data_provider = "marketdata"

[providers.marketdata]
api_token = "market-token"
mode = "cached"
max_retries = 5
request_interval_seconds = 0.5
backoff_seconds = 0.75
""".strip(),
        encoding="utf-8",
    )

    lines = describe_runtime_config(load_runtime_config(config_path))

    assert "Provider:" in lines
    assert "  providers.marketdata.api_token: set" in lines
    assert "  providers.marketdata.mode: cached" in lines
    assert "  providers.marketdata.max_retries: 5" in lines
    assert "  providers.marketdata.request_interval_seconds: 0.5" in lines
    assert "  providers.marketdata.backoff_seconds: 0.75" in lines
    assert all("market-token" not in line for line in lines)


def test_describe_runtime_config_includes_yfinance_retry_settings(tmp_path: Path):
    """yfinance provider output should show every retry and pacing setting."""
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[settings]
data_provider = "yfinance"

[providers.yfinance]
request_interval_seconds = 0.25
max_retries = 2
backoff_seconds = 0.5
""".strip(),
        encoding="utf-8",
    )

    lines = describe_runtime_config(load_runtime_config(config_path))

    assert "Provider:" in lines
    assert "  providers.yfinance.request_interval_seconds: 0.25" in lines
    assert "  providers.yfinance.max_retries: 2" in lines
    assert "  providers.yfinance.backoff_seconds: 0.5" in lines


def test_describe_runtime_config_includes_massive_retry_settings(tmp_path: Path):
    """Massive provider output should show every retry and pacing setting."""
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[settings]
data_provider = "massive"

[providers.massive]
api_key = "secret"
snapshot_page_limit = 125
request_interval_seconds = 2.0
max_retries = 4
backoff_seconds = 1.5
""".strip(),
        encoding="utf-8",
    )

    lines = describe_runtime_config(load_runtime_config(config_path))

    assert "Provider:" in lines
    assert "  providers.massive.api_key: set" in lines
    assert "  providers.massive.snapshot_page_limit: 125" in lines
    assert "  providers.massive.request_interval_seconds: 2.0" in lines
    assert "  providers.massive.max_retries: 4" in lines
    assert "  providers.massive.backoff_seconds: 1.5" in lines
    assert all("secret" not in line for line in lines)


def test_provider_registry_exposes_supported_providers():
    """The shared factory registry should enumerate the supported provider set."""
    assert set(PROVIDER_FACTORIES) == {"yfinance", "massive", "marketdata"}


def test_provider_package_import_is_lazy():
    """Importing the provider registry must not import unused vendor provider modules."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys\n"
                "import opx_chain.providers\n"
                "print('opx_chain.providers.yfinance' in sys.modules)\n"
                "print('opx_chain.providers.massive' in sys.modules)\n"
                "print('opx_chain.providers.marketdata' in sys.modules)\n"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.splitlines() == ["False", "False", "False"]
