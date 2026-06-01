"""Runtime configuration loading for the options fetch pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib

from opx_chain.config_coercion import (
    ConfigError,
    coerce_bool as _coerce_bool,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
    coerce_list as _coerce_list,
    coerce_path as _coerce_path,
    coerce_str as _coerce_str,
)
from opx_chain.paths import (
    get_cache_dir,
    get_data_dir,
    get_default_config_path,
    get_default_debug_dump_dir,
    get_default_provider_cache_dir,
    resolve_relative_path,
)
from opx_chain.tickers import is_valid_ticker
from opx_chain.version import __version__

SUPPORTED_PROVIDERS = frozenset({"yfinance", "massive", "marketdata"})
SCRIPT_VERSION = __version__
DEFAULT_CONFIG_PATH_OVERRIDE: Path | None = None
DEFAULT_TICKERS = ("TSLA", "NVDA", "UBER", "MSFT", "GOOGL", "ORCL", "PLTR")
DEFAULT_DATA_PROVIDER = "yfinance"
DEFAULT_MIN_BID = None  # disabled by default; previously 0.50
DEFAULT_MIN_OPEN_INTEREST = 100
DEFAULT_MIN_VOLUME = 10
DEFAULT_MAX_SPREAD_PCT_OF_MID = 0.25
DEFAULT_RISK_FREE_RATE = 0.045
DEFAULT_HV_LOOKBACK_DAYS = 30
DEFAULT_TRADING_DAYS_PER_YEAR = 252
DEFAULT_STALE_QUOTE_SECONDS = 10800
DEFAULT_ENABLE_FILTERS = True
DEFAULT_ENABLE_VALIDATION = True
DEFAULT_PRICE_CONTEXT_ENABLE = False
DEFAULT_PRICE_CONTEXT_LOOKBACK_DAYS = 260
DEFAULT_PRICE_CONTEXT_MAX_AGE_DAYS = 7
DEFAULT_OPTION_SCORE_INCOME_WEIGHT = 0.30
DEFAULT_OPTION_SCORE_LIQUIDITY_WEIGHT = 0.30
DEFAULT_OPTION_SCORE_RISK_WEIGHT = 0.25
DEFAULT_OPTION_SCORE_EFFICIENCY_WEIGHT = 0.15
DEFAULT_MAX_STRIKE_DISTANCE_PCT = 0.35
DEFAULT_MAX_EXPIRATION_WEEKS = 34
SUPPORTED_MARKETDATA_MODES = frozenset({"live", "cached", "delayed"})
DEFAULT_MARKETDATA_MAX_RETRIES = 3
DEFAULT_MARKETDATA_REQUEST_INTERVAL_SECONDS = 0.0
DEFAULT_MARKETDATA_BACKOFF_SECONDS = 1.0
DEFAULT_YFINANCE_REQUEST_INTERVAL_SECONDS = 0.0
DEFAULT_YFINANCE_MAX_RETRIES = 0
DEFAULT_YFINANCE_BACKOFF_SECONDS = 1.0
DEFAULT_VIEWER_HOST = "127.0.0.1"
DEFAULT_VIEWER_PORT = 8000
DEFAULT_AUTO_FALLBACK_TO_YFINANCE = False
MAX_MASSIVE_SNAPSHOT_PAGE_LIMIT = 250
DEFAULT_MASSIVE_SNAPSHOT_PAGE_LIMIT = MAX_MASSIVE_SNAPSHOT_PAGE_LIMIT
DEFAULT_MASSIVE_REQUEST_INTERVAL_SECONDS = 12.0
DEFAULT_MASSIVE_MAX_RETRIES = 3
DEFAULT_MASSIVE_BACKOFF_SECONDS = 1.0
DEFAULT_DEBUG_DUMP_PROVIDER_PAYLOAD = False
_RUNTIME_CONFIG_OVERRIDE: RuntimeConfig | None = None
US_MARKET_TIMEZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True)
# pylint: disable=too-many-instance-attributes
class RuntimeConfig:
    """Resolved runtime settings used by the application."""

    tickers: tuple[str, ...]
    min_bid: float | None
    min_open_interest: int
    min_volume: int
    max_spread_pct_of_mid: float
    risk_free_rate: float
    hv_lookback_days: int
    trading_days_per_year: int
    option_score_income_weight: float
    option_score_liquidity_weight: float
    option_score_risk_weight: float
    option_score_efficiency_weight: float
    data_provider: str
    stale_quote_seconds: int
    enable_filters: bool
    enable_validation: bool
    price_context_enable: bool
    price_context_lookback_days: int
    price_context_max_age_days: int
    max_strike_distance_pct: float
    max_expiration_weeks: int | None
    max_expiration: str | None
    today: date
    massive_api_key: str | None
    marketdata_api_token: str | None
    marketdata_mode: str | None
    marketdata_max_retries: int
    marketdata_request_interval_seconds: float
    marketdata_backoff_seconds: float
    yfinance_request_interval_seconds: float
    yfinance_max_retries: int
    yfinance_backoff_seconds: float
    massive_snapshot_page_limit: int
    massive_request_interval_seconds: float
    massive_max_retries: int
    massive_backoff_seconds: float
    debug_dump_provider_payload: bool
    debug_dump_dir: Path
    viewer_host: str
    viewer_port: int
    config_path: Path
    storage_enabled: bool = False
    storage_backend: str = "filesystem"
    storage_max_runs_retained: int = 0
    storage_dataset_format: str = "csv"
    storage_also_write_csv: bool = True
    storage_dir: Path | None = None       # absolute storage base; defaults to XDG data dir
    auto_fallback_to_yfinance: bool = DEFAULT_AUTO_FALLBACK_TO_YFINANCE
    provider_cache_backend: str = "none"
    provider_cache_dir: Path = field(default_factory=get_default_provider_cache_dir)
    provider_snapshot_ttl: int = 300
    provider_chain_ttl: int = 300
    provider_events_ttl: int = 86400
    provider_price_context_ttl: int = 86400
    config_warnings: tuple[str, ...] = field(default_factory=tuple)


def _default_max_expiration(today, weeks):
    return (today + timedelta(weeks=weeks)).isoformat()


def market_calendar_today(now: datetime | None = None) -> date:
    """Return today's date on the U.S. market calendar."""
    current = now or datetime.now(tz=US_MARKET_TIMEZONE)
    if current.tzinfo is None:
        current = current.replace(tzinfo=US_MARKET_TIMEZONE)
    else:
        current = current.astimezone(US_MARKET_TIMEZONE)
    return current.date()


def _resolve_path_setting(
    raw_value,
    *,
    field_name,
    default: Path,
    base_dir: Path,
    warnings: list[str],
) -> Path:
    """Resolve a config path, anchoring relative paths to the appropriate XDG base."""
    value = _resolve_config_value(
        raw_value,
        field_name=field_name,
        default=default,
        coercer=_coerce_path,
        warnings=warnings,
    )
    return resolve_relative_path(Path(value), base_dir=base_dir)


def _resolve_optional_path_setting(
    raw_value,
    *,
    field_name,
    base_dir: Path,
    warnings: list[str],
) -> Path | None:
    """Resolve an optional config path, anchoring relative values to an XDG base."""
    value = _resolve_config_value(
        raw_value,
        field_name=field_name,
        default=None,
        coercer=_coerce_path,
        warnings=warnings,
    )
    if value is None:
        return None
    return resolve_relative_path(Path(value), base_dir=base_dir)


def _read_config_data(config_path: Path, warnings: list[str] | None = None) -> dict:
    if not config_path.exists():
        return {}
    try:
        with config_path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        if warnings is not None:
            warnings.append(
                f"Config file {config_path} could not be parsed"
                f" (TOML error: {exc}); using defaults."
            )
        return {}
    except OSError as exc:
        if warnings is not None:
            warnings.append(
                f"Config file {config_path} could not be read ({exc}); using defaults."
            )
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def _value_or_default(value, default):
    return default if value is None else value


def _append_default_warning(
    warnings: list[str],
    field_name: str,
    default,
    *,
    rejected_value=None,
    reason: str | None = None,
) -> None:
    detail = ""
    if rejected_value is not None:
        detail = f" rejected value {rejected_value!r}"
    if reason:
        detail = f"{detail} ({reason})"
    warnings.append(f"{field_name}:{detail}; using default {default!r}.")


def _resolve_config_value(  # pylint: disable=too-many-arguments
    raw_value,
    *,
    field_name,
    default,
    coercer,
    warnings,
    validator=None,
    constraint: str | None = None,
):
    try:
        value = _value_or_default(coercer(raw_value, field_name=field_name), default)
    except ConfigError as exc:
        _append_default_warning(
            warnings,
            field_name,
            default,
            rejected_value=raw_value,
            reason=str(exc),
        )
        return default
    if validator is not None and not validator(value):
        _append_default_warning(
            warnings,
            field_name,
            default,
            rejected_value=value,
            reason=constraint or "failed validation",
        )
        return default
    return value


def _resolve_table(value, *, field_name, warnings):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    _append_default_warning(warnings, field_name, {})
    return {}


def _clamp_massive_snapshot_page_limit(value: int, warnings: list[str]) -> int:
    """Clamp Massive snapshot page size to the endpoint's documented maximum."""
    if value <= 0:
        _append_default_warning(
            warnings,
            "providers.massive.snapshot_page_limit",
            DEFAULT_MASSIVE_SNAPSHOT_PAGE_LIMIT,
            rejected_value=value,
            reason="must be > 0",
        )
        return DEFAULT_MASSIVE_SNAPSHOT_PAGE_LIMIT
    if value > MAX_MASSIVE_SNAPSHOT_PAGE_LIMIT:
        warnings.append(
            "providers.massive.snapshot_page_limit: clamped to 250 because "
            "/v3/snapshot/options/{underlyingAsset} rejects larger values."
        )
        return MAX_MASSIVE_SNAPSHOT_PAGE_LIMIT
    return value


def _valid_ticker_sequence(values: tuple[str, ...]) -> bool:
    """Return True when every configured ticker has valid symbol syntax."""
    return all(is_valid_ticker(value) for value in values)


def load_runtime_config(  # pylint: disable=too-many-locals
    config_path: Path | None = None, *, today: date | None = None
) -> RuntimeConfig:
    """Load runtime config from the user config file, falling back to defaults."""
    default_config_path = DEFAULT_CONFIG_PATH_OVERRIDE or get_default_config_path()
    resolved_path = (config_path or default_config_path).expanduser()
    warnings: list[str] = []
    data = _read_config_data(resolved_path, warnings)
    settings = _resolve_table(data.get("settings", {}), field_name="settings", warnings=warnings)
    providers = _resolve_table(data.get("providers", {}), field_name="providers", warnings=warnings)
    massive_settings = _resolve_table(
        providers.get("massive", {}),
        field_name="providers.massive",
        warnings=warnings,
    )
    marketdata_settings = _resolve_table(
        providers.get("marketdata", {}),
        field_name="providers.marketdata",
        warnings=warnings,
    )
    yfinance_settings = _resolve_table(
        providers.get("yfinance", {}),
        field_name="providers.yfinance",
        warnings=warnings,
    )
    price_context_settings = _resolve_table(
        data.get("price_context", {}),
        field_name="price_context",
        warnings=warnings,
    )

    runtime_day = today or market_calendar_today()
    data_provider = _resolve_config_value(
        settings.get("data_provider"),
        field_name="settings.data_provider",
        default=DEFAULT_DATA_PROVIDER,
        coercer=_coerce_str,
        warnings=warnings,
        validator=lambda value: value in SUPPORTED_PROVIDERS,
        constraint=f"must be one of {sorted(SUPPORTED_PROVIDERS)!r}",
    )
    massive_warnings = warnings if data_provider == "massive" else []
    marketdata_warnings = warnings if data_provider == "marketdata" else []
    auto_fallback_to_yfinance = _resolve_config_value(
        settings.get("auto_fallback_to_yfinance"),
        field_name="settings.auto_fallback_to_yfinance",
        default=DEFAULT_AUTO_FALLBACK_TO_YFINANCE,
        coercer=_coerce_bool,
        warnings=warnings,
    )
    storage_settings = _resolve_table(
        data.get("storage", {}), field_name="storage", warnings=warnings
    )
    massive_api_key = _resolve_config_value(
        massive_settings.get("api_key"),
        field_name="providers.massive.api_key",
        default=None,
        coercer=_coerce_str,
        warnings=massive_warnings,
    )
    marketdata_api_token = _resolve_config_value(
        marketdata_settings.get("api_token"),
        field_name="providers.marketdata.api_token",
        default=None,
        coercer=_coerce_str,
        warnings=marketdata_warnings,
    )
    marketdata_mode = _resolve_config_value(
        marketdata_settings.get("mode"),
        field_name="providers.marketdata.mode",
        default=None,
        coercer=_coerce_str,
        warnings=marketdata_warnings,
        validator=lambda value: value is None or value in SUPPORTED_MARKETDATA_MODES,
        constraint=f"must be one of {sorted(SUPPORTED_MARKETDATA_MODES)!r}",
    )
    if data_provider == "massive" and not massive_api_key:
        if not auto_fallback_to_yfinance:
            raise ConfigError(
                "Configured settings.data_provider='massive' requires "
                "providers.massive.api_key. Set "
                "settings.auto_fallback_to_yfinance=true to opt into yfinance fallback."
            )
        warnings.append(
            "providers.massive.api_key: using default None and falling back to 'yfinance' "
            "because settings.auto_fallback_to_yfinance=true."
        )
        data_provider = DEFAULT_DATA_PROVIDER
    if data_provider == "marketdata" and not marketdata_api_token:
        if not auto_fallback_to_yfinance:
            raise ConfigError(
                "Configured settings.data_provider='marketdata' requires "
                "providers.marketdata.api_token. Set "
                "settings.auto_fallback_to_yfinance=true to opt into yfinance fallback."
            )
        warnings.append(
            "providers.marketdata.api_token: using default None and falling back to "
            "'yfinance' because settings.auto_fallback_to_yfinance=true."
        )
        data_provider = DEFAULT_DATA_PROVIDER
    yfinance_warnings = warnings if data_provider == "yfinance" else []

    config = RuntimeConfig(
        tickers=_resolve_config_value(
            settings.get("tickers"),
            field_name="settings.tickers",
            default=DEFAULT_TICKERS,
            coercer=_coerce_list,
            warnings=warnings,
            validator=_valid_ticker_sequence,
            constraint="must contain valid ticker symbols",
        ),
        min_bid=_resolve_config_value(
            settings.get("filters_min_bid"),
            field_name="settings.filters_min_bid",
            default=DEFAULT_MIN_BID,
            coercer=_coerce_float,
            warnings=warnings,
            validator=lambda value: value is None or value > 0,
            constraint="must be positive or null",
        ),
        min_open_interest=_resolve_config_value(
            settings.get("filters_min_open_interest"),
            field_name="settings.filters_min_open_interest",
            default=DEFAULT_MIN_OPEN_INTEREST,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        min_volume=_resolve_config_value(
            settings.get("filters_min_volume"),
            field_name="settings.filters_min_volume",
            default=DEFAULT_MIN_VOLUME,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        max_spread_pct_of_mid=_resolve_config_value(
            settings.get("filters_max_spread_pct_of_mid"),
            field_name="settings.filters_max_spread_pct_of_mid",
            default=DEFAULT_MAX_SPREAD_PCT_OF_MID,
            coercer=_coerce_float,
            warnings=warnings,
            validator=lambda value: value > 0,
            constraint="must be > 0",
        ),
        risk_free_rate=_resolve_config_value(
            settings.get("risk_free_rate"),
            field_name="settings.risk_free_rate",
            default=DEFAULT_RISK_FREE_RATE,
            coercer=_coerce_float,
            warnings=warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        hv_lookback_days=_resolve_config_value(
            settings.get("hv_lookback_days"),
            field_name="settings.hv_lookback_days",
            default=DEFAULT_HV_LOOKBACK_DAYS,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda value: value > 0,
            constraint="must be > 0",
        ),
        trading_days_per_year=_resolve_config_value(
            settings.get("trading_days_per_year"),
            field_name="settings.trading_days_per_year",
            default=DEFAULT_TRADING_DAYS_PER_YEAR,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda v: v > 0,
            constraint="must be > 0",
        ),
        option_score_income_weight=_resolve_config_value(
            settings.get("option_score_income_weight"),
            field_name="settings.option_score_income_weight",
            default=DEFAULT_OPTION_SCORE_INCOME_WEIGHT,
            coercer=_coerce_float,
            warnings=warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        option_score_liquidity_weight=_resolve_config_value(
            settings.get("option_score_liquidity_weight"),
            field_name="settings.option_score_liquidity_weight",
            default=DEFAULT_OPTION_SCORE_LIQUIDITY_WEIGHT,
            coercer=_coerce_float,
            warnings=warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        option_score_risk_weight=_resolve_config_value(
            settings.get("option_score_risk_weight"),
            field_name="settings.option_score_risk_weight",
            default=DEFAULT_OPTION_SCORE_RISK_WEIGHT,
            coercer=_coerce_float,
            warnings=warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        option_score_efficiency_weight=_resolve_config_value(
            settings.get("option_score_efficiency_weight"),
            field_name="settings.option_score_efficiency_weight",
            default=DEFAULT_OPTION_SCORE_EFFICIENCY_WEIGHT,
            coercer=_coerce_float,
            warnings=warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        data_provider=data_provider,
        stale_quote_seconds=_resolve_config_value(
            settings.get("stale_quote_seconds"),
            field_name="settings.stale_quote_seconds",
            default=DEFAULT_STALE_QUOTE_SECONDS,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        enable_filters=_resolve_config_value(
            settings.get("filters_enable"),
            field_name="settings.filters_enable",
            default=DEFAULT_ENABLE_FILTERS,
            coercer=_coerce_bool,
            warnings=warnings,
        ),
        enable_validation=_resolve_config_value(
            settings.get("enable_validation"),
            field_name="settings.enable_validation",
            default=DEFAULT_ENABLE_VALIDATION,
            coercer=_coerce_bool,
            warnings=warnings,
        ),
        price_context_enable=_resolve_config_value(
            price_context_settings.get("enable"),
            field_name="price_context.enable",
            default=DEFAULT_PRICE_CONTEXT_ENABLE,
            coercer=_coerce_bool,
            warnings=warnings,
        ),
        price_context_lookback_days=_resolve_config_value(
            price_context_settings.get("lookback_days"),
            field_name="price_context.lookback_days",
            default=DEFAULT_PRICE_CONTEXT_LOOKBACK_DAYS,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda value: value >= 20,
            constraint="must be >= 20",
        ),
        price_context_max_age_days=_resolve_config_value(
            price_context_settings.get("max_age_days"),
            field_name="price_context.max_age_days",
            default=DEFAULT_PRICE_CONTEXT_MAX_AGE_DAYS,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        debug_dump_provider_payload=_resolve_config_value(
            settings.get("debug_dump_provider_payload"),
            field_name="settings.debug_dump_provider_payload",
            default=DEFAULT_DEBUG_DUMP_PROVIDER_PAYLOAD,
            coercer=_coerce_bool,
            warnings=warnings,
        ),
        debug_dump_dir=_resolve_path_setting(
            settings.get("debug_dump_dir"),
            field_name="settings.debug_dump_dir",
            default=get_default_debug_dump_dir(),
            base_dir=get_data_dir(),
            warnings=warnings,
        ),
        viewer_host=_resolve_config_value(
            settings.get("viewer_host"),
            field_name="settings.viewer_host",
            default=DEFAULT_VIEWER_HOST,
            coercer=_coerce_str,
            warnings=warnings,
        ),
        viewer_port=_resolve_config_value(
            settings.get("viewer_port"),
            field_name="settings.viewer_port",
            default=DEFAULT_VIEWER_PORT,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda value: 1 <= value <= 65535,
            constraint="must be between 1 and 65535",
        ),
        max_strike_distance_pct=_resolve_config_value(
            settings.get("filters_max_strike_distance_pct"),
            field_name="settings.filters_max_strike_distance_pct",
            default=DEFAULT_MAX_STRIKE_DISTANCE_PCT,
            coercer=_coerce_float,
            warnings=warnings,
            validator=lambda value: value > 0,
            constraint="must be > 0",
        ),
        max_expiration_weeks=_resolve_config_value(
            settings.get("max_expiration_weeks"),
            field_name="settings.max_expiration_weeks",
            default=DEFAULT_MAX_EXPIRATION_WEEKS,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda value: value is None or value >= 0,
            constraint="must be >= 0 or null",
        ),
        max_expiration=None,
        today=runtime_day,
        massive_api_key=massive_api_key,
        marketdata_api_token=marketdata_api_token,
        marketdata_mode=marketdata_mode,
        marketdata_max_retries=_resolve_config_value(
            marketdata_settings.get("max_retries"),
            field_name="providers.marketdata.max_retries",
            default=DEFAULT_MARKETDATA_MAX_RETRIES,
            coercer=_coerce_int,
            warnings=marketdata_warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        marketdata_request_interval_seconds=_resolve_config_value(
            marketdata_settings.get("request_interval_seconds"),
            field_name="providers.marketdata.request_interval_seconds",
            default=DEFAULT_MARKETDATA_REQUEST_INTERVAL_SECONDS,
            coercer=_coerce_float,
            warnings=marketdata_warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        marketdata_backoff_seconds=_resolve_config_value(
            marketdata_settings.get("backoff_seconds"),
            field_name="providers.marketdata.backoff_seconds",
            default=DEFAULT_MARKETDATA_BACKOFF_SECONDS,
            coercer=_coerce_float,
            warnings=marketdata_warnings,
            validator=lambda value: value > 0,
            constraint="must be > 0",
        ),
        yfinance_request_interval_seconds=_resolve_config_value(
            yfinance_settings.get("request_interval_seconds"),
            field_name="providers.yfinance.request_interval_seconds",
            default=DEFAULT_YFINANCE_REQUEST_INTERVAL_SECONDS,
            coercer=_coerce_float,
            warnings=yfinance_warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        yfinance_max_retries=_resolve_config_value(
            yfinance_settings.get("max_retries"),
            field_name="providers.yfinance.max_retries",
            default=DEFAULT_YFINANCE_MAX_RETRIES,
            coercer=_coerce_int,
            warnings=yfinance_warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        yfinance_backoff_seconds=_resolve_config_value(
            yfinance_settings.get("backoff_seconds"),
            field_name="providers.yfinance.backoff_seconds",
            default=DEFAULT_YFINANCE_BACKOFF_SECONDS,
            coercer=_coerce_float,
            warnings=yfinance_warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        massive_snapshot_page_limit=_clamp_massive_snapshot_page_limit(_resolve_config_value(
            massive_settings.get("snapshot_page_limit"),
            field_name="providers.massive.snapshot_page_limit",
            default=DEFAULT_MASSIVE_SNAPSHOT_PAGE_LIMIT,
            coercer=_coerce_int,
            warnings=massive_warnings,
        ), massive_warnings),
        massive_request_interval_seconds=_resolve_config_value(
            massive_settings.get("request_interval_seconds"),
            field_name="providers.massive.request_interval_seconds",
            default=DEFAULT_MASSIVE_REQUEST_INTERVAL_SECONDS,
            coercer=_coerce_float,
            warnings=massive_warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        massive_max_retries=_resolve_config_value(
            massive_settings.get("max_retries"),
            field_name="providers.massive.max_retries",
            default=DEFAULT_MASSIVE_MAX_RETRIES,
            coercer=_coerce_int,
            warnings=massive_warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        massive_backoff_seconds=_resolve_config_value(
            massive_settings.get("backoff_seconds"),
            field_name="providers.massive.backoff_seconds",
            default=DEFAULT_MASSIVE_BACKOFF_SECONDS,
            coercer=_coerce_float,
            warnings=massive_warnings,
            validator=lambda value: value >= 0,
            constraint="must be >= 0",
        ),
        config_path=resolved_path,
        auto_fallback_to_yfinance=auto_fallback_to_yfinance,
        storage_enabled=_resolve_config_value(
            storage_settings.get("enable"),
            field_name="storage.enable",
            default=False,
            coercer=_coerce_bool,
            warnings=warnings,
        ),
        storage_backend=_resolve_config_value(
            storage_settings.get("backend"),
            field_name="storage.backend",
            default="filesystem",
            coercer=_coerce_str,
            warnings=warnings,
            validator=lambda v: v in {"filesystem", "sqlite"},
            constraint="must be one of ['filesystem', 'sqlite']",
        ),
        storage_max_runs_retained=_resolve_config_value(
            storage_settings.get("max_runs_retained"),
            field_name="storage.max_runs_retained",
            default=0,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda v: v >= 0,
            constraint="must be >= 0",
        ),
        storage_dataset_format=_resolve_config_value(
            storage_settings.get("dataset_format"),
            field_name="storage.dataset_format",
            default="csv",
            coercer=_coerce_str,
            warnings=warnings,
            validator=lambda v: v in {"csv", "parquet"},
            constraint="must be one of ['csv', 'parquet']",
        ),
        storage_also_write_csv=_resolve_config_value(
            storage_settings.get("also_write_csv"),
            field_name="storage.also_write_csv",
            default=True,
            coercer=_coerce_bool,
            warnings=warnings,
        ),
        storage_dir=_resolve_optional_path_setting(
            storage_settings.get("dir"),
            field_name="storage.dir",
            base_dir=get_data_dir(),
            warnings=warnings,
        ),
        provider_cache_backend=_resolve_config_value(
            storage_settings.get("cache_backend"),
            field_name="storage.cache_backend",
            default="none",
            coercer=_coerce_str,
            warnings=warnings,
            validator=lambda v: v in {"none", "filesystem"},
            constraint="must be one of ['filesystem', 'none']",
        ),
        provider_cache_dir=_resolve_path_setting(
            storage_settings.get("cache_dir"),
            field_name="storage.cache_dir",
            default=get_default_provider_cache_dir(),
            base_dir=get_cache_dir(),
            warnings=warnings,
        ),
        provider_snapshot_ttl=_resolve_config_value(
            storage_settings.get("snapshot_ttl"),
            field_name="storage.snapshot_ttl",
            default=300,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda v: v > 0,
            constraint="must be > 0",
        ),
        provider_chain_ttl=_resolve_config_value(
            storage_settings.get("chain_ttl"),
            field_name="storage.chain_ttl",
            default=300,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda v: v > 0,
            constraint="must be > 0",
        ),
        provider_events_ttl=_resolve_config_value(
            storage_settings.get("events_ttl"),
            field_name="storage.events_ttl",
            default=86400,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda v: v > 0,
            constraint="must be > 0",
        ),
        provider_price_context_ttl=_resolve_config_value(
            storage_settings.get("price_context_ttl"),
            field_name="storage.price_context_ttl",
            default=86400,
            coercer=_coerce_int,
            warnings=warnings,
            validator=lambda v: v > 0,
            constraint="must be > 0",
        ),
        config_warnings=tuple(warnings),
    )
    object.__setattr__(
        config,
        "max_expiration",
        (
            None
            if config.max_expiration_weeks in {None, 0}
            else _default_max_expiration(runtime_day, config.max_expiration_weeks)
        ),
    )
    if (
        config.option_score_income_weight
        + config.option_score_liquidity_weight
        + config.option_score_risk_weight
        + config.option_score_efficiency_weight
        <= 0
    ):
        warnings.append(
            "settings.option_score_*_weight: total weight must be positive; using defaults."
        )
        object.__setattr__(config, "option_score_income_weight", DEFAULT_OPTION_SCORE_INCOME_WEIGHT)
        object.__setattr__(
            config,
            "option_score_liquidity_weight",
            DEFAULT_OPTION_SCORE_LIQUIDITY_WEIGHT,
        )
        object.__setattr__(config, "option_score_risk_weight", DEFAULT_OPTION_SCORE_RISK_WEIGHT)
        object.__setattr__(
            config,
            "option_score_efficiency_weight",
            DEFAULT_OPTION_SCORE_EFFICIENCY_WEIGHT,
        )
        object.__setattr__(config, "config_warnings", tuple(warnings))
    return config


def load_storage_dir_config(config_path: Path | None = None) -> Path | None:
    """Load only storage.dir without validating provider credentials."""
    default_config_path = DEFAULT_CONFIG_PATH_OVERRIDE or get_default_config_path()
    resolved_path = (config_path or default_config_path).expanduser()
    warnings: list[str] = []
    data = _read_config_data(resolved_path, warnings)
    storage_settings = _resolve_table(
        data.get("storage", {}),
        field_name="storage",
        warnings=warnings,
    )
    return _resolve_optional_path_setting(
        storage_settings.get("dir"),
        field_name="storage.dir",
        base_dir=get_data_dir(),
        warnings=warnings,
    )


@lru_cache(maxsize=1)
def _load_runtime_config_for_market_day(market_day: date) -> RuntimeConfig:
    """Return config cached only for the active market-calendar date."""
    return load_runtime_config(today=market_day)


def get_runtime_config() -> RuntimeConfig:
    """Return the cached runtime config for the current process."""
    if _RUNTIME_CONFIG_OVERRIDE is not None:
        return _RUNTIME_CONFIG_OVERRIDE
    return _load_runtime_config_for_market_day(market_calendar_today())


def set_runtime_config_override(config: RuntimeConfig | None) -> None:
    """Override the process runtime config for one-off entrypoint behavior."""
    global _RUNTIME_CONFIG_OVERRIDE  # pylint: disable=global-statement
    _RUNTIME_CONFIG_OVERRIDE = config
    _load_runtime_config_for_market_day.cache_clear()


def get_runtime_config_override() -> RuntimeConfig | None:
    """Return the active process runtime config override, if any."""
    return _RUNTIME_CONFIG_OVERRIDE


def reset_runtime_config() -> None:
    """Clear the cached runtime config, primarily for tests."""
    set_runtime_config_override(None)
    _load_runtime_config_for_market_day.cache_clear()


def get_provider_credentials(provider_name: str) -> dict[str, str]:
    """Return credentials for the selected provider without exposing config internals."""
    config = get_runtime_config()
    if provider_name == "massive" and config.massive_api_key:
        return {"api_key": config.massive_api_key}
    if provider_name == "marketdata" and config.marketdata_api_token:
        return {"api_token": config.marketdata_api_token}
    return {}


def describe_runtime_config(config: RuntimeConfig) -> tuple[str, ...]:
    """Return human-readable lines describing the resolved runtime configuration."""
    min_bid_label = config.min_bid if config.min_bid is not None else "disabled"
    max_exp_weeks = config.max_expiration_weeks
    max_exp_label = max_exp_weeks if max_exp_weeks is not None else "disabled"
    config_exists = "exists" if config.config_path.exists() else "missing"
    lines: list[str] = [
        "General:",
        f"  config: {config.config_path} ({config_exists})",
        f"  provider: {config.data_provider}",
        f"  tickers: {', '.join(config.tickers)}",
        f"  max_expiration_weeks: {max_exp_label}",
        f"  max_expiration: {config.max_expiration or 'disabled'}",
        "Filters:",
        f"  filters_enable: {config.enable_filters}",
        f"  filters_min_bid: {min_bid_label}",
        f"  filters_min_open_interest: {config.min_open_interest}",
        f"  filters_min_volume: {config.min_volume}",
        f"  filters_max_spread_pct_of_mid: {config.max_spread_pct_of_mid}",
        f"  filters_max_strike_distance_pct: {config.max_strike_distance_pct}",
        "Diagnostics:",
        f"  enable_validation: {config.enable_validation}",
        f"  debug_dump_provider_payload: {config.debug_dump_provider_payload}",
        "Price context:",
        f"  enable: {config.price_context_enable}",
        f"  lookback_days: {config.price_context_lookback_days}",
        f"  max_age_days: {config.price_context_max_age_days}",
    ]
    if config.data_provider == "marketdata":
        token_label = "set" if config.marketdata_api_token else "not set"
        lines += [
            "Provider:",
            f"  providers.marketdata.api_token: {token_label}",
            f"  providers.marketdata.mode: {config.marketdata_mode or 'default'}",
            f"  providers.marketdata.max_retries: {config.marketdata_max_retries}",
            f"  providers.marketdata.request_interval_seconds: "
            f"{config.marketdata_request_interval_seconds}",
            f"  providers.marketdata.backoff_seconds: {config.marketdata_backoff_seconds}",
        ]
    elif config.data_provider == "massive":
        key_label = "set" if config.massive_api_key else "not set"
        lines += [
            "Provider:",
            f"  providers.massive.api_key: {key_label}",
            f"  providers.massive.snapshot_page_limit: {config.massive_snapshot_page_limit}",
            f"  providers.massive.request_interval_seconds: "
            f"{config.massive_request_interval_seconds}",
            f"  providers.massive.max_retries: {config.massive_max_retries}",
            f"  providers.massive.backoff_seconds: {config.massive_backoff_seconds}",
        ]
    elif config.data_provider == "yfinance":
        lines += [
            "Provider:",
            f"  providers.yfinance.request_interval_seconds: "
            f"{config.yfinance_request_interval_seconds}",
            f"  providers.yfinance.max_retries: {config.yfinance_max_retries}",
            f"  providers.yfinance.backoff_seconds: {config.yfinance_backoff_seconds}",
        ]
    if config.storage_enabled:
        lines += [
            "Storage:",
            f"  backend: {config.storage_backend}",
            f"  dataset_format: {config.storage_dataset_format}",
            f"  also_write_csv: {config.storage_also_write_csv}",
            f"  cache: {config.provider_cache_backend}",
            f"  price_context_ttl: {config.provider_price_context_ttl}",
        ]
    else:
        lines += ["Storage:", "  enable: false"]
    return tuple(lines)
