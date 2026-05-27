"""Tests for shared timestamp helpers."""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from opx_chain.timestamps import (
    UTC_COMPACT_TIMESTAMP_FORMAT,
    UTC_Z_MICROSECONDS_FORMAT,
    UTC_Z_SECONDS_FORMAT,
    datetime_to_iso,
    format_utc_compact,
    format_utc_z_microseconds,
    format_utc_z_seconds,
    iso_to_datetime,
    parse_iso_datetime,
    utc_now,
    utc_now_timestamp,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "opx_chain"


def test_parse_iso_datetime_accepts_trailing_z_suffix():
    """Zulu timestamps should parse through the shared trailing-Z policy."""
    parsed = parse_iso_datetime("2026-04-22T12:00:00Z")

    assert parsed.utcoffset() == timezone.utc.utcoffset(parsed)
    assert parsed.isoformat() == "2026-04-22T12:00:00+00:00"


def test_parse_iso_datetime_normalizes_naive_values_to_utc():
    """Legacy retained naive timestamps should not leak offset-naive datetimes."""
    parsed = parse_iso_datetime("2026-04-22T12:00:00")

    assert parsed.utcoffset() == timezone.utc.utcoffset(parsed)
    assert parsed.isoformat() == "2026-04-22T12:00:00+00:00"


def test_parse_iso_datetime_does_not_replace_embedded_z():
    """Only a trailing Z should be normalized, not every Z in the string."""
    with pytest.raises(ValueError):
        parse_iso_datetime("2026-04-22TZ12:00:00Z")


def test_datetime_to_iso_and_iso_to_datetime_preserve_none_and_timezone():
    """Optional datetime serialization should share one storage policy."""
    value = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)

    assert datetime_to_iso(value) == "2026-04-22T12:00:00+00:00"
    assert iso_to_datetime("2026-04-22T12:00:00Z") == value
    assert datetime_to_iso(None) is None
    assert iso_to_datetime(None) is None


def test_datetime_to_iso_normalizes_naive_values_to_utc():
    """Storage writes should not create new offset-naive timestamp text."""
    value = datetime(2026, 4, 22, 12, 0)

    assert datetime_to_iso(value) == "2026-04-22T12:00:00+00:00"


def test_timestamp_helpers_preserve_pandas_nat_as_missing():
    """Pandas NaT should stay a missing timestamp, not stringify or raise."""
    assert datetime_to_iso(pd.NaT) is None
    assert format_utc_compact(pd.NaT) is None
    assert format_utc_z_seconds(pd.NaT) is None
    assert format_utc_z_microseconds(pd.NaT) is None


def test_utc_now_helpers_return_utc_values():
    """Datetime and pandas callers should share UTC clock helpers."""
    assert utc_now().utcoffset() == timezone.utc.utcoffset(None)

    timestamp = utc_now_timestamp()
    assert isinstance(timestamp, pd.Timestamp)
    assert str(timestamp.tz) == "UTC"


def test_utc_timestamp_format_helpers_share_canonical_formats():
    """Runtime timestamp displays should use shared format constants."""
    value = datetime(2026, 5, 9, 12, 34, 56, 789123, tzinfo=timezone.utc)

    assert format_utc_compact(value) == "20260509_123456"
    assert format_utc_z_seconds(value) == "2026-05-09T12:34:56Z"
    assert format_utc_z_microseconds(value) == "2026-05-09T12:34:56.789123Z"


def test_production_timestamp_formats_stay_centralized():
    """Production code should import timestamp format helpers, not repeat literals."""
    assert PACKAGE_ROOT.exists()
    canonical_formats = (
        UTC_COMPACT_TIMESTAMP_FORMAT,
        UTC_Z_SECONDS_FORMAT,
        UTC_Z_MICROSECONDS_FORMAT,
    )
    offenders: list[str] = []
    scanned_files = 0
    for path in PACKAGE_ROOT.rglob("*.py"):
        if path.name == "timestamps.py":
            continue
        scanned_files += 1
        source = path.read_text(encoding="utf-8")
        for timestamp_format in canonical_formats:
            if f'"{timestamp_format}"' in source or f"'{timestamp_format}'" in source:
                offenders.append(f"{path.relative_to(PROJECT_ROOT)}: {timestamp_format}")

    assert scanned_files > 0
    assert not offenders
