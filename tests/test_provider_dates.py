"""Tests for shared provider date parsing helpers."""

from datetime import date, datetime, timezone

import pandas as pd

from opx_chain.providers._dates import parse_event_date


def test_parse_event_date_infers_numeric_epoch_units():
    """Numeric event dates should accept seconds and millisecond epochs."""
    assert parse_event_date(1710942000) == date(2024, 3, 20)
    assert parse_event_date(1710942000000) == date(2024, 3, 20)


def test_parse_event_date_uses_market_calendar_for_utc_timestamp():
    """UTC timestamps should resolve to the U.S. market-calendar date."""
    raw_date = datetime(2026, 4, 30, 0, 30, tzinfo=timezone.utc)

    assert parse_event_date(raw_date) == date(2026, 4, 29)


def test_parse_event_date_preserves_date_only_values():
    """Provider date objects and strings represent calendar dates, not instants."""
    assert parse_event_date(date(2026, 6, 8)) == date(2026, 6, 8)
    assert parse_event_date("2026-06-08T00:00:00Z") == date(2026, 6, 8)


def test_parse_event_date_preserves_existing_string_and_missing_behavior():
    """Date strings parse strictly while missing values remain blank."""
    assert parse_event_date("2026-04-30T20:00:00Z") == date(2026, 4, 30)
    assert parse_event_date(pd.NaT) is None
