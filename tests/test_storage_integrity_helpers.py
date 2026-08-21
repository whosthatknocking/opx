"""Shared storage integrity preparation and metadata transition tests."""
# pylint: disable=line-too-long,missing-function-docstring

from dataclasses import replace
from datetime import datetime, timezone

import pandas as pd
import pytest

from opx_chain import SCHEMA_VERSION
from opx_chain.integrity import (
    OptionChainDataIntegrityError,
    OptionChainDatasetFactsStatus,
    OptionChainIntegrityCode,
    OptionChainIntegrityStatus,
    OptionChainSchemaCompatibilityError,
    evaluate_option_chain_dataset_facts_status,
    evaluate_option_chain_integrity_status,
)
from opx_chain.storage.integrity import (
    prepare_option_chain_dataset,
    record_integrity_from_mapping,
    record_integrity_to_dict,
    record_with_validated_metadata,
    validate_stored_option_chain_snapshot,
)
from opx_chain.storage.models import DatasetRecord, DatasetWrite


def _frame(**overrides) -> pd.DataFrame:
    row = {
        "data_source": "synthetic-provider",
        "underlying_symbol": "SYNTH",
        "contract_symbol": "SYNTH260821C00100000",
        "option_type": "call",
        "expiration_date": "2026-08-21",
        "strike": 100.0,
        "underlying_price": 105.0,
        "underlying_price_time": "2026-08-21T15:30:00Z",
        "bid": 4.5,
        "ask": 4.7,
        "contract_size": 100,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _record(prepared, **overrides) -> DatasetRecord:
    defaults = {
        "dataset_id": "dataset-1",
        "run_id": "run-1",
        "created_at": datetime(2026, 8, 21, tzinfo=timezone.utc),
        "provider": "synthetic-provider",
        "schema_version": SCHEMA_VERSION,
        "row_count": 1,
        "format": "csv",
        "location": "/tmp/dataset.csv",
        "content_hash": prepared.content_hash,
    }
    return DatasetRecord(**{**defaults, **overrides})


def test_prepare_validates_exact_serialized_bytes_and_derives_facts():
    prepared = prepare_option_chain_dataset(
        DatasetWrite(_frame(), "synthetic-provider", SCHEMA_VERSION),
        dataset_id="dataset-1",
        checked_at=datetime(2026, 8, 21, tzinfo=timezone.utc),
    )
    assert prepared.integrity.status == "valid"
    assert prepared.integrity.dataset_id == "dataset-1"
    assert prepared.integrity.content_hash == prepared.content_hash
    assert prepared.dataset_facts.content_hash == prepared.content_hash
    assert prepared.dataset_facts.tickers == ("SYNTH",)


def test_prepare_rejects_unsupported_declared_schema_before_publication():
    with pytest.raises(OptionChainSchemaCompatibilityError):
        prepare_option_chain_dataset(
            DatasetWrite(_frame(), "synthetic-provider", SCHEMA_VERSION + 1),
            dataset_id="dataset-1",
        )


def test_prepare_rejects_semantic_corruption_before_serialization():
    with pytest.raises(OptionChainDataIntegrityError) as captured:
        prepare_option_chain_dataset(
            DatasetWrite(_frame(option_type="put"), "synthetic-provider", SCHEMA_VERSION),
            dataset_id="dataset-1",
        )
    assert captured.value.summary.dataset_id is None


def test_stored_hash_mismatch_becomes_invalid_and_clears_facts():
    prepared = prepare_option_chain_dataset(
        DatasetWrite(_frame(), "synthetic-provider", SCHEMA_VERSION),
        dataset_id="dataset-1",
    )
    valid_record = record_with_validated_metadata(
        _record(prepared),
        summary=prepared.integrity,
        facts=prepared.dataset_facts,
    )
    outcome = validate_stored_option_chain_snapshot(valid_record, b"changed")
    assert outcome.frame is None
    assert outcome.record.integrity_status is OptionChainIntegrityStatus.INVALID
    assert outcome.record.dataset_facts_status is OptionChainDatasetFactsStatus.UNKNOWN
    assert outcome.record.dataset_facts is None
    assert (
        outcome.error.summary.counts_by_code[
            OptionChainIntegrityCode.DATASET_CONTENT_HASH_MISMATCH
        ]
        == 1
    )


def test_exact_byte_restoration_can_return_hash_mismatch_record_to_valid():
    prepared = prepare_option_chain_dataset(
        DatasetWrite(_frame(), "synthetic-provider", SCHEMA_VERSION),
        dataset_id="dataset-1",
    )
    original = _record(prepared)
    mismatch = validate_stored_option_chain_snapshot(original, b"changed").record
    restored = validate_stored_option_chain_snapshot(mismatch, prepared.content)
    assert restored.error is None
    assert restored.record.integrity_status is OptionChainIntegrityStatus.VALID
    assert restored.record.dataset_facts_status is OptionChainDatasetFactsStatus.AVAILABLE


def test_effective_integrity_and_facts_states_are_independent():
    prepared = prepare_option_chain_dataset(
        DatasetWrite(_frame(), "synthetic-provider", SCHEMA_VERSION),
        dataset_id="dataset-1",
    )
    valid = record_with_validated_metadata(
        _record(prepared), summary=prepared.integrity, facts=prepared.dataset_facts
    )
    facts_unknown = replace(
        valid,
        dataset_facts_status=OptionChainDatasetFactsStatus.UNKNOWN,
        dataset_facts=None,
    )
    assert evaluate_option_chain_integrity_status(facts_unknown) is OptionChainIntegrityStatus.VALID
    assert evaluate_option_chain_dataset_facts_status(facts_unknown) is OptionChainDatasetFactsStatus.UNKNOWN
    stale_validator = replace(valid, integrity_validator_version=0)
    assert evaluate_option_chain_integrity_status(stale_validator) is OptionChainIntegrityStatus.UNKNOWN
    assert evaluate_option_chain_dataset_facts_status(stale_validator) is OptionChainDatasetFactsStatus.AVAILABLE


def test_additive_metadata_round_trip_and_legacy_defaults():
    prepared = prepare_option_chain_dataset(
        DatasetWrite(_frame(), "synthetic-provider", SCHEMA_VERSION),
        dataset_id="dataset-1",
    )
    valid = record_with_validated_metadata(
        _record(prepared), summary=prepared.integrity, facts=prepared.dataset_facts
    )
    parsed = record_integrity_from_mapping(record_integrity_to_dict(valid))
    assert parsed["integrity_summary"] == valid.integrity_summary
    assert parsed["dataset_facts"] == valid.dataset_facts
    legacy = record_integrity_from_mapping({})
    assert legacy["integrity_status"] is OptionChainIntegrityStatus.UNKNOWN
    assert legacy["dataset_facts_status"] is OptionChainDatasetFactsStatus.UNKNOWN
