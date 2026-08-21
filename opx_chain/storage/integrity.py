"""Shared storage-side option-chain validation and metadata transitions."""
# pylint: disable=too-many-arguments

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Mapping

import pandas as pd

from opx_chain import SCHEMA_VERSION
from opx_chain._integrity_validation import (
    project_option_chain_integrity_summary,
    validate_option_chain_frame,
)
from opx_chain.integrity import (
    OPTION_CHAIN_INTEGRITY_SUMMARY_SCHEMA_VERSION,
    OPTION_CHAIN_INTEGRITY_VALIDATOR_VERSION,
    OptionChainDataIntegrityError,
    OptionChainDatasetFacts,
    OptionChainDatasetFactsStatus,
    OptionChainIntegrityBoundary,
    OptionChainIntegrityCode,
    OptionChainIntegrityFinding,
    OptionChainIntegritySeverity,
    OptionChainIntegrityStatus,
    OptionChainIntegritySummary,
    OptionChainSchemaCompatibilityError,
    ValidatedOptionChainDataset,
    canonical_option_contract_key,
    compute_option_chain_dataset_facts,
    evaluate_option_chain_dataset_facts_status,
    evaluate_option_chain_integrity_status,
)
from opx_chain.storage._disk import content_hash_for_bytes
from opx_chain.storage.models import DatasetRecord, DatasetWrite, record_to_handle
from opx_chain.storage.serializers import get_serializer
from opx_chain.timestamps import datetime_to_iso, iso_to_datetime


SUPPORTED_OPTION_CHAIN_SCHEMA_VERSIONS = (SCHEMA_VERSION,)


@dataclass(frozen=True)
class PreparedOptionChainDataset:
    """Exact serialized bytes plus metadata proven before publication."""

    content: bytes
    content_hash: str
    frame: pd.DataFrame
    integrity: OptionChainIntegritySummary
    dataset_facts: OptionChainDatasetFacts


@dataclass(frozen=True)
class StoredOptionChainValidation:
    """Complete metadata replacement and optional usable checked frame."""

    record: DatasetRecord
    frame: pd.DataFrame | None
    error: OptionChainDataIntegrityError | None


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _schema_compatible(dataset_id: str, schema_version: int) -> None:
    if schema_version not in SUPPORTED_OPTION_CHAIN_SCHEMA_VERSIONS:
        raise OptionChainSchemaCompatibilityError(
            dataset_id,
            schema_version,
            SUPPORTED_OPTION_CHAIN_SCHEMA_VERSIONS,
        )


def _serialization_error(
    *,
    code: OptionChainIntegrityCode,
    boundary: OptionChainIntegrityBoundary,
    dataset_id: str | None,
    content_hash: str | None,
    provider: str,
    total_rows: int,
    expected: object,
    actual: object,
    checked_at: datetime,
) -> OptionChainDataIntegrityError:
    finding = OptionChainIntegrityFinding(
        severity=OptionChainIntegritySeverity.FATAL,
        boundary=boundary,
        code=code,
        row_index=None,
        ticker=None,
        contract_symbol=None,
        field="artifact",
        expected=str(expected),
        actual=str(actual),
    )
    summary = project_option_chain_integrity_summary(
        (finding,),
        total_rows=total_rows,
        checked_at=checked_at,
        dataset_id=dataset_id,
        content_hash=content_hash,
        provider=provider,
    )
    return OptionChainDataIntegrityError(summary)


def _canonical_identities(frame: pd.DataFrame) -> tuple[tuple, ...]:
    return tuple(
        canonical_option_contract_key(
            row.underlying_symbol,
            row.option_type,
            row.strike,
            row.expiration_date,
        )
        for row in frame.itertuples(index=False)
    )


def prepare_option_chain_dataset(
    dataset: DatasetWrite,
    *,
    dataset_id: str,
    checked_at: datetime | None = None,
) -> PreparedOptionChainDataset:
    """Validate, serialize, reparse, and validate exactly what storage will publish."""
    checked_at = checked_at or _utc_now()
    _schema_compatible(dataset_id, dataset.schema_version)
    validate_option_chain_frame(
        dataset.data,
        boundary=OptionChainIntegrityBoundary.EXPORT,
        checked_at=checked_at,
        provider=dataset.provider,
    )
    serializer = get_serializer(dataset.format)
    try:
        content = serializer.serialize_bytes(dataset.data)
        content_hash = content_hash_for_bytes(content)
        frame = serializer.deserialize_bytes(content)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        raise _serialization_error(
            code=OptionChainIntegrityCode.SERIALIZATION_INTEGRITY_FAILED,
            boundary=OptionChainIntegrityBoundary.SERIALIZED_ARTIFACT,
            dataset_id=None,
            content_hash=None,
            provider=dataset.provider,
            total_rows=len(dataset.data),
            expected=f"round-trip {dataset.format} artifact",
            actual=type(exc).__name__,
            checked_at=checked_at,
        ) from exc
    try:
        summary = validate_option_chain_frame(
            frame,
            boundary=OptionChainIntegrityBoundary.SERIALIZED_ARTIFACT,
            checked_at=checked_at,
            dataset_id=dataset_id,
            content_hash=content_hash,
            provider=dataset.provider,
        )
        if len(frame) != len(dataset.data):
            raise ValueError(f"row count {len(dataset.data)} became {len(frame)}")
        if _canonical_identities(frame) != _canonical_identities(dataset.data):
            raise ValueError("canonical contract identity or row order changed")
    except OptionChainDataIntegrityError:
        raise
    except Exception as exc:  # pylint: disable=broad-exception-caught
        raise _serialization_error(
            code=OptionChainIntegrityCode.SERIALIZATION_INTEGRITY_FAILED,
            boundary=OptionChainIntegrityBoundary.SERIALIZED_ARTIFACT,
            dataset_id=dataset_id,
            content_hash=content_hash,
            provider=dataset.provider,
            total_rows=len(frame),
            expected="same row count, order, and canonical identities",
            actual=str(exc),
            checked_at=checked_at,
        ) from exc
    return PreparedOptionChainDataset(
        content=content,
        content_hash=content_hash,
        frame=frame,
        integrity=summary,
        dataset_facts=compute_option_chain_dataset_facts(
            frame,
            content_hash=content_hash,
        ),
    )


def record_with_validated_metadata(
    record: DatasetRecord,
    *,
    summary: OptionChainIntegritySummary,
    facts: OptionChainDatasetFacts,
) -> DatasetRecord:
    """Return the complete current valid metadata replacement."""
    if summary.status != "valid" or summary.content_hash != record.content_hash:
        raise ValueError("valid metadata must bind to the immutable dataset hash")
    if facts.content_hash != record.content_hash:
        raise ValueError("dataset facts must bind to the immutable dataset hash")
    return replace(
        record,
        integrity_status=OptionChainIntegrityStatus.VALID,
        integrity_schema_version=OPTION_CHAIN_INTEGRITY_SUMMARY_SCHEMA_VERSION,
        integrity_validator_version=OPTION_CHAIN_INTEGRITY_VALIDATOR_VERSION,
        integrity_checked_at=summary.checked_at,
        integrity_content_hash=summary.content_hash,
        integrity_summary=summary,
        dataset_facts_status=OptionChainDatasetFactsStatus.AVAILABLE,
        dataset_facts=facts,
    )


def record_with_invalid_metadata(
    record: DatasetRecord,
    *,
    summary: OptionChainIntegritySummary,
) -> DatasetRecord:
    """Return the complete current invalid metadata replacement."""
    if summary.status != "invalid":
        raise ValueError("invalid metadata requires an invalid summary")
    return replace(
        record,
        integrity_status=OptionChainIntegrityStatus.INVALID,
        integrity_schema_version=OPTION_CHAIN_INTEGRITY_SUMMARY_SCHEMA_VERSION,
        integrity_validator_version=OPTION_CHAIN_INTEGRITY_VALIDATOR_VERSION,
        integrity_checked_at=summary.checked_at,
        integrity_content_hash=summary.content_hash,
        integrity_summary=summary,
        dataset_facts_status=OptionChainDatasetFactsStatus.UNKNOWN,
        dataset_facts=None,
    )


def validate_stored_option_chain_snapshot(
    record: DatasetRecord,
    content: bytes,
    *,
    checked_at: datetime | None = None,
) -> StoredOptionChainValidation:
    """Validate one immutable byte snapshot and return its durable transition."""
    checked_at = checked_at or _utc_now()
    _schema_compatible(record.dataset_id, record.schema_version)
    actual_hash = content_hash_for_bytes(content)
    if actual_hash != record.content_hash:
        error = _serialization_error(
            code=OptionChainIntegrityCode.DATASET_CONTENT_HASH_MISMATCH,
            boundary=OptionChainIntegrityBoundary.STORED_ARTIFACT,
            dataset_id=record.dataset_id,
            content_hash=actual_hash,
            provider=record.provider,
            total_rows=record.row_count,
            expected=record.content_hash,
            actual=actual_hash,
            checked_at=checked_at,
        )
        return StoredOptionChainValidation(
            record=record_with_invalid_metadata(record, summary=error.summary),
            frame=None,
            error=error,
        )
    serializer = get_serializer(record.format)
    try:
        frame = serializer.deserialize_bytes(content)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        error = _serialization_error(
            code=OptionChainIntegrityCode.DATASET_SCHEMA_INVALID,
            boundary=OptionChainIntegrityBoundary.STORED_ARTIFACT,
            dataset_id=record.dataset_id,
            content_hash=actual_hash,
            provider=record.provider,
            total_rows=record.row_count,
            expected=f"parseable {record.format} artifact",
            actual=type(exc).__name__,
            checked_at=checked_at,
        )
        return StoredOptionChainValidation(
            record=record_with_invalid_metadata(record, summary=error.summary),
            frame=None,
            error=error,
        )
    try:
        summary = validate_option_chain_frame(
            frame,
            boundary=OptionChainIntegrityBoundary.STORED_ARTIFACT,
            checked_at=checked_at,
            dataset_id=record.dataset_id,
            content_hash=actual_hash,
            provider=record.provider,
        )
        if len(frame) != record.row_count:
            raise ValueError(f"declared {record.row_count} rows; parsed {len(frame)}")
    except OptionChainDataIntegrityError as error:
        return StoredOptionChainValidation(
            record=record_with_invalid_metadata(record, summary=error.summary),
            frame=None,
            error=error,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        error = _serialization_error(
            code=OptionChainIntegrityCode.DATASET_SCHEMA_INVALID,
            boundary=OptionChainIntegrityBoundary.STORED_ARTIFACT,
            dataset_id=record.dataset_id,
            content_hash=actual_hash,
            provider=record.provider,
            total_rows=len(frame),
            expected=f"row_count={record.row_count}",
            actual=str(exc),
            checked_at=checked_at,
        )
        return StoredOptionChainValidation(
            record=record_with_invalid_metadata(record, summary=error.summary),
            frame=None,
            error=error,
        )
    facts = compute_option_chain_dataset_facts(frame, content_hash=actual_hash)
    return StoredOptionChainValidation(
        record=record_with_validated_metadata(record, summary=summary, facts=facts),
        frame=frame,
        error=None,
    )


def validated_dataset_from_outcome(
    outcome: StoredOptionChainValidation,
) -> ValidatedOptionChainDataset:
    """Return a public checked result or raise the outcome's typed error."""
    if outcome.error is not None:
        raise outcome.error
    if (
        outcome.frame is None
        or outcome.record.integrity_summary is None
        or outcome.record.dataset_facts is None
    ):
        raise RuntimeError("validated option-chain outcome is incomplete")
    return ValidatedOptionChainDataset(
        handle=record_to_handle(outcome.record),
        frame=outcome.frame,
        integrity=outcome.record.integrity_summary,
        dataset_facts=outcome.record.dataset_facts,
    )


def record_integrity_to_dict(record: DatasetRecord) -> dict[str, object]:
    """Serialize the additive integrity/facts metadata fields losslessly."""
    integrity_status = evaluate_option_chain_integrity_status(record)
    facts_status = evaluate_option_chain_dataset_facts_status(record)
    return {
        "integrity_status": integrity_status.value,
        "integrity_schema_version": record.integrity_schema_version,
        "integrity_validator_version": record.integrity_validator_version,
        "integrity_checked_at": datetime_to_iso(record.integrity_checked_at),
        "integrity_content_hash": record.integrity_content_hash,
        "integrity_summary": (
            record.integrity_summary.to_dict()
            if isinstance(record.integrity_summary, OptionChainIntegritySummary)
            else None
        ),
        "dataset_facts_status": facts_status.value,
        "dataset_facts": (
            record.dataset_facts.to_dict()
            if isinstance(record.dataset_facts, OptionChainDatasetFacts)
            else None
        ),
    }


def record_integrity_from_mapping(data: Mapping[str, object]) -> dict[str, object]:
    """Parse additive metadata, defaulting absent or malformed legacy values safely."""
    try:
        integrity_status = OptionChainIntegrityStatus(
            data.get("integrity_status", OptionChainIntegrityStatus.UNKNOWN.value)
        )
    except (TypeError, ValueError):
        integrity_status = OptionChainIntegrityStatus.UNKNOWN
    try:
        facts_status = OptionChainDatasetFactsStatus(
            data.get("dataset_facts_status", OptionChainDatasetFactsStatus.UNKNOWN.value)
        )
    except (TypeError, ValueError):
        facts_status = OptionChainDatasetFactsStatus.UNKNOWN
    try:
        summary_value = data.get("integrity_summary")
        summary = (
            OptionChainIntegritySummary.from_dict(summary_value)
            if isinstance(summary_value, Mapping)
            else None
        )
    except (KeyError, TypeError, ValueError):
        summary = None
    try:
        facts_value = data.get("dataset_facts")
        facts = (
            OptionChainDatasetFacts.from_dict(facts_value)
            if isinstance(facts_value, Mapping)
            else None
        )
    except (KeyError, TypeError, ValueError):
        facts = None
    try:
        checked_at = iso_to_datetime(data.get("integrity_checked_at"))
    except (TypeError, ValueError):
        checked_at = None
    return {
        "integrity_status": integrity_status,
        "integrity_schema_version": data.get("integrity_schema_version"),
        "integrity_validator_version": data.get("integrity_validator_version"),
        "integrity_checked_at": checked_at,
        "integrity_content_hash": data.get("integrity_content_hash"),
        "integrity_summary": summary,
        "dataset_facts_status": facts_status,
        "dataset_facts": facts,
    }
