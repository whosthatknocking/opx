"""Dataset serializer protocol and implementations."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Protocol

import pandas as pd

from opx_chain.storage.atomic import atomic_write_bytes


class DatasetSerializer(Protocol):  # pylint: disable=too-few-public-methods
    """Serialize a DataFrame to a file path. Returns bytes written."""

    format: str  # "csv" | "parquet"

    def serialize_bytes(self, df: pd.DataFrame) -> bytes: ...  # pylint: disable=missing-function-docstring

    def deserialize_bytes(self, content: bytes) -> pd.DataFrame: ...  # pylint: disable=missing-function-docstring

    def serialize(self, df: pd.DataFrame, path: str) -> int: ...  # pylint: disable=missing-function-docstring


class CsvSerializer:  # pylint: disable=too-few-public-methods
    """CSV implementation of DatasetSerializer."""

    format = "csv"

    def serialize_bytes(self, df: pd.DataFrame) -> bytes:
        """Return df serialized as CSV bytes."""
        return df.to_csv(index=False).encode("utf-8")

    def serialize(self, df: pd.DataFrame, path: str) -> int:
        """Write df to path as CSV. Returns bytes written."""
        content = self.serialize_bytes(df)
        atomic_write_bytes(Path(path), content)
        return len(content)

    def deserialize_bytes(self, content: bytes) -> pd.DataFrame:
        """Read one caller-owned DataFrame from CSV bytes."""
        return pd.read_csv(BytesIO(content), low_memory=False)


class ParquetSerializer:  # pylint: disable=too-few-public-methods
    """Parquet implementation of DatasetSerializer. Requires pyarrow."""

    format = "parquet"

    def __init__(self) -> None:
        ensure_parquet_available()

    def serialize_bytes(self, df: pd.DataFrame) -> bytes:
        """Return df serialized as Parquet bytes."""
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        return buffer.getvalue()

    def serialize(self, df: pd.DataFrame, path: str) -> int:
        """Write df to path as Parquet. Returns bytes written."""
        content = self.serialize_bytes(df)
        atomic_write_bytes(Path(path), content)
        return len(content)

    def deserialize_bytes(self, content: bytes) -> pd.DataFrame:
        """Read one caller-owned DataFrame from Parquet bytes."""
        return pd.read_parquet(BytesIO(content), engine="pyarrow")


_SERIALIZERS: dict[str, DatasetSerializer] = {
    CsvSerializer.format: CsvSerializer(),
}


def get_serializer(fmt: str) -> DatasetSerializer:
    """Return the serializer for the given format name."""
    if fmt == ParquetSerializer.format:
        return ParquetSerializer()
    if fmt in _SERIALIZERS:
        return _SERIALIZERS[fmt]
    raise ValueError(f"Unsupported dataset format: {fmt!r}")


def ensure_parquet_available() -> None:
    """Raise a consistent error when the optional parquet dependency is missing."""
    try:
        import pyarrow as _pyarrow  # pylint: disable=import-outside-toplevel
        del _pyarrow
    except ImportError as exc:
        raise RuntimeError(
            "Parquet serialization requires pyarrow. "
            "Install it with: pip install 'opx-chain[parquet]'"
        ) from exc
