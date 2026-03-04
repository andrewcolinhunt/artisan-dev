"""Data artifact schema for generic CSV-based example operations.

Provides a domain-agnostic artifact type that stores tabular CSV data
with parsed column names and row counts.
"""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Any, ClassVar, Self

import polars as pl
from pydantic import Field

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.common import get_compound_extension
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.utils.filename import strip_extensions


class DataArtifact(Artifact):
    """Generic data artifact for CSV content.

    Stores raw CSV bytes along with parsed column names and row count.
    Designed as a domain-agnostic artifact type for example operations.
    """

    POLARS_SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "content": pl.Binary,
        "original_name": pl.String,
        "extension": pl.String,
        "size_bytes": pl.Int64,
        "columns": pl.String,
        "row_count": pl.Int32,
        "metadata": pl.String,
        "external_path": pl.String,
    }

    artifact_type: str = Field(default="data", frozen=True)
    content: bytes | None = Field(
        default=None,
        description="Raw CSV bytes. None for ID-only artifacts.",
    )
    original_name: str | None = Field(
        default=None,
        description="Original filename stem (no extension).",
    )
    extension: str | None = Field(
        default=None,
        description="File extension (e.g. '.csv').",
    )
    size_bytes: int | None = Field(
        default=None,
        ge=0,
        description="Content size in bytes.",
    )
    columns: list[str] | None = Field(
        default=None,
        description="CSV column headers.",
    )
    row_count: int | None = Field(
        default=None,
        ge=0,
        description="Number of data rows (excluding header).",
    )

    def _materialize_content(self, directory: Path) -> Path:
        """Write CSV content to a file in the given directory.

        Args:
            directory: Target directory for the output file.

        Returns:
            Path to the written file.

        Raises:
            ValueError: If content is None or original_name is not set.
        """
        if self.content is None:
            raise ValueError("Cannot materialize: artifact not hydrated")
        if self.original_name is None:
            raise ValueError("Cannot materialize: original_name not set")
        filename = f"{self.original_name}{self.extension or '.csv'}"
        path = directory / filename
        path.write_bytes(self.content)
        self.materialized_path = path
        return path

    @classmethod
    def draft(
        cls,
        content: bytes,
        original_name: str,
        step_number: int,
        metadata: dict[str, Any] | None = None,
        external_path: str | None = None,
    ) -> DataArtifact:
        """Create a draft DataArtifact, parsing CSV headers and row count.

        Args:
            content: Raw CSV bytes.
            original_name: Original filename (extensions will be stripped).
            step_number: Pipeline step number.
            metadata: Optional metadata dict.
            external_path: Optional path to the original external file.

        Returns:
            Draft DataArtifact with columns and row_count populated.
        """
        columns, row_count = _parse_csv_metadata(content)
        return cls(
            artifact_id=None,
            origin_step_number=step_number,
            content=content,
            original_name=strip_extensions(original_name),
            extension=get_compound_extension(original_name),
            size_bytes=len(content),
            columns=columns,
            row_count=row_count,
            metadata=metadata or {},
            external_path=external_path,
        )

    def to_row(self) -> dict[str, Any]:
        """Serialize to a flat dict matching POLARS_SCHEMA columns.

        JSON-encodes ``columns`` and ``metadata`` for Parquet storage.
        """
        return {
            "artifact_id": self.artifact_id,
            "origin_step_number": self.origin_step_number,
            "content": self.content,
            "original_name": self.original_name,
            "extension": self.extension,
            "size_bytes": self.size_bytes,
            "columns": json.dumps(self.columns) if self.columns is not None else None,
            "row_count": self.row_count,
            "metadata": json.dumps(self.metadata or {}),
            "external_path": self.external_path,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Self:
        """Reconstruct a DataArtifact from a Parquet row dict.

        Reverses the JSON encoding applied by ``to_row`` for
        ``columns`` and ``metadata``.

        Args:
            row: Dict with keys matching POLARS_SCHEMA columns.
        """
        metadata_raw = row.get("metadata")
        columns_raw = row.get("columns")
        return cls(
            artifact_id=row["artifact_id"],
            origin_step_number=row.get("origin_step_number"),
            content=row.get("content"),
            original_name=row.get("original_name"),
            extension=row.get("extension"),
            size_bytes=row.get("size_bytes"),
            columns=json.loads(columns_raw) if columns_raw else None,
            row_count=row.get("row_count"),
            metadata=json.loads(metadata_raw) if metadata_raw else {},
            external_path=row.get("external_path"),
        )


def _parse_csv_metadata(content: bytes) -> tuple[list[str], int]:
    """Parse CSV headers and count data rows.

    Args:
        content: Raw CSV bytes.

    Returns:
        Tuple of (column_names, row_count).
    """
    text = content.decode("utf-8")
    if not text.strip():
        return [], 0
    reader = csv.reader(io.StringIO(text))
    try:
        headers = next(reader)
    except StopIteration:
        return [], 0
    row_count = sum(1 for _ in reader)
    return headers, row_count


class DataTypeDef(ArtifactTypeDef):
    """Type definition for DataArtifact."""

    key = "data"
    table_path = "artifacts/data"
    model = DataArtifact
