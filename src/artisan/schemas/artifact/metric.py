"""Metric artifact schema.

Stores JSON-encoded measurement values (scores, statistics, etc.)
as content-addressed artifacts.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar, Self

import polars as pl
from pydantic import Field

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.common import (
    JsonContentMixin,
    get_compound_extension,
    metadata_from_json,
    metadata_to_json,
)
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.utils.filename import strip_extensions


class MetricArtifact(JsonContentMixin, Artifact):
    """Artifact storing JSON-encoded measurement values.

    Holds key-value metric data (scores, statistics, properties)
    serialized as JSON bytes.
    """

    POLARS_SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "content": pl.Binary,
        "original_name": pl.String,
        "extension": pl.String,
        "metadata": pl.String,
        "external_path": pl.String,
    }

    artifact_type: str = Field(
        default=ArtifactTypes.METRIC,
        frozen=True,
    )
    content: bytes | None = Field(
        default=None,
        description="JSON-encoded metric values. None for ID-only artifacts.",
    )
    original_name: str | None = Field(
        default=None,
        description="Key name for lineage inference (stem only, no extension)",
    )
    extension: str | None = Field(
        default=None,
        description="File extension (.json typically). None for ID-only artifacts.",
    )

    def _materialize_content(self, directory: Path) -> Path:
        """Write metric JSON to a file in the given directory.

        Args:
            directory: Target directory for the output file.

        Returns:
            Path to the written file.

        Raises:
            ValueError: If content is None.
        """
        if self.content is None:
            msg = "Cannot materialize: artifact not hydrated"
            raise ValueError(msg)
        if self.original_name:
            filename = f"{self.original_name}{self.extension or '.json'}"
        else:
            filename = f"{self.artifact_id}.json"
        path = directory / filename
        path.write_bytes(self.content)
        self.materialized_path = path
        return path

    @classmethod
    def draft(
        cls,
        content: dict[str, Any],
        original_name: str,
        step_number: int,
        metadata: dict[str, Any] | None = None,
    ) -> MetricArtifact:
        """Create a draft from a metric values dict.

        Args:
            content: Metric key-value pairs (JSON-serializable).
            original_name: Filename for lineage inference (extensions stripped).
            step_number: Pipeline step number.
            metadata: Optional metadata dict.
        """
        encoded = json.dumps(content, sort_keys=True).encode("utf-8")
        return cls(
            artifact_id=None,
            origin_step_number=step_number,
            content=encoded,
            original_name=strip_extensions(original_name),
            extension=get_compound_extension(original_name),
            metadata=metadata or {},
        )

    def to_row(self) -> dict[str, Any]:
        """Serialize to a flat dict matching POLARS_SCHEMA columns.

        JSON-encodes ``metadata`` for Parquet storage.
        """
        return {
            "artifact_id": self.artifact_id,
            "origin_step_number": self.origin_step_number,
            "content": self.content,
            "original_name": self.original_name,
            "extension": self.extension,
            "metadata": metadata_to_json(self.metadata),
            "external_path": self.external_path,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Self:
        """Reconstruct from a Parquet row dict.

        Reverses the JSON encoding applied by ``to_row`` for
        ``metadata``.

        Args:
            row: Dict with keys matching POLARS_SCHEMA columns.
        """
        return cls(
            artifact_id=row["artifact_id"],
            origin_step_number=row.get("origin_step_number"),
            content=row.get("content"),
            original_name=row.get("original_name"),
            extension=row.get("extension"),
            metadata=metadata_from_json(row.get("metadata")),
            external_path=row.get("external_path"),
        )
