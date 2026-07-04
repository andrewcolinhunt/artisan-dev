"""Metric artifact schema.

Stores JSON-encoded measurement values (scores, statistics, etc.)
as content-addressed artifacts.
"""

from __future__ import annotations

import json
import os
from typing import Any, ClassVar

import polars as pl
from pydantic import Field

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.common import (
    JsonContentMixin,
    get_compound_extension,
)
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.utils.filename import strip_extensions


class MetricArtifact(JsonContentMixin, Artifact):
    """Artifact storing JSON-encoded measurement values.

    Holds key-value metric data (scores, statistics, properties)
    serialized as JSON bytes.
    """

    POLARS_SCHEMA: ClassVar[dict[str, type[pl.DataType]]] = {
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

    def _materialize_content(self, directory: str, *, fs: Any = None) -> str:
        """Write metric JSON to a file in the given directory.

        Args:
            directory: Target directory for the output file.

        Returns:
            Path to the written file.

        Raises:
            ValueError: If content is None or artifact_id is not set.
        """
        if self.content is None:
            msg = "Cannot materialize: artifact not hydrated"
            raise ValueError(msg)
        if self.artifact_id is None:
            msg = "Cannot materialize: artifact not finalized (no artifact_id)"
            raise ValueError(msg)
        filename = f"{self.artifact_id}{self.extension or '.json'}"
        path = os.path.join(directory, filename)
        with open(path, "wb") as f:
            f.write(self.content)
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
