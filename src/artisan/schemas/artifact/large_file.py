"""Large-file artifact schema for external one-to-one file storage.

Each artifact represents a single large file stored externally.
Content lives at external_path; Delta stores only metadata (hash,
size, name). For files too large to embed in Parquet: model weights,
embedding matrices, simulation outputs, HDF5 datasets.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, ClassVar, Self

import polars as pl
from pydantic import Field

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.common import metadata_from_json, metadata_to_json
from artisan.schemas.artifact.registry import ArtifactTypeDef


class LargeFileArtifact(Artifact):
    """Artifact representing a large file stored externally.

    Content lives at external_path. Delta stores only metadata
    (hash, size, name). For files too large to embed in Parquet:
    model weights, embedding matrices, simulation outputs.
    """

    POLARS_SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "content_hash": pl.String,
        "size_bytes": pl.Int64,
        "original_name": pl.String,
        "extension": pl.String,
        "metadata": pl.String,
        "external_path": pl.String,
    }

    artifact_type: str = Field(default="large_file", frozen=True)
    content_hash: str | None = Field(
        default=None,
        description="Hash of the file bytes.",
    )
    size_bytes: int | None = Field(
        default=None,
        ge=0,
        description="File size in bytes.",
    )
    original_name: str | None = Field(
        default=None,
        description="Human-readable filename stem.",
    )
    extension: str | None = Field(
        default=None,
        description="File extension (e.g., .bin, .npy, .pt).",
    )

    _default_hydrate: ClassVar[bool] = False

    def _finalize_content(self) -> bytes | None:
        """Hash metadata including external_path for content-addressed ID.

        The same file at different locations produces distinct artifact IDs.
        """
        if self.content_hash is None:
            return None
        return json.dumps(
            {
                "content_hash": self.content_hash,
                "external_path": self.external_path,
            },
            sort_keys=True,
        ).encode("utf-8")

    def _materialize_content(self, directory: Path, *, fs: Any = None) -> Path:
        """Copy the file from external_path to the target directory.

        Uses artifact_id as filename (per Design 1 convention).

        Args:
            directory: Target directory for the output file.
            fs: Optional fsspec filesystem for reading source from cloud.

        Returns:
            Path to the copied file.

        Raises:
            ValueError: If external_path is not set.
        """
        if self.external_path is None:
            msg = "Cannot materialize: external_path not set"
            raise ValueError(msg)
        filename = f"{self.artifact_id}{self.extension or ''}"
        dest = directory / filename
        if fs is not None:
            fs.get(self.external_path, str(dest))
        else:
            shutil.copy2(self.external_path, str(dest))
        self.materialized_path = dest
        return dest

    @classmethod
    def draft(
        cls,
        content_hash: str,
        size_bytes: int,
        step_number: int,
        external_path: str,
        original_name: str | None = None,
        extension: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> LargeFileArtifact:
        """Create a draft large-file artifact.

        Args:
            content_hash: xxh3_128 hash of the file bytes.
            size_bytes: File size in bytes.
            step_number: Pipeline step number.
            external_path: Path to the file in files_root.
            original_name: Human-readable filename stem.
            extension: File extension (e.g., .bin, .npy).
            metadata: Optional metadata dict.
        """
        return cls(
            artifact_id=None,
            origin_step_number=step_number,
            content_hash=content_hash,
            size_bytes=size_bytes,
            external_path=external_path,
            original_name=original_name,
            extension=extension,
            metadata=metadata or {},
        )

    def to_row(self) -> dict[str, Any]:
        """Serialize to a flat dict matching POLARS_SCHEMA columns."""
        return {
            "artifact_id": self.artifact_id,
            "origin_step_number": self.origin_step_number,
            "content_hash": self.content_hash,
            "size_bytes": self.size_bytes,
            "original_name": self.original_name,
            "extension": self.extension,
            "metadata": metadata_to_json(self.metadata),
            "external_path": self.external_path,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Self:
        """Reconstruct from a Parquet row dict.

        Args:
            row: Dict with keys matching POLARS_SCHEMA columns.
        """
        return cls(
            artifact_id=row["artifact_id"],
            origin_step_number=row.get("origin_step_number"),
            content_hash=row.get("content_hash"),
            size_bytes=row.get("size_bytes"),
            original_name=row.get("original_name"),
            extension=row.get("extension"),
            metadata=metadata_from_json(row.get("metadata")),
            external_path=row.get("external_path"),
        )


class LargeFileTypeDef(ArtifactTypeDef):
    """Type definition for LargeFileArtifact."""

    key = "large_file"
    table_path = "artifacts/large_files"
    model = LargeFileArtifact
