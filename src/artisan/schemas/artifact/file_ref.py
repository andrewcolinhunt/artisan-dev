"""File-reference artifact schema.

Stores a reference to an external file by path and content hash,
without embedding the file bytes in Delta Lake storage.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar, Self

import polars as pl
from pydantic import Field, PrivateAttr

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.types import ArtifactTypes


class FileRefArtifact(Artifact):
    """Artifact referencing an external file by path and content hash.

    Unlike content-embedding artifacts (DataArtifact, MetricArtifact),
    this stores a pointer to the file rather than its bytes. The
    ``content_hash`` links back to the output artifact that produced
    the file.
    """

    POLARS_SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "content_hash": pl.String,
        "path": pl.String,
        "size_bytes": pl.Int64,
        "original_name": pl.String,
        "extension": pl.String,
        "metadata": pl.String,
        "external_path": pl.String,
    }

    artifact_type: str = Field(
        default=ArtifactTypes.FILE_REF,
        frozen=True,
    )
    content_hash: str | None = Field(
        default=None,
        description="xxh3_128 hash of file bytes (links to output artifact). "
        "None for ID-only artifacts.",
    )
    path: str | None = Field(
        default=None,
        description="Original file path. None for ID-only artifacts.",
    )
    size_bytes: int | None = Field(
        default=None,
        ge=0,
        description="File size at submission time. None for ID-only artifacts.",
    )
    original_name: str | None = Field(
        default=None,
        description="Original filename stem for lineage inference.",
    )
    extension: str | None = Field(
        default=None,
        description="File extension from original path.",
    )

    _cached_content: bytes | None = PrivateAttr(default=None)

    def read_content(self) -> bytes:
        """Read and cache file content from the original path.

        Raises:
            ValueError: If path is None (not hydrated).
        """
        if self._cached_content is None:
            if self.path is None:
                raise ValueError("Cannot read content: artifact not hydrated")
            self._cached_content = Path(self.path).read_bytes()
        return self._cached_content

    def _materialize_content(self, directory: Path) -> Path:
        """Copy the referenced file into the given directory.

        Args:
            directory: Target directory for the output file.

        Returns:
            Path to the written file.

        Raises:
            ValueError: If path is None (not hydrated).
        """
        if self.path is None:
            raise ValueError("Cannot materialize: artifact not hydrated")
        path = directory / Path(self.path).name
        path.write_bytes(self.read_content())
        self.materialized_path = path
        return path

    @classmethod
    def draft(
        cls,
        path: str,
        content_hash: str,
        size_bytes: int,
        step_number: int,
        metadata: dict[str, Any] | None = None,
        original_name: str | None = None,
        extension: str | None = None,
    ) -> FileRefArtifact:
        """Create a draft file-reference artifact.

        Args:
            path: Filesystem path to the referenced file.
            content_hash: xxh3_128 hash of the file bytes.
            size_bytes: File size at submission time.
            step_number: Pipeline step number.
            metadata: Optional metadata dict.
            original_name: Filename stem for lineage inference.
            extension: File extension from original path.
        """
        return cls(
            artifact_id=None,
            origin_step_number=step_number,
            path=path,
            content_hash=content_hash,
            size_bytes=size_bytes,
            metadata=metadata or {},
            original_name=original_name,
            extension=extension,
        )

    def _finalize_content(self) -> bytes | None:
        """Return reference metadata bytes for hashing.

        Hashes ``content_hash``, ``path``, and ``size_bytes`` to
        produce a deterministic artifact ID.
        """
        if self.content_hash is None:
            return None
        return json.dumps(
            {
                "content_hash": self.content_hash,
                "path": self.path,
                "size_bytes": self.size_bytes,
            },
            sort_keys=True,
        ).encode("utf-8")

    def to_row(self) -> dict[str, Any]:
        """Serialize to a flat dict matching POLARS_SCHEMA columns.

        JSON-encodes ``metadata`` for Parquet storage.
        """
        return {
            "artifact_id": self.artifact_id,
            "origin_step_number": self.origin_step_number,
            "content_hash": self.content_hash,
            "path": self.path,
            "size_bytes": self.size_bytes,
            "original_name": self.original_name,
            "extension": self.extension,
            "metadata": json.dumps(self.metadata or {}),
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
        metadata_raw = row.get("metadata")
        return cls(
            artifact_id=row["artifact_id"],
            origin_step_number=row.get("origin_step_number"),
            content_hash=row.get("content_hash"),
            path=row.get("path"),
            size_bytes=row.get("size_bytes"),
            original_name=row.get("original_name"),
            extension=row.get("extension"),
            metadata=json.loads(metadata_raw) if metadata_raw else {},
            external_path=row.get("external_path"),
        )
