"""Large-file artifact schema for external one-to-one file storage.

Each artifact represents a single large file stored externally.
Content lives at external_path; Delta stores only metadata (hash,
size, name). For files too large to embed in Parquet: model weights,
embedding matrices, simulation outputs, HDF5 datasets.
"""

from __future__ import annotations

import os
from typing import Any, ClassVar

import polars as pl
from pydantic import Field

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external import copy_verified_file, verify_complete_file
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.utils.hashing import canonical_json_bytes


class LargeFileArtifact(Artifact):
    """Artifact representing a large file stored externally.

    Content lives at external_path. Delta stores only metadata
    (hash, size, name). For files too large to embed in Parquet:
    model weights, embedding matrices, simulation outputs.
    """

    POLARS_SCHEMA: ClassVar[dict[str, type[pl.DataType]]] = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "content_hash": pl.String,
        "size_bytes": pl.Int64,
        "original_name": pl.String,
        "extension": pl.String,
        "metadata": pl.String,
    }

    EXTERNALLY_BACKED: ClassVar[bool] = True
    LOCATOR_FIELDS: ClassVar[frozenset[str]] = frozenset({"external_path"})

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

    def _identity_payload(self) -> bytes | None:
        """Return the location-independent byte descriptor."""
        if self.content_hash is None or self.size_bytes is None:
            return None
        return canonical_json_bytes(
            {
                "content_hash": self.content_hash,
                "size_bytes": self.size_bytes,
            }
        )

    def verify_external_content(self, *, fs: Any = None) -> None:
        """Stream the external file and verify its digest and size."""
        self._assert_identity_intact()
        if self.external_path is None:
            msg = "Cannot verify LargeFileArtifact without a location"
            raise ValueError(msg)
        verify_complete_file(
            artifact_id=self.artifact_id,
            artifact_type=self.artifact_type,
            uri=self.external_path,
            expected_digest=self.content_hash,
            expected_size=self.size_bytes,
            fs=fs,
        )

    def _materialize_content(self, directory: str, *, fs: Any = None) -> str:
        """Copy the file from external_path to the target directory.

        Uses artifact_id as the output filename.

        Args:
            directory: Target directory for the output file.
            fs: Optional fsspec filesystem for reading source from cloud.
                When None, infers fs from ``self.external_path`` via
                ``fsspec.core.url_to_fs``. ``shutil.copy2`` is used only
                when the resolved fs is the local filesystem (preserves
                metadata for local-to-local copies); otherwise ``fs.get``
                is used.

        Returns:
            Path to the copied file.

        Raises:
            ValueError: If external_path is not set.
        """
        if self.external_path is None:
            msg = "Cannot materialize: external_path not set"
            raise ValueError(msg)
        self._assert_identity_intact()
        filename = f"{self.artifact_id}{self.extension or ''}"
        dest = os.path.join(directory, filename)
        copy_verified_file(
            artifact_id=self.artifact_id,
            artifact_type=self.artifact_type,
            uri=self.external_path,
            destination=dest,
            expected_digest=self.content_hash,
            expected_size=self.size_bytes,
            fs=fs,
        )
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

        Returns:
            Draft LargeFileArtifact for the external file.
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


class LargeFileTypeDef(ArtifactTypeDef):
    """Type definition for LargeFileArtifact."""

    key = "large_file"
    table_path = "artifacts/large_files"
    model = LargeFileArtifact
