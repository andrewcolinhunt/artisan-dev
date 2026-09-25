"""File-reference artifact schema.

Stores a reference to an external file by path and content hash,
without embedding the file bytes in Delta Lake storage.
"""

from __future__ import annotations

import os
from typing import Any, ClassVar

import polars as pl
from pydantic import Field, PrivateAttr

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external import (
    copy_verified_file,
    read_verified_file,
    verify_complete_file,
)
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.utils.hashing import canonical_json_bytes


class FileRefArtifact(Artifact):
    """Artifact referencing an external file by path and content hash.

    Unlike content-embedding artifacts (DataArtifact, MetricArtifact),
    this stores a pointer to the file rather than its bytes. The
    ``content_hash`` verifies the raw file bytes. It differs from the artifact
    ID, which also includes the artifact type and semantic metadata.
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
    LOCATOR_FIELDS: ClassVar[frozenset[str]] = frozenset({"path"})

    artifact_type: str = Field(
        default=ArtifactTypes.FILE_REF,
        frozen=True,
    )
    content_hash: str | None = Field(
        default=None,
        description="xxh3_128 integrity digest of the raw file bytes. "
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
        description="Original human-readable filename stem.",
    )
    extension: str | None = Field(
        default=None,
        description="File extension from original path.",
    )

    _cached_content: bytes | None = PrivateAttr(default=None)

    def read_content(self, *, fs: Any = None) -> bytes:
        """Read and cache file content from the original path.

        Args:
            fs: Optional fsspec filesystem for reading from cloud storage.
                When None, infers the filesystem from ``self.path`` via
                ``fsspec.core.url_to_fs`` — local paths resolve to
                ``LocalFileSystem``, ``s3://...`` to ``S3FileSystem``, etc.
                Artifacts have no ``StorageConfig`` back-reference so
                step 1 of the resolve_fs rule isn't available; callers
                that need configured-storage credentials must pass ``fs``
                explicitly.

        Returns:
            The file's bytes, cached after the first read.

        Raises:
            ValueError: If path is None (not hydrated).
        """
        if self._cached_content is None:
            if self.path is None:
                msg = "Cannot read content: artifact has no location"
                raise ValueError(msg)
            self._cached_content = read_verified_file(
                artifact_id=self.artifact_id,
                artifact_type=self.artifact_type,
                uri=self.path,
                expected_digest=self.content_hash,
                expected_size=self.size_bytes,
                fs=fs,
            )
        return self._cached_content

    def verify_external_content(self, *, fs: Any = None) -> None:
        """Stream the referenced file and verify its digest and size."""
        self._assert_identity_intact()
        if self.path is None:
            msg = "Cannot verify FileRefArtifact without a location"
            raise ValueError(msg)
        verify_complete_file(
            artifact_id=self.artifact_id,
            artifact_type=self.artifact_type,
            uri=self.path,
            expected_digest=self.content_hash,
            expected_size=self.size_bytes,
            fs=fs,
        )

    def _materialize_content(self, directory: str, *, fs: Any = None) -> str:
        """Copy the referenced file into the given directory.

        Args:
            directory: Target directory for the output file.
            fs: Optional fsspec filesystem for reading source from cloud.

        Returns:
            Path to the written file.

        Raises:
            ValueError: If path is None (not hydrated).
        """
        if self.path is None:
            msg = "Cannot materialize: artifact not hydrated"
            raise ValueError(msg)
        self._assert_identity_intact()
        dest = os.path.join(directory, os.path.basename(self.path))
        copy_verified_file(
            artifact_id=self.artifact_id,
            artifact_type=self.artifact_type,
            uri=self.path,
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
            original_name: Original human-readable filename stem.
            extension: File extension from original path.

        Returns:
            Draft FileRefArtifact for the referenced file.
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
