"""Large-file artifact schema for external one-to-one file storage.

Each artifact represents a single large file stored externally.
Content lives at external_path; Delta stores only metadata (hash,
size, name). For files too large to embed in Parquet: model weights,
embedding matrices, simulation outputs, HDF5 datasets.
"""

from __future__ import annotations

import json
import os
import shutil
from typing import Any, ClassVar

import polars as pl
from pydantic import Field

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.execution.fs import resolve_fs


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
        filename = f"{self.artifact_id}{self.extension or ''}"
        dest = os.path.join(directory, filename)
        if fs is not None:
            fs.get(self.external_path, dest)
        else:
            from fsspec.implementations.local import LocalFileSystem

            resolved_fs, source_path = resolve_fs(self.external_path, storage=None)
            if isinstance(resolved_fs, LocalFileSystem):
                # Preserve metadata (mtime, mode) for local-to-local.
                shutil.copy2(source_path, dest)
            else:
                resolved_fs.get(source_path, dest)
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


class LargeFileTypeDef(ArtifactTypeDef):
    """Type definition for LargeFileArtifact."""

    key = "large_file"
    table_path = "artifacts/large_files"
    model = LargeFileArtifact
