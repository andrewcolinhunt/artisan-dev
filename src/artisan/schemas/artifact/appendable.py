"""Appendable artifact schema for JSONL-based appendable files.

Each artifact represents one record within a shared JSONL file.
Many AppendableArtifacts share the same external_path. The file
is appendable: workers write per-worker files, then a consolidation
curator concatenates them into a single combined file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar, Self

import polars as pl
from pydantic import Field

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.common import metadata_from_json, metadata_to_json
from artisan.schemas.artifact.registry import ArtifactTypeDef


class AppendableArtifact(Artifact):
    """Artifact representing one record in an appendable JSONL file.

    Many AppendableArtifacts share the same external_path (the JSONL
    file). Each is addressed by record_id within the file. Delta stores
    only per-record metadata; record data lives in the JSONL file.
    """

    POLARS_SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "record_id": pl.String,
        "content_hash": pl.String,
        "size_bytes": pl.Int64,
        "original_name": pl.String,
        "extension": pl.String,
        "metadata": pl.String,
        "external_path": pl.String,
    }

    artifact_type: str = Field(default="appendable", frozen=True)
    record_id: str | None = Field(
        default=None,
        description="Unique identifier for this record within the file.",
    )
    content_hash: str | None = Field(
        default=None,
        description="Hash of this record's JSON content.",
    )
    size_bytes: int | None = Field(
        default=None,
        ge=0,
        description="Size of this record's JSON line in bytes.",
    )
    original_name: str | None = Field(
        default=None,
        description="Record key for lineage inference (stem only).",
    )
    extension: str | None = Field(
        default=None,
        description="File extension (.jsonl typically).",
    )

    _default_hydrate: ClassVar[bool] = False

    def _finalize_content(self) -> bytes | None:
        """Hash metadata including external_path for content-addressed ID.

        The same record at different paths (per-worker vs consolidated)
        produces distinct artifact_ids.
        """
        if self.content_hash is None:
            return None
        return json.dumps(
            {
                "content_hash": self.content_hash,
                "record_id": self.record_id,
                "external_path": self.external_path,
            },
            sort_keys=True,
        ).encode("utf-8")

    def _materialize_content(self, directory: Path, *, fs: Any = None) -> Path:
        """Extract this record from the JSONL file and write as JSON.

        Args:
            directory: Target directory for the output file.
            fs: Optional fsspec filesystem for reading source from cloud.

        Returns:
            Path to the written JSON file.

        Raises:
            ValueError: If external_path is not set.
        """
        if self.external_path is None:
            msg = "Cannot materialize: external_path not set"
            raise ValueError(msg)
        record = self._read_record(fs=fs)
        filename = f"{self.artifact_id}.json"
        path = directory / filename
        path.write_text(json.dumps(record, indent=2))
        self.materialized_path = path
        return path

    def _read_record(self, *, fs: Any = None) -> dict[str, Any]:
        """Read this record from the JSONL file by record_id.

        Args:
            fs: Optional fsspec filesystem for reading from cloud storage.

        Returns:
            The parsed JSON record dict.

        Raises:
            ValueError: If record_id is not found in the file.
        """
        if fs is not None:
            opener = fs.open(self.external_path, "r")
        else:
            opener = open(self.external_path)
        with opener as f:
            for line in f:
                record = json.loads(line)
                if record.get("record_id") == self.record_id:
                    return record
        msg = f"Record {self.record_id} not found in {self.external_path}"
        raise ValueError(msg)

    @classmethod
    def draft(
        cls,
        record_id: str,
        content_hash: str,
        size_bytes: int,
        step_number: int,
        external_path: str,
        original_name: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> AppendableArtifact:
        """Create a draft appendable artifact.

        Args:
            record_id: Unique identifier within the file.
            content_hash: xxh3_128 hash of this record's JSON line.
            size_bytes: Size of this record's JSON line in bytes.
            step_number: Pipeline step number.
            external_path: Path to the JSONL file.
            original_name: Record key for lineage inference.
            metadata: Optional metadata dict.
        """
        return cls(
            artifact_id=None,
            origin_step_number=step_number,
            record_id=record_id,
            content_hash=content_hash,
            size_bytes=size_bytes,
            external_path=external_path,
            original_name=original_name,
            extension=".jsonl",
            metadata=metadata or {},
        )

    def to_row(self) -> dict[str, Any]:
        """Serialize to a flat dict matching POLARS_SCHEMA columns."""
        return {
            "artifact_id": self.artifact_id,
            "origin_step_number": self.origin_step_number,
            "record_id": self.record_id,
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
            record_id=row.get("record_id"),
            content_hash=row.get("content_hash"),
            size_bytes=row.get("size_bytes"),
            original_name=row.get("original_name"),
            extension=row.get("extension"),
            metadata=metadata_from_json(row.get("metadata")),
            external_path=row.get("external_path"),
        )


class AppendableTypeDef(ArtifactTypeDef):
    """Type definition for AppendableArtifact."""

    key = "appendable"
    table_path = "artifacts/appendables"
    model = AppendableArtifact
