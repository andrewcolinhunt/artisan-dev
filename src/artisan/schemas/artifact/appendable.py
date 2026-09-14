"""Appendable artifact schema for JSONL-based appendable files.

Each artifact represents one record within a shared JSONL file.
Many AppendableArtifacts share the same external_path. The file
is appendable: workers write per-worker files, then a consolidation
curator concatenates them into a single combined file.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from typing import Any, BinaryIO, ClassVar

import polars as pl
from pydantic import Field

from artisan.errors import ArtifactIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external import open_external, sanitized_uri
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.utils.hashing import (
    STREAM_CHUNK_BYTES,
    canonical_json_bytes,
    compute_content_digest,
)

MAX_APPENDABLE_RECORD_BYTES = 64 * 1024 * 1024


class AppendableArtifact(Artifact):
    """Artifact representing one record in an appendable JSONL file.

    Many AppendableArtifacts share the same external_path (the JSONL
    file). Each is addressed by record_id within the file. Delta stores
    only per-record metadata; record data lives in the JSONL file.
    """

    POLARS_SCHEMA: ClassVar[dict[str, type[pl.DataType]]] = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "record_id": pl.String,
        "content_hash": pl.String,
        "size_bytes": pl.Int64,
        "original_name": pl.String,
        "extension": pl.String,
        "metadata": pl.String,
    }

    EXTERNALLY_BACKED: ClassVar[bool] = True
    LOCATOR_FIELDS: ClassVar[frozenset[str]] = frozenset({"external_path"})

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

    def _identity_payload(self) -> bytes | None:
        """Return the container-independent record descriptor."""
        if (
            self.record_id is None
            or self.content_hash is None
            or self.size_bytes is None
        ):
            return None
        return canonical_json_bytes(
            {
                "content_hash": self.content_hash,
                "record_id": self.record_id,
                "size_bytes": self.size_bytes,
            }
        )

    def verify_external_content(self, *, fs: Any = None) -> None:
        """Validate the unique matching JSONL record with bounded reads."""
        self._assert_identity_intact()
        self._read_verified_record(fs=fs)

    def _materialize_content(self, directory: str, *, fs: Any = None) -> str:
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
        self._assert_identity_intact()
        record = self._read_verified_record(fs=fs)
        filename = f"{self.artifact_id}.json"
        path = os.path.join(directory, filename)
        with open(path, "w") as f:
            f.write(json.dumps(record, indent=2))
        self.materialized_path = path
        return path

    def _read_record(self, *, fs: Any = None) -> dict[str, Any]:
        """Read this record from the JSONL file by record_id.

        Args:
            fs: Optional fsspec filesystem for reading from cloud storage.
                When None, infers fs from ``self.external_path`` via
                ``fsspec.core.url_to_fs``.

        Returns:
            The parsed JSON record dict.

        Raises:
            ValueError: If external_path is not set or record_id is not found.
        """
        return self._read_verified_record(fs=fs)

    def _read_verified_record(self, *, fs: Any = None) -> dict[str, Any]:
        """Scan the container and return one verified matching record."""
        if self.external_path is None:
            msg = "Cannot read AppendableArtifact without a location"
            raise ValueError(msg)
        matches: list[tuple[bytes, dict[str, Any]]] = []
        with open_external(self.external_path, fs) as source:
            for record_bytes in _bounded_records(source):
                try:
                    record = json.loads(record_bytes)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    msg = (
                        f"Malformed appendable JSON at "
                        f"{sanitized_uri(self.external_path)!r}"
                    )
                    raise ArtifactIntegrityError(msg) from exc
                if not isinstance(record, dict):
                    msg = "Appendable JSONL records must be objects"
                    raise ArtifactIntegrityError(msg)
                if record.get("record_id") == self.record_id:
                    matches.append((record_bytes, record))

        if len(matches) != 1:
            msg = (
                f"Appendable record {self.record_id!r} occurs {len(matches)} times "
                f"in {sanitized_uri(self.external_path)!r}"
            )
            raise ArtifactIntegrityError(msg)
        content, record = matches[0]
        actual = (compute_content_digest(content), len(content))
        expected = (self.content_hash, self.size_bytes)
        if actual != expected:
            msg = (
                f"Appendable artifact {self.artifact_id} failed integrity at "
                f"{sanitized_uri(self.external_path)!r}: expected {expected!r}, "
                f"got {actual!r}"
            )
            raise ArtifactIntegrityError(msg)
        return record

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

        Returns:
            Draft AppendableArtifact for the record.
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


class AppendableTypeDef(ArtifactTypeDef):
    """Type definition for AppendableArtifact."""

    key = "appendable"
    table_path = "artifacts/appendables"
    model = AppendableArtifact


def _bounded_records(source: BinaryIO) -> Iterator[bytes]:
    """Yield JSONL records without relying on backend-specific readline APIs."""
    pending = bytearray()
    while chunk := source.read(STREAM_CHUNK_BYTES):
        pending.extend(chunk)
        while (boundary := pending.find(b"\n")) >= 0:
            yield _validate_record_bytes(bytes(pending[:boundary]))
            del pending[: boundary + 1]
        if len(pending) > MAX_APPENDABLE_RECORD_BYTES + 1:
            _raise_oversized_record()
    if pending:
        yield _validate_record_bytes(bytes(pending))


def _validate_record_bytes(record: bytes) -> bytes:
    """Remove an optional carriage return and enforce the record size limit."""
    if record.endswith(b"\r"):
        record = record[:-1]
    if len(record) > MAX_APPENDABLE_RECORD_BYTES:
        _raise_oversized_record()
    return record


def _raise_oversized_record() -> None:
    msg = f"Appendable record exceeds {MAX_APPENDABLE_RECORD_BYTES} bytes"
    raise ArtifactIntegrityError(msg)
