"""Execution configuration artifact schema and helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar, Self

import polars as pl
from pydantic import Field, PrivateAttr

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.common import (
    JsonContentMixin,
    get_compound_extension,
    metadata_from_json,
    metadata_to_json,
)
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.utils.filename import strip_extensions


def find_artifact_references(obj: Any) -> list[str]:
    """Collect artifact IDs from ``{"$artifact": id}`` patterns.

    Recursively traverses dicts and lists, returning every artifact ID
    found in a ``{"$artifact": "<id>"}`` singleton dict.

    Args:
        obj: Parsed JSON structure (dict, list, or scalar).

    Returns:
        List of artifact ID strings, in traversal order.
    """
    refs: list[str] = []
    if isinstance(obj, dict):
        if "$artifact" in obj and len(obj) == 1:
            refs.append(obj["$artifact"])
        else:
            for value in obj.values():
                refs.extend(find_artifact_references(value))
    elif isinstance(obj, list):
        for item in obj:
            refs.extend(find_artifact_references(item))
    return refs


def substitute_references(obj: Any, id_to_path: dict[str, str]) -> Any:
    """Replace ``{"$artifact": id}`` patterns with materialized paths.

    Args:
        obj: Parsed JSON structure containing reference patterns.
        id_to_path: Mapping from artifact ID to filesystem path string.

    Returns:
        A new structure with all references replaced by path strings.

    Raises:
        ValueError: If a referenced artifact ID has no entry in
            ``id_to_path``.
    """
    if isinstance(obj, dict):
        if "$artifact" in obj and len(obj) == 1:
            artifact_id = obj["$artifact"]
            if artifact_id not in id_to_path:
                msg = f"No path for referenced artifact: {artifact_id}"
                raise ValueError(msg)
            return id_to_path[artifact_id]
        return {k: substitute_references(v, id_to_path) for k, v in obj.items()}
    if isinstance(obj, list):
        return [substitute_references(item, id_to_path) for item in obj]
    return obj


class ExecutionConfigArtifact(JsonContentMixin, Artifact):
    """Artifact storing JSON-encoded execution configuration.

    Holds key-value parameters for a computation step. Supports
    ``{"$artifact": id}`` reference patterns that are resolved to
    materialized paths at execution time.
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
        default=ArtifactTypes.CONFIG,
        frozen=True,
    )
    content: bytes | None = Field(
        default=None,
        description="JSON-encoded config values. None for ID-only artifacts.",
    )
    original_name: str | None = Field(
        default=None,
        description="Key name for lineage inference (stem only, no extension)",
    )
    extension: str | None = Field(
        default=None,
        description="File extension (.json typically). None for ID-only artifacts.",
    )

    _cached_refs: list[str] | None = PrivateAttr(default=None)

    def get_artifact_references(self) -> list[str]:
        """Return artifact IDs from ``$artifact`` references (cached)."""
        if self._cached_refs is not None:
            return self._cached_refs
        if self.content is None:
            self._cached_refs = []
            return self._cached_refs
        self._cached_refs = find_artifact_references(self.values)
        return self._cached_refs

    def materialize_to(
        self,
        directory: Path,
        resolved_paths: dict[str, Path] | None = None,
        *,
        format: str | None = None,
        fs: Any = None,
    ) -> Path:
        """Write config JSON to disk, resolving artifact references.

        When ``resolved_paths`` is provided, ``{"$artifact": id}``
        patterns are replaced with the corresponding filesystem paths
        before writing.

        Args:
            directory: Target directory for the output file.
            resolved_paths: Mapping from artifact ID to materialized
                path. Required when the config contains references.
            format: Not supported; raises if provided.

        Returns:
            Path to the written JSON file.

        Raises:
            ValueError: If format conversion is requested, content is
                None, or referenced artifacts are missing from
                ``resolved_paths``.
        """
        if format is not None:
            msg = f"ExecutionConfigArtifact does not support format conversion (got {format!r})"
            raise ValueError(msg)
        if self.content is None:
            msg = "Cannot materialize: artifact not hydrated"
            raise ValueError(msg)

        stem = self.artifact_id
        ext = self.extension or ".json"
        config_path = directory / f"{stem}{ext}"

        ref_ids = self.get_artifact_references()
        if ref_ids and resolved_paths is not None:
            missing = [ref_id for ref_id in ref_ids if ref_id not in resolved_paths]
            if missing:
                msg = f"Missing paths for referenced artifacts: {missing}"
                raise ValueError(msg)
            id_to_path = {ref_id: str(resolved_paths[ref_id]) for ref_id in ref_ids}
            resolved = substitute_references(self.values, id_to_path)
            config_path.write_text(json.dumps(resolved, indent=2))
        else:
            config_path.write_bytes(self.content)

        self.materialized_path = config_path
        return config_path

    @classmethod
    def draft(
        cls,
        content: dict[str, Any],
        original_name: str,
        step_number: int,
        metadata: dict[str, Any] | None = None,
    ) -> ExecutionConfigArtifact:
        """Create a draft from a config dict.

        Args:
            content: Config key-value pairs (JSON-serializable).
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
