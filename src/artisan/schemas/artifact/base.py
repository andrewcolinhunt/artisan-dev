"""Base artifact model shared by all concrete artifact types."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, ClassVar, Self

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, field_validator

from artisan.schemas.artifact.common import metadata_from_json, metadata_to_json
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.utils.hashing import compute_artifact_id


class Artifact(BaseModel):
    """Base class for all artifact types.

    All artifacts share these common fields, which map to Delta Lake table columns.

    Artifacts support a draft/finalize pattern:
    - Draft: artifact_id=None, mutable, created via Subclass.draft()
    - Finalized: artifact_id set, semantically immutable, via artifact.finalize()

    Attributes:
        artifact_id: Content-addressed ID (xxh3_128 hash). None for drafts.
        artifact_type: Discriminator for artifact type (plain string).
        origin_step_number: Pipeline step where this artifact was originally produced.
        metadata: JSON-serializable dict for extensibility.
        materialized_path: Runtime-only path where content was written for execution.
    """

    model_config = ConfigDict(extra="forbid")  # NOT frozen - drafts are mutable

    # Column set for Delta/Parquet storage — the single source of truth that
    # to_row/from_row iterate. Concrete subclasses assign it.
    POLARS_SCHEMA: ClassVar[dict[str, type[pl.DataType]]]

    artifact_id: str | None = Field(
        default=None,
        description="Content-addressed ID. None for drafts, present for finalized.",
    )
    artifact_type: str = Field(
        ...,
        description="Type discriminator for this artifact",
    )
    origin_step_number: int | None = Field(
        default=None,
        ge=0,
        description="Pipeline step where this artifact was originally produced. "
        "None for ID-only artifacts.",
    )

    # Class variable: default hydration behavior
    _default_hydrate: ClassVar[bool] = True
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata (JSON-serializable)",
    )
    external_path: str | None = Field(
        default=None,
        description="Path to external content on disk.",
    )
    materialized_path: str | None = Field(
        default=None,
        exclude=True,
        description="Temporary path where content was written for execution.",
    )

    @field_validator("artifact_id")
    @classmethod
    def validate_artifact_id(cls, value: str | None) -> str | None:
        """Validate artifact_id is exactly 32 characters when present."""
        if value is not None and len(value) != 32:
            msg = "artifact_id must be exactly 32 characters when present"
            raise ValueError(msg)
        return value

    @field_validator("artifact_type")
    @classmethod
    def validate_concrete_type(cls, value: str) -> str:
        """Reject ArtifactTypes.ANY on concrete artifacts."""
        if value == ArtifactTypes.ANY:
            msg = (
                f"artifact_type={ArtifactTypes.ANY!r} is a spec-only sentinel "
                "and cannot appear on concrete Artifact instances"
            )
            raise ValueError(msg)
        return value

    @property
    def is_draft(self) -> bool:
        """True if this artifact has not been finalized (artifact_id is None)."""
        return self.artifact_id is None

    @property
    def is_finalized(self) -> bool:
        """True if this artifact has been finalized (artifact_id is present)."""
        return self.artifact_id is not None

    @property
    def is_hydrated(self) -> bool:
        """True if this artifact has been fully hydrated (not ID-only).

        ID-only artifacts have only artifact_id and artifact_type populated,
        with all other fields as None.
        """
        return self.origin_step_number is not None

    def materialize_to(
        self, directory: str, *, format: str | None = None, fs: Any = None
    ) -> str:
        """Write content to disk and set materialized_path.

        Rejects format conversion by default; subclasses that support
        it should override ``materialize_to()`` entirely.

        Args:
            directory: Directory to write files into.
            format: Not supported by default; raises if provided.
            fs: Optional fsspec filesystem for reading source files
                from cloud storage. None uses local stdlib.

        Returns:
            Path to the written file.

        Raises:
            ValueError: If format conversion is requested.
        """
        if format is not None:
            msg = (
                f"{type(self).__name__} does not support "
                f"format conversion (got {format!r})"
            )
            raise ValueError(msg)
        return self._materialize_content(directory, fs=fs)

    def _materialize_content(self, _directory: str, *, fs: Any = None) -> str:
        """Write artifact content to disk.

        Subclasses must implement this to write their content.

        Args:
            _directory: Target directory for output files.
            fs: Optional fsspec filesystem for reading source files.

        Raises:
            NotImplementedError: Subclass must implement.
        """
        msg = f"{type(self).__name__} must implement _materialize_content()"
        raise NotImplementedError(msg)

    def finalize(self) -> Artifact:
        """Compute artifact_id from ``_finalize_content()`` and mark as finalized.

        Returns:
            Self with artifact_id set. No-op if already finalized.

        Raises:
            ValueError: If ``_finalize_content()`` returns None.
        """
        if self.artifact_id is not None:
            return self
        hashable = self._finalize_content()
        if hashable is None:
            msg = "Cannot finalize: artifact not hydrated"
            raise ValueError(msg)
        self.artifact_id = compute_artifact_id(hashable)
        return self

    def _finalize_content(self) -> bytes | None:
        """Return the bytes to hash for ``artifact_id``.

        Default: returns ``self.content`` if present. Subclasses without
        a ``content`` field (e.g. FileRefArtifact) should override.
        """
        return getattr(self, "content", None)

    def to_row(self) -> dict[str, Any]:
        """Serialize to a flat dict keyed by ``POLARS_SCHEMA`` columns.

        Every column reads the same-named field via ``getattr``, except
        columns whose stored form differs (see ``_row_encoders``): the
        JSON-encoded ``metadata`` and any subclass-specific columns.
        ``POLARS_SCHEMA`` is the single source of truth for the column set.
        """
        encoders = self._row_encoders()
        return {
            column: encoders[column]() if column in encoders else getattr(self, column)
            for column in self.POLARS_SCHEMA
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Self:
        """Reconstruct an artifact from a ``POLARS_SCHEMA`` row dict.

        Inverse of ``to_row``: columns in ``_row_decoders`` pass through
        their decoder; all others map straight to the same-named field.

        Args:
            row: Dict with keys matching ``POLARS_SCHEMA`` columns.
        """
        decoders = cls._row_decoders()
        return cls(
            **{
                column: decoders[column](row.get(column))
                if column in decoders
                else row.get(column)
                for column in cls.POLARS_SCHEMA
            }
        )

    def _row_encoders(self) -> dict[str, Callable[[], Any]]:
        """``to_row`` encoders for columns whose stored form differs from the
        raw field value. Subclasses extend via ``super()`` (e.g. DataArtifact
        adds its ``columns`` JSON encoding)."""
        return {"metadata": lambda: metadata_to_json(self.metadata)}

    @classmethod
    def _row_decoders(cls) -> dict[str, Callable[[Any], Any]]:
        """``from_row`` decoders, the inverse of ``_row_encoders``."""
        return {"metadata": metadata_from_json}
