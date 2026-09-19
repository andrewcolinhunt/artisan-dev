"""Base artifact model shared by all concrete artifact types."""

from __future__ import annotations

from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Self

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator

from artisan.errors import ArtifactIntegrityError
from artisan.schemas.artifact.common import metadata_from_json, metadata_to_json
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.utils.hashing import canonical_json_bytes, compute_artifact_id


class Artifact(BaseModel):
    """Base class for all artifact types.

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
    EXTERNALLY_BACKED: ClassVar[bool] = False
    LOCATOR_FIELDS: ClassVar[frozenset[str]] = frozenset()

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
        "None when no pipeline origin has been assigned.",
    )

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

    _identity_payload_snapshot: bytes | None = PrivateAttr(default=None)
    _protected_fields_snapshot: bytes | None = PrivateAttr(default=None)

    def __setattr__(self, name: str, value: Any) -> None:
        """Protect durable model fields after finalization."""
        artifact_id = getattr(self, "artifact_id", None)
        mutable_fields = {"materialized_path", *self.LOCATOR_FIELDS}
        if (
            artifact_id is not None
            and name in type(self).model_fields
            and name not in mutable_fields
            and getattr(self, name, None) != value
        ):
            msg = f"Cannot modify finalized artifact field {name!r}"
            raise TypeError(msg)
        super().__setattr__(name, value)

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
        """Return whether semantic content or external descriptors are loaded.

        Origin metadata and external file locations do not determine hydration.
        """
        return self._identity_payload() is not None

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
        self._assert_identity_intact()
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
        """Compute the type-domain ID and protect durable semantics.

        Returns:
            Self with artifact_id set. No-op if already finalized.

        Raises:
            ValueError: If the artifact is not hydrated.
        """
        if self.artifact_id is not None:
            if (
                self._identity_payload_snapshot is None
                and self._identity_payload() is not None
            ):
                self._validate_stored_identity()
            else:
                self._assert_identity_intact()
            return self
        self._validate_identity_descriptors()
        payload = self._identity_payload()
        if payload is None:
            msg = "Cannot finalize: artifact not hydrated"
            raise ValueError(msg)
        from artisan.schemas.artifact.registry import ArtifactTypeDef

        type_def = ArtifactTypeDef.get(self.artifact_type)
        if not isinstance(self, type_def.model):
            msg = (
                f"artifact_type {self.artifact_type!r} is registered for "
                f"{type_def.model.__name__}, not {type(self).__name__}"
            )
            raise TypeError(msg)
        self.artifact_id = compute_artifact_id(
            self.artifact_type,
            payload,
            self._identity_metadata(),
        )
        self._establish_identity_snapshot()
        return self

    def _identity_payload(self) -> bytes | None:
        """Return identity bytes from loaded fields, without external I/O.

        Default: returns ``self.content`` if present. Subclasses without
        a ``content`` field (e.g. FileRefArtifact) should override. Return None
        when the content or descriptors needed for identity are not loaded.
        """
        return getattr(self, "content", None)

    def _identity_metadata(self) -> dict[str, object]:
        """Return framework-owned semantic identity metadata."""
        identity: dict[str, object] = {"metadata": self.metadata}
        for field_name in ("original_name", "extension"):
            if field_name in type(self).model_fields:
                identity[field_name] = getattr(self, field_name)
        return identity

    def _validate_identity_descriptors(self) -> None:
        """Validate fields derived from canonical content before hashing."""

    def verify_external_content(self, *, fs: Any = None) -> None:
        """Verify externally stored bytes; embedded artifacts are a no-op."""
        self._assert_identity_intact()

    def _protected_state(self) -> dict[str, Any]:
        """Return durable fields whose nested values must not drift."""
        excluded = {"materialized_path", *self.LOCATOR_FIELDS}
        return {
            name: getattr(self, name)
            for name in type(self).model_fields
            if name not in excluded
        }

    def _protected_state_bytes(self) -> bytes:
        """Serialize durable model state deterministically for comparison."""
        return canonical_json_bytes(_snapshot_json_value(self._protected_state()))

    def _establish_identity_snapshot(self) -> None:
        """Capture canonical content and durable semantic state."""
        payload = self._identity_payload()
        if payload is None:
            msg = "Cannot snapshot an unhydrated artifact"
            raise ValueError(msg)
        self._identity_payload_snapshot = bytes(payload)
        self._protected_fields_snapshot = self._protected_state_bytes()

    def _assert_identity_intact(self) -> None:
        """Fail if nested durable state drifted after finalization."""
        if self.artifact_id is None or self._identity_payload_snapshot is None:
            return
        if (
            self._identity_payload() != self._identity_payload_snapshot
            or self._protected_state_bytes() != self._protected_fields_snapshot
        ):
            msg = (
                f"Finalized {self.artifact_type} artifact "
                f"{self.artifact_id} was mutated"
            )
            raise ArtifactIntegrityError(msg)

    def _validate_stored_identity(self) -> None:
        """Recompute a hydrated row's ID and establish its snapshot."""
        payload = self._identity_payload()
        if payload is None or self.artifact_id is None:
            msg = "Cannot validate an unhydrated artifact row"
            raise ArtifactIntegrityError(msg)
        expected = compute_artifact_id(
            self.artifact_type,
            payload,
            self._identity_metadata(),
        )
        if expected != self.artifact_id:
            msg = (
                f"Stored {self.artifact_type} artifact ID {self.artifact_id} "
                f"does not match payload ({expected})"
            )
            raise ArtifactIntegrityError(msg)
        self._establish_identity_snapshot()

    def to_row(self) -> dict[str, Any]:
        """Serialize to a flat dict keyed by ``POLARS_SCHEMA`` columns.

        Every column reads the same-named field via ``getattr``, except
        columns whose stored form differs (see ``_row_encoders``): the
        JSON-encoded ``metadata`` and any subclass-specific columns.
        ``POLARS_SCHEMA`` is the single source of truth for the column set.
        """
        if self._identity_payload() is None:
            msg = "Cannot serialize an ID-only artifact as a hydrated row"
            raise ArtifactIntegrityError(msg)
        if self.artifact_id is not None and self._identity_payload_snapshot is None:
            self._validate_stored_identity()
        else:
            self._assert_identity_intact()
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
        artifact = cls.model_validate(
            {
                column: decoders[column](row.get(column))
                if column in decoders
                else row.get(column)
                for column in cls.POLARS_SCHEMA
            }
        )
        artifact._validate_stored_identity()
        return artifact

    def _row_encoders(self) -> dict[str, Callable[[], Any]]:
        """``to_row`` encoders for columns whose stored form differs from the
        raw field value. Subclasses extend via ``super()`` (e.g. DataArtifact
        adds its ``columns`` JSON encoding)."""
        return {"metadata": lambda: metadata_to_json(self.metadata)}

    @classmethod
    def _row_decoders(cls) -> dict[str, Callable[[Any], Any]]:
        """``from_row`` decoders, the inverse of ``_row_encoders``."""
        return {"metadata": metadata_from_json}


def _snapshot_json_value(value: Any) -> Any:
    """Convert model state to a deterministic JSON-safe representation."""
    if isinstance(value, bytes):
        return {"$bytes": value.hex()}
    if isinstance(value, dict):
        return {str(key): _snapshot_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_snapshot_json_value(item) for item in value]
    if isinstance(value, set):
        return sorted(_snapshot_json_value(item) for item in value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    return value
