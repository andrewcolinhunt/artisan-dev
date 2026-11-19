"""Optional ordinary artifact types for externally owned file installations.

Import this module before creating a new store and include it in trusted worker
application modules. Ordinary schema imports do not register these tables.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, ClassVar, Self

import polars as pl
from pydantic import Field, field_validator, model_validator

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external_descriptor import (
    ExternalFile,
    ExternalFilesDescriptor,
)
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.utils.hashing import canonical_json_bytes

__all__ = [
    "ExternalFile",
    "ExternalFilesArtifact",
    "ExternalFilesDescriptor",
    "ModelWeightsArtifact",
]


class ExternalFilesArtifact(Artifact):
    """An ordinary artifact whose payload remains in an external installation."""

    EXTERNAL_REFERENCE: ClassVar[bool] = True
    POLARS_SCHEMA: ClassVar[dict[str, type[pl.DataType]]] = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "descriptor": pl.String,
        "metadata": pl.String,
    }

    artifact_type: str = Field(default="external_files", frozen=True)
    descriptor: ExternalFilesDescriptor | None = None

    def external_descriptor(self) -> ExternalFilesDescriptor:
        """Return the immutable inventory without accessing external files."""
        self._assert_identity_intact()
        if self.descriptor is None:
            msg = "External file artifact is not hydrated"
            raise ValueError(msg)
        return self.descriptor

    def _identity_payload(self) -> bytes | None:
        return (
            self.descriptor.canonical_bytes() if self.descriptor is not None else None
        )

    def _protected_state(self) -> dict[str, Any]:
        state = super()._protected_state()
        state["descriptor"] = (
            self.descriptor.model_dump(mode="json") if self.descriptor else None
        )
        return state

    def _materialize_content(self, _directory: str, *, fs: Any = None) -> str:
        msg = "External references do not support payload materialization"
        raise NotImplementedError(msg)

    def read_content(self, *, fs: Any = None) -> bytes:
        """Reject loading externally owned payloads through artifact storage."""
        msg = "External references do not support payload reads; use external_root"
        raise NotImplementedError(msg)

    def _row_encoders(self) -> dict[str, Callable[[], Any]]:
        return {
            **super()._row_encoders(),
            "descriptor": lambda: self.external_descriptor().canonical_bytes().decode(),
        }

    @classmethod
    def _row_decoders(cls) -> dict[str, Callable[[Any], Any]]:
        return {
            **super()._row_decoders(),
            "descriptor": ExternalFilesDescriptor.model_validate_json,
        }

    @classmethod
    def draft(
        cls,
        descriptor: ExternalFilesDescriptor,
        *,
        metadata: dict[str, Any] | None = None,
        step_number: int | None = None,
    ) -> Self:
        """Describe external files without acquiring or registering their bytes."""
        return cls(
            descriptor=descriptor,
            metadata=metadata or {},
            origin_step_number=step_number,
        )


class ModelWeightsArtifact(ExternalFilesArtifact):
    """External files with model-family semantics and ordinary artifact behavior."""

    POLARS_SCHEMA: ClassVar[dict[str, type[pl.DataType]]] = {
        **ExternalFilesArtifact.POLARS_SCHEMA,
        "model_family": pl.String,
    }
    artifact_type: str = Field(default="model_weights", frozen=True)
    model_family: str | None = None

    @field_validator("model_family")
    @classmethod
    def _family(cls, value: str | None) -> str | None:
        if value is not None and (not value or value != value.strip()):
            msg = "Model family must be a nonempty identifier without outer whitespace"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _hydrated_family(self) -> Self:
        if self.descriptor is not None and self.model_family is None:
            msg = "Model weights require model_family when a descriptor is supplied"
            raise ValueError(msg)
        return self

    def _identity_payload(self) -> bytes | None:
        if self.descriptor is None or self.model_family is None:
            return None
        return canonical_json_bytes(
            {
                "descriptor": self.descriptor.model_dump(mode="json"),
                "model_family": self.model_family,
            }
        )

    @classmethod
    def draft_weights(
        cls,
        descriptor: ExternalFilesDescriptor,
        *,
        model_family: str,
        metadata: dict[str, Any] | None = None,
        step_number: int | None = None,
    ) -> Self:
        """Describe a pinned model checkpoint with its family semantics."""
        return cls(
            descriptor=descriptor,
            model_family=model_family,
            metadata=metadata or {},
            origin_step_number=step_number,
        )


class ExternalFilesTypeDef(ArtifactTypeDef):
    key = "external_files"
    table_path = "artifacts/external_files"
    model = ExternalFilesArtifact
    required_references = None


class ModelWeightsTypeDef(ArtifactTypeDef):
    key = "model_weights"
    table_path = "artifacts/model_weights"
    model = ModelWeightsArtifact
    required_references = None
