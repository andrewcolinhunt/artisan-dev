"""Immutable source identities for exact pipeline output selection."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.assignment import ContentDigest


class StoreIdentity(BaseModel):
    """Identify a configured retained store without its access credentials."""

    model_config = ConfigDict(
        frozen=True, extra="forbid", revalidate_instances="always"
    )

    delta_root: str = Field(min_length=1, strict=True)
    files_root: str = Field(min_length=1, strict=True)
    steps_table_id: str = Field(min_length=1, strict=True)


class StepSource(BaseModel):
    """Identify one exact step attempt in its authoritative store."""

    model_config = ConfigDict(
        frozen=True, extra="forbid", revalidate_instances="always"
    )

    store: StoreIdentity
    step_run_id: ContentDigest


class OutputReference(BaseModel):
    """Select an output role from one exact step attempt.

    Results and futures populate the source automatically. The reference may
    be used by another pipeline run in the same store; resolution never follows
    the receiving run's step positions or a later retry of the source.
    """

    source: StepSource
    role: str = Field(
        min_length=1, strict=True, description='Output role name (for example "data").'
    )
    artifact_type: str = Field(
        default=ArtifactTypes.ANY,
        description="Artifact type for validation (ArtifactTypes.ANY = any type).",
    )

    model_config = ConfigDict(
        frozen=True, extra="forbid", revalidate_instances="always"
    )
