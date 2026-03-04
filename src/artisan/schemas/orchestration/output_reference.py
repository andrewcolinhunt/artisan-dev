"""Lazy output reference resolved at pipeline dispatch time."""

from __future__ import annotations

from pydantic import BaseModel, Field

from artisan.schemas.artifact.types import ArtifactTypes


class OutputReference(BaseModel):
    """Lazy reference to outputs from a pipeline step.

    OutputReference is returned by StepResult.output(). It acts as a placeholder
    that is resolved to concrete artifact IDs at dispatch time.
    """

    source_step: int = Field(..., description="Step number that produced outputs.")
    role: str = Field(..., description='Output role name (for example "data").')
    artifact_type: str = Field(
        default=ArtifactTypes.ANY,
        description="Artifact type for validation (ArtifactTypes.ANY = any type).",
    )

    model_config = {"frozen": True}

    def __hash__(self) -> int:
        """Allow use as set members and dict keys."""
        return hash((self.source_step, self.role, self.artifact_type))
