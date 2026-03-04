"""Input specification for operation inputs.

``InputSpec`` declares what artifacts an operation accepts and how
they are delivered (materialized to disk or passed in memory).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.schemas.artifact.types import ArtifactTypes


class InputSpec(BaseModel):
    """Specification for a single named input of an operation.

    Attributes:
        artifact_type: Expected artifact type (data, metric, file_ref,
            config). Use ArtifactTypes.ANY (default) for "any type accepted",
            or a concrete type string.
        required: Whether this input is required. Default True.
        description: Documentation for this input.
        materialize: If True (default), write artifact to disk and pass Path.
            If False, pass artifact content directly in memory.

    Examples:
        # Required data input (default: materialized to disk)
        InputSpec(artifact_type="data", required=True)

        # Optional reference data
        InputSpec(
            artifact_type="data",
            required=False,
            description="Optional reference for comparison",
        )

        # Any artifact type (for generic operations)
        InputSpec(artifact_type=ArtifactTypes.ANY, required=True)

        # In-memory config (no file I/O)
        InputSpec(
            artifact_type="metric",
            materialize=False,
            description="Configuration passed in memory",
        )
    """

    model_config = ConfigDict(frozen=True)

    artifact_type: str = ArtifactTypes.ANY
    required: bool = True
    description: str = ""
    materialize: bool = True  # If False, pass content directly in memory

    # Hydration control
    hydrate: bool = True
    """Control artifact hydration.

    - True (default): Load full artifact content
    - False: ID-only, for passthrough operations
    """

    materialize_as: str | None = Field(
        default=None,
        description="Target file format for materialization (e.g. '.dat'). "
        "When set, content is converted from native format. "
        "Only applies when materialize=True.",
    )

    with_associated: tuple[str, ...] = ()
    """Artifact types to auto-resolve via provenance associations.

    When set, the framework resolves associated artifacts (direct descendants
    of the specified types) for each primary artifact in this role. Operations
    access them via ``inputs.associated_artifacts(artifact, type_str)``.

    Example:
        InputSpec(artifact_type="data", with_associated=("data_annotation",))
    """

    @model_validator(mode="after")
    def _validate_materialize_as(self) -> InputSpec:
        """Ensure materialize_as is only set when materialize=True."""
        if self.materialize_as is not None and not self.materialize:
            msg = "materialize_as requires materialize=True"
            raise ValueError(msg)
        return self

    def __hash__(self) -> int:
        """Make InputSpec hashable for use in sets/dicts."""
        return hash(
            (
                self.artifact_type,
                self.required,
                self.description,
                self.materialize,
                self.hydrate,
                self.materialize_as,
                self.with_associated,
            )
        )

    def accepts_type(self, artifact_type: str) -> bool:
        """Check if this input spec accepts the given artifact type.

        Args:
            artifact_type: The artifact type to check.

        Returns:
            True if accepted, False otherwise.
            If self.artifact_type is ArtifactTypes.ANY, accepts any type.
        """
        return ArtifactTypes.matches(self.artifact_type, artifact_type)
