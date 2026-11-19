"""Output specification for operation outputs.

``OutputSpec`` declares what artifacts an operation produces and
their lineage relationships, enabling compile-time validation of
step connections.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs._validators import validate_artifact_type_str


class OutputSpec(BaseModel):
    """Specification for a single named output of an operation.

    Attributes:
        artifact_type: Type of artifact this output produces.
            Use ArtifactTypes.ANY for outputs that accept any concrete type
            (e.g., passthrough/routing operations like Filter, Merge).
        description: Human-readable description.
        required: Whether the output role must be present with a nonempty
            artifact list. Execution validation fails if it is missing or
            empty. This does not specify an output count per input.
        derives_from: Allowed parent roles for every output occurrence. Each role
            is required unless listed in optional_parent_roles.
            Supported patterns:
            - {"inputs": ["role"]}: Explicit input role(s) as parent
            - {"outputs": ["role"]}: Output role(s) as parent (output->output)
            - {"inputs": ["reference"], "outputs": ["data"]}: Parents from both
                namespaces; required roles are checked independently.
            - {"inputs": []}: Generative operation (no lineage - no parents)

            Artifact-producing operations must set this explicitly.
            Composite outputs must leave it as None: their lineage is projected
            from the selected child outputs. Passthrough outputs also use None
            to preserve their existing identity.
            Operations still declare exact parents in ArtifactResult.lineage.
            Multiple parents from each required role are allowed.

            IMPORTANT: Empty dict {} is INVALID and will raise ValidationError.

        optional_parent_roles: Declared parent roles that may be absent from an
            individual result. Exact references are still required when used.
            A name listed in both namespaces is optional in both.

    Examples:
        # Data derived from specific input
        OutputSpec(
            artifact_type="csv",
            description="Processed data files",
            derives_from={"inputs": ["data"]},
        )

        # Metric derived from OUTPUT data (output->output edge)
        OutputSpec(
            artifact_type="metric",
            description="Score of output data",
            derives_from={"outputs": ["data"]},
        )

        # Generative operation (no input lineage - e.g., random data generator)
        OutputSpec(
            artifact_type="csv",
            description="Randomly generated data",
            derives_from={"inputs": []},  # Explicit: no parents
        )

        # Passthrough output (FilterOp pattern — curator, ArtifactTypes.ANY)
        OutputSpec(
            artifact_type=ArtifactTypes.ANY,
            description="Artifact that passed the filter",
        )
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    artifact_type: str = ArtifactTypes.ANY
    description: str = ""
    required: bool = True
    derives_from: dict[str, list[str]] | None = None
    optional_parent_roles: tuple[str, ...] = Field(
        default=(), exclude_if=lambda roles: not roles
    )

    @model_validator(mode="after")
    def _validate_optional_parents(self) -> OutputSpec:
        roles = self.optional_parent_roles
        allowed = {
            role for roles in (self.derives_from or {}).values() for role in roles
        }
        if len(roles) != len(set(roles)) or any(
            not role or role not in allowed for role in roles
        ):
            msg = "Optional parent roles must be unique declared parent roles"
            raise ValueError(msg)
        return self

    @field_validator("artifact_type")
    @classmethod
    def _validate_artifact_type(cls, v: str) -> str:
        """Reject unregistered artifact type strings."""
        return validate_artifact_type_str(v)

    @field_validator("derives_from")
    @classmethod
    def _validate_lineage_config(
        cls, v: dict[str, list[str]] | None
    ) -> dict[str, list[str]] | None:
        """Validate derives_from configuration.

        Raises:
            ValueError: If value is an empty dict, contains invalid keys or
                duplicate roles within a namespace, or has an empty output list.
        """
        if v is None:
            return v

        # Reject empty dict - ambiguous intent
        if not v:
            msg = (
                "Empty dict {} is not valid. "
                "Use {'inputs': [...]} for declared lineage, or {'inputs': []} for generative operations."
            )
            raise ValueError(msg)

        # Validate keys - only "inputs" and "outputs" allowed
        valid_keys = {"inputs", "outputs"}
        invalid_keys = set(v.keys()) - valid_keys
        if invalid_keys:
            msg = (
                f"Invalid keys in derives_from: {invalid_keys}. "
                f"Only 'inputs' and 'outputs' are allowed."
            )
            raise ValueError(msg)

        for reference_kind, roles in v.items():
            if reference_kind == "outputs" and not roles:
                msg = "Output lineage must reference at least one output role."
                raise ValueError(msg)
            if len(roles) != len(set(roles)):
                msg = f"Duplicate roles are not allowed in derives_from: {roles!r}"
                raise ValueError(msg)

        return v

    def __hash__(self) -> int:
        """Make OutputSpec hashable for use in sets/dicts."""
        lineage_tuple = None
        if self.derives_from:
            lineage_tuple = tuple(
                (k, tuple(v)) for k, v in sorted(self.derives_from.items())
            )
        return hash(
            (
                self.artifact_type,
                self.description,
                self.required,
                lineage_tuple,
                self.optional_parent_roles,
            )
        )
