"""Output specification for operation outputs.

``OutputSpec`` declares what artifacts an operation produces and
their lineage relationships, enabling compile-time validation of
step connections.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, field_validator

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
        derives_from: Required and allowed parent roles for every output occurrence.
            Three patterns supported:
            - {"inputs": ["role"]}: Explicit input role(s) as parent
            - {"outputs": ["role"]}: Output role(s) as parent (output->output)
            - {"inputs": []}: Generative operation (no lineage - no parents)

            None is valid only for passthrough outputs.
            Artifact-producing operations must set this explicitly.
            Operations still declare exact parents in ArtifactResult.lineage.
            Multiple parents from each required role are allowed.

            IMPORTANT: Empty dict {} is INVALID and will raise ValidationError.
            Combined {"inputs": [...], "outputs": [...]} is NOT supported.

    Examples:
        # Data derived from specific input
        OutputSpec(
            artifact_type="data",
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
            artifact_type="data",
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
            ValueError: If value is an empty dict, contains invalid
                keys, or combines both "inputs" and "outputs".
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

        # Reject combined inputs+outputs
        if "inputs" in v and "outputs" in v:
            msg = (
                "Combined inputs+outputs pattern is no longer supported. "
                "Use separate output roles instead."
            )
            raise ValueError(msg)

        reference_kind, roles = next(iter(v.items()))
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
            )
        )
