"""Output specification for operation outputs.

``OutputSpec`` declares what artifacts an operation produces and
their lineage relationships, enabling compile-time validation of
step connections.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, field_validator

from artisan.schemas.artifact.types import ArtifactTypes


class OutputSpec(BaseModel):
    """Specification for a single named output of an operation.

    Attributes:
        artifact_type: Type of artifact this output produces.
            Use ArtifactTypes.ANY for outputs that accept any concrete type
            (e.g., passthrough/routing operations like Filter, Merge).
        description: Human-readable description.
        required: Whether output is expected. Warns if missing (doesn't fail).
        infer_lineage_from: Declares which artifacts this output derives from.
            Three patterns supported:
            - {"inputs": ["role"]}: Explicit input role(s) as parent
            - {"outputs": ["role"]}: Output role(s) as parent (output->output)
            - {"inputs": []}: Generative operation (no lineage - no parents)

            None is valid for curator operations (passthrough).
            Creator operations must set this explicitly.

            IMPORTANT: Empty dict {} is INVALID and will raise ValidationError.
            Combined {"inputs": [...], "outputs": [...]} is NOT supported.

    Examples:
        # Data derived from specific input
        OutputSpec(
            artifact_type="data",
            description="Processed data files",
            infer_lineage_from={"inputs": ["data"]},
        )

        # Metric derived from OUTPUT data (output->output edge)
        OutputSpec(
            artifact_type="metric",
            description="Score of output data",
            infer_lineage_from={"outputs": ["data"]},
        )

        # Generative operation (no input lineage - e.g., random data generator)
        OutputSpec(
            artifact_type="data",
            description="Randomly generated data",
            infer_lineage_from={"inputs": []},  # Explicit: no parents
        )

        # Passthrough output (FilterOp pattern — curator, ArtifactTypes.ANY)
        OutputSpec(
            artifact_type=ArtifactTypes.ANY,
            description="Artifact that passed the filter",
        )
    """

    model_config = ConfigDict(frozen=True)

    artifact_type: str = ArtifactTypes.ANY
    description: str = ""
    required: bool = True
    infer_lineage_from: dict[str, list[str]] | None = None

    @field_validator("infer_lineage_from")
    @classmethod
    def validate_lineage_config(
        cls, v: dict[str, list[str]] | None
    ) -> dict[str, list[str]] | None:
        """Validate infer_lineage_from configuration.

        Raises:
            ValueError: If value is an empty dict, contains invalid
                keys, or combines both "inputs" and "outputs".
        """
        if v is None:
            return v

        # Reject empty dict - ambiguous intent
        if not v:
            raise ValueError(
                "Empty dict {} is not valid. "
                "Use {'inputs': [...]} for declared lineage, or {'inputs': []} for generative operations."
            )

        # Validate keys - only "inputs" and "outputs" allowed
        valid_keys = {"inputs", "outputs"}
        invalid_keys = set(v.keys()) - valid_keys
        if invalid_keys:
            raise ValueError(
                f"Invalid keys in infer_lineage_from: {invalid_keys}. "
                f"Only 'inputs' and 'outputs' are allowed."
            )

        # Reject combined inputs+outputs
        if "inputs" in v and "outputs" in v:
            raise ValueError(
                "Combined inputs+outputs pattern is no longer supported. "
                "Use separate output roles instead."
            )

        return v

    def __hash__(self) -> int:
        """Make OutputSpec hashable for use in sets/dicts."""
        lineage_tuple = None
        if self.infer_lineage_from:
            lineage_tuple = tuple(
                (k, tuple(v)) for k, v in sorted(self.infer_lineage_from.items())
            )
        return hash(
            (
                self.artifact_type,
                self.description,
                self.required,
                lineage_tuple,
            )
        )
