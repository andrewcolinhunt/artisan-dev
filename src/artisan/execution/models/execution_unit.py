"""Transport container carrying work from the orchestrator to executors.

Specifies WHAT to execute (operation, inputs, caching metadata).
WHERE execution happens is specified separately by RuntimeEnvironment.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator

from artisan.operations.base.operation_definition import OperationDefinition


class ExecutionUnit(BaseModel):
    """A batch of work dispatched from the orchestrator to an executor.

    Maps each input role to a list of artifact IDs.  All roles must have
    the same batch size unless the operation declares
    ``independent_input_streams``.

    Attributes:
        operation (OperationDefinition): Fully configured operation instance.
        inputs: Role-keyed lists of 32-char hex artifact IDs.
        execution_spec_id: Deterministic xxh3_128 hash for cache lookup.
        step_number: Pipeline step number for artifact metadata.
        group_ids: Per-index group IDs from framework pairing (optional).
        user_overrides: User-provided parameter overrides before merge.
    """

    operation: OperationDefinition
    inputs: dict[str, list[str]] = Field(default_factory=dict)
    execution_spec_id: str = Field(
        default="",
        description="Deterministic hash for caching (xxh3_128)",
    )
    step_number: int = Field(
        default=0,
        ge=0,
        description="Pipeline step number",
    )
    group_ids: list[str] | None = Field(
        default=None,
        description="Per-index group_id list from framework pairing, sliced by batching",
    )
    user_overrides: dict[str, Any] | None = Field(
        default=None,
        description="Original user-provided parameter overrides (before default merge)",
    )
    step_run_id: str | None = Field(
        default=None,
        description="Step run ID for output isolation scoping",
    )

    @model_validator(mode="after")
    def validate_inputs(self) -> ExecutionUnit:
        """Validate input roles, artifact ID format, and batch size consistency."""
        op_inputs = getattr(self.operation, "inputs", None)
        if op_inputs is None:
            op_name = type(self.operation).__name__
            msg = (
                f"{op_name} must define inputs. "
                f"Add a ClassVar like: inputs: ClassVar[dict[str, InputSpec]] = {{}}"
            )
            raise ValueError(msg)

        # Check if operation accepts runtime-defined inputs
        runtime_defined = getattr(self.operation, "runtime_defined_inputs", False)

        if runtime_defined:
            # Runtime-defined inputs: skip role name validation
            # Any role names are allowed (user-provided or auto-generated)
            pass
        else:
            # Fixed inputs: validate against inputs
            op_name = type(self.operation).__name__
            for role in self.inputs:
                if role not in op_inputs:
                    msg = (
                        f"Unexpected input role '{role}' for {op_name}. "
                        f"Expected: {list(op_inputs.keys())}"
                    )
                    raise ValueError(msg)

        # Validate all values are lists of 32-char hex strings
        for role, artifact_ids in self.inputs.items():
            if not isinstance(artifact_ids, list):
                msg = f"inputs['{role}'] must be a list, got {type(artifact_ids).__name__}"
                raise ValueError(msg)
            for artifact_id in artifact_ids:
                if not isinstance(artifact_id, str) or len(artifact_id) != 32:
                    msg = (
                        f"artifact_id must be 32-char hex string, got: {artifact_id!r}"
                    )
                    raise ValueError(msg)

        # Validate all roles have same batch size
        # Some operations (e.g., MergeOp) allow independent input streams
        independent = getattr(self.operation, "independent_input_streams", False)
        if self.inputs and not independent:
            sizes = {role: len(ids) for role, ids in self.inputs.items()}
            unique_sizes = set(sizes.values())
            if len(unique_sizes) > 1:
                msg = f"All input roles must have same batch size. Got: {sizes}"
                raise ValueError(msg)

        return self

    def get_batch_size(self) -> int:
        """Return the number of items in this batch (0 for generative ops)."""
        if not self.inputs:
            return 0
        return len(next(iter(self.inputs.values())))

    def get_input_artifact_ids(self) -> dict[str, list[str]]:
        """Return a copy of the input artifact IDs keyed by role."""
        return dict(self.inputs)
