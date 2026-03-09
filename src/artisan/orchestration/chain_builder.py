"""Builder for chaining multiple creator operations into a single step.

ChainBuilder provides a fluent API for composing operations that execute
within a single worker, passing artifacts in-memory between them.
"""

from __future__ import annotations

import logging
from typing import Any

from artisan.execution.executors.curator import is_curator_operation
from artisan.execution.models.execution_chain import ChainIntermediates
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

logger = logging.getLogger(__name__)


class ChainBuilder:
    """Fluent builder for composing creator operations into a chain.

    Usage::

        chain = pipeline.chain(
            inputs={"data": pipeline.output("gen", "data")},
        )
        chain.add(DataTransformer, params={"mode": "normalize"})
        chain.add(MetricCalculator)
        result = chain.run()

    Attributes:
        intermediates: How to handle intermediate artifacts.
    """

    def __init__(
        self,
        pipeline: Any,  # PipelineManager — avoid circular import
        inputs: dict[str, Any] | None = None,
        backend: Any | None = None,
        resources: dict[str, Any] | None = None,
        execution: dict[str, Any] | None = None,
        intermediates: ChainIntermediates = ChainIntermediates.DISCARD,
        name: str | None = None,
    ) -> None:
        """Initialize a chain builder.

        Args:
            pipeline: Parent PipelineManager.
            inputs: Initial inputs for the first operation.
            backend: Backend override for the chain step.
            resources: Chain-level resource overrides.
            execution: Chain-level execution overrides.
            intermediates: How to handle intermediate artifacts.
            name: Custom step name. Defaults to joined operation names.
        """
        self._pipeline = pipeline
        self._inputs = inputs
        self._backend = backend
        self._resources = resources
        self._execution = execution
        self._intermediates = intermediates
        self._name = name

        self._operations: list[
            tuple[
                type[OperationDefinition], dict[str, Any] | None, dict[str, Any] | None
            ]
        ] = []
        self._role_mappings: list[dict[str, str] | None] = []

    def add(
        self,
        operation: type[OperationDefinition],
        params: dict[str, Any] | None = None,
        environment: str | dict[str, Any] | None = None,
        tool: dict[str, Any] | None = None,
        role_mapping: dict[str, str] | None = None,
    ) -> ChainBuilder:
        """Add an operation to the chain.

        Args:
            operation: OperationDefinition subclass to add.
            params: Parameter overrides for this operation.
            environment: Environment override for this operation.
            tool: Tool overrides for this operation.
            role_mapping: Explicit role remapping from previous output
                roles to this operation's input roles. None means
                identity mapping (match by name).

        Returns:
            Self for fluent chaining.

        Raises:
            TypeError: If operation is a curator (chains only support creators).
            TypeError: If previous output type is incompatible with next input.
        """
        # Validate: no curators in chains
        if is_curator_operation(operation):
            msg = (
                f"Curator operations cannot be used in chains. "
                f"'{operation.name}' overrides execute_curator()."
            )
            raise TypeError(msg)

        # Validate role compatibility with previous operation
        if self._operations:
            prev_op_class = self._operations[-1][0]
            _validate_role_compatibility(
                prev_op_class.outputs, operation.inputs, role_mapping
            )

        # Store config as merged dict for the chain executor
        from artisan.orchestration.engine.step_executor import _merge_config_overrides

        config = _merge_config_overrides(environment, tool)
        self._operations.append((operation, params, config))
        if len(self._operations) > 1:
            self._role_mappings.append(role_mapping)

        return self

    def run(
        self,
        failure_policy: Any | None = None,
        compact: bool = True,
    ) -> Any:  # StepResult
        """Execute the chain (blocking).

        Args:
            failure_policy: Override pipeline-level failure policy.
            compact: Run Delta Lake compaction after commit.

        Returns:
            StepResult with output references and execution metadata.

        Raises:
            ValueError: If no operations have been added.
        """
        return self.submit(failure_policy=failure_policy, compact=compact).result()

    def submit(
        self,
        failure_policy: Any | None = None,
        compact: bool = True,
    ) -> Any:  # StepFuture
        """Submit the chain for execution (non-blocking).

        Args:
            failure_policy: Override pipeline-level failure policy.
            compact: Run Delta Lake compaction after commit.

        Returns:
            StepFuture for wiring to downstream steps.

        Raises:
            ValueError: If no operations have been added.
        """
        if not self._operations:
            msg = "Cannot execute an empty chain. Call add() first."
            raise ValueError(msg)

        # Build step name from operation names
        step_name = self._name or "_chain_".join(
            op_cls.name for op_cls, _, _ in self._operations
        )

        # Use the last operation for output metadata
        last_op_class = self._operations[-1][0]

        return self._pipeline._submit_chain(
            operations=self._operations,
            role_mappings=self._role_mappings,
            inputs=self._inputs,
            backend=self._backend,
            resources=self._resources,
            execution=self._execution,
            intermediates=self._intermediates,
            failure_policy=failure_policy,
            compact=compact,
            name=step_name,
            final_operation=last_op_class,
        )


def _validate_role_compatibility(
    prev_outputs: dict[str, OutputSpec],
    next_inputs: dict[str, InputSpec],
    role_mapping: dict[str, str] | None,
) -> None:
    """Validate that previous outputs are compatible with next inputs.

    Args:
        prev_outputs: Output specs from the previous operation.
        next_inputs: Input specs for the next operation.
        role_mapping: Explicit role remapping, or None for identity.

    Raises:
        TypeError: If an output type is incompatible with the mapped input.
    """
    if role_mapping is not None:
        # Validate explicit mappings
        for out_role, in_role in role_mapping.items():
            if out_role not in prev_outputs:
                msg = (
                    f"Role mapping references unknown output role '{out_role}'. "
                    f"Available: {sorted(prev_outputs.keys())}"
                )
                raise TypeError(msg)
            if in_role not in next_inputs:
                msg = (
                    f"Role mapping targets unknown input role '{in_role}'. "
                    f"Available: {sorted(next_inputs.keys())}"
                )
                raise TypeError(msg)
            _check_type_compat(
                prev_outputs[out_role], next_inputs[in_role], out_role, in_role
            )
    else:
        # Identity mapping: check overlapping role names
        for role in prev_outputs:
            if role in next_inputs:
                _check_type_compat(prev_outputs[role], next_inputs[role], role, role)


def _check_type_compat(
    out_spec: OutputSpec,
    in_spec: InputSpec,
    out_role: str,
    in_role: str,
) -> None:
    """Check that an output spec's artifact type is accepted by an input spec.

    Args:
        out_spec: Output specification.
        in_spec: Input specification.
        out_role: Output role name (for error message).
        in_role: Input role name (for error message).

    Raises:
        TypeError: If types are incompatible.
    """
    from artisan.schemas.artifact.types import ArtifactTypes

    # ANY on either side is always compatible
    if out_spec.artifact_type == ArtifactTypes.ANY:
        return
    if not in_spec.accepts_type(out_spec.artifact_type):
        msg = (
            f"Type mismatch: output '{out_role}' produces '{out_spec.artifact_type}' "
            f"but input '{in_role}' expects '{in_spec.artifact_type}'."
        )
        raise TypeError(msg)
