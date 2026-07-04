"""CompositeContext — build-time context for ``CompositeDefinition.compose``.

Each ``ctx.run()`` delegates to the parent pipeline's ``submit()``, so every
internal operation becomes an independent pipeline step that inherits
dispatch, caching, lifecycle records, cancellation, and provenance from the
ordinary step machinery.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from artisan.composites.base.results import CompositeStepHandle
from artisan.schemas.composites.composite_ref import CompositeRef
from artisan.schemas.enums import FailurePolicy
from artisan.schemas.operation_config.compute import ComputeProvider
from artisan.schemas.operation_config.compute_resources import ComputeResources

if TYPE_CHECKING:
    from artisan.composites.base.composite_definition import CompositeDefinition
    from artisan.orchestration.pipeline_manager import PipelineManager
    from artisan.orchestration.runners import RunnerBase
    from artisan.orchestration.step_future import StepFuture
    from artisan.schemas.orchestration.output_reference import OutputReference
    from artisan.schemas.specs.output_spec import OutputSpec

logger = logging.getLogger(__name__)


class CompositeContext:
    """Build-time context passed to ``CompositeDefinition.compose``.

    Each ``run()`` submits a real pipeline step via the parent pipeline.
    Composite-level override kwargs from ``submit_composite`` act as per-knob
    defaults: an explicit kwarg on ``run()`` wins wholesale for that knob,
    otherwise the composite-level default fills it. ``params`` is never
    inherited — composite params belong to the composite's own ``Params``
    consumed inside ``compose()``.
    """

    def __init__(
        self,
        pipeline: PipelineManager,
        input_refs: dict[str, OutputReference],
        composite: CompositeDefinition,
        step_name_prefix: str,
        step_defaults: dict[str, Any],
    ) -> None:
        """Initialize the context.

        Args:
            pipeline: Parent pipeline that each ``run()`` submits into.
            input_refs: Resolved external inputs keyed by composite input role.
            composite: Instantiated composite whose ``compose()`` runs here.
            step_name_prefix: Prefix for every child step name.
            step_defaults: Composite-level override defaults for child steps,
                keyed by knob name (``step_runner``, ``environment``, ...).
        """
        self._pipeline = pipeline
        self._input_refs = input_refs
        self._composite = composite
        self._step_name_prefix = step_name_prefix
        self._step_defaults = step_defaults
        self._output_map: dict[str, OutputReference] = {}
        # Captured futures for each child step submitted via this context.
        # Top-level run_composite() drains these via .wait(). Nested
        # composites do NOT need to drain themselves — each child still calls
        # self._pipeline.submit(...), so the parent pipeline's _active_futures
        # owns them.
        self._child_futures: list[StepFuture] = []

    def input(self, role: str) -> CompositeRef:
        """Reference a declared input of this composite.

        Args:
            role: Input role name.

        Returns:
            CompositeRef wrapping the parent pipeline's OutputReference.

        Raises:
            ValueError: If role is not a declared input.
        """
        if role not in self._input_refs:
            available = sorted(self._input_refs.keys())
            msg = f"Unknown input role '{role}'. Available: {available}"
            raise ValueError(msg)
        return CompositeRef(
            source=None,
            output_reference=self._input_refs[role],
            role=role,
        )

    def run(
        self,
        operation: type,
        inputs: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        runner_resources: dict[str, Any] | None = None,
        batch_strategy: dict[str, Any] | None = None,
        step_runner: str | RunnerBase | None = None,
        environment: str | dict[str, Any] | None = None,
        tool: dict[str, Any] | None = None,
        compute_resources: dict[str, Any] | ComputeResources | None = None,
        compute_provider: str | dict[str, Any] | ComputeProvider | None = None,
        skip_cache: bool | None = None,
        failure_policy: FailurePolicy | None = None,
        compact: bool | None = None,
    ) -> CompositeStepHandle:
        """Submit an operation or nested composite as a child pipeline step.

        Any knob left unset (``None``) inherits the composite-level default
        for that knob; an explicit value wins wholesale. ``params`` is never
        inherited from the composite level.

        Args:
            operation: OperationDefinition or CompositeDefinition subclass.
            inputs: Input wiring as {role: CompositeRef}.
            params: Parameter overrides for this operation.
            runner_resources: Resource overrides. Defaults from composite level.
            batch_strategy: Batching overrides. Defaults from composite level.
            step_runner: Step runner. Defaults from composite level.
            environment: Environment override. Defaults from composite level.
            tool: Tool overrides. Defaults from composite level.
            compute_resources: Hardware resources. Defaults from composite level.
            compute_provider: Compute provider. Defaults from composite level.
            skip_cache: Bypass cache. Defaults from composite level, else False.
            failure_policy: Failure policy. Defaults from composite level.
            compact: Run Delta compaction. Defaults from composite level, else True.

        Returns:
            CompositeStepHandle wrapping the child step's StepFuture.
        """
        from artisan.composites.base.composite_definition import CompositeDefinition

        # Resolve composite-level defaults per knob: an explicit per-op value
        # wins, otherwise the composite-level default fills the knob.
        defaults = self._step_defaults
        if runner_resources is None:
            runner_resources = defaults.get("runner_resources")
        if batch_strategy is None:
            batch_strategy = defaults.get("batch_strategy")
        if step_runner is None:
            step_runner = defaults.get("step_runner")
        if environment is None:
            environment = defaults.get("environment")
        if tool is None:
            tool = defaults.get("tool")
        if compute_resources is None:
            compute_resources = defaults.get("compute_resources")
        if compute_provider is None:
            compute_provider = defaults.get("compute_provider")
        if failure_policy is None:
            failure_policy = defaults.get("failure_policy")
        if skip_cache is None:
            skip_cache = defaults.get("skip_cache", False)
        if compact is None:
            compact = defaults.get("compact", True)

        translated_inputs = self._translate_inputs(inputs or {})

        op_name = getattr(operation, "name", operation.__name__)
        step_name = f"{self._step_name_prefix}.{op_name}"

        if issubclass(operation, CompositeDefinition):
            return self._run_nested_composite(
                operation,
                translated_inputs,
                params,
                step_name,
                runner_resources,
                batch_strategy,
                step_runner,
                environment,
                tool,
                compute_resources,
                compute_provider,
                skip_cache,
                failure_policy,
                compact,
            )

        future = self._pipeline.submit(
            operation,
            inputs=translated_inputs,
            params=params,
            step_runner=step_runner,
            runner_resources=runner_resources,
            compute_resources=compute_resources,
            batch_strategy=batch_strategy,
            environment=environment,
            tool=tool,
            compute_provider=compute_provider,
            failure_policy=failure_policy,
            compact=compact,
            skip_cache=skip_cache,
            name=step_name,
        )
        self._child_futures.append(future)

        return CompositeStepHandle(
            step_future=future,
            operation_outputs=getattr(operation, "outputs", {}),
        )

    def output(self, role: str, ref: CompositeRef) -> None:
        """Map an internal result to a declared output of this composite.

        Args:
            role: Composite output role name.
            ref: CompositeRef from an internal ctx.run().output().

        Raises:
            ValueError: If role is not a declared output, or ref carries no
                OutputReference.
        """
        composite_outputs = getattr(type(self._composite), "outputs", {})
        if composite_outputs and role not in composite_outputs:
            available = sorted(composite_outputs.keys())
            msg = f"Unknown output role '{role}'. Available: {available}"
            raise ValueError(msg)
        if ref.output_reference is None:
            msg = f"CompositeRef for output role '{role}' has no OutputReference"
            raise ValueError(msg)
        self._output_map[role] = ref.output_reference

    def get_output_map(self) -> dict[str, OutputReference]:
        """Return the recorded output mappings."""
        return dict(self._output_map)

    def get_child_futures(self) -> list[StepFuture]:
        """Return the StepFutures for every child step submitted via this context."""
        return list(self._child_futures)

    def get_output_types(self) -> dict[str, str | None]:
        """Return output types from composite definition."""
        composite_outputs: dict[str, OutputSpec] = getattr(
            type(self._composite), "outputs", {}
        )
        return {
            role: spec.artifact_type if spec.artifact_type else None
            for role, spec in composite_outputs.items()
        }

    # ----- Helpers -----

    def _translate_inputs(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """Convert CompositeRef inputs to OutputReferences."""
        translated: dict[str, Any] = {}
        for role, ref in inputs.items():
            if isinstance(ref, CompositeRef):
                if ref.output_reference is not None:
                    translated[role] = ref.output_reference
                else:
                    msg = f"CompositeRef for role '{role}' has no OutputReference"
                    raise ValueError(msg)
            else:
                translated[role] = ref
        return translated

    def _run_nested_composite(
        self,
        composite_class: type[CompositeDefinition],
        translated_inputs: dict[str, Any],
        params: dict[str, Any] | None,
        step_name: str,
        runner_resources: dict[str, Any] | None,
        batch_strategy: dict[str, Any] | None,
        step_runner: str | RunnerBase | None,
        environment: str | dict[str, Any] | None,
        tool: dict[str, Any] | None,
        compute_resources: dict[str, Any] | ComputeResources | None,
        compute_provider: str | dict[str, Any] | ComputeProvider | None,
        skip_cache: bool,
        failure_policy: FailurePolicy | None,
        compact: bool,
    ) -> CompositeStepHandle:
        """Recursively expand a nested composite.

        Delegates to ``pipeline.submit_composite`` with the already-resolved
        overrides, so they become the nested composite's own composite-level
        defaults and propagate to its child steps.
        """
        from artisan.composites.base.results import CompositeResult

        result = self._pipeline.submit_composite(
            composite_class,
            inputs=translated_inputs,
            params=params,
            name=step_name,
            step_runner=step_runner,
            runner_resources=runner_resources,
            batch_strategy=batch_strategy,
            environment=environment,
            tool=tool,
            compute_resources=compute_resources,
            compute_provider=compute_provider,
            failure_policy=failure_policy,
            compact=compact,
            skip_cache=skip_cache,
        )
        assert isinstance(result, CompositeResult)

        return _NestedHandle(
            nested_result=result,
            operation_outputs=getattr(composite_class, "outputs", {}),
        )


class _NestedHandle(CompositeStepHandle):
    """Handle for a nested composite submitted from within ``compose()``."""

    def __init__(
        self,
        *,
        nested_result: Any,
        operation_outputs: dict[str, OutputSpec] | None = None,
    ) -> None:
        super().__init__(operation_outputs=operation_outputs)
        self._nested_result = nested_result

    def output(self, role: str) -> CompositeRef:
        """Get output reference from the nested composite."""
        if self._operation_outputs and role not in self._operation_outputs:
            available = sorted(self._operation_outputs.keys())
            msg = f"Unknown output role '{role}'. Available: {available}"
            raise ValueError(msg)
        out_ref = self._nested_result.output(role)
        return CompositeRef(
            source=None,
            output_reference=out_ref,
            role=role,
        )
