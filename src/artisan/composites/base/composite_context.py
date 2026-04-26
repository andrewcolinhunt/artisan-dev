"""CompositeContext implementations for collapsed and expanded execution.

CollapsedCompositeContext runs operations eagerly in-process on a worker.
ExpandedCompositeContext delegates to the parent pipeline's submit().
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from artisan.execution.models.artifact_source import ArtifactSource
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.composites.composite_ref import (
    CompositeRef,
    CompositeStepHandle,
)
from artisan.schemas.enums import FailurePolicy
from artisan.schemas.operation_config.compute import ComputeProvider
from artisan.schemas.operation_config.compute_resources import ComputeResources

if TYPE_CHECKING:
    from artisan.composites.base.composite_definition import CompositeDefinition
    from artisan.execution.models.execution_composite import CompositeIntermediates
    from artisan.orchestration.runners import RunnerBase
    from artisan.orchestration.step_future import StepFuture
    from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
    from artisan.schemas.orchestration.output_reference import OutputReference
    from artisan.schemas.specs.output_spec import OutputSpec
    from artisan.storage.core.artifact_store import ArtifactStore

logger = logging.getLogger(__name__)


class CompositeContext(ABC):
    """Abstract base for composite execution contexts."""

    @abstractmethod
    def input(self, role: str) -> CompositeRef:
        """Reference a declared input of this composite."""

    @abstractmethod
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
    ) -> CompositeStepHandle:
        """Execute an operation or nested composite."""

    @abstractmethod
    def output(self, role: str, ref: CompositeRef) -> None:
        """Map an internal result to a declared output of this composite."""


@dataclass
class _InternalOpResult:
    """Tracks results from one internal ctx.run() call."""

    artifacts: dict[str, list[Artifact]]
    edges: list[ArtifactProvenanceEdge]
    timings: dict[str, float] = field(default_factory=dict)


class CollapsedCompositeContext(CompositeContext):
    """Context for collapsed (single-worker) composite execution.

    Operations execute eagerly in-process. Artifacts are passed in-memory
    between internal operations.
    """

    def __init__(
        self,
        *,
        sources: dict[str, ArtifactSource],
        composite: CompositeDefinition,
        runtime_env: RuntimeEnvironment,
        step_number: int,
        execution_run_id: str,
        intermediates: CompositeIntermediates,
        artifact_store: ArtifactStore,
    ) -> None:
        self._sources = sources
        self._composite = composite
        self._runtime_env = runtime_env
        self._step_number = step_number
        self._execution_run_id = execution_run_id
        self._intermediates = intermediates
        self._artifact_store = artifact_store

        # Track internal operations' results
        self._op_results: list[_InternalOpResult] = []
        # Track output mappings: composite output role -> CompositeRef
        self._output_map: dict[str, CompositeRef] = {}
        # Counter for internal operations
        self._internal_op_count = 0

    def input(self, role: str) -> CompositeRef:
        """Reference a declared input of this composite.

        Args:
            role: Input role name.

        Returns:
            CompositeRef backed by the resolved ArtifactSource.

        Raises:
            ValueError: If role is not a declared input.
        """
        if role not in self._sources:
            available = sorted(self._sources.keys())
            msg = f"Unknown input role '{role}'. Available: {available}"
            raise ValueError(msg)
        return CompositeRef(
            source=self._sources[role],
            output_reference=None,
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
    ) -> CompositeStepHandle:
        """Execute an operation eagerly in-process.

        Args:
            operation: OperationDefinition or CompositeDefinition subclass.
            inputs: Input wiring as {role: CompositeRef}.
            params: Parameter overrides.
            runner_resources: Ignored in collapsed mode (logged as debug).
            batch_strategy: Ignored in collapsed mode (logged as debug).
            step_runner: Ignored in collapsed mode (logged as debug).
            environment: Environment override for the operation.
            tool: Tool overrides for the operation.

        Returns:
            CompositeStepHandle with in-memory artifacts.
        """
        from artisan.composites.base.composite_definition import CompositeDefinition
        from artisan.execution.executors.curator import is_curator_operation
        from artisan.operations.base.operation_definition import OperationDefinition

        if runner_resources or step_runner or batch_strategy:
            logger.debug(
                "Per-operation runner_resources/step_runner/batch_strategy "
                "overrides ignored in collapsed composite mode for %s",
                getattr(operation, "name", operation.__name__),
            )

        # Resolve CompositeRef inputs to ArtifactSources
        op_sources = self._resolve_inputs(inputs or {})

        if issubclass(operation, CompositeDefinition):
            return self._run_nested_composite(operation, op_sources, params)

        if not issubclass(operation, OperationDefinition):
            msg = (
                f"Expected OperationDefinition or CompositeDefinition subclass, "
                f"got {operation}"
            )
            raise TypeError(msg)

        if is_curator_operation(operation):
            return self._run_curator(operation, op_sources, params)
        return self._run_creator(operation, op_sources, params, environment, tool)

    def output(self, role: str, ref: CompositeRef) -> None:
        """Map an internal result to a declared output of this composite.

        Args:
            role: Composite output role name.
            ref: CompositeRef from an internal ctx.run().output().

        Raises:
            ValueError: If role is not a declared output.
        """
        composite_outputs = getattr(type(self._composite), "outputs", {})
        if composite_outputs and role not in composite_outputs:
            available = sorted(composite_outputs.keys())
            msg = f"Unknown output role '{role}'. Available: {available}"
            raise ValueError(msg)
        self._output_map[role] = ref

    # ----- Internal operation runners -----

    def _run_creator(
        self,
        operation: type,
        sources: dict[str, ArtifactSource],
        params: dict[str, Any] | None,
        environment: str | dict[str, Any] | None = None,
        tool: dict[str, Any] | None = None,
    ) -> CompositeStepHandle:
        """Run a creator operation via run_creator_lifecycle()."""
        from artisan.execution.executors.creator import run_creator_lifecycle
        from artisan.execution.models.execution_unit import ExecutionUnit
        from artisan.orchestration.engine.step_executor import instantiate_operation
        from artisan.utils.hashing import compute_execution_spec_id

        op_config: dict[str, Any] | None = None
        if environment or tool:
            op_config = {}
            if environment:
                op_config["environment"] = environment
            if tool:
                op_config["tool"] = tool

        env_override = op_config.get("environment") if op_config else None
        tool_override = op_config.get("tool") if op_config else None
        instance = instantiate_operation(
            operation, params, environment=env_override, tool=tool_override
        )

        # Build dummy spec_id for internal operation
        spec_id = compute_execution_spec_id(
            operation_name=instance.name,
            inputs={},
            params=params,
        )

        unit = ExecutionUnit(
            operation=instance,
            inputs={},
            execution_spec_id=spec_id,
            step_number=self._step_number,
        )

        result = run_creator_lifecycle(
            unit,
            self._runtime_env,
            worker_id=0,
            execution_run_id=self._execution_run_id,
            sources=sources if sources else None,
        )

        self._op_results.append(
            _InternalOpResult(
                artifacts=result.artifacts,
                edges=result.edges,
                timings=result.timings,
            )
        )
        self._internal_op_count += 1

        return CompositeStepHandle(
            artifacts=result.artifacts,
            operation_outputs=getattr(type(instance), "outputs", {}),
        )

    def _run_curator(
        self,
        operation: type,
        sources: dict[str, ArtifactSource],
        params: dict[str, Any] | None,
    ) -> CompositeStepHandle:
        """Run a curator operation in-process on the worker.

        Pre-commits pending artifacts to Delta if needed for hydration,
        then executes the curator directly.
        """
        import polars as pl

        from artisan.orchestration.engine.step_executor import instantiate_operation
        from artisan.schemas.execution.curator_result import (
            ArtifactResult,
            PassthroughResult,
        )

        instance = instantiate_operation(operation, params)

        # Pre-curator commit: commit any pending internal artifacts so the
        # curator can query them from the artifact store
        self._pre_curator_commit()

        # Build input DataFrames from resolved artifact IDs
        input_ids: dict[str, list[str]] = {}
        for role, source in sources.items():
            if source._artifacts is not None:
                input_ids[role] = [
                    a.artifact_id for a in source._artifacts if a.artifact_id
                ]
            elif source._ids is not None:
                input_ids[role] = list(source._ids)
            else:
                input_ids[role] = []

        input_dfs = {
            role: pl.DataFrame({"artifact_id": ids}) for role, ids in input_ids.items()
        }

        # Execute curator in-process
        result = instance.execute_curator(
            inputs=input_dfs,
            step_number=self._step_number,
            artifact_store=self._artifact_store,
        )

        if not result.success:
            msg = f"Curator {instance.name} failed: {result.error or 'Unknown error'}"
            raise RuntimeError(msg)

        # Collect results based on result type
        op_outputs = getattr(type(instance), "outputs", {})

        match result:
            case ArtifactResult():
                from artisan.execution.utils import finalize_artifacts

                finalized = finalize_artifacts(result.artifacts)
                # Build edges for artifact results
                edges: list[ArtifactProvenanceEdge] = []
                self._op_results.append(
                    _InternalOpResult(
                        artifacts=finalized,
                        edges=edges,
                    )
                )
                self._internal_op_count += 1
                return CompositeStepHandle(
                    artifacts=finalized,
                    operation_outputs=op_outputs,
                )

            case PassthroughResult():
                # For passthrough, reconstruct artifacts from IDs
                passthrough_artifacts: dict[str, list[Artifact]] = {}
                for role, ids in result.passthrough.items():
                    arts = []
                    for aid in ids:
                        loaded = self._artifact_store.get_artifact(aid)
                        if loaded:
                            arts.append(loaded)
                    passthrough_artifacts[role] = arts

                self._op_results.append(
                    _InternalOpResult(
                        artifacts=passthrough_artifacts,
                        edges=[],
                    )
                )
                self._internal_op_count += 1
                return CompositeStepHandle(
                    artifacts=passthrough_artifacts,
                    operation_outputs=op_outputs,
                )

            case _:
                msg = f"Unexpected result type from curator: {type(result).__name__}"  # type: ignore[unreachable]  # defensive fallback for future result types
                raise TypeError(msg)

    def _run_nested_composite(
        self,
        composite_class: type[CompositeDefinition],
        sources: dict[str, ArtifactSource],
        params: dict[str, Any] | None,
    ) -> CompositeStepHandle:
        """Run a nested composite recursively in collapsed mode."""

        # Instantiate the nested composite
        init_kwargs: dict[str, Any] = {}
        if params:
            init_kwargs["params"] = params
        nested = composite_class(**init_kwargs)

        # Create a nested context
        nested_ctx = CollapsedCompositeContext(
            sources=sources,
            composite=nested,
            runtime_env=self._runtime_env,
            step_number=self._step_number,
            execution_run_id=self._execution_run_id,
            intermediates=self._intermediates,
            artifact_store=self._artifact_store,
        )

        # Execute nested compose()
        nested.compose(nested_ctx)

        # Collect nested results into parent
        for op_result in nested_ctx._op_results:
            self._op_results.append(op_result)

        # Build output artifacts from nested output map
        output_artifacts: dict[str, list[Artifact]] = {}
        for role, ref in nested_ctx._output_map.items():
            if ref.source is not None and ref.source._artifacts is not None:
                output_artifacts[role] = list(ref.source._artifacts)
            else:
                output_artifacts[role] = []

        self._internal_op_count += 1

        return CompositeStepHandle(
            artifacts=output_artifacts,
            operation_outputs=getattr(composite_class, "outputs", {}),
        )

    # ----- Helpers -----

    def _resolve_inputs(self, inputs: dict[str, Any]) -> dict[str, ArtifactSource]:
        """Convert CompositeRef inputs to ArtifactSources."""
        sources: dict[str, ArtifactSource] = {}
        for role, ref in inputs.items():
            if isinstance(ref, CompositeRef):
                if ref.source is not None:
                    sources[role] = ref.source
                else:
                    msg = (
                        f"CompositeRef for role '{role}' has no source "
                        "(expanded-mode ref used in collapsed context)"
                    )
                    raise ValueError(msg)
            else:
                msg = (
                    f"Expected CompositeRef for input role '{role}', "
                    f"got {type(ref).__name__}"
                )
                raise TypeError(msg)
        return sources

    def _pre_curator_commit(self) -> None:
        """Commit pending internal artifacts to Delta for curator hydration."""
        from artisan.storage.io.commit import DeltaCommitter
        from artisan.storage.io.staging import StagingManager

        staging_root = self._runtime_env.staging_root
        delta_root = self._runtime_env.delta_root
        if staging_root and delta_root:
            try:
                fs = self._runtime_env.storage.filesystem()
                storage_options = self._runtime_env.storage.delta_storage_options()
                staging_manager = StagingManager(staging_root, fs)
                committer = DeltaCommitter(
                    delta_root,
                    staging_manager,
                    fs=fs,
                    storage_options=storage_options,
                )
                committer.commit_all_tables(
                    cleanup_staging=False,
                    step_number=self._step_number,
                    operation_name=f"_composite_{self._composite.name}_pre_curator",
                )
            except Exception:
                logger.debug(
                    "Pre-curator commit had no pending data (expected for "
                    "composites with no prior creator output)"
                )

    def get_output_map(self) -> dict[str, CompositeRef]:
        """Return the recorded output mappings."""
        return dict(self._output_map)

    def get_all_artifacts(self) -> list[dict[str, list[Artifact]]]:
        """Return per-operation artifact dicts."""
        return [r.artifacts for r in self._op_results]

    def get_all_edges(self) -> list[list[ArtifactProvenanceEdge]]:
        """Return per-operation edge lists."""
        return [r.edges for r in self._op_results]

    def get_all_timings(self) -> list[dict[str, float]]:
        """Return per-operation timing dicts."""
        return [r.timings for r in self._op_results]


class ExpandedCompositeContext(CompositeContext):
    """Context for expanded composite execution.

    Each ctx.run() delegates to the parent pipeline's submit(), creating
    independent pipeline steps.
    """

    def __init__(
        self,
        *,
        pipeline: Any,
        input_refs: dict[str, OutputReference],
        composite: CompositeDefinition,
        step_name_prefix: str,
    ) -> None:
        self._pipeline = pipeline
        self._input_refs = input_refs
        self._composite = composite
        self._step_name_prefix = step_name_prefix
        self._output_map: dict[str, OutputReference] = {}
        # Captured futures for each child step submitted via this context.
        # Top-level run_composite(expand=True) drains these via .wait().
        # Nested expanded composites do NOT need to drain themselves —
        # each child still calls self._pipeline.submit(...), so the
        # parent pipeline's _active_futures owns them.
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
        skip_cache: bool = False,
        failure_policy: FailurePolicy | None = None,
        compact: bool = True,
    ) -> CompositeStepHandle:
        """Delegate to parent pipeline's submit().

        Args:
            operation: OperationDefinition or CompositeDefinition subclass.
            inputs: Input wiring as {role: CompositeRef}.
            params: Parameter overrides.
            runner_resources: Resource overrides (forwarded to pipeline.submit).
            batch_strategy: Execution overrides (forwarded to pipeline.submit).
            step_runner: Step runner (forwarded to pipeline.submit).
            environment: Environment override.
            tool: Tool overrides.
            compute_resources: Hardware resources for the compute provider.
            compute_provider: Compute provider override (string or dict).
            skip_cache: Bypass cache lookups for this step.
            failure_policy: Override pipeline-level failure policy.
            compact: Run Delta Lake compaction after commit.

        Returns:
            CompositeStepHandle wrapping the parent pipeline's StepFuture.
        """
        from artisan.composites.base.composite_definition import CompositeDefinition

        # Translate CompositeRef inputs to OutputReferences
        translated_inputs = self._translate_inputs(inputs or {})

        op_name = getattr(operation, "name", operation.__name__)
        step_name = f"{self._step_name_prefix}.{op_name}"

        # Handle nested composites
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

        # Regular operation — delegate to parent pipeline
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
            ValueError: If role is not a declared output or ref has no OutputReference.
        """
        composite_outputs = getattr(type(self._composite), "outputs", {})
        if composite_outputs and role not in composite_outputs:
            available = sorted(composite_outputs.keys())
            msg = f"Unknown output role '{role}'. Available: {available}"
            raise ValueError(msg)
        if ref.output_reference is None:
            msg = (
                f"CompositeRef for output role '{role}' has no OutputReference "
                "(collapsed-mode ref used in expanded context)"
            )
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
                    msg = (
                        f"CompositeRef for role '{role}' has no OutputReference "
                        "(collapsed-mode ref used in expanded context)"
                    )
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
        runner_resources: dict[str, Any] | None = None,
        batch_strategy: dict[str, Any] | None = None,
        step_runner: str | RunnerBase | None = None,
        environment: str | dict[str, Any] | None = None,
        tool: dict[str, Any] | None = None,
        compute_resources: dict[str, Any] | ComputeResources | None = None,
        compute_provider: str | dict[str, Any] | ComputeProvider | None = None,
        skip_cache: bool = False,
        failure_policy: FailurePolicy | None = None,
        compact: bool = True,
    ) -> CompositeStepHandle:
        """Recursively expand a nested composite.

        Delegates to ``pipeline.submit_composite(expand=True)`` so the same
        per-ctx.run() overrides flow through the nested composite's child
        steps via the parent pipeline's centralized expansion path.
        """
        from artisan.schemas.composites.composite_ref import ExpandedCompositeResult

        result = self._pipeline.submit_composite(
            composite_class,
            inputs=translated_inputs,
            params=params,
            name=step_name,
            expand=True,
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
        assert isinstance(result, ExpandedCompositeResult)

        return _ExpandedNestedHandle(
            expanded_result=result,
            operation_outputs=getattr(composite_class, "outputs", {}),
        )


class _ExpandedNestedHandle(CompositeStepHandle):
    """Handle for a nested composite that was expanded in expanded mode."""

    def __init__(
        self,
        *,
        expanded_result: Any,
        operation_outputs: dict[str, OutputSpec] | None = None,
    ) -> None:
        super().__init__(operation_outputs=operation_outputs)
        self._expanded_result = expanded_result

    def output(self, role: str) -> CompositeRef:
        """Get output reference from the expanded nested composite."""
        if self._operation_outputs and role not in self._operation_outputs:
            available = sorted(self._operation_outputs.keys())
            msg = f"Unknown output role '{role}'. Available: {available}"
            raise ValueError(msg)
        out_ref = self._expanded_result.output(role)
        return CompositeRef(
            source=None,
            output_reference=out_ref,
            role=role,
        )
