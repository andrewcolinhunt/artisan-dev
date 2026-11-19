"""Interpret an admitted static recipe within one local executor."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol

from fsspec import AbstractFileSystem

from artisan.composites.base.compiled import CompiledPlan
from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.executor import Executor
from artisan.execution.inputs.partitioning import partition_local_inputs
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.lineage.validation import validate_artifacts_match_specs
from artisan.execution.scientific_failure import scientific_phase
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.composites.grouping import AnchoredNameGrouping
from artisan.schemas.composites.input_contracts import (
    partition_work_size,
    split_shared_inputs,
    validate_nonempty_selection,
    validate_partition_shape,
)
from artisan.schemas.composites.plan import (
    ExecutionRecipe,
    MixedOutputContract,
    RoleBinding,
)
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.result_support import ArtifactResultSupport
from artisan.schemas.provenance.occurrence import ArtifactOccurrence, OccurrenceAddress
from artisan.schemas.provenance.projection import (
    BoundaryInputAttribution,
    BoundaryProjection,
    FrozenBoundaryContext,
)
from artisan.schemas.specs.input_models import FrozenInputContext
from artisan.schemas.specs.output_spec import OutputSpec

type OccurrenceStreams = dict[str, tuple[OccurrenceAddress, ...]]
type ExecuteAdapter = Callable[[OperationDefinition, tuple[str, ...]], ExecuteRouter]


class OperationInvoker(Protocol):
    """Run a creator partition with paths and ownership supplied by its caller."""

    def __call__(
        self,
        operation: OperationDefinition,
        context: FrozenInputContext,
        *,
        execute_router: ExecuteRouter,
        batch_size: int,
        materialization_references: Mapping[str, Mapping[str, Artifact]],
        step_number: int | None = None,
        fs: AbstractFileSystem | None = None,
        call_path: tuple[str, ...] = (),
    ) -> ArtifactResult: ...


@dataclass(frozen=True)
class PlanExecution:
    """Ordinary scientific result with private retained acceptance evidence.

    Artifacts and their files remain owned by the caller's open executor. This
    value neither publishes them nor grants standalone adoption authority.
    """

    result: ArtifactResult
    projection: BoundaryProjection
    retained_artifacts: Mapping[OccurrenceAddress, Artifact]


def execute_compiled_plan(
    plan: CompiledPlan,
    inputs: Mapping[str, Sequence[Artifact]],
    executor: Executor,
    *,
    invocation_id: str,
    execute_adapter: ExecuteAdapter,
    context_artifacts: Mapping[OccurrenceAddress, Artifact] | None = None,
    frozen_context: FrozenBoundaryContext | None = None,
    input_attribution: tuple[BoundaryInputAttribution, ...] | None = None,
    expected_input_context_digest: str | None = None,
    step_number: int | None = None,
    fs: AbstractFileSystem | None = None,
) -> PlanExecution:
    """Execute one admitted boundary without regrouping or splitting its inputs.

    Root input positions receive fresh occurrence addresses. External contextual
    dependencies require an exact inventory and frozen membership; local calls
    never load artifacts, query a store, submit workers, or publish results.
    Omitted producer attribution explicitly marks inputs as unscoped. Controller
    admission supplies verified accepted producers separately from these slots.
    """
    _validate_plan_bindings(plan)
    if inputs and not any(inputs.values()):
        msg = "All-empty boundary inputs contain no admitted work"
        raise ValueError(msg)
    runtime = _PlanRuntime(
        executor.invoke,
        invocation_id,
        execute_adapter,
        frozen_context or FrozenBoundaryContext(),
        step_number,
        fs,
    )
    boundary = runtime.admit(inputs, context_artifacts or {}, input_attribution)
    if expected_input_context_digest is not None:
        admitted = runtime.graph.project(
            {}, {}, frozen_context=runtime.context, input_attribution=input_attribution
        )
        if admitted.input_context_digest != expected_input_context_digest:
            msg = "Invocation inputs differ from the admitted boundary context"
            raise ValueError(msg)
    exports = runtime.execute(plan, boundary, path=(), admitted=True)
    return _finish_plan(runtime, plan, boundary, exports, input_attribution)


def validate_local_inputs(
    plan: CompiledPlan, inputs: Mapping[str, Sequence[Artifact]]
) -> None:
    """Validate a standalone boundary, including zero-work grouping, before lookup."""
    _validate_plan_bindings(plan)
    _validate_local_policy(plan)
    _validate_input_artifacts(plan.recipe, inputs)
    if isinstance(plan.recipe.policy.group_by, AnchoredNameGrouping):
        # Validate the complete selection before a standalone cache lookup.
        for _ in partition_local_inputs(
            inputs,
            plan.recipe.policy,
            name_for=lambda artifact: getattr(artifact, "original_name", None),
        ):
            pass
    for role, values in inputs.items():
        for index, artifact in enumerate(values):
            ArtifactOccurrence(
                OccurrenceAddress(invocation_id="validation", role=role, index=index),
                artifact,
            )


def _validate_local_policy(plan: CompiledPlan) -> None:
    if not isinstance(
        plan.recipe.policy.group_by, AnchoredNameGrouping
    ) and plan.recipe.policy.group_by not in (
        None,
        GroupByStrategy.ZIP,
        GroupByStrategy.CROSS_PRODUCT,
    ):
        msg = "Local input pairing supports only implicit pairing, ZIP or CROSS_PRODUCT"
        raise ValueError(msg)
    for call in plan.calls:
        _validate_local_policy(call.plan)


def execute_local_plan(
    plan: CompiledPlan,
    inputs: Mapping[str, Sequence[Artifact]],
    invoke: OperationInvoker,
    *,
    invocation_id: str,
    execute_adapter: ExecuteAdapter,
    context_artifacts: Mapping[OccurrenceAddress, Artifact],
    frozen_context: FrozenBoundaryContext,
) -> PlanExecution:
    """Partition a standalone root while retaining one exact visible boundary."""
    runtime = _PlanRuntime(
        invoke, invocation_id, execute_adapter, frozen_context, None, None
    )
    boundary = runtime.admit(inputs, context_artifacts, None)
    specs = _output_specs(plan.recipe)
    exports: dict[str, list[OccurrenceAddress]] = {role: [] for role in specs}
    for index, partition in enumerate(
        partition_local_inputs(
            boundary, plan.recipe.policy, name_for=runtime.original_name
        )
    ):
        runtime.invocation_id = f"{invocation_id}/root/{index}"
        produced = runtime.execute(plan, partition, path=(), admitted=True)
        validate_artifacts_match_specs(
            {
                role: [runtime.graph.nodes[key].artifact for key in values]
                for role, values in produced.items()
            },
            specs,
        )
        for role, values in produced.items():
            exports[role].extend(values)
    runtime.invocation_id = invocation_id
    return _finish_plan(runtime, plan, boundary, exports, None)


def _finish_plan(
    runtime: _PlanRuntime,
    plan: CompiledPlan,
    boundary: OccurrenceStreams,
    exports: Mapping[str, Sequence[OccurrenceAddress]],
    input_attribution: tuple[BoundaryInputAttribution, ...] | None,
) -> PlanExecution:
    specs = _output_specs(plan.recipe)
    with scientific_phase("output_projection"):
        projection = runtime.graph.project(
            exports,
            specs,
            frozen_context=runtime.context,
            input_attribution=input_attribution,
        )
        result = runtime.graph.visible_result(
            runtime.invocation_id, boundary, exports, specs
        )
    inventory = {
        item.address: runtime.graph.nodes[item.address].artifact
        for item in projection.occurrences
    }
    result.support = ArtifactResultSupport(projection, inventory)
    external = projection.boundary_addresses()
    retained = {
        address: artifact
        for address, artifact in inventory.items()
        if address not in external
    }
    return PlanExecution(result, projection, MappingProxyType(retained))


def _validate_input_artifacts(
    recipe: ExecutionRecipe, inputs: Mapping[str, Sequence[Artifact]]
) -> None:
    split_shared_inputs(inputs, recipe.policy.shared_input_roles)
    specs = {spec.role: spec for spec in recipe.inputs}
    missing = {
        role for role, spec in specs.items() if spec.required and role not in inputs
    }
    if missing or inputs.keys() - specs.keys():
        msg = "Invocation inputs have missing required or undeclared roles"
        raise ValueError(msg)
    for role, values in inputs.items():
        for artifact in values:
            if not ArtifactTypes.matches(
                specs[role].artifact_type, artifact.artifact_type
            ):
                msg = f"Input role {role!r} has an incompatible artifact type"
                raise ValueError(msg)
            if specs[role].hydrate and not artifact.is_hydrated:
                msg = f"Input role {role!r} requires hydrated artifacts"
                raise ValueError(msg)


def _validate_plan_bindings(plan: CompiledPlan) -> None:
    """Check all live bindings before any scientific phase can execute."""
    plan.instantiate()
    if len(plan.calls) != len(plan.recipe.calls):
        msg = "Compiled call bindings do not match the frozen recipe"
        raise ValueError(msg)
    for compiled, recorded in zip(plan.calls, plan.recipe.calls, strict=True):
        if compiled.name != recorded.name or compiled.plan.recipe != recorded.recipe:
            msg = "Compiled call bindings do not match the frozen recipe"
            raise ValueError(msg)
        _validate_plan_bindings(compiled.plan)


def _output_specs(recipe: ExecutionRecipe) -> dict[str, OutputSpec]:
    outputs = {}
    for spec in recipe.outputs:
        lineage: dict[str, list[str]] | None
        if isinstance(spec, MixedOutputContract):
            lineage = {
                "inputs": list(spec.input_parent_roles),
                "outputs": list(spec.output_parent_roles),
            }
        else:
            lineage = (
                None
                if spec.parent_kind is None
                else {spec.parent_kind: list(spec.parent_roles)}
            )
        outputs[spec.role] = OutputSpec(
            artifact_type=spec.artifact_type,
            required=spec.required,
            derives_from=lineage,
            optional_parent_roles=spec.optional_parent_roles,
        )
    return outputs


class _PlanRuntime:
    """Keep exact producer streams and frozen context within one assignment."""

    def __init__(
        self,
        invoke: OperationInvoker,
        invocation_id: str,
        execute_adapter: ExecuteAdapter,
        context: FrozenBoundaryContext,
        step_number: int | None,
        fs: AbstractFileSystem | None,
    ) -> None:
        self.invoke = invoke
        self.invocation_id = invocation_id
        self.execute_adapter = execute_adapter
        self.context = context
        self.step_number = step_number
        self.fs = fs
        self.graph = OccurrenceGraph(boundary_invocation=invocation_id)
        self.external: set[OccurrenceAddress] = set()

    def admit(
        self,
        inputs: Mapping[str, Sequence[Artifact]],
        captured: Mapping[OccurrenceAddress, Artifact],
        attribution: tuple[BoundaryInputAttribution, ...] | None,
    ) -> OccurrenceStreams:
        for role in inputs:
            self.graph.declare_input_role(role)
        boundary = {
            role: tuple(self.graph.add_input(role, artifact) for artifact in values)
            for role, values in inputs.items()
        }
        roots = {address for values in boundary.values() for address in values}
        required = self.context.occurrence_addresses() - roots
        if set(captured) != required:
            msg = "Context artifact inventory must exactly match frozen boundary membership"
            raise ValueError(msg)
        for address, artifact in captured.items():
            self.graph.add_context_input(address, artifact)
        self.external = roots | set(captured)
        self.graph.validate_boundary_context(
            self.context, input_attribution=attribution
        )
        return boundary

    def execute(
        self,
        plan: CompiledPlan,
        inputs: OccurrenceStreams,
        *,
        path: tuple[str, ...],
        admitted: bool = False,
    ) -> OccurrenceStreams:
        self._validate_inputs(plan.recipe, inputs)
        if not admitted:
            validate_nonempty_selection(inputs, plan.recipe.policy)
        if plan.recipe.definition.kind == "creator":
            return self._creator(plan, inputs, path, admitted=admitted)
        calls: dict[str, OccurrenceStreams] = {}
        for compiled, recorded in zip(plan.calls, plan.recipe.calls, strict=True):
            child_inputs = _resolve_bindings(recorded.inputs, inputs, calls)
            calls[compiled.name] = self.execute(
                compiled.plan,
                child_inputs,
                path=(*path, compiled.name),
            )
        return _resolve_bindings(plan.recipe.exports, inputs, calls, export=True)

    def _validate_inputs(
        self, recipe: ExecutionRecipe, inputs: OccurrenceStreams
    ) -> None:
        _validate_input_artifacts(
            recipe,
            {
                role: tuple(
                    self.graph.nodes[self.graph.original(address)].artifact
                    for address in values
                )
                for role, values in inputs.items()
            },
        )

    def original_name(self, address: OccurrenceAddress) -> object:
        return getattr(
            self.graph.nodes[self.graph.original(address)].artifact,
            "original_name",
            None,
        )

    def _creator(
        self,
        plan: CompiledPlan,
        inputs: OccurrenceStreams,
        path: tuple[str, ...],
        *,
        admitted: bool,
    ) -> OccurrenceStreams:
        policy = plan.recipe.policy
        if admitted:
            validate_partition_shape(inputs, policy, name_for=self.original_name)
        partitions = (
            (inputs,)
            if admitted
            else partition_local_inputs(inputs, policy, name_for=self.original_name)
        )
        outputs: dict[str, list[OccurrenceAddress]] = {
            spec.role: [] for spec in plan.recipe.outputs
        }
        for index, partition in enumerate(partitions):
            operation = plan.instantiate()
            if not isinstance(operation, OperationDefinition):
                msg = "Creator recipe has a non-creator code binding"
                raise TypeError(msg)
            identity = (
                self.invocation_id
                + "/"
                + json.dumps([path, index], separators=(",", ":"))
            )
            context, references = self._input_context(identity, partition, plan.recipe)
            router = self.execute_adapter(operation, path)
            try:
                result = self.invoke(
                    operation,
                    context,
                    batch_size=partition_work_size(
                        partition, policy, name_for=self.original_name
                    ),
                    execute_router=router,
                    materialization_references=references,
                    step_number=self.step_number,
                    fs=self.fs,
                    call_path=path,
                )
            except AssignmentExecutionError as error:
                raise AssignmentExecutionError(
                    str(error),
                    phase=error.phase,
                    call_path=path,
                    error_envelope=error.error_envelope,
                ) from error
            with scientific_phase("output_lineage", call_path=path):
                produced = self.graph.record_result(
                    identity, partition, result, _output_specs(plan.recipe)
                )
            for role, values in produced.items():
                outputs[role].extend(values)
        return {role: tuple(values) for role, values in outputs.items()}

    def _input_context(
        self,
        identity: str,
        inputs: OccurrenceStreams,
        recipe: ExecutionRecipe,
    ) -> tuple[FrozenInputContext, dict[str, dict[str, Artifact]]]:
        artifacts = {
            role: [
                self.graph.nodes[self.graph.original(address)].artifact
                for address in values
            ]
            for role, values in inputs.items()
        }
        associations: dict[tuple[str, int, str], tuple[Artifact, ...]] = {}
        typed: dict[tuple[str, int], dict[str, Artifact]] = {}
        materialized: dict[str, dict[str, Artifact]] = {}
        specs = {spec.role: spec for spec in recipe.inputs}
        for role, values in inputs.items():
            spec = specs[role]
            for index, supplied in enumerate(values):
                address = self.graph.original(supplied)
                artifact = self.graph.nodes[address].artifact
                for kind in spec.with_associated:
                    associations[(role, index, kind)] = tuple(
                        self.graph.nodes[member].artifact
                        for member in self._associated(address, kind)
                    )
                if isinstance(artifact, ExecutionConfigArtifact) and (
                    spec.materialize or spec.resolve_references
                ):
                    references = self._references(address)
                    assert artifact.artifact_id is not None
                    materialized[artifact.artifact_id] = references
                    if spec.resolve_references:
                        typed[(role, index)] = references
        return FrozenInputContext(
            identity, artifacts, associations, typed
        ), materialized

    def _associated(
        self, primary: OccurrenceAddress, kind: str
    ) -> tuple[OccurrenceAddress, ...]:
        if primary in self.external:
            return self.context.associated(primary, kind)
        return tuple(
            address
            for address, node in self.graph.nodes.items()
            if node.alias_of is None
            and node.artifact.artifact_type == kind
            and primary in tuple(self.graph.original(parent) for parent in node.parents)
        )

    def _references(self, config: OccurrenceAddress) -> dict[str, Artifact]:
        if config in self.external:
            members = self.context.referenced(config)
        else:
            members = tuple(
                reference.address
                for reference in self.graph.required_references(
                    config, frozen_context=self.context
                )
            )
        result = {}
        for address in members:
            artifact = self.graph.nodes[address].artifact
            assert artifact.artifact_id is not None
            result[artifact.artifact_id] = artifact
        return result


def _resolve_bindings(
    bindings: tuple[RoleBinding, ...],
    inputs: OccurrenceStreams,
    calls: Mapping[str, OccurrenceStreams],
    *,
    export: bool = False,
) -> OccurrenceStreams:
    resolved: OccurrenceStreams = {}
    for binding in bindings:
        values: list[OccurrenceAddress] = []
        present = not binding.sources or export
        for source in binding.sources:
            streams = inputs if source.call is None else calls[source.call]
            if source.role in streams:
                present = True
                values.extend(streams[source.role])
        if present:
            resolved[binding.role] = tuple(values)
    return resolved
