"""Compile creator definitions and compositions without dispatch or storage."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace

from pydantic import BaseModel

from artisan.composites.base.called_operations import (
    CalledOperation,
    CapturedCalledOperation,
    capture_called_operations,
)
from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.composite_context import CompositeContext
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.composites.base.root_selection import (
    RootSelection,
    normalize_root_selection,
)
from artisan.composites.base.snapshots import (
    execution_contract,
    instantiate_parameters,
    invocation_policy,
    parameter_class,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.composites.plan import ExecutionRecipe, InvocationPolicy
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.tool_spec import ToolSpec

type Definition = type[OperationDefinition] | type[CompositeDefinition]
type Parameters = BaseModel | Mapping[str, object] | None


def compile_definition(
    definition: Definition,
    *,
    params: Parameters = None,
    called_operations: Mapping[str, CalledOperation] | None = None,
    group_by: GroupByStrategy | None = None,
    batch_strategy: BatchStrategy | Mapping[str, object] | None = None,
    tool: ToolSpec | Mapping[str, object] | None = None,
) -> CompiledPlan:
    """Compile trusted code and fixed inputs without execution or provisioning."""
    instance = instantiate_parameters(_definition_class(definition), params)
    selection = normalize_root_selection(
        instance, group_by=group_by, batch_strategy=batch_strategy, tool=tool
    )
    plan = _compile_instance(definition, instance, (), selection)
    return bind_called_operations(plan, capture_called_operations(called_operations))


def bind_called_operations(
    plan: CompiledPlan, captured: tuple[CapturedCalledOperation, ...]
) -> CompiledPlan:
    """Attach the frozen run selection to its scientific identity."""
    if not captured:
        return plan
    recipe = ExecutionRecipe.model_validate(
        {
            **plan.recipe.model_dump(),
            "schema_version": 4,
            "called_operations": [item.contract for item in captured],
            "cacheable": plan.recipe.cacheable
            and all(item.contract.cacheable for item in captured),
        }
    )
    return replace(plan, recipe=recipe, called_operations=captured)


def compile_captured_definition(
    definition: Definition,
    recipe: ExecutionRecipe,
    *,
    called_operations: Mapping[str, CalledOperation] | None = None,
) -> CompiledPlan:
    """Rebuild permitted root choices using trusted code and captured science.

    Callers must compare the complete rebuilt recipe with their accepted evidence.
    Recorded names never resolve or import definitions. Used input defaults keep
    their captured content identities without consulting current declarations.
    """
    model = parameter_class(definition)
    params = (
        model.model_validate_json(recipe.params_json, extra="forbid", by_name=True)
        if model is not None
        else None
    )
    instance = instantiate_parameters(_definition_class(definition), params)
    declared = invocation_policy(instance)
    policy = (
        declared
        if declared.independent_input_streams
        else InvocationPolicy.model_validate(
            {
                **recipe.policy.model_dump(),
                "independent_input_streams": declared.independent_input_streams,
                "per_artifact_dispatch": declared.per_artifact_dispatch,
                "shared_input_roles": declared.shared_input_roles,
            }
        )
    )
    tool = (
        recipe.execution.tool
        if execution_contract(instance).mode == recipe.execution.mode == "command"
        else None
    )
    plan = _compile_instance(
        definition, instance, (), RootSelection(policy, tool), captured=recipe
    )
    if recipe.called_operations and not called_operations:
        msg = "Replay requires trusted configured operation definitions"
        raise ValueError(msg)
    return bind_called_operations(plan, capture_called_operations(called_operations))


def _compile(
    definition: Definition,
    params: Parameters,
    stack: tuple[Definition, ...],
    *,
    captured: ExecutionRecipe | None = None,
) -> CompiledPlan:
    definition = _definition_class(definition)
    if definition in stack:
        msg = f"Recursive composite definition {definition.name!r}"
        raise ValueError(msg)
    instance = instantiate_parameters(definition, params)
    selection = RootSelection(
        policy=InvocationPolicy() if isinstance(instance, CompositeDefinition) else None
    )
    return _compile_instance(definition, instance, stack, selection, captured=captured)


def _compile_instance(
    definition: Definition,
    instance: OperationDefinition | CompositeDefinition,
    stack: tuple[Definition, ...],
    selection: RootSelection,
    *,
    captured: ExecutionRecipe | None = None,
) -> CompiledPlan:
    recipe = selection.snapshot(instance)
    if isinstance(instance, OperationDefinition):
        _validate_creator_policy(instance, nested=bool(stack))
        return CompiledPlan(
            definition,
            ExecutionRecipe(
                schema_version=recipe.recipe_version, **recipe.model_dump()
            ),
            root_selection=selection,
        )
    context = CompositeContext(recipe, (*stack, definition), captured=captured)
    instance.compose(context)
    return replace(context.finish(), root_selection=selection)


def _definition_class(value: object) -> Definition:
    if not isinstance(value, type) or not issubclass(
        value, (OperationDefinition, CompositeDefinition)
    ):
        msg = "Calls require an operation or composite definition class"
        raise TypeError(msg)
    return value


def _validate_creator_policy(instance: OperationDefinition, *, nested: bool) -> None:
    if instance._kind() != "creator":
        msg = "Curators cannot run inside compositions or standalone executors"
        raise ValueError(msg)
    if instance.runtime_defined_inputs:
        msg = "Composition calls require statically declared input roles"
        raise ValueError(msg)
    if nested and instance.group_by not in (
        None,
        GroupByStrategy.ZIP,
        GroupByStrategy.CROSS_PRODUCT,
    ):
        msg = "Composition calls do not support NAME or LINEAGE grouping"
        raise ValueError(msg)
    if instance.independent_input_streams and (
        instance.per_artifact_dispatch
        or instance.group_by is not None
        or instance.is_command_op()
    ):
        msg = "Independent streams require a native whole-stream reducer without grouping or execute splitting"
        raise ValueError(msg)
