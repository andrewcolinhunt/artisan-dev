"""Freeze caller configuration before scheduling private pipeline execution."""

from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import cast

from pydantic import BaseModel

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import Definition, Parameters, compile_definition
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.composites.base.root_selection import (
    merge_batch_strategy,
    normalize_root_selection,
)
from artisan.composites.base.snapshots import (
    Instance,
    instantiate_parameters,
    parameter_class,
    parameter_snapshot,
)
from artisan.execution.recording.commands import CommandRecorder
from artisan.execution.recording.durable import configuration_recorder
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.composites.grouping import AnchoredNameGrouping
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.runner_resources import (
    RunnerResources,
    merge_runner_resources,
)
from artisan.schemas.operation_config.tool_spec import ToolSpec


@dataclass(frozen=True)
class PipelineSubmission:
    """Detached caller choices; composition and software lookup remain deferred."""

    definition: Definition
    params_json: str = field(repr=False)
    group_by: GroupByStrategy | AnchoredNameGrouping | None
    batch_patch: BatchStrategy | None
    tool_patch: Mapping[str, object] | None
    resources: RunnerResources
    batch_strategy: BatchStrategy
    recorder: CommandRecorder = field(repr=False)

    @property
    def is_curator(self) -> bool:
        """Whether this root uses the existing store-bound curator contract."""
        return issubclass(self.definition, OperationDefinition) and (
            self.definition._kind() == "curator"
        )

    def instantiate(self) -> Instance:
        """Restore only the caller-authorized class and detached parameters."""
        model = parameter_class(self.definition)
        params = (
            model.model_validate_json(self.params_json, extra="forbid", by_name=True)
            if model is not None
            else None
        )
        return instantiate_parameters(self.definition, params)

    def compile(self) -> CompiledPlan:
        """Compile after the controller has persisted its pending owner."""
        instance = self.instantiate()
        plan = compile_definition(
            self.definition,
            params=getattr(instance, "params", None),
            group_by=self.group_by,
            batch_strategy=self.batch_patch,
            tool=self.tool_patch,
        )
        pending = [plan]
        compiled = []
        while pending:
            current = pending.pop()
            compiled.append(current)
            configuration_recorder(current.instantiate(), recorder=self.recorder)
            pending.extend(call.plan for call in current.calls)
        for current in compiled:
            params = json.loads(current.recipe.params_json)
            if self.recorder.sanitize_json(params) != params:
                msg = "Scientific recipe parameters contain private configuration"
                raise ValueError(msg)
        recipe = plan.recipe.model_dump(mode="json")
        if self.recorder.sanitize_json(recipe) != recipe:
            msg = "Scientific recipe overlaps private submission configuration"
            raise ValueError(msg)
        return plan


def freeze_submission(
    definition: Definition,
    *,
    params: Parameters = None,
    group_by: GroupByStrategy | AnchoredNameGrouping | None = None,
    batch_strategy: BatchStrategy | Mapping[str, object] | None = None,
    tool: ToolSpec | Mapping[str, object] | None = None,
    runner_resources: RunnerResources | Mapping[str, object] | None = None,
) -> PipelineSubmission:
    """Validate call shape and copy supplied values without compiling or provisioning."""
    candidate = cast(object, definition)
    if not isinstance(candidate, type) or not issubclass(
        candidate, (OperationDefinition, CompositeDefinition)
    ):
        msg = "Pipeline submission requires an operation or composite definition class"
        raise TypeError(msg)
    instance = instantiate_parameters(definition, params)
    batch_patch = _model_patch(batch_strategy, BatchStrategy)
    tool_patch = (
        None
        if tool is None
        else MappingProxyType(
            deepcopy(
                tool.model_dump(exclude_unset=True)
                if isinstance(tool, ToolSpec)
                else dict(tool)
            )
        )
    )
    normalize_root_selection(
        instance, group_by=group_by, batch_strategy=batch_patch, tool=tool_patch
    )
    is_curator = isinstance(instance, OperationDefinition) and (
        type(instance)._kind() == "curator"
    )
    params_json = parameter_snapshot(instance, allow_private=is_curator)
    recorder = configuration_recorder(instance)
    recorder._learn_json(dict(tool_patch) if tool_patch is not None else None)
    if is_curator and (
        group_by is not None
        or {"artifacts_per_unit", "max_artifacts_per_unit"}
        & _patch_values(batch_patch).keys()
    ):
        msg = "Curator inputs are whole streams; scientific batching is not supported"
        raise ValueError(msg)
    if not is_curator and recorder.sanitize_json(json.loads(params_json)) != json.loads(
        params_json
    ):
        msg = "Scientific parameters cannot contain credentials; use runtime injections"
        raise ValueError(msg)
    return PipelineSubmission(
        definition=definition,
        params_json=params_json,
        group_by=group_by,
        batch_patch=batch_patch,
        tool_patch=tool_patch,
        resources=merge_runner_resources(instance.runner_resources, runner_resources),
        batch_strategy=merge_batch_strategy(instance.batch_strategy, batch_patch),
        recorder=recorder,
    )


def _model_patch[M: BaseModel](
    value: M | Mapping[str, object] | None, model: type[M]
) -> M | None:
    if value is None:
        return None
    values = (
        value.model_dump(mode="python", exclude_unset=True)
        if isinstance(value, model)
        else dict(value)
    )
    return model.model_validate(values).model_copy(deep=True)


def _patch_values(value: BaseModel | None) -> dict[str, object]:
    return {} if value is None else value.model_dump(exclude_unset=True)
