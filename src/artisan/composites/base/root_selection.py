"""Resolve root submission choices without inheriting them into child calls."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from pydantic import BaseModel

from artisan.composites.base.snapshots import (
    Instance,
    definition_snapshot,
    execution_contract,
    invocation_policy,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.composites.grouping import AnchoredNameGrouping
from artisan.schemas.composites.plan import (
    DefinitionContract,
    InvocationPolicy,
    ToolContract,
)
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.tool_spec import ToolSpec

_SCIENTIFIC_BATCH_FIELDS = {"artifacts_per_unit", "max_artifacts_per_unit"}


@dataclass(frozen=True)
class RootSelection:
    """Live reconstruction choices; the resulting recipe is durable authority."""

    policy: InvocationPolicy | None = None
    tool: ToolContract | None = None

    def snapshot(self, instance: Instance) -> DefinitionContract:
        """Apply permitted choices to a fresh instance and capture its contract."""
        if self.policy is not None:
            declared = invocation_policy(instance)
            if (
                self.policy.independent_input_streams
                != declared.independent_input_streams
                or self.policy.per_artifact_dispatch != declared.per_artifact_dispatch
                or self.policy.shared_input_roles != declared.shared_input_roles
                or self.policy.required_nonempty_inputs
                != declared.required_nonempty_inputs
            ):
                msg = "Root selection cannot change declared invocation behavior"
                raise ValueError(msg)
            _validate_anchored_override(declared, self.policy)
            instance.group_by = self.policy.group_by
            instance.batch_strategy = BatchStrategy.model_validate(
                {
                    **instance.batch_strategy.model_dump(),
                    **self.policy.model_dump(include=_SCIENTIFIC_BATCH_FIELDS),
                }
            )
        if self.tool is not None:
            _require_command_tool(instance)
            assert isinstance(instance, OperationDefinition)
            instance.tool = ToolSpec.model_validate(self.tool.model_dump())
        snapshot = definition_snapshot(instance)
        return (
            snapshot.model_copy(update={"policy": self.policy})
            if self.policy is not None
            else snapshot
        )


def normalize_root_selection(
    instance: Instance,
    *,
    group_by: GroupByStrategy | AnchoredNameGrouping | None = None,
    batch_strategy: BatchStrategy | Mapping[str, object] | None = None,
    tool: ToolSpec | Mapping[str, object] | None = None,
) -> RootSelection:
    """Freeze supplied scientific patches, retaining explicit nullable fields.

    Transport fields are validated but are not copied into the scientific plan.
    Whole-stream rejection uses supplied keys before merging, so a request to
    split those streams cannot disappear merely because it equals a default.
    """
    if group_by is not None and not isinstance(
        cast(object, group_by), (GroupByStrategy, AnchoredNameGrouping)
    ):
        msg = "group_by must be a GroupByStrategy member or AnchoredNameGrouping"
        raise TypeError(msg)
    batch_patch = _patch(batch_strategy, BatchStrategy)
    policy = invocation_policy(instance)
    if (
        isinstance(policy.group_by, AnchoredNameGrouping)
        and group_by is not None
        and group_by != policy.group_by
    ):
        msg = "Caller selection cannot change the declared anchored grouping"
        raise ValueError(msg)
    scientific = bool(_SCIENTIFIC_BATCH_FIELDS & batch_patch.keys())
    if policy.independent_input_streams and (group_by is not None or scientific):
        msg = "Whole-stream creators reject explicit pairing or scientific batching"
        raise ValueError(msg)
    batch = merge_batch_strategy(instance.batch_strategy, batch_patch)
    selected_policy = None
    if group_by is not None or scientific:
        selected_policy = InvocationPolicy.model_validate(
            {
                **policy.model_dump(),
                "group_by": policy.group_by if group_by is None else group_by,
                **batch.model_dump(include=_SCIENTIFIC_BATCH_FIELDS),
            }
        )
    if selected_policy is not None:
        _validate_anchored_override(policy, selected_policy)
    selected_tool = None
    if tool is not None:
        _require_command_tool(instance)
        assert isinstance(instance, OperationDefinition)
        assert instance.tool is not None
        resolved = ToolSpec.model_validate(
            {**instance.tool.model_dump(), **_patch(tool, ToolSpec)}
        )
        selected_tool = ToolContract(**resolved.model_dump())
    return RootSelection(selected_policy, selected_tool)


def _validate_anchored_override(
    declared: InvocationPolicy, selected: InvocationPolicy
) -> None:
    if not isinstance(declared.group_by, AnchoredNameGrouping):
        if isinstance(selected.group_by, AnchoredNameGrouping):
            msg = "Anchored grouping must be declared by the operation"
            raise ValueError(msg)
        return
    if selected.group_by != declared.group_by:
        msg = "Caller selection cannot change the declared anchored grouping"
        raise ValueError(msg)
    maximum = declared.max_artifacts_per_unit
    if selected.max_artifacts_per_unit != maximum or (
        maximum is not None and selected.artifacts_per_unit > maximum
    ):
        msg = "Anchored group bounds must retain the declared maximum"
        raise ValueError(msg)


def merge_batch_strategy(
    defaults: BatchStrategy,
    override: BatchStrategy | Mapping[str, object] | None,
) -> BatchStrategy:
    """Validate a detached batch patch while preserving explicit nullable keys."""
    return BatchStrategy.model_validate(
        {**defaults.model_dump(), **_patch(override, BatchStrategy)}
    )


def _require_command_tool(instance: Instance) -> None:
    if (
        not isinstance(instance, OperationDefinition)
        or execution_contract(instance).mode != "command"
        or instance.tool is None
    ):
        msg = "Tool overrides require an ordinary command root with a declared tool"
        raise ValueError(msg)


def _patch(
    value: BaseModel | Mapping[str, object] | None, model: type[BaseModel]
) -> dict[str, object]:
    if value is None:
        return {}
    if isinstance(value, model):
        return value.model_dump(mode="python", exclude_unset=True)
    if isinstance(value, Mapping):
        return dict(value)
    msg = f"Expected {model.__name__} or a patch mapping"
    raise TypeError(msg)
