"""Resolve command environment declarations from an authorized compiled plan."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, cast

from artisan.composites.base.compiled import CompiledPlan
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.operation_config.environment import Environment


@dataclass(frozen=True)
class ToolEnvironmentSelection:
    """One static command call; a missing declaration deliberately inherits."""

    call_path: tuple[str, ...]
    mode: Literal["command", "python_adapter"]
    declaration: Environment | None


def select_tool_environments(
    plan: CompiledPlan,
) -> tuple[ToolEnvironmentSelection, ...]:
    """Resolve each command declaration once, before admission or serialization.

    Prepared software is bound independently of scientific recipe identity.
    Launchers use this fixed call-path selection instead of rereading mutable
    class defaults while executing a dispatched assignment.
    """
    return _select(plan, ())


def _select(
    plan: CompiledPlan, path: tuple[str, ...]
) -> tuple[ToolEnvironmentSelection, ...]:
    instance = plan.instantiate()
    if isinstance(instance, OperationDefinition):
        if plan.recipe.execution.mode == "native":
            if instance.environment is not None:
                msg = "Native operations cannot declare command environments; use the explicit command adapter"
                raise ValueError(msg)
            return ()
        return (
            ToolEnvironmentSelection(
                call_path=path,
                mode=cast(
                    "Literal['command', 'python_adapter']", plan.recipe.execution.mode
                ),
                declaration=instance.environment,
            ),
        )
    return tuple(
        selection
        for call in plan.calls
        for selection in _select(call.plan, (*path, call.name))
    )
