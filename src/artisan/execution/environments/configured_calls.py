"""Reuse prepared software for configuration-selected native callbacks."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from artisan.composites.base.compiled import CompiledPlan
from artisan.execution.environments.native import lookup_native_environment
from artisan.execution.environments.requirements import select_tool_environments
from artisan.execution.environments.worker_qualification import (
    controller_worker_sources,
    lookup_worker_qualification,
    prepare_worker_qualification,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.operation_config.environment import Environment

if TYPE_CHECKING:
    from artisan.orchestration.runners.local import LocalRunner


def callback_environments(plan: CompiledPlan) -> tuple[Environment, ...]:
    """Collect enclosing command callers and selected controller commands."""
    declarations = [item.declaration for item in select_tool_environments(plan)]
    declarations.extend(
        item.environment
        for item in plan.called_operations
        if item.contract.calls_operations
        or item.contract.execution.mode == "python_adapter"
    )
    return tuple(dict.fromkeys(item for item in declarations if item is not None))


def prepare_callback_callers(
    plan: CompiledPlan, runner: LocalRunner, application_modules: tuple[str, ...]
) -> None:
    """Capture reusable metadata for each environment that hosts Python callbacks."""
    for declaration in callback_environments(plan):
        target = lookup_native_environment(declaration, cache_root=runner.cache_root)
        names, _ = controller_worker_sources(
            plan,
            (OperationDefinition,),
            declaration,
            target,
            application_modules=application_modules,
        )
        prepare_worker_qualification(
            target,
            names,
            cache_root=runner.cache_root,
            pixi=runner.pixi,
            system_env=runner.system_env,
            verify_current=False,
        )


def resolve_callback_caller(
    plan: CompiledPlan,
    declaration: Environment,
    target: PreparedNativeEnvironment,
    *,
    cache_root: Path,
    application_modules: tuple[str, ...],
) -> PreparedNativeEnvironment:
    """Check the actual caller's package coverage without installing software."""
    names, _ = controller_worker_sources(
        plan,
        (OperationDefinition,),
        declaration,
        target,
        application_modules=application_modules,
    )
    return lookup_worker_qualification(
        target,
        names,
        cache_root=cache_root,
        verify_current=False,
    )
