"""Stable public API for third-party Artisan lifecycle runners."""

from __future__ import annotations

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.orchestration.engine.batching import pack_units
from artisan.orchestration.engine.dispatch import (
    execute_unit,
    execute_unit_batch,
    failure_results_for_units,
    validate_batch_results,
)
from artisan.orchestration.engine.lifecycle_router import LifecycleRouter
from artisan.orchestration.runners.base import (
    OrchestratorTraits,
    RunnerBase,
    WorkerTraits,
)
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.runner_resources import RunnerResources

__all__ = [
    "BatchStrategy",
    "ExecutionUnit",
    "LifecycleRouter",
    "OrchestratorTraits",
    "RunnerBase",
    "RunnerResources",
    "RuntimeEnvironment",
    "UnitResult",
    "WorkerTraits",
    "execute_unit",
    "execute_unit_batch",
    "failure_results_for_units",
    "pack_units",
    "validate_batch_results",
]
