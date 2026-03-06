"""Local backend — ProcessPool execution on the orchestrator machine."""

from __future__ import annotations

import warnings
from collections.abc import Callable
from pathlib import Path

from prefect.task_runners import ProcessPoolTaskRunner

from artisan.orchestration.backends.base import (
    BackendBase,
    OrchestratorTraits,
    WorkerTraits,
)
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.operation_config.resource_config import ResourceConfig


class LocalBackend(BackendBase):
    """ProcessPool execution on the orchestrator machine.

    Args:
        default_max_workers: Default process pool size. Overridden by
            operation.execution.max_workers when set.
    """

    name = "local"
    worker_traits = WorkerTraits()
    orchestrator_traits = OrchestratorTraits()

    def __init__(self, default_max_workers: int = 4) -> None:
        self._default_max_workers = default_max_workers

    def create_flow(
        self,
        resources: ResourceConfig,
        execution: ExecutionConfig,
        step_number: int,
        job_name: str,
    ) -> Callable[[str, RuntimeEnvironment], list[dict]]:
        """Build a local ProcessPool flow."""
        max_workers = execution.max_workers or self._default_max_workers
        return self._build_prefect_flow(ProcessPoolTaskRunner(max_workers=max_workers))

    def capture_logs(
        self,
        results: list[dict],
        staging_root: Path,
        failure_logs_root: Path | None,
        operation_name: str,
    ) -> None:
        """No-op — local logs are in the orchestrator's stdout."""

    def validate_operation(self, operation: OperationDefinition) -> None:
        """Warn if SLURM-specific resources are configured on a local backend."""
        r = operation.resources
        if r.gres or r.partition != "cpu" or r.extra_slurm_kwargs:
            warnings.warn(
                f"Operation {operation.name!r} has SLURM-specific resources "
                f"(gres={r.gres!r}, partition={r.partition!r}) but backend is "
                f"'local'. These will be ignored.",
                stacklevel=2,
            )
