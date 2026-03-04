"""SLURM backend — job array submission via submitit."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.backends.base import (
    BackendBase,
    OrchestratorTraits,
    WorkerTraits,
)
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment


class SlurmBackend(BackendBase):
    """SLURM job array submission via submitit."""

    name = "slurm"
    worker_traits = WorkerTraits(
        worker_id_env_var="SLURM_ARRAY_TASK_ID",
        shared_filesystem=True,
    )
    orchestrator_traits = OrchestratorTraits(
        shared_filesystem=True,
        staging_verification_timeout=60.0,
    )

    def create_flow(
        self,
        operation: OperationDefinition,
        step_number: int,
    ) -> Callable[[str, RuntimeEnvironment], list[dict]]:
        """Build a Prefect flow that dispatches units via SLURM job arrays."""
        from prefect_submitit import SlurmTaskRunner

        r = operation.resources
        e = operation.execution
        slurm_kwargs: dict[str, Any] = dict(r.extra_slurm_kwargs)
        if r.gres:
            slurm_kwargs["slurm_gres"] = r.gres

        task_runner = SlurmTaskRunner(
            partition=r.partition,
            time_limit=r.time_limit,
            mem_gb=r.mem_gb,
            cpus_per_task=r.cpus_per_task,
            gpus_per_node=slurm_kwargs.pop("gpus_per_node", 0),
            units_per_worker=e.units_per_worker,
            slurm_job_name=f"s{step_number}_{e.job_name or operation.name}",
            **slurm_kwargs,
        )
        return self._build_prefect_flow(task_runner)

    def capture_logs(
        self,
        results: list[dict],
        staging_root: Path,
        failure_logs_root: Path | None,
        operation_name: str,
    ) -> None:
        """Write SLURM worker stdout/stderr into staged parquet files."""
        from artisan.orchestration.engine.dispatch import _patch_worker_logs

        _patch_worker_logs(results, staging_root, failure_logs_root, operation_name)
