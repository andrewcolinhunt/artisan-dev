"""SLURM backend — job array submission via submitit."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from artisan.orchestration.backends.base import (
    BackendBase,
    OrchestratorTraits,
    WorkerTraits,
)
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.resource_config import ResourceConfig


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
        resources: ResourceConfig,
        execution: ExecutionConfig,
        step_number: int,
        job_name: str,
        log_folder: Path | None = None,
    ) -> Callable[[str, RuntimeEnvironment], list[UnitResult]]:
        """Build a Prefect flow that dispatches units via SLURM job arrays."""
        from prefect_submitit import SlurmTaskRunner

        slurm_kwargs: dict[str, Any] = dict(resources.extra)
        if resources.gpus > 0:
            slurm_kwargs["slurm_gres"] = f"gpu:{resources.gpus}"
        if log_folder is not None:
            slurm_kwargs["log_folder"] = str(log_folder)

        task_runner = SlurmTaskRunner(
            partition=slurm_kwargs.pop("partition", "cpu"),
            time_limit=resources.time_limit,
            mem_gb=resources.memory_gb,
            cpus_per_task=resources.cpus,
            gpus_per_node=slurm_kwargs.pop("gpus_per_node", 0),
            units_per_worker=execution.units_per_worker,
            slurm_job_name=f"s{step_number}_{job_name}",
            **slurm_kwargs,
        )
        return self._build_prefect_flow(task_runner)

    def capture_logs(
        self,
        results: list[UnitResult],
        staging_root: Path,
        failure_logs_root: Path | None,
        operation_name: str,
    ) -> None:
        """Write SLURM worker stdout/stderr into staged parquet files."""
        from artisan.orchestration.engine.dispatch import _patch_worker_logs

        _patch_worker_logs(results, staging_root, failure_logs_root, operation_name)
