"""SLURM intra-allocation backend — srun dispatch within an existing allocation."""

from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Any

from artisan.orchestration.backends.base import (
    BackendBase,
    OrchestratorTraits,
    WorkerTraits,
)
from artisan.orchestration.engine.dispatch_handle import DispatchHandle
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.resource_config import ResourceConfig


class SlurmIntraBackend(BackendBase):
    """Execute within an existing SLURM allocation via srun.

    Unlike ``SlurmBackend`` which submits independent ``sbatch`` jobs to the
    SLURM queue, this backend distributes work directly to allocated resources
    using ``srun``. This eliminates queue latency when resources are already
    reserved via ``salloc`` or ``sbatch``.

    Requires ``prefect-submitit`` v0.1.6+ with srun execution mode support.
    """

    name = "slurm_intra"
    worker_traits = WorkerTraits(
        worker_id_env_var="SLURM_STEP_ID",
        shared_filesystem=True,
    )
    orchestrator_traits = OrchestratorTraits(
        shared_filesystem=True,
        staging_verification_timeout=60.0,
    )

    def create_dispatch_handle(
        self,
        resources: ResourceConfig,
        execution: ExecutionConfig,
        step_number: int,
        job_name: str,
        log_folder: Path | None = None,
        staging_root: Path | None = None,
    ) -> DispatchHandle:
        """Build a dispatch handle that uses srun within an existing allocation.

        Args:
            resources: Hardware resource allocation.
            execution: Batching and scheduling configuration.
            step_number: Pipeline step number (for naming).
            job_name: Human-readable name for logging.
            log_folder: Directory for SLURM log files.
            staging_root: Root directory for staging files.

        Returns:
            Configured SlurmDispatchHandle using srun execution mode.
        """
        from prefect_submitit import SlurmTaskRunner

        from artisan.orchestration.backends.slurm import SlurmDispatchHandle

        slurm_kwargs: dict[str, Any] = dict(resources.extra)
        if log_folder is not None:
            slurm_kwargs["log_folder"] = str(log_folder)

        # Pass gpus directly as gpus_per_node rather than routing through
        # slurm_gres. The SrunBackend builds --gres=gpu:N from gpus_per_node.
        # Partition and slurm_job_name are omitted — srun dispatches within
        # the existing allocation, and srun steps have no independent job names.
        task_runner = SlurmTaskRunner(
            execution_mode="srun",
            time_limit=resources.time_limit,
            mem_gb=resources.memory_gb,
            gpus_per_node=resources.gpus,
            cpus_per_task=resources.cpus,
            units_per_worker=execution.units_per_worker,
            **slurm_kwargs,
        )
        return SlurmDispatchHandle(
            task_runner=task_runner,
            job_name=f"s{step_number}_srun",
            staging_root=staging_root,
            step_number=step_number,
        )

    def capture_logs(
        self,
        results: list[UnitResult],
        staging_root: Path,
        failure_logs_root: Path | None,
        operation_name: str,
    ) -> None:
        """Write srun worker stderr into staged parquet files."""
        from artisan.orchestration.engine.dispatch import _patch_worker_logs

        _patch_worker_logs(results, staging_root, failure_logs_root, operation_name)

    def validate_operation(self, operation: Any) -> None:
        """Warn if not inside a SLURM allocation.

        Args:
            operation: Operation to validate.
        """
        if not os.environ.get("SLURM_JOB_ID"):
            warnings.warn(
                f"Backend 'slurm_intra' selected for {operation.name!r} but "
                f"SLURM_JOB_ID is not set. Are you inside an salloc/sbatch?",
                stacklevel=2,
            )
