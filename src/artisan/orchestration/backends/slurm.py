"""SLURM backend — job array submission via submitit."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Any

from artisan.execution.models.execution_composite import ExecutionComposite
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.orchestration.backends.base import (
    BackendBase,
    OrchestratorTraits,
    WorkerTraits,
)
from artisan.orchestration.engine.dispatch_handle import DispatchHandle, _HandleState
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.resource_config import ResourceConfig

logger = logging.getLogger(__name__)


class SlurmDispatchHandle(DispatchHandle):
    """Dispatch handle for SLURM job array execution.

    Writes units to a pickle file on the shared filesystem, then
    dispatches via a Prefect ``SlurmTaskRunner``. Cancellation calls
    ``scancel --name`` to kill submitted jobs.

    Args:
        task_runner: Configured SLURM task runner.
        job_name: SLURM job name (used for ``scancel --name``).
        staging_root: Root directory for staging files (pickle write).
        step_number: Pipeline step number (for pickle filename).
    """

    def __init__(
        self,
        task_runner: Any,
        job_name: str,
        staging_root: Path | None,
        step_number: int,
    ) -> None:
        super().__init__()
        self._task_runner = task_runner
        self._job_name = job_name
        self._staging_root = staging_root
        self._step_number = step_number

    def dispatch(
        self,
        units: list[ExecutionUnit | ExecutionComposite],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Write units to shared FS, then submit via SLURM task runner."""
        self._assert_idle()
        self._state = _HandleState.DISPATCHED

        from artisan.orchestration.engine.dispatch import _save_units

        if self._staging_root is None:
            msg = "staging_root is required for SLURM dispatch"
            raise ValueError(msg)
        units_path = _save_units(units, self._staging_root, self._step_number)

        task_runner = self._task_runner

        def _flow_fn() -> list[UnitResult]:
            from prefect import flow, unmapped

            from artisan.orchestration.engine.dispatch import (
                _collect_results,
                _load_units,
                execute_unit_task,
            )

            @flow(task_runner=task_runner)
            def step_flow() -> list[UnitResult]:
                loaded = _load_units(units_path)
                futures = execute_unit_task.map(
                    loaded, runtime_env=unmapped(runtime_env)
                )
                return _collect_results(futures)

            return step_flow()

        self._start_background(_flow_fn)

    def cancel(self) -> None:
        """Cancel SLURM jobs by name via ``scancel``."""
        try:
            subprocess.run(
                ["scancel", "--name", self._job_name],
                check=False,
                capture_output=True,
            )
        except Exception:
            logger.debug("scancel --name %s failed (best-effort)", self._job_name)


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

    def create_dispatch_handle(
        self,
        resources: ResourceConfig,
        execution: ExecutionConfig,
        step_number: int,
        job_name: str,
        log_folder: Path | None = None,
        staging_root: Path | None = None,
    ) -> DispatchHandle:
        """Build a SLURM dispatch handle for job array submission."""
        from prefect_submitit import SlurmTaskRunner

        slurm_kwargs: dict[str, Any] = dict(resources.extra)
        if resources.gpus > 0:
            slurm_kwargs["slurm_gres"] = f"gpu:{resources.gpus}"
        if log_folder is not None:
            slurm_kwargs["log_folder"] = str(log_folder)

        slurm_job_name = f"s{step_number}_{job_name}"
        task_runner = SlurmTaskRunner(
            partition=slurm_kwargs.pop("partition", "cpu"),
            time_limit=resources.time_limit,
            mem_gb=resources.memory_gb,
            cpus_per_task=resources.cpus,
            gpus_per_node=slurm_kwargs.pop("gpus_per_node", 0),
            units_per_worker=execution.units_per_worker,
            slurm_job_name=slurm_job_name,
            **slurm_kwargs,
        )
        return SlurmDispatchHandle(
            task_runner=task_runner,
            job_name=slurm_job_name,
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
        """Write SLURM worker stdout/stderr into staged parquet files."""
        from artisan.orchestration.engine.dispatch import _patch_worker_logs

        _patch_worker_logs(results, staging_root, failure_logs_root, operation_name)
