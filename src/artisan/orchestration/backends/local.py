"""Local backend — ProcessPool execution on the orchestrator machine."""

from __future__ import annotations

import multiprocessing
import signal
import warnings
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
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
from artisan.utils.spawn import suppress_main_reimport


def _ignore_sigint() -> None:
    """Worker initializer: ignore SIGINT so the parent handles cancellation."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class SIGINTSafeProcessPoolTaskRunner(ProcessPoolTaskRunner):
    """ProcessPoolTaskRunner whose workers ignore SIGINT.

    Prevents child processes from receiving KeyboardInterrupt, which causes
    noisy tracebacks. The parent process handles SIGINT via PipelineManager's
    signal handler and propagates cancellation cleanly.

    Also prevents ``multiprocessing.spawn`` from re-importing the caller's
    ``__main__`` module in worker processes.
    """

    def __enter__(self) -> SIGINTSafeProcessPoolTaskRunner:
        # Workers are spawned lazily, so the guard stays until __exit__.
        self._spawn_guard = suppress_main_reimport()
        self._spawn_guard.__enter__()

        result = super().__enter__()
        # Replace the process pool with one whose workers ignore SIGINT
        if self._executor is not None:
            self._executor.shutdown(wait=False)
        mp_context = multiprocessing.get_context("spawn")
        self._executor = ProcessPoolExecutor(
            max_workers=self._max_workers,
            mp_context=mp_context,
            initializer=_ignore_sigint,
        )
        return result

    def __exit__(self, *args: object, **kwargs: object) -> None:
        try:
            super().__exit__(*args, **kwargs)
        finally:
            self._spawn_guard.__exit__(None, None, None)


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
        """Build a local ProcessPool flow.

        GPU operations default to sequential execution (max_workers=1) to
        avoid GPU memory contention and CUDA context conflicts. CPU
        operations use the configured pool size.
        """
        if execution.max_workers is not None:
            max_workers = execution.max_workers
        elif resources.gpus > 0:
            max_workers = 1
        else:
            max_workers = self._default_max_workers

        return self._build_prefect_flow(
            SIGINTSafeProcessPoolTaskRunner(max_workers=max_workers)
        )

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
        if r.extra:
            warnings.warn(
                f"Operation {operation.name!r} has SLURM-specific resources "
                f"(extra={r.extra!r}) but backend is 'local'. "
                f"These will be ignored.",
                stacklevel=2,
            )
