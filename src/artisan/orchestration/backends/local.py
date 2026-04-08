"""Local backend — ProcessPool execution on the orchestrator machine."""

from __future__ import annotations

import multiprocessing
import signal
import warnings
from concurrent.futures import ProcessPoolExecutor

from prefect.task_runners import ProcessPoolTaskRunner

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


class LocalDispatchHandle(DispatchHandle):
    """Dispatch handle for local ProcessPool execution.

    Units are passed to workers in-memory via multiprocessing pickle
    serialization — no intermediate pickle file is written.

    Args:
        task_runner: Configured ProcessPool task runner.
    """

    def __init__(self, task_runner: SIGINTSafeProcessPoolTaskRunner) -> None:
        super().__init__()
        self._task_runner = task_runner

    def dispatch(
        self,
        units: list[ExecutionUnit | ExecutionComposite],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Start local ProcessPool execution in a background thread."""
        self._assert_idle()
        self._state = _HandleState.DISPATCHED

        task_runner = self._task_runner

        def _flow_fn() -> list[UnitResult]:
            from prefect import flow, unmapped

            from artisan.orchestration.engine.dispatch import (
                _collect_results,
                execute_unit_task,
            )

            @flow(task_runner=task_runner)
            def step_flow() -> list[UnitResult]:
                futures = execute_unit_task.map(
                    units, runtime_env=unmapped(runtime_env)
                )
                return _collect_results(futures)

            return step_flow()

        self._start_background(_flow_fn)

    def cancel(self) -> None:
        """No-op — local ProcessPool workers cannot be interrupted."""


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

    def create_dispatch_handle(
        self,
        resources: ResourceConfig,
        execution: ExecutionConfig,
        step_number: int,
        job_name: str,
        log_folder: str | None = None,
        staging_root: str | None = None,
    ) -> DispatchHandle:
        """Build a local ProcessPool dispatch handle.

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

        return LocalDispatchHandle(
            SIGINTSafeProcessPoolTaskRunner(max_workers=max_workers)
        )

    def capture_logs(
        self,
        results: list[UnitResult],
        staging_root: str,
        failure_logs_root: str | None,
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
