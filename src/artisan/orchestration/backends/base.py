"""Backend abstraction base classes.

Defines the ABC and trait dataclasses that all execution backends
implement. Users interact with pre-built instances via the ``Backend``
namespace, not with ``BackendBase`` directly.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment


@dataclass(frozen=True)
class WorkerTraits:
    """Worker-side behavior that varies by backend.

    These values are embedded in RuntimeEnvironment and serialized
    to worker processes. They control I/O behavior on the worker.

    Attributes:
        worker_id_env_var: Environment variable for worker ID (e.g. SLURM_ARRAY_TASK_ID).
        shared_filesystem: Whether workers share a filesystem with the orchestrator.
    """

    worker_id_env_var: str | None = None
    shared_filesystem: bool = False

    @property
    def needs_staging_fsync(self) -> bool:
        """NFS requires explicit fsync for cross-node visibility."""
        return self.shared_filesystem


@dataclass(frozen=True)
class OrchestratorTraits:
    """Orchestrator-side post-dispatch behavior.

    These control what the step executor does between dispatch and commit.
    Read by the step executor, never sent to workers.

    Attributes:
        shared_filesystem: Whether the staging filesystem is shared (NFS).
        staging_verification_timeout: Seconds to wait for staging files to appear.
    """

    shared_filesystem: bool = False
    staging_verification_timeout: float = 60.0

    @property
    def needs_staging_verification(self) -> bool:
        """NFS attribute caching requires polling for file visibility."""
        return self.shared_filesystem


class BackendBase(ABC):
    """A complete execution backend.

    Bundles compute dispatch, storage traits, and worker configuration
    into a single object. Subclasses implement concrete backends.
    Users access pre-built instances via the Backend namespace
    (e.g., Backend.SLURM), not this class directly.

    Subclasses must define three ClassVar attributes:
        name: Short string identifier (e.g. "local", "slurm").
        worker_traits: WorkerTraits instance.
        orchestrator_traits: OrchestratorTraits instance.
    """

    name: ClassVar[str]
    worker_traits: ClassVar[WorkerTraits]
    orchestrator_traits: ClassVar[OrchestratorTraits]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Validate that required ClassVar attributes are defined."""
        super().__init_subclass__(**kwargs)
        for attr in ("name", "worker_traits", "orchestrator_traits"):
            if not hasattr(cls, attr):
                raise TypeError(
                    f"BackendBase subclass {cls.__name__!r} must define {attr!r}"
                )

    @abstractmethod
    def create_flow(
        self,
        operation: OperationDefinition,
        step_number: int,
    ) -> Callable[[str, RuntimeEnvironment], list[dict]]:
        """Build a configured Prefect flow for this backend.

        Args:
            operation: Fully configured operation instance.
            step_number: Pipeline step number (for naming).

        Returns:
            Callable that takes (units_path, runtime_env) and returns result dicts.
        """
        ...

    @abstractmethod
    def capture_logs(
        self,
        results: list[dict],
        staging_root: Path,
        failure_logs_root: Path | None,
        operation_name: str,
    ) -> None:
        """Post-dispatch: capture backend-specific worker logs into results.

        Args:
            results: Result dicts from dispatch (may contain worker_log key).
            staging_root: Root staging directory.
            failure_logs_root: Root directory for failure log files.
            operation_name: Operation name for log directory structure.
        """
        ...

    def validate_operation(self, operation: OperationDefinition) -> None:
        """Validate that operation config is compatible with this backend.

        Called before dispatch. Default is a no-op. Override to add checks.

        Args:
            operation: Operation to validate.
        """

    def _build_prefect_flow(
        self,
        task_runner: Any,
    ) -> Callable[[str, RuntimeEnvironment], list[dict]]:
        """Build a Prefect flow that maps execute_unit_task over units.

        Units are loaded from a pickle file on disk to avoid exceeding
        PostgreSQL's JSONB size limit for Prefect flow parameters.

        Args:
            task_runner: Prefect TaskRunner instance.

        Returns:
            Flow callable accepting (units_path, runtime_env).
        """
        from prefect import flow, unmapped

        from artisan.orchestration.engine.dispatch import (
            _collect_results,
            _load_units,
            execute_unit_task,
        )

        @flow(task_runner=task_runner)
        def step_flow(
            units_path: str,
            runtime_env: RuntimeEnvironment,
        ) -> list[dict]:
            units = _load_units(Path(units_path))
            futures = execute_unit_task.map(units, runtime_env=unmapped(runtime_env))
            return _collect_results(futures)

        return step_flow
