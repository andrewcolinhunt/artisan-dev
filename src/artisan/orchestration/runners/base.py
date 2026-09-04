"""Public step-runner abstraction and execution traits.

Defines the ABC and trait dataclasses that all step runners implement.
Third-party providers subclass ``RunnerBase`` and pass instances to Artisan.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar

from artisan.orchestration.engine.lifecycle_router import LifecycleRouter
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.runner_resources import RunnerResources


@dataclass(frozen=True)
class WorkerTraits:
    """Worker-side behavior that varies by step_runner.

    These values are embedded in RuntimeEnvironment and serialized
    to worker processes. They control I/O behavior on the worker.

    Attributes:
        worker_id_env_var: Environment variable for worker ID (e.g. SLURM_ARRAY_TASK_ID).
        shared_filesystem: Whether workers share a filesystem with the orchestrator.
    """

    worker_id_env_var: str | None = None
    shared_filesystem: bool = False


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


class RunnerBase(ABC):
    """A complete execution step_runner.

    Bundles compute dispatch, storage traits, and worker configuration
    into a single object. Subclasses implement concrete runners.
    Core exposes a pre-built local runner. External providers subclass this
    class and supply configured instances directly.

    Subclasses must define three ClassVar attributes:
        name: Stable identifier used in execution provenance.
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
                msg = f"RunnerBase subclass {cls.__name__!r} must define {attr!r}"
                raise TypeError(msg)

    @abstractmethod
    def create_lifecycle_router(
        self,
        runner_resources: RunnerResources,
        batch_strategy: BatchStrategy,
        step_number: int,
        job_name: str,
        log_folder: str | None = None,
        staging_root: str | None = None,
    ) -> LifecycleRouter:
        """Build a configured lifecycle router for this step_runner.

        Args:
            runner_resources: Hardware resource allocation.
            batch_strategy: Batching and scheduling configuration.
            step_number: Pipeline step number (for naming).
            job_name: Human-readable name for logging and scheduler labels.
            log_folder: Directory for scheduler log files (e.g. submitit logs).
            staging_root: Root directory for staging files (shared-FS runners).

        Returns:
            Configured lifecycle router.
        """
        ...

    def validate_operation(self, operation: Any) -> None:  # noqa: B027
        """Validate that operation config is compatible with this step_runner.

        Called before dispatch. Default is a deliberate no-op (not abstract);
        subclasses override to add checks.

        Args:
            operation: Operation to validate.
        """
