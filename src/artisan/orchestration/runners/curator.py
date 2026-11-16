"""Provider contract for one admitted store-bound curator invocation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol

from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.runners.assignment import AssignmentSoftware
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.worker_ownership import OwnershipState, WorkerOwner
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import CancellationAcknowledgement

if TYPE_CHECKING:
    from artisan.execution.curator_worker import AdmittedCurator


class CuratorHandle(Protocol):
    """Retain invocation and shared-worker ownership after a staged result arrives."""

    @property
    def admission(self) -> CuratorAdmissionReference: ...

    @property
    def worker_owner(self) -> WorkerOwner | None: ...

    def result(self, timeout: float | None = None) -> StagingResult: ...

    def done(self) -> bool: ...

    def ownership_state(self) -> OwnershipState: ...

    def cancel(self) -> CancellationAcknowledgement: ...


class CuratorLaunchError(RuntimeError):
    """Retain every started owner when a curator transport cannot finish startup."""

    def __init__(
        self,
        message: str,
        *,
        handles: Sequence[CuratorHandle],
        cancellation: CancellationAcknowledgement,
        worker_owners: Sequence[WorkerOwner] = (),
    ) -> None:
        super().__init__(message)
        self.handles = tuple(handles)
        self.cancellation = cancellation
        self.worker_owners = tuple(worker_owners)


class CuratorRunner(Protocol):
    """Resolve installed software and execute a complete curator stream once.

    Resolution must close any provider measurement process before returning.
    Failed resolution retains its worker owners in CuratorLaunchError with no
    scientific handles; it cannot authorize admission or cache lookup.
    """

    @property
    def name(self) -> str: ...

    def resolve_curator(
        self,
        operation: OperationDefinition,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> AssignmentSoftware: ...

    def launch_curator(
        self,
        curator: AdmittedCurator,
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> CuratorHandle: ...
