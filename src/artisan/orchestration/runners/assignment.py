"""Provider contract for admitting and transporting complete scientific assignments."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from artisan.composites.base.compiled import CompiledPlan
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.schemas.execution.assignment import AssignmentAttempt, SealedDelivery
from artisan.schemas.execution.worker_ownership import OwnershipState, WorkerOwner
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import CancellationAcknowledgement


class AssignmentSoftware(Protocol):
    """The resolved provider binding exposes one complete software identity."""

    @property
    def software_digest(self) -> str: ...

    @property
    def software_cacheable(self) -> bool:
        """Whether the observed software covers a stable scientific dependency set."""
        ...


class AssignmentHandle(Protocol):
    """Exact unit ownership retained until execution and cleanup have settled."""

    @property
    def attempt(self) -> AssignmentAttempt: ...

    @property
    def worker_owner(self) -> WorkerOwner | None: ...

    def result(self, timeout: float | None = None) -> SealedDelivery: ...

    def done(self) -> bool: ...

    def ownership_state(self) -> OwnershipState:
        """Include shared transport cleanup even when this unit's result is ready."""
        ...

    def cancel(self) -> CancellationAcknowledgement: ...


class AssignmentLaunchError(RuntimeError):
    """Preserve partially started work and aggregate transport closure evidence."""

    def __init__(
        self,
        message: str,
        *,
        handles: Sequence[AssignmentHandle],
        cancellation: CancellationAcknowledgement,
        worker_owners: Sequence[WorkerOwner] = (),
    ) -> None:
        super().__init__(message)
        self.handles = tuple(handles)
        self.cancellation = cancellation
        self.worker_owners = tuple(worker_owners)


class AssignmentRunner(Protocol):
    """Resolve prepared software and launch one ordered process pack.

    Constructors and these methods never provision software. The controller limits
    concurrent packs and keeps each pack's slot until all its results are observed
    and its unit/shared transport ownership is closed.
    Providers return one exact handle per admitted unit or retain started handles
    in AssignmentLaunchError. An acknowledgement alone is not cleanup evidence.
    Resolution may measure prepared software in an owned provider process, but it
    must close that process before returning a binding. On failure it raises
    AssignmentLaunchError with retained worker owners and no scientific handles.
    """

    @property
    def name(self) -> str: ...

    def resolve(
        self,
        plan: CompiledPlan,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> AssignmentSoftware: ...

    def launch(
        self,
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[AssignmentHandle, ...]: ...
