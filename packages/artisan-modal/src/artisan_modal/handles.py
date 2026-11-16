"""Exact creator and curator result identities over one retained Modal pack owner."""

from __future__ import annotations

from dataclasses import dataclass

from artisan.orchestration.runner_api import (
    AssignmentAttempt,
    AssignmentReply,
    CancellationAcknowledgement,
    CuratorAdmissionReference,
    CuratorReply,
    OwnershipState,
    SealedDelivery,
    StagingResult,
)
from artisan_modal.pack_owner import ModalPackOwner


@dataclass(frozen=True)
class ModalAssignmentHandle:
    attempt: AssignmentAttempt
    worker_owner: ModalPackOwner
    index: int = 0
    sensitive_values: tuple[str, ...] = ()

    def result(self, timeout: float | None = None) -> SealedDelivery:
        return AssignmentReply.model_validate_json(
            self.worker_owner.result(self.index, timeout)
        ).result(self.attempt, sensitive_values=self.sensitive_values)

    def done(self) -> bool:
        return self.worker_owner.done(self.index)

    def ownership_state(self) -> OwnershipState:
        return self.worker_owner.ownership_state()

    def cancel(self) -> CancellationAcknowledgement:
        return self.worker_owner.cancel(self.index)


@dataclass(frozen=True)
class ModalCuratorHandle:
    admission: CuratorAdmissionReference
    worker_owner: ModalPackOwner
    index: int = 0
    sensitive_values: tuple[str, ...] = ()

    def result(self, timeout: float | None = None) -> StagingResult:
        return CuratorReply.model_validate_json(
            self.worker_owner.result(self.index, timeout)
        ).result(self.admission, sensitive_values=self.sensitive_values)

    def done(self) -> bool:
        return self.worker_owner.done(self.index)

    def ownership_state(self) -> OwnershipState:
        return self.worker_owner.ownership_state()

    def cancel(self) -> CancellationAcknowledgement:
        return self.worker_owner.cancel(self.index)
