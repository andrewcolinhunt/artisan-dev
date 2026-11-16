"""Real native science and sealed storage behind a small controller-test runner."""

from __future__ import annotations

import threading
from collections.abc import Sequence
from concurrent.futures import Future
from dataclasses import dataclass

from artisan.composites.base.compiled import CompiledPlan
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.execution.composition import execute_compiled_plan
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.scientific_failure import assignment_science
from artisan.execution.session import ExecutionSession
from artisan.orchestration.runners.assignment import (
    AssignmentSoftware,
)
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    DeliveryIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.schemas.execution.worker_ownership import OwnershipState, WorkerOwner
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.storage.io.admission import capture_admission
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_seal import stage_retained_result
from artisan.utils.command_supervisor import close_command_scope


@dataclass(frozen=True)
class NativeSoftware:
    software_digest: str = "c" * 64
    software_cacheable: bool = True
    application_modules: tuple[str, ...] = ()


@dataclass(frozen=True)
class NativeHandle:
    attempt: AssignmentAttempt
    future: Future[SealedDelivery]

    @property
    def worker_owner(self) -> WorkerOwner | None:
        return None

    def result(self, timeout: float | None = None) -> SealedDelivery:
        return self.future.result(timeout=timeout)

    def done(self) -> bool:
        return self.future.done()

    def ownership_state(self) -> OwnershipState:
        return OwnershipState.CLOSED if self.future.done() else OwnershipState.PENDING

    def cancel(self) -> CancellationAcknowledgement:
        if self.future.cancel():
            return CancellationAcknowledgement(CancellationStatus.CONFIRMED)
        return CancellationAcknowledgement(
            CancellationStatus.REJECTED
            if self.future.done()
            else CancellationStatus.UNKNOWN
        )


class NativeRunner:
    """Execute each submitted pack sequentially in one thread of this test process."""

    name = "native-test"

    def __init__(self, committer: RetainedCommitter) -> None:
        self.committer = committer
        self.packs: list[tuple[int, ...]] = []
        self.resolutions = 0

    def resolve(
        self,
        plan: CompiledPlan,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> NativeSoftware:
        plan.instantiate()
        self.resolutions += 1
        return NativeSoftware()

    def launch(
        self,
        assignments: Sequence[AdmittedAssignment],
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> tuple[NativeHandle, ...]:
        self.packs.append(
            tuple(unit.admission.attempt.assignment.unit_index for unit in assignments)
        )
        handles = tuple(
            NativeHandle(unit.admission.attempt, Future()) for unit in assignments
        )
        threading.Thread(
            target=self._execute, args=(tuple(assignments), handles, dispatch)
        ).start()
        return handles

    def _execute(
        self,
        assignments: tuple[AdmittedAssignment, ...],
        handles: tuple[NativeHandle, ...],
        dispatch: AssignmentDispatch,
    ) -> None:
        for assignment, handle in zip(assignments, handles, strict=True):
            if not handle.future.set_running_or_notify_cancel():
                continue
            try:
                handle.future.set_result(self._invoke(assignment, dispatch))
            except BaseException as error:
                handle.future.set_exception(error)

    def _invoke(
        self, assignment: AdmittedAssignment, dispatch: AssignmentDispatch
    ) -> SealedDelivery:
        delivery = DeliveryIdentity.new(assignment.admission.attempt)
        fs = dispatch.storage.filesystem()
        with (
            capture_admission(
                assignment.admission,
                delta_root=dispatch.delta_root,
                files_root=dispatch.files_root,
                fs=fs,
                verify_producer=self.committer._verify_producer,
            ) as captured,
            ExecutionSession(working_root=dispatch.working_root) as session,
            assignment_science(),
        ):
            boundary = captured.record.boundary
            roots = {
                address
                for values in boundary.input_roles().values()
                for address in values
            }
            result = execute_compiled_plan(
                assignment.plan,
                {
                    role: [captured.artifacts[address] for address in values]
                    for role, values in boundary.input_roles().items()
                },
                session,
                invocation_id=delivery.attempt.assignment.assignment_id,
                execute_adapter=lambda _operation, _path: LocalExecuteRouter(),
                context_artifacts={
                    address: artifact
                    for address, artifact in captured.artifacts.items()
                    if address not in roots
                },
                frozen_context=boundary.frozen_context,
                input_attribution=boundary.input_attribution,
                expected_input_context_digest=delivery.attempt.input_context_digest,
                step_number=captured.record.step_number,
                fs=fs,
            )
            assert close_command_scope()
            return stage_retained_result(
                dispatch.staging_root,
                SuccessfulDelivery(
                    delivery=delivery, computation_complete=True, cleanup_complete=True
                ),
                result.projection,
                result.retained_artifacts,
                fs=fs,
            )
