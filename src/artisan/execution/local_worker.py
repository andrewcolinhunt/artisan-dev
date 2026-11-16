"""Creator-specific preparation and reply binding over shared supervised calls."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from artisan.execution.assignment_reply import AssignmentReply, AssignmentReplyContext
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.execution.local_call import InertWorkerCall, LocalCallHandle
from artisan.execution.worker_bootstrap import serialize_worker_call
from artisan.schemas.execution.assignment import AssignmentAttempt, SealedDelivery
from artisan.schemas.execution.worker_ownership import WorkerOwner
from artisan.utils.command_supervisor import reject_nested_native_scope

if TYPE_CHECKING:
    from artisan.execution.assignment import Assignment, RetainedResultSealer
    from artisan.execution.local_assignment import PreparedLocalBindings


class LocalAssignmentHandle(LocalCallHandle[SealedDelivery]):
    """Bind one admitted creator attempt to its shared supervised call owner."""

    def __init__(
        self,
        assignment: Assignment | AdmittedAssignment,
        bindings: PreparedLocalBindings,
        sealer: RetainedResultSealer | None,
        *,
        working_root: Path,
        runtime_env: Mapping[str, str],
        admission_dispatch: AssignmentDispatch | None = None,
        dispatch: Callable[[LocalCallHandle[SealedDelivery], Path], SealedDelivery]
        | None = None,
        worker_owner: WorkerOwner | None = None,
    ) -> None:
        if isinstance(assignment, AdmittedAssignment):
            if (
                admission_dispatch is None
                or sealer is not None
                or dict(runtime_env) != dict(admission_dispatch.runtime_env)
            ):
                msg = "Persisted local assignments require their exact storage dispatch and built-in sealer"
                raise ValueError(msg)
        elif admission_dispatch is not None or sealer is None:
            msg = "Live assignments require an explicit result sealer"
            raise ValueError(msg)
        self._assignment = assignment
        self._admission_dispatch = admission_dispatch
        self._bindings = bindings
        self._sealer = sealer
        super().__init__(
            bindings.worker_profile,
            working_root=working_root,
            runtime_env=runtime_env,
            preserve_working=(
                admission_dispatch.preserve_working
                if admission_dispatch is not None
                else False
            ),
            dispatch=dispatch,
            worker_owner=worker_owner,
        )

    @property
    def attempt(self) -> AssignmentAttempt:
        """Expose the exact admitted unit for provider-neutral controller ownership."""
        return self._assignment.attempt

    def _preflight(self) -> None:
        from artisan.execution.assignment import verify_assignment

        verify_assignment(self._assignment, self._bindings)

    def _decode_reply(self, encoded: bytes) -> SealedDelivery:
        return AssignmentReply.model_validate_json(encoded).result(
            self.attempt, sensitive_values=self.sensitive_values
        )

    def _write_payload(self, directory: Path) -> None:
        from artisan.execution.admitted_worker import execute_admitted_assignment
        from artisan.execution.assignment import execute_assignment

        function: Callable[..., SealedDelivery]
        args: tuple[object, ...]
        kwargs: dict[str, object]
        if isinstance(self._assignment, AdmittedAssignment):
            assert self._admission_dispatch is not None
            function = execute_admitted_assignment
            args = (self._assignment, self._bindings, self._admission_dispatch)
            kwargs = {"working_root": directory / "worker"}
        else:
            assert self._sealer is not None
            function = execute_assignment
            args = (self._assignment, self._bindings, self._sealer)
            kwargs = {
                "working_root": directory / "worker",
                "runtime_env": self._runtime_env,
            }
        call = serialize_worker_call(
            function,
            compatibility=self._bindings.compatibility,
            args=args,
            kwargs=kwargs,
            application_modules=self._bindings.application_modules,
        )
        self._check_cancelled()
        context = AssignmentReplyContext(
            attempt=self.attempt,
            payload_sha256=call.header.payload_sha256,
            sensitive_values=(
                *self._bindings.diagnostic_sensitive_values,
                *self._runtime_env.values(),
                *self._owner.attachment.values(),
            ),
        )
        InertWorkerCall(
            call.header, call.payload, context.model_dump_json().encode()
        ).write(directory)


def launch_local_assignment(
    assignment: Assignment,
    bindings: PreparedLocalBindings,
    sealer: RetainedResultSealer,
    *,
    working_root: Path,
    runtime_env: Mapping[str, str] | None = None,
    units_per_worker: int = 1,
) -> LocalAssignmentHandle:
    """Start one independently owned worker; batch packing has a separate entry.

    Software must already exist. This private entry does not activate public
    runner submission, promise CPU/memory reservations, or accept durable output.
    """
    if units_per_worker != 1:
        msg = "Single-assignment launch requires units_per_worker=1"
        raise ValueError(msg)
    reject_nested_native_scope()
    if not working_root.is_absolute():
        msg = "Local assignment working root must be an absolute owned path"
        raise ValueError(msg)
    working_root.mkdir(parents=True, exist_ok=True)
    return LocalAssignmentHandle(
        assignment,
        bindings,
        sealer,
        working_root=working_root,
        runtime_env=runtime_env or {},
    )
