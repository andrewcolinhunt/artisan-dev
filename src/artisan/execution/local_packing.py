"""Pack sequential calls into one qualified interpreter with separate unit owners."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import CancelledError, Future
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Protocol, cast

from pydantic import BaseModel, ConfigDict

from artisan.execution.assignment import Assignment, RetainedResultSealer
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.execution.compute.prepared import freeze_runtime_injections
from artisan.execution.local_assignment import PreparedLocalBindings
from artisan.execution.local_call import (
    InertLocalCallHandle,
    InertWorkerCall,
    LocalCallHandle,
    LocalCallStartupError,
    _write_private,
)
from artisan.execution.local_profile import LocalWorkerProfile
from artisan.execution.local_worker import LocalAssignmentHandle
from artisan.execution.worker_reply import execute_call_directory
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.execution.worker_ownership import OwnershipState, WorkerOwner
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus
from artisan.utils._command_supervisor import CLEANUP_SECONDS
from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    CommandSupervisor,
    ManagedCommand,
    OwnershipOutcome,
    reject_nested_native_scope,
    validate_worker_attachment,
)
from artisan.utils.configuration_secrets import COMMAND_ATTACHMENT_ENV


@dataclass(frozen=True)
class LocalAssignmentRequest:
    """One admitted unit and its selected runtime values, independent of packing."""

    assignment: Assignment | AdmittedAssignment
    bindings: PreparedLocalBindings
    sealer: RetainedResultSealer | None
    runtime_env: Mapping[str, str] = field(default_factory=dict)
    admission_dispatch: AssignmentDispatch | None = None


class _UnitEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    directory: Path
    attachment: dict[str, str]


class LocalPackStartupError[ResultT](RuntimeError):
    """Keep unit handles and transport evidence after partially started submission."""

    def __init__(
        self,
        handles: tuple[LocalCallHandle[ResultT], ...],
        transport_outcomes: tuple[OwnershipOutcome, ...],
        worker_owners: tuple[WorkerOwner, ...] = (),
    ) -> None:
        super().__init__(
            "Local pack startup failed; affected unit handles are retained"
        )
        self.handles = handles
        self.transport_outcomes = transport_outcomes
        self.worker_owners = worker_owners


class _CallFactory[ResultT](Protocol):
    def __call__(
        self,
        *,
        dispatch: Callable[[LocalCallHandle[ResultT], Path], ResultT],
        worker_owner: WorkerOwner,
    ) -> LocalCallHandle[ResultT]: ...


class _LocalPack[ResultT]:
    """Own only the shared transport; each handle owns one unit's commands."""

    def __init__(
        self,
        profile: LocalWorkerProfile,
        factories: Sequence[_CallFactory[ResultT]],
        working_root: Path,
        runtime_env: Mapping[str, str],
    ) -> None:
        self.profile = profile
        self.runtime_env = runtime_env
        self.directory = Path(
            tempfile.mkdtemp(prefix="artisan-pack-", dir=working_root)
        )
        try:
            self.owner = CommandSupervisor.start()
        except BaseException:
            shutil.rmtree(self.directory)
            raise
        self.ready: dict[LocalCallHandle[ResultT], tuple[Path, Future[ResultT]]] = {}
        self.condition = threading.Condition()
        self.cleanup_lock = threading.Lock()
        self.handles: list[LocalCallHandle[ResultT]] = []
        self.failed: BaseException | None = None
        self.outcome: OwnershipOutcome | None = None
        self._thread: threading.Thread | None = None
        self._finished = threading.Event()
        self._stop = threading.Event()
        try:
            for factory in factories:
                self.handles.append(factory(dispatch=self.dispatch, worker_owner=self))
            self._thread = threading.Thread(target=self.run, name="artisan-local-pack")
            self._thread.start()
        except BaseException as error:
            if isinstance(error, LocalCallStartupError):
                self.handles.append(cast(LocalCallHandle[ResultT], error.handle))
            outcome = self.abort(error, time.monotonic() + CLEANUP_SECONDS)
            outcomes: tuple[OwnershipOutcome, ...] = (outcome,)
            if isinstance(error, CommandOwnershipError):
                outcomes += (error.cleanup_outcome,)
            raise LocalPackStartupError(
                tuple(self.handles), outcomes, (self,)
            ) from error

    def ownership_state(self) -> OwnershipState:
        """Keep the transport pending after individual unit replies arrive."""
        with self.condition:
            if self.outcome is not None and self.outcome.state == "unknown":
                return OwnershipState.UNKNOWN
            if not self._finished.is_set():
                return OwnershipState.PENDING
            return (
                OwnershipState(self.outcome.state)
                if self.outcome is not None
                else OwnershipState.PENDING
            )

    def record_outcome(self, outcome: OwnershipOutcome) -> OwnershipOutcome:
        with self.condition:
            if self.outcome is None or self.outcome.state != "unknown":
                self.outcome = outcome
            return self.outcome

    def request_close(self, deadline: float) -> None:
        """Close the whole worker only when its controller cancels the pack."""
        self._stop.set()
        if self.ownership_state() != OwnershipState.CLOSED:
            self.owner.request_close(deadline)

    def abort(self, error: BaseException, deadline: float) -> OwnershipOutcome:
        """Close owned work within one deadline, retaining unsettled unit handles."""
        self._stop.set()
        failed = False
        for handle in self.handles:
            try:
                failed |= handle.cancel().status == CancellationStatus.UNKNOWN
            except BaseException:
                failed = True
        self.fail_pending(error)
        try:
            self.owner.request_close(deadline)
            outcome = self.owner.finish(deadline)
        except BaseException:
            outcome = OwnershipOutcome("unknown", reason="Pack startup cleanup failed")
        for handle in self.handles:
            try:
                handle.result(timeout=max(0, deadline - time.monotonic()))
            except FutureTimeout:
                break
            except BaseException:
                pass
        running = self._thread is not None and self._thread.ident is not None
        if running and not self._finished.wait(max(0, deadline - time.monotonic())):
            failed = True
        if failed:
            outcome = OwnershipOutcome(
                "unknown", reason="Pack startup cleanup unsettled"
            )
        if outcome.state == "closed" and not running:
            try:
                self.remove_directory()
            except OSError:
                outcome = OwnershipOutcome(
                    "unknown", reason="Pack scratch cleanup failed"
                )
        outcome = self.record_outcome(outcome)
        if not running:
            self._finished.set()
        return outcome

    def remove_directory(self) -> None:
        with self.cleanup_lock:
            if self.directory.exists():
                shutil.rmtree(self.directory)

    def dispatch(self, handle: LocalCallHandle[ResultT], directory: Path) -> ResultT:
        reply: Future[ResultT] = Future()
        with self.condition:
            if self.failed is not None:
                raise self.failed
            self.ready[handle] = directory, reply
            self.condition.notify_all()
        return reply.result()

    def fail_pending(self, error: BaseException) -> None:
        with self.condition:
            self.failed = error
            for _directory, reply in self.ready.values():
                if not reply.done():
                    reply.set_exception(error)
            self.condition.notify_all()

    def collect(self, _line: str = "") -> None:
        with self.condition:
            for handle, (directory, reply) in self.ready.items():
                path = directory / "reply.json"
                if reply.done() or not path.exists():
                    continue
                try:
                    reply.set_result(handle._decode_reply(path.read_bytes()))
                except Exception as error:
                    reply.set_exception(error)

    def run(self) -> None:
        process: ManagedCommand | None = None
        try:
            with self.condition:
                while any(
                    handle not in self.ready and not handle.done()
                    for handle in self.handles
                ):
                    self.condition.wait(0.05)
            entries = []
            for handle in self.handles:
                if handle not in self.ready:
                    continue
                directory, reply = self.ready[handle]
                try:
                    handle._check_cancelled()
                except CancelledError as error:
                    reply.set_exception(error)
                    continue
                entries.append(
                    _UnitEntry(directory=directory, attachment=handle._owner.attachment)
                )
            if not entries:
                return
            index = self.directory / "units.json"
            _write_private(
                index,
                json.dumps(
                    [entry.model_dump(mode="json") for entry in entries]
                ).encode(),
            )
            environment = self.profile.environment(
                self.directory / "launcher", self.runtime_env
            )
            command = environment.wrap_command(
                [
                    "python",
                    "-I",
                    "-m",
                    __name__,
                    str(index),
                ]
            )
            variables = environment.prepare_env()
            if self._stop.is_set():
                raise CancelledError()
            process = ManagedCommand(
                self.owner.attachment,
                command,
                variables,
                str(self.directory),
                merged=False,
                external=False,
            )
            process.communicate(on_line=self.collect)
            self.collect()
            if process.returncode != 0:
                msg = "Packed local worker exited before completing its units"
                raise RuntimeError(msg)
        except BaseException as error:
            self.owner.request_close(time.monotonic() + CLEANUP_SECONDS)
            if process is not None:
                process.interrupt(error)
            self.fail_pending(error)
        finally:
            try:
                self.finish_transport(process)
            finally:
                self.fail_pending(
                    CommandOwnershipError("Packed worker did not complete this unit")
                )
                self._finished.set()

    def finish_transport(self, process: ManagedCommand | None) -> None:
        """Publish closure only after transport collection and scratch removal."""
        failed = False
        try:
            if process is not None:
                process.dispose()
        except BaseException:
            failed = True
        try:
            outcome = self.owner.finish(time.monotonic() + CLEANUP_SECONDS)
            with self.condition:
                if outcome.state == "closed" and (
                    self.outcome is None or self.outcome.state != "unknown"
                ):
                    self.remove_directory()
        except BaseException:
            failed = True
        if failed:
            outcome = OwnershipOutcome(
                "unknown", reason="Pack transport cleanup failed"
            )
        self.record_outcome(outcome)


def launch_local_assignments(
    requests: Sequence[LocalAssignmentRequest],
    *,
    working_root: Path,
    units_per_worker: int,
) -> tuple[LocalAssignmentHandle, ...]:
    """Launch real sequential process packs while retaining separate unit handles.

    Each pack requires identical worker and transport profiles; tool targets may
    differ. A running native Python phase cannot be forcibly cancelled alone in
    a shared interpreter. Its cancellation remains unresolved until it returns.
    """
    reject_nested_native_scope()
    if units_per_worker < 1 or not working_root.is_absolute():
        msg = "Packing requires a positive size and absolute owned working root"
        raise ValueError(msg)
    groups = [
        requests[index : index + units_per_worker]
        for index in range(0, len(requests), units_per_worker)
    ]
    for group in groups:
        first = group[0].bindings
        if any(
            item.bindings.worker.digest != first.worker.digest
            or item.bindings.compatibility != first.compatibility
            or item.bindings.launcher != first.launcher
            or item.bindings.native_gpu != first.native_gpu
            for item in group
        ):
            msg = "A process pack requires one worker, transport profile and GPU allocation"
            raise ValueError(msg)
    factories: list[
        tuple[LocalWorkerProfile, tuple[_CallFactory[SealedDelivery], ...]]
    ] = []
    for group in groups:
        factories.append(
            (
                group[0].bindings.worker_profile,
                tuple(
                    partial(
                        LocalAssignmentHandle,
                        request.assignment,
                        request.bindings,
                        request.sealer,
                        working_root=working_root,
                        runtime_env=request.runtime_env,
                        admission_dispatch=request.admission_dispatch,
                    )
                    for request in group
                ),
            )
        )
    return cast(
        tuple[LocalAssignmentHandle, ...], _launch_packs(factories, working_root)
    )


def launch_inert_calls(
    calls: Sequence[InertWorkerCall],
    profile: LocalWorkerProfile,
    *,
    working_root: Path,
    runtime_env: Mapping[str, str] | None = None,
    preserve_working: bool = False,
) -> tuple[InertLocalCallHandle, ...]:
    """Run one ordered pack of inert calls under independent unit owners.

    The profile selects node-local activation. Each trusted call must separately
    verify its admitted provider/scientific software. No installation or live
    payload decoding happens in this launcher. Native Python cannot be forcibly
    cancelled per unit while sharing an interpreter; whole-worker closure is
    available through each handle's retained worker owner.

    Providers may supply selected literal allocation variables. These are launch
    observations, not software identity; import/loader/ownership overrides reject
    before any owner starts. Secrets use the existing call redaction path.
    """
    reject_nested_native_scope()
    injections = freeze_runtime_injections(runtime_env or {})
    if not working_root.is_absolute():
        msg = "Inert packing requires an absolute owned working root"
        raise ValueError(msg)
    if any(call.header.compatibility != profile.worker.compatibility for call in calls):
        msg = "An inert process pack requires the exact qualified worker compatibility"
        raise ValueError(msg)
    if not calls:
        return ()
    factories: tuple[_CallFactory[bytes], ...] = tuple(
        partial(
            InertLocalCallHandle,
            call,
            profile,
            working_root=working_root,
            runtime_env=injections,
            preserve_working=preserve_working,
        )
        for call in calls
    )
    return cast(
        tuple[InertLocalCallHandle, ...],
        _launch_packs(((profile, factories),), working_root, runtime_env=injections),
    )


def _launch_packs[ResultT](
    groups: Sequence[tuple[LocalWorkerProfile, Sequence[_CallFactory[ResultT]]]],
    working_root: Path,
    *,
    runtime_env: Mapping[str, str] | None = None,
) -> tuple[LocalCallHandle[ResultT], ...]:
    working_root.mkdir(parents=True, exist_ok=True)
    packs: list[_LocalPack[ResultT]] = []
    try:
        for profile, factories in groups:
            packs.append(
                _LocalPack(profile, factories, working_root, runtime_env or {})
            )
    except BaseException as error:
        deadline = time.monotonic() + CLEANUP_SECONDS
        outcomes = tuple(pack.abort(error, deadline) for pack in packs)
        handles = tuple(handle for pack in packs for handle in pack.handles)
        owners: tuple[WorkerOwner, ...] = tuple(packs)
        if isinstance(error, LocalPackStartupError):
            outcomes += error.transport_outcomes
            handles += error.handles
            owners += error.worker_owners
        elif isinstance(error, CommandOwnershipError):
            outcomes += (error.cleanup_outcome,)
        raise LocalPackStartupError(handles, outcomes, owners) from error
    return tuple(handle for pack in packs for handle in pack.handles)


def main() -> None:
    """Validate each unit attachment and header before loading its live payload."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("index", type=Path)
    args = parser.parse_args()
    if not all(key in os.environ for key in COMMAND_ATTACHMENT_ENV):
        msg = "Packed worker requires transport ownership"
        raise ValueError(msg)
    validate_worker_attachment()
    entries = [
        _UnitEntry.model_validate(value)
        for value in json.loads(args.index.read_bytes())
    ]
    for entry in entries:
        encoded = execute_call_directory(entry.directory, attachment=entry.attachment)
        temporary = entry.directory / "reply.pending"
        _write_private(temporary, encoded)
        temporary.replace(entry.directory / "reply.json")
        sys.stdout.write("Packed unit completed\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
