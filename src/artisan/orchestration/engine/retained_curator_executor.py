"""Coordinate one whole-stream curator through admission and durable acceptance."""

from __future__ import annotations

import contextvars
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import CancelledError, Executor, Future

from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.curator_worker import AdmittedCurator
from artisan.execution.recording.commands import CommandRecorder
from artisan.execution.recording.durable import configuration_recorder
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.curator_replay import (
    CuratorReplayPreparation,
    prepare_curator_replay_admission,
)
from artisan.orchestration.engine.curator_inputs import prepare_curator_inputs
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.runners.assignment import AssignmentSoftware
from artisan.orchestration.runners.curator import (
    CuratorHandle,
    CuratorLaunchError,
    CuratorRunner,
)
from artisan.orchestration.step_future import StepFuture
from artisan.schemas.execution.curator_admission import (
    CuratorAdmission,
    CuratorAdmissionReference,
)
from artisan.schemas.execution.replay import ReplayDiagnostic
from artisan.schemas.execution.worker_ownership import OwnershipState, WorkerOwner
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import (
    TERMINAL_STEP_STATUSES,
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.curator_staging import read_staged_curator_result
from artisan.storage.core.retained_reader import AcceptedArtifact
from artisan.utils._command_supervisor import CLEANUP_SECONDS
from artisan.utils.command_supervisor import CommandOwnershipError
from artisan.utils.external_tools import redact_values


class RetainedCuratorExecution:
    """Own one curator stream, its pending lifecycle, and every started worker.

    Input and software resolution happen after the pending record. A finished
    future does not release workers whose cleanup remains unknown.
    """

    def __init__(
        self,
        record: StepStartRecord,
        *,
        operation: OperationDefinition,
        resolve_inputs: Callable[[], Mapping[str, Sequence[AcceptedArtifact | str]]]
        | None,
        tracker: RetainedStepTracker,
        runner: CuratorRunner,
        dispatch: AssignmentDispatch,
        runner_resources: RunnerResources,
        skip_cache: bool = False,
        before_resolve: Callable[[], StepResult | None] | None = None,
        recorder: CommandRecorder | None = None,
        replay: CuratorReplayPreparation | None = None,
    ) -> None:
        self.record = record
        self.operation = operation
        self.resolve_inputs = resolve_inputs
        self.tracker = tracker
        self.results = CuratorResults(
            tracker, dispatch.staging_root, preserve_staging=dispatch.preserve_staging
        )
        self.reader = CuratorReader(tracker.committer)
        self.runner = runner
        self.dispatch = dispatch
        self.runner_resources = runner_resources.model_copy(deep=True)
        if (replay is None) == (resolve_inputs is None):
            msg = "Choose ordinary input resolution or a prepared curator replay"
            raise ValueError(msg)
        if replay is not None and (
            operation is not replay.operation
            or dict(dispatch.runtime_env) != replay.runtime_env
        ):
            msg = (
                "Curator replay must dispatch its restored operation and runtime values"
            )
            raise ValueError(msg)
        self.replay = replay
        self._replay_diagnostic: ReplayDiagnostic | None = None
        self.skip_cache = skip_cache or replay is not None
        self.before_resolve = before_resolve
        self.recorder = configuration_recorder(operation, recorder=recorder)
        if replay is not None:
            self.recorder.add_sensitive_data(replay.recorder.sensitive_values)
        self.recorder._learn_json(dispatch.storage)
        self.recorder.add_sensitive_data(dict(dispatch.runtime_env))
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._reference: CuratorAdmissionReference | None = None
        self._handles: list[CuratorHandle] = []
        self._worker_owners: list[WorkerOwner] = []
        self._future: Future[StepResult] | None = None
        self._cancel_deadline: float | None = None
        self._user_cancelled = False
        self._closure_unknown = False

    @property
    def handles(self) -> tuple[CuratorHandle, ...]:
        """Retain invocation ownership after result delivery or failed startup."""
        with self._lock:
            return tuple(self._handles)

    @property
    def worker_owners(self) -> tuple[WorkerOwner, ...]:
        """Retain transport owners even when startup produced no invocation handle."""
        with self._lock:
            return tuple(self._worker_owners)

    def ownership_state(self) -> OwnershipState:
        """Report worker closure independently of the terminal future."""
        with self._lock:
            if self._closure_unknown:
                return OwnershipState.UNKNOWN
            if self._future is None or not self._future.done():
                return OwnershipState.PENDING
        return self._owners_state()

    def submit(self, pool: Executor) -> StepFuture:
        """Persist the pending owner before invoking any operational resolver."""
        with self._lock:
            if self._future is not None:
                msg = "A curator execution can only be submitted once"
                raise RuntimeError(msg)
            state = self.tracker.create_attempt(self.record)
            try:
                self._future = pool.submit(contextvars.copy_context().run, self._run)
            except Exception as error:
                self._future = Future()
                self._future.set_result(self._finish_error(error))
            return StepFuture(
                state.step_number,
                state.step_name,
                state.output_roles,
                state.output_types,
                self._future,
                lambda: self.tracker.current_state(self.record.step_run_id).status,
            )

    def run(self, pool: Executor) -> StepResult:
        """Use the same admission and acceptance path for synchronous execution."""
        return self.submit(pool).result()

    def cancel(self) -> CancellationAcknowledgement:
        """Fence publication before asking retained owners to stop their workers."""
        with self._lock:
            if self._future is None:
                msg = "Submit the curator before requesting cancellation"
                raise RuntimeError(msg)
            if self._future.done():
                if self.ownership_state() == OwnershipState.CLOSED:
                    return CancellationAcknowledgement(
                        CancellationStatus.REJECTED, "Curator already completed"
                    )
                self._request_closure(time.monotonic() + CLEANUP_SECONDS)
                return CancellationAcknowledgement(
                    CancellationStatus.UNKNOWN, "Curator worker cleanup is unresolved"
                )
            self._user_cancelled = True
            self._stop.set()
            self._cancel_deadline = self._cancel_deadline or (
                time.monotonic() + CLEANUP_SECONDS
            )
        state = self._record_cancellation(CancellationStatus.REQUESTED)
        if (
            state.status in TERMINAL_STEP_STATUSES
            and self._owners_state() == OwnershipState.CLOSED
            and not self._closure_unknown
        ):
            return CancellationAcknowledgement(
                CancellationStatus.REJECTED, "Curator already selected its result"
            )
        self._request_closure(self._cancel_deadline)
        if self._closure_unknown or time.monotonic() >= self._cancel_deadline:
            self._closure_unknown = True
            return CancellationAcknowledgement(
                CancellationStatus.UNKNOWN, "Curator cleanup could not be confirmed"
            )
        return CancellationAcknowledgement(
            CancellationStatus.REQUESTED, "Waiting for curator execution and cleanup"
        )

    def _run(self) -> StepResult:
        started = time.monotonic()
        try:
            self._check_stopped()
            if self.before_resolve is not None:
                skipped = self.before_resolve()
                if skipped is not None:
                    return self.tracker.require_prework_skip(
                        self.record.step_run_id, skipped
                    )
                self._check_stopped()
            software = self.runner.resolve_curator(
                self.operation,
                self.runner_resources,
                application_modules=self.dispatch.application_modules,
            )
            self._check_stopped()
            if self.replay is not None:
                admission, self._replay_diagnostic = prepare_curator_replay_admission(
                    self.replay,
                    self.record,
                    software_digest=software.software_digest,
                    software_cacheable=software.software_cacheable,
                    committer=self.tracker.committer,
                )
            else:
                assert self.resolve_inputs is not None
                inputs = self.resolve_inputs()
                self._check_stopped()
                admission = prepare_curator_inputs(
                    self.operation,
                    self.record,
                    inputs,
                    software_digest=software.software_digest,
                    software_cacheable=software.software_cacheable,
                    committer=self.tracker.committer,
                )
            with self.tracker.committer._lock:
                self._check_stopped()
                self._reference = self.results.admit(admission)
            self._check_stopped()
            if not self.skip_cache:
                hit = lookup_curator(self._reference, reader=self.reader)
                self._check_stopped()
                if hit is not None:
                    return self.results.reuse(hit)
            handle = self._launch(self._reference, software)
            staged = self._collect(handle)
            self._check_stopped()
            return self._finish_staged(admission, staged, started)
        except CommandOwnershipError as error:
            self._closure_unknown = True
            return self._finish_error(error)
        except CuratorLaunchError as error:
            if self._reference is None:
                self._retain(error.handles, error.worker_owners)
                self._closure_unknown |= (
                    error.cancellation.status == CancellationStatus.UNKNOWN
                )
                if error.handles:
                    message = "Software resolution returned scientific curator handles"
                    return self._finish_error(ValueError(message))
            return self._finish_error(error)
        except Exception as error:
            return self._finish_error(error)

    def _launch(
        self, reference: CuratorAdmissionReference, software: AssignmentSoftware
    ) -> CuratorHandle:
        self._check_stopped()
        try:
            handle = self.runner.launch_curator(
                AdmittedCurator(
                    self.operation,
                    reference,
                    self._replay_diagnostic,
                    sensitive_values=self.recorder.sensitive_values,
                ),
                software=software,
                dispatch=self.dispatch,
            )
        except CuratorLaunchError as error:
            self._retain(error.handles, error.worker_owners)
            self._closure_unknown |= (
                error.cancellation.status == CancellationStatus.UNKNOWN
            )
            raise
        self._retain((handle,))
        if handle.admission != reference:
            msg = "Runner returned another curator admission"
            raise ValueError(msg)
        return handle

    def _retain(
        self, handles: Sequence[CuratorHandle], owners: Sequence[WorkerOwner] = ()
    ) -> None:
        with self._lock:
            self._handles.extend(handles)
            for owner in (*owners, *(handle.worker_owner for handle in handles)):
                if owner is not None and not any(
                    owner is existing for existing in self._worker_owners
                ):
                    self._worker_owners.append(owner)

    def _collect(self, handle: CuratorHandle) -> StagingResult:
        while True:
            self._check_stopped()
            state = self._owners_state()
            if state == OwnershipState.UNKNOWN:
                self._closure_unknown = True
                msg = "Curator worker cleanup could not be confirmed"
                raise CommandOwnershipError(msg)
            if handle.done() and state == OwnershipState.CLOSED:
                return handle.result(timeout=0)
            time.sleep(0.02)

    def _finish_staged(
        self, admission: CuratorAdmission, staged: StagingResult, started: float
    ) -> StepResult:
        outcome = staged.cancellation_acknowledgement
        if outcome is not None and outcome.status != CancellationStatus.REJECTED:
            self._closure_unknown |= outcome.status == CancellationStatus.UNKNOWN
            msg = "Curator worker reported cancellation before publication"
            raise RuntimeError(msg)
        if staged.execution_run_id is None:
            raise RuntimeError(staged.error or "Curator returned no sealed execution")
        result = read_staged_curator_result(
            admission,
            staged.execution_run_id,
            staging_root=self.dispatch.staging_root,
            committer=self.tracker.committer,
            duration_seconds=time.monotonic() - started,
        )
        self._check_stopped()
        assert self._reference is not None
        return self.results.finish(self._reference, staged.execution_run_id, result)

    def _finish_error(self, error: Exception) -> StepResult:
        self._stop.set()
        with self._lock:
            self._cancel_deadline = self._cancel_deadline or (
                time.monotonic() + CLEANUP_SECONDS
            )
        while True:
            self._request_closure(self._cancel_deadline)
            if self._closure_unknown or self._owners_settled():
                break
            if time.monotonic() >= self._cancel_deadline:
                self._closure_unknown = True
                break
            time.sleep(0.02)
        if self._closure_unknown or self._user_cancelled:
            self._record_cancellation(CancellationStatus.REQUESTED)
            self._record_cancellation(
                CancellationStatus.UNKNOWN
                if self._closure_unknown
                else CancellationStatus.CONFIRMED
            )
        state = self.tracker.current_state(self.record.step_run_id)
        if state.status in TERMINAL_STEP_STATUSES:
            return state.to_step_result()
        message = redact_values(
            self.recorder.sanitize(str(error) or type(error).__name__),
            tuple(self.dispatch.runtime_env.values()),
        )
        if self._reference is not None:
            return self.results.finish_without_execution(self._reference, error=message)
        cancelled = state.cancellation_status == CancellationStatus.CONFIRMED
        if not cancelled and state.status == StepStatus.PENDING:
            state = self.tracker.transition(
                state.step_run_id, state.status, StepStatus.RUNNING
            )
        result = StepResult(
            step_run_id=state.step_run_id,
            step_number=state.step_number,
            step_name=state.step_name,
            status=StepStatus.CANCELLED if cancelled else StepStatus.FAILED,
            cancellation_status=state.cancellation_status,
            error=message,
        )
        return self.tracker.transition(
            state.step_run_id, state.status, result.status, result=result
        ).to_step_result()

    def _record_cancellation(self, status: CancellationStatus) -> StepState:
        with self.tracker.committer._lock, self.tracker._lock:
            state = self.tracker.current_state(self.record.step_run_id)
            if (
                status == CancellationStatus.REQUESTED
                and state.cancellation_status is not None
            ):
                status = state.cancellation_status
            return self.results.record_cancellation(
                self.record.step_run_id, CancellationAcknowledgement(status)
            )

    def _owners_state(self) -> OwnershipState:
        try:
            owners: tuple[CuratorHandle | WorkerOwner, ...] = (
                *self.handles,
                *self.worker_owners,
            )
            states = [item.ownership_state() for item in owners]
        except Exception:
            self._closure_unknown = True
            return OwnershipState.UNKNOWN
        if OwnershipState.UNKNOWN in states:
            return OwnershipState.UNKNOWN
        if OwnershipState.PENDING in states:
            return OwnershipState.PENDING
        return OwnershipState.CLOSED

    def _owners_settled(self) -> bool:
        try:
            state = self._owners_state()
            self._closure_unknown |= state == OwnershipState.UNKNOWN
            return state == OwnershipState.CLOSED and all(
                item.done() for item in self.handles
            )
        except Exception:
            self._closure_unknown = True
            return False

    def _request_closure(self, deadline: float) -> None:
        for handle in self.handles:
            try:
                outcome = handle.cancel()
                self._closure_unknown |= outcome.status == CancellationStatus.UNKNOWN
            except Exception:
                self._closure_unknown = True
        for owner in self.worker_owners:
            try:
                owner.request_close(deadline)
            except Exception:
                self._closure_unknown = True

    def _check_stopped(self) -> None:
        if self._stop.is_set():
            raise CancelledError()
