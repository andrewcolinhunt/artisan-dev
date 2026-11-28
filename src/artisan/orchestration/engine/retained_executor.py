"""Coordinate finite admitted work through the shared step lifecycle."""

from __future__ import annotations

import contextvars
import json
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import CancelledError, Executor, Future
from dataclasses import replace
from functools import partial
from typing import cast
from uuid import uuid4

from artisan.composites.base.compiled import CompiledPlan
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.external_access import capture_external_access
from artisan.execution.recording.commands import CommandRecorder
from artisan.execution.recording.retained_replay import (
    record_replay_diagnostic,
    record_selected_failed_replay_diagnostic,
)
from artisan.orchestration.engine.retained_inputs import PreparedAssignmentInputs
from artisan.orchestration.engine.retained_step_results import RetainedStepResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.retained_replay import RetainedReplayPreparation
from artisan.orchestration.runners.assignment import (
    AssignmentHandle,
    AssignmentLaunchError,
    AssignmentRunner,
    AssignmentSoftware,
)
from artisan.orchestration.step_future import StepFuture
from artisan.schemas.enums import CachePolicy, FailurePolicy
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import AssignmentAttempt
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.retained_replay import RetainedReplayDiagnostic
from artisan.schemas.execution.step_admission import StepAdmission
from artisan.schemas.execution.worker_ownership import OwnershipState, WorkerOwner
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.output_reference import StepSource
from artisan.schemas.orchestration.step_lifecycle import (
    TERMINAL_STEP_STATUSES,
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.cache.retained_lookup import (
    RetainedCacheHit,
    lookup_assignment,
    lookup_step,
)
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.admission import publish_admission
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.step_result_plan import StepUnitFailure
from artisan.utils._command_supervisor import CLEANUP_SECONDS
from artisan.utils.command_supervisor import CommandOwnershipError
from artisan.utils.external_tools import redact_values


class RetainedStepExecution:
    """Own one step's controller work; the manager retains numbering and futures.

    Resolvers run only after a pending owner exists. Provider handles remain on
    this object even when cancellation cannot prove that execution has stopped.
    """

    def __init__(
        self,
        record: StepStartRecord,
        *,
        resolve_plan: Callable[[], CompiledPlan],
        resolve_inputs: Callable[[CompiledPlan], tuple[PreparedAssignmentInputs, ...]],
        tracker: RetainedStepTracker,
        runner: AssignmentRunner,
        dispatch: AssignmentDispatch,
        runner_resources: RunnerResources,
        batch_strategy: BatchStrategy,
        skip_cache: bool = False,
        cache_policy: CachePolicy = CachePolicy.ALL_SUCCEEDED,
        failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
        recorder: CommandRecorder | None = None,
        before_resolve: Callable[[], StepResult | None] | None = None,
        replay: RetainedReplayPreparation | None = None,
        capture_plan: bool = False,
    ) -> None:
        self.record = record
        self.resolve_plan = resolve_plan
        self.capture_plan = capture_plan
        self.resolve_inputs = resolve_inputs
        self.tracker = tracker
        self.results = RetainedStepResults(tracker)
        self.reader = self.results.reader
        self.runner = runner
        self.dispatch = dispatch
        self.runner_resources = runner_resources.model_copy(deep=True)
        self.batch_strategy = batch_strategy.model_copy(deep=True)
        self.replay = replay
        self.replay_diagnostic: RetainedReplayDiagnostic | None = None
        if replay is not None and (
            record.step_run_id != replay.identity.step_run_id
            or record.replay_of_execution_run_id != replay.delivery.delivery_id
        ):
            msg = "Diagnostic execution requires its exact fresh replay owner"
            raise ValueError(msg)
        self.skip_cache = skip_cache or replay is not None
        if not isinstance(cast(object, cache_policy), CachePolicy):
            msg = "cache_policy must be a CachePolicy member"
            raise TypeError(msg)
        self.cache_policy = cache_policy
        self.failure_policy = failure_policy
        self.before_resolve = before_resolve
        self.recorder = recorder or CommandRecorder()
        self.recorder._learn_json(dispatch.storage)
        self.recorder.add_sensitive_data(dict(dispatch.runtime_env))
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._admissions: list[AdmissionReference] = []
        self._handles: list[AssignmentHandle] = []
        self._worker_owners: list[WorkerOwner] = []
        self._future: Future[StepResult] | None = None
        self._plan: CompiledPlan | None = None
        self._cancel_deadline: float | None = None
        self._user_cancelled = False
        self._closure_unknown = False
        self._observed_failures: dict[
            int, tuple[AssignmentHandle, StepUnitFailure]
        ] = {}

    @classmethod
    def from_replay(
        cls,
        replay: RetainedReplayPreparation,
        *,
        committer: RetainedCommitter,
        runner: AssignmentRunner,
        dispatch: AssignmentDispatch,
        runner_resources: RunnerResources | None = None,
    ) -> RetainedStepExecution:
        """Run one frozen unit under a fresh, isolated diagnostic pipeline owner."""
        recipe = replay.plan.recipe
        record = StepStartRecord(
            step_run_id=replay.identity.step_run_id,
            replay_of_execution_run_id=replay.delivery.delivery_id,
            step_number=replay.source.admission.step_number,
            step_name=recipe.definition.name,
            operation_class=f"{recipe.definition.module}.{recipe.definition.qualname}",
            params_json=recipe.params_json,
            input_refs_json="{}",
            compute_backend=runner.name,
            compute_options_json="{}",
            output_roles_json=json.dumps(sorted(item.role for item in recipe.outputs)),
            output_types_json=json.dumps(
                {item.role: item.artifact_type for item in recipe.outputs}
            ),
        )
        return cls(
            record,
            resolve_plan=lambda: replay.plan,
            resolve_inputs=lambda _plan: (
                PreparedAssignmentInputs(
                    replay.identity, replay.boundary, replay.artifacts
                ),
            ),
            tracker=RetainedStepTracker(committer, f"replay-{uuid4().hex}"),
            runner=runner,
            dispatch=replace(dispatch, runtime_env=replay.runtime_env),
            runner_resources=runner_resources or RunnerResources.model_validate({}),
            batch_strategy=BatchStrategy.model_validate({}),
            replay=replay,
        )

    @property
    def handles(self) -> tuple[AssignmentHandle, ...]:
        """Keep transport owners reachable after partial startup or unknown closure."""
        with self._lock:
            return tuple(self._handles)

    def ownership_state(self) -> OwnershipState:
        """Keep unresolved owners visible independently of the completed step future."""
        with self._lock:
            if self._closure_unknown:
                return OwnershipState.UNKNOWN
            if self._future is None or not self._future.done():
                return OwnershipState.PENDING
        return self._pack_ownership((*self.handles, *self.worker_owners))

    @property
    def worker_owners(self) -> tuple[WorkerOwner, ...]:
        """Retain transport authority even when startup returned no unit handle."""
        with self._lock:
            return tuple(self._worker_owners)

    def submit(self, pool: Executor) -> StepFuture:
        """Persist ownership and optionally capture the plan before scheduling work."""
        with self._lock:
            if self._future is not None:
                msg = "A step execution can only be submitted once"
                raise RuntimeError(msg)
            state = self.tracker.create_attempt(self.record)
            try:
                if self.capture_plan:
                    self._plan = self.resolve_plan()
                self._future = pool.submit(contextvars.copy_context().run, self._run)
            except Exception as error:
                self._future = Future()
                self._future.set_result(self._fail_unadmitted(error))
            return StepFuture(
                state.step_number,
                state.step_name,
                state.output_roles,
                state.output_types,
                self._future,
                lambda: self.tracker.current_state(self.record.step_run_id).status,
                source=StepSource(
                    store=self.tracker.committer.store_identity,
                    step_run_id=self.record.step_run_id,
                ),
            )

    def run(self, pool: Executor) -> StepResult:
        """Use the same submission and durable result path synchronously."""
        return self.submit(pool).result()

    def cancel(self) -> CancellationAcknowledgement:
        """Fence current units before requesting provider cleanup; never retry."""
        with self._lock:
            if self._future is None:
                msg = "Submit the step before requesting cancellation"
                raise RuntimeError(msg)
            if self._future.done():
                if self.ownership_state() != OwnershipState.CLOSED:
                    self._request_worker_closure(time.monotonic() + CLEANUP_SECONDS)
                    return CancellationAcknowledgement(
                        CancellationStatus.UNKNOWN,
                        "Step result is complete but worker cleanup is unresolved",
                    )
                return CancellationAcknowledgement(
                    CancellationStatus.REJECTED, "Step execution already completed"
                )
            self._user_cancelled = True
            self._stop.set()
            self._cancel_deadline = self._cancel_deadline or (
                time.monotonic() + CLEANUP_SECONDS
            )
        state = self._record_cancellation(CancellationStatus.REQUESTED)
        if state.status in TERMINAL_STEP_STATUSES:
            if state.cancellation_status == CancellationStatus.UNKNOWN or (
                self._pack_ownership((*self.handles, *self.worker_owners))
                != OwnershipState.CLOSED
            ):
                self._request_worker_closure(self._cancel_deadline)
                return CancellationAcknowledgement(
                    CancellationStatus.UNKNOWN,
                    "Terminal step still retains unresolved worker cleanup",
                )
            return CancellationAcknowledgement(
                CancellationStatus.REJECTED,
                "Step has already selected its terminal result",
            )
        outcome = self._cancel_live()
        if time.monotonic() >= self._cancel_deadline:
            self._closure_unknown = True
            return CancellationAcknowledgement(
                CancellationStatus.UNKNOWN,
                "Controller work has not settled before the cleanup deadline",
            )
        return outcome

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
            plan = self._plan if self._plan is not None else self.resolve_plan()
            self._check_stopped()
            prepared = self.resolve_inputs(plan)
            self._check_stopped()
            _validate_partition_owners(prepared, self.record)
            if not prepared:
                return self.tracker.admit(
                    self._step_admission(plan, None)
                ).to_step_result()
            software = self.runner.resolve(
                plan,
                self.runner_resources,
                application_modules=self.dispatch.application_modules,
            )
            if plan.called_operations and not getattr(
                software, "supports_configured_calls", False
            ):
                msg = "Selected runner does not support configured operation calls"
                raise ValueError(msg)
            self._check_stopped()
            if self.replay is not None:
                self.replay_diagnostic = self.replay.validate_selection(
                    self.tracker.committer, software.software_digest
                )
                self._check_stopped()
            self._publish(plan, prepared, software)
        except CancelledError:
            return self._cancel_unadmitted()
        except AssignmentLaunchError as error:
            self._retain_owners(error.handles, error.worker_owners)
            self._closure_unknown |= (
                error.cancellation.status == CancellationStatus.UNKNOWN
            )
            if error.handles:
                message = "Software resolution returned scientific assignment handles"
                return self._finish_error(ValueError(message), started)
            return self._finish_error(error, started)
        except Exception as error:
            return self._fail_unadmitted(error)
        try:
            self.tracker.admit(self._step_admission(plan, software.software_digest))
            self._check_stopped()
            if not self.skip_cache:
                whole = self.results.lookup(self.record.step_run_id, self.cache_policy)
                self._check_stopped()
                if whole is not None:
                    return self.results.reuse(
                        whole, duration_seconds=time.monotonic() - started
                    )
            pending = self._reuse(plan)
            failures = self._terminal_failures(self._dispatch(pending, software))
            self._settle_cancellation()
            result = self.results.finish(
                self.record.step_run_id,
                failures=failures,
                duration_seconds=time.monotonic() - started,
                failure_policy=self.failure_policy,
            )
            if self.replay_diagnostic is not None:
                for failure in failures.values():
                    if failure.failed_delivery is not None:
                        record_selected_failed_replay_diagnostic(
                            failure.failed_delivery,
                            self.replay_diagnostic,
                            committer=self.tracker.committer,
                        )
            return result
        except Exception as error:
            return self._finish_error(error, started)

    def _publish(
        self,
        plan: CompiledPlan,
        prepared: Sequence[PreparedAssignmentInputs],
        software: AssignmentSoftware,
    ) -> None:
        for unit in prepared:
            self._check_stopped()
            reference = _publish_assignment(
                unit,
                plan,
                self.record,
                software,
                self.tracker.committer,
            )
            with self._lock:
                self._admissions.append(reference)
        self._check_stopped()

    def _step_admission(
        self, plan: CompiledPlan, software_digest: str | None
    ) -> StepAdmission:
        return StepAdmission(
            step_run_id=self.record.step_run_id,
            step_number=self.record.step_number,
            recipe=plan.recipe,
            software_digest=software_digest,
            assignments=tuple(self._admissions),
        )

    def _reuse(self, plan: CompiledPlan) -> tuple[AdmittedAssignment, ...]:
        reused = set()
        if not self.skip_cache and not self._stop.is_set():
            for hit in _cache_hits(self._admissions, self.reader):
                if self._stop.is_set():
                    break
                self.tracker.committer.journal.reuse(
                    hit.admission,
                    hit.receipt,
                    verify_source=partial(verify_cache_source, reader=self.reader),
                )
                reused.add(hit.admission.attempt.assignment)
        return tuple(
            AdmittedAssignment(plan, reference)
            for reference in self._admissions
            if reference.attempt.assignment not in reused
        )

    def _dispatch(
        self, assignments: Sequence[AdmittedAssignment], software: AssignmentSoftware
    ) -> dict[int, StepUnitFailure]:
        size = self.batch_strategy.units_per_worker
        pending = deque(
            tuple(assignments[index : index + size])
            for index in range(0, len(assignments), size)
        )
        active: list[tuple[AssignmentHandle, ...]] = []
        observed: set[AssignmentAttempt] = set()
        failures: dict[int, StepUnitFailure] = {}
        limit = self.batch_strategy.max_workers or len(pending)
        while pending or active:
            while pending and len(active) < limit and not self._stop.is_set():
                pack = pending.popleft()
                handles = self._launch(pack, software)
                if handles:
                    active.append(handles)
            for running in active:
                for handle in running:
                    if handle.attempt not in observed and handle.done():
                        self._collect(handle, failures)
                        observed.add(handle.attempt)
            retained = []
            for running in active:
                ownership = self._pack_ownership(running)
                if ownership == OwnershipState.UNKNOWN:
                    self._closure_unknown = True
                    self._stop.set()
                if ownership != OwnershipState.CLOSED or any(
                    handle.attempt not in observed for handle in running
                ):
                    retained.append(running)
            active = retained
            if self._stop.is_set():
                for pack in pending:
                    for unit in pack:
                        failures[unit.admission.attempt.assignment.unit_index] = (
                            StepUnitFailure(
                                error="Assignment was not dispatched after execution stopped"
                            )
                        )
                pending.clear()
                outcome = self._cancel_live()
                if outcome.status == CancellationStatus.UNKNOWN:
                    for running in active:
                        for handle in running:
                            failures.setdefault(
                                handle.attempt.assignment.unit_index,
                                StepUnitFailure(
                                    error="Assignment cleanup could not be confirmed"
                                ),
                            )
                    break
            if active:
                time.sleep(0.02)
        return failures

    def _pack_ownership(
        self, handles: Sequence[AssignmentHandle | WorkerOwner]
    ) -> OwnershipState:
        states = [handle.ownership_state() for handle in handles]
        if OwnershipState.UNKNOWN in states:
            return OwnershipState.UNKNOWN
        if OwnershipState.PENDING in states:
            return OwnershipState.PENDING
        return OwnershipState.CLOSED

    def _launch(
        self,
        pack: tuple[AdmittedAssignment, ...],
        software: AssignmentSoftware,
    ) -> tuple[AssignmentHandle, ...]:
        try:
            handles = self.runner.launch(
                pack, software=software, dispatch=self.dispatch
            )
        except AssignmentLaunchError as error:
            handles = error.handles
            self._retain_owners(handles, error.worker_owners)
            expected = {unit.admission.attempt for unit in pack}
            actual = [handle.attempt for handle in handles]
            if len(actual) != len(set(actual)) or set(actual) - expected:
                msg = "Runner startup error returned foreign or duplicate assignment ownership"
                raise ValueError(msg) from error
            self._closure_unknown |= (
                error.cancellation.status == CancellationStatus.UNKNOWN
            )
            raise
        self._retain_owners(handles)
        if tuple(handle.attempt for handle in handles) != tuple(
            unit.admission.attempt for unit in pack
        ):
            msg = "Runner returned changed or incomplete assignment ownership"
            raise ValueError(msg)
        return handles

    def _retain_owners(
        self,
        handles: Sequence[AssignmentHandle],
        owners: Sequence[WorkerOwner] = (),
    ) -> None:
        with self._lock:
            self._handles.extend(handles)
            for owner in (
                *owners,
                *(handle.worker_owner for handle in handles),
            ):
                if owner is not None and not any(
                    owner is existing for existing in self._worker_owners
                ):
                    self._worker_owners.append(owner)

    def _collect(
        self, handle: AssignmentHandle, failures: dict[int, StepUnitFailure]
    ) -> None:
        try:
            receipt = handle.result(timeout=0)
        except CommandOwnershipError:
            self._closure_unknown = True
            raise
        except (AssignmentExecutionError, CancelledError) as error:
            if isinstance(error, CancelledError) and not self._stop.is_set():
                raise
            delivery = (
                error.delivery if isinstance(error, AssignmentExecutionError) else None
            )
            if delivery is not None and delivery.attempt != handle.attempt:
                msg = "Runner returned a failed delivery for another assignment attempt"
                raise ValueError(msg) from error
            failure = StepUnitFailure(
                error=self._diagnostic(error), failed_delivery=delivery
            )
            index = handle.attempt.assignment.unit_index
            if isinstance(error, AssignmentExecutionError):
                self._observed_failures[index] = handle, failure
            self.tracker.committer.journal.cancel(handle.attempt)
            failures[index] = StepUnitFailure(error=failure.error)
            if self.failure_policy == FailurePolicy.FAIL_FAST:
                self._stop.set()
            return
        if receipt.delivery.attempt != handle.attempt:
            msg = "Runner returned a delivery for another assignment attempt"
            raise ValueError(msg)
        state = self.tracker.committer.journal.state(handle.attempt.assignment)
        if state is not None and state.cancelled and state.winner is None:
            failures[handle.attempt.assignment.unit_index] = StepUnitFailure(
                error="Assignment was fenced before controller acceptance"
            )
            return
        self.tracker.committer.accept(
            receipt, preserve_staging=self.dispatch.preserve_staging
        )
        if self.replay_diagnostic is not None:
            record_replay_diagnostic(
                receipt,
                self.replay_diagnostic,
                delta_root=self.tracker.committer.delta_root,
                fs=self.tracker.committer.fs,
            )

    def _cancel_live(self) -> CancellationAcknowledgement:
        with self._lock:
            references = tuple(self._admissions)
            handles = tuple(self._handles)
            self._cancel_deadline = self._cancel_deadline or (
                time.monotonic() + CLEANUP_SECONDS
            )
        for reference in references:
            self.tracker.committer.journal.cancel(reference.attempt)
        outcomes = []
        for handle in handles:
            try:
                outcomes.append(handle.cancel())
            except Exception:
                self._closure_unknown = True
        self._closure_unknown |= any(
            item.status == CancellationStatus.UNKNOWN for item in outcomes
        )
        self._request_worker_closure(self._cancel_deadline)
        expired = time.monotonic() >= self._cancel_deadline
        if self._closure_unknown or (expired and not self._handles_settled()):
            self._closure_unknown = True
            return CancellationAcknowledgement(
                CancellationStatus.UNKNOWN, "Assignment cleanup could not be confirmed"
            )
        return CancellationAcknowledgement(
            CancellationStatus.REQUESTED, "Waiting for execution and provider cleanup"
        )

    def _request_worker_closure(self, deadline: float) -> None:
        for owner in self.worker_owners:
            try:
                owner.request_close(deadline)
            except Exception:
                self._closure_unknown = True

    def _settle_cancellation(self) -> None:
        if self._closure_unknown:
            self._record_cancellation(CancellationStatus.REQUESTED)
            self._record_cancellation(CancellationStatus.UNKNOWN)
        elif self._user_cancelled:
            self._record_cancellation(CancellationStatus.CONFIRMED)

    def _finish_error(self, error: Exception, started: float) -> StepResult:
        self._stop.set()
        while True:
            outcome = self._cancel_live()
            if outcome.status == CancellationStatus.UNKNOWN or self._handles_settled():
                break
            time.sleep(0.02)
        failures = self._terminal_failures(
            {
                reference.attempt.assignment.unit_index: StepUnitFailure(
                    error=self._diagnostic(error)
                )
                for reference in self._admissions
            }
        )
        self._settle_cancellation()
        state = self.tracker.current_state(self.record.step_run_id)
        if state.status == StepStatus.PENDING:
            return (
                self._cancel_unadmitted()
                if self._user_cancelled
                else self._fail_unadmitted(error)
            )
        return self.results.finish(
            state.step_run_id,
            failures=failures,
            duration_seconds=time.monotonic() - started,
            failure_policy=self.failure_policy,
            infrastructure_error=None
            if self._user_cancelled
            else self._diagnostic(error),
        )

    def _terminal_failures(
        self, failures: dict[int, StepUnitFailure]
    ) -> dict[int, StepUnitFailure]:
        selected = dict(failures)
        for index, (handle, failure) in self._observed_failures.items():
            try:
                owner = handle.worker_owner
                owners = (handle,) if owner is None else (handle, owner)
                closed = self._pack_ownership(owners) == OwnershipState.CLOSED
            except Exception:
                closed = False
            self._closure_unknown |= not closed
            selected[index] = (
                failure if closed else StepUnitFailure(error=failure.error)
            )
        return selected

    def _handles_settled(self) -> bool:
        try:
            ownership = self._pack_ownership((*self.handles, *self.worker_owners))
            self._closure_unknown |= ownership == OwnershipState.UNKNOWN
            return all(handle.done() for handle in self.handles) and (
                ownership == OwnershipState.CLOSED
            )
        except Exception:
            self._closure_unknown = True
            return False

    def _record_cancellation(self, status: CancellationStatus) -> StepState:
        with self.tracker.committer._lock, self.tracker._lock:
            state = self.tracker.current_state(self.record.step_run_id)
            if (
                status == CancellationStatus.REQUESTED
                and state.cancellation_status is not None
            ):
                status = state.cancellation_status
            return self.results.record_cancellation(
                self.record.step_run_id,
                CancellationAcknowledgement(status),
            )

    def _check_stopped(self) -> None:
        if self._stop.is_set():
            raise CancelledError()

    def _cancel_unadmitted(self) -> StepResult:
        self._cancel_live()
        self._settle_cancellation()
        state = self.tracker.current_state(self.record.step_run_id)
        if self._closure_unknown and state.status == StepStatus.PENDING:
            state = self.tracker.transition(
                state.step_run_id, state.status, StepStatus.RUNNING
            )
        result = StepResult(
            step_run_id=state.step_run_id,
            step_number=state.step_number,
            step_name=state.step_name,
            status=StepStatus.FAILED if self._closure_unknown else StepStatus.CANCELLED,
            cancellation_status=CancellationStatus.UNKNOWN
            if self._closure_unknown
            else CancellationStatus.CONFIRMED,
            error="Controller cleanup could not be confirmed"
            if self._closure_unknown
            else None,
        )
        return self.tracker.transition(
            state.step_run_id, state.status, result.status, result=result
        ).to_step_result()

    def _fail_unadmitted(self, error: Exception) -> StepResult:
        if self._user_cancelled:
            return self._cancel_unadmitted()
        for reference in self._admissions:
            self.tracker.committer.journal.cancel(reference.attempt)
        state = self.tracker.current_state(self.record.step_run_id)
        if state.status == StepStatus.PENDING:
            state = self.tracker.transition(
                state.step_run_id, state.status, StepStatus.RUNNING
            )
        result = StepResult(
            step_run_id=state.step_run_id,
            step_number=state.step_number,
            step_name=state.step_name,
            status=StepStatus.FAILED,
            cancellation_status=state.cancellation_status,
            error=self._diagnostic(error),
        )
        return self.tracker.transition(
            state.step_run_id, state.status, result.status, result=result
        ).to_step_result()

    def _diagnostic(self, error: Exception) -> str:
        message = str(error) or type(error).__name__
        if isinstance(error, AssignmentExecutionError):
            call = "/".join(error.call_path) or "<root>"
            phase = f" [{error.phase}]" if error.phase is not None else ""
            message = f"Call {call}{phase}: {message}"
        return redact_values(
            self.recorder.sanitize(message),
            tuple(self.dispatch.runtime_env.values()),
        )


def _publish_assignment(
    prepared: PreparedAssignmentInputs,
    plan: CompiledPlan,
    record: StepStartRecord,
    software: AssignmentSoftware,
    committer: RetainedCommitter,
) -> AdmissionReference:
    attempt = AssignmentAttempt(
        assignment=prepared.assignment,
        attempt=0,
        generation=0,
        recipe_digest=plan.recipe.digest,
        software_digest=software.software_digest,
        software_cacheable=software.software_cacheable,
        input_context_digest=prepared.boundary.input_context_digest,
        replay_of_execution_run_id=record.replay_of_execution_run_id,
    )
    reference = publish_admission(
        attempt,
        plan.recipe,
        prepared.boundary,
        prepared.artifacts,
        step_number=record.step_number,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
        artifact_fs=committer.fs,
        verify_producer=committer._verify_producer,
        external_access=capture_external_access(
            attempt, prepared.artifacts.values(), software
        ),
    )
    committer.journal.admit(attempt, admission_digest=reference.record_digest)
    return reference


def _validate_partition_owners(
    prepared: Sequence[PreparedAssignmentInputs], record: StepStartRecord
) -> None:
    if any(
        unit.assignment.step_run_id != record.step_run_id
        or unit.assignment.unit_index != index
        for index, unit in enumerate(prepared)
    ):
        msg = "Input resolver changed the pending step's ordered assignment owners"
        raise ValueError(msg)


def _cache_hits(
    admissions: Sequence[AdmissionReference], reader: RetainedReader
) -> Iterator[RetainedCacheHit]:
    complete = lookup_step(admissions, reader=reader)
    if complete is not None:
        yield from complete
        return
    for admission in admissions:
        hit = lookup_assignment(admission, reader=reader)
        if hit is not None:
            yield hit
