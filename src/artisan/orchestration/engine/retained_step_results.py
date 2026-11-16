"""Publish current ordered unit selections through the existing step lifecycle."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import CancelledError
from contextlib import ExitStack

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.retained_step_tracker import (
    RetainedStepTracker,
    _validate_identity,
)
from artisan.schemas.enums import CachePolicy, FailurePolicy, TablePath
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.step_admission import ADMISSION_DIGEST, StepAdmission
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepDisposition,
    StepStatus,
    classify_step_status,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.cache.retained_step_lookup import (
    WholeStepCacheHit,
    canonical_step_source,
    eligible_step_status,
    lookup_completed_step,
    reused_step_result,
    verify_step_inputs,
    verify_step_reuse_source,
)
from artisan.storage.core.committed_scan import verify_snapshot_effect
from artisan.storage.core.retained_reader import (
    AcceptedRole,
    RetainedReader,
)
from artisan.storage.io.admission import read_admission_record
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.delta_files import append_table_files
from artisan.storage.io.logical_control import (
    complete_control,
    control_for,
    planned_control,
)
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.step_result_plan import (
    CurrentStepUnits,
    StepResultPlan,
    StepUnitFailure,
    StepUnitFence,
    StepUnitResolution,
    WholeStepReuse,
    capture_step_terminal,
    publish_step_result_plan,
    read_step_result_plan,
    validate_unit_admission,
    verify_step_decisions,
)
from artisan.storage.io.table_effect import apply_table_effect
from artisan.utils.path import uri_join


class RetainedStepResults:
    """One controller publishes terminal membership after unit acceptance or reuse.

    Unit receipts remain independent recoverable results. A step plan binds
    current outcomes or one exact completed source, preserving original producers.
    """

    def __init__(
        self,
        tracker: RetainedStepTracker,
        *,
        checkpoint: Callable[[str], None] | None = None,
    ) -> None:
        self.tracker = tracker
        self.committer = tracker.committer
        self.reader = RetainedReader(self.committer)
        self.checkpoint = checkpoint

    def record_cancellation(
        self, step_run_id: str, outcome: CancellationAcknowledgement
    ) -> StepState:
        """Recover a selected terminal result before considering later cancellation.

        Publishing the immutable result plan wins against a later cancellation,
        including when a reopened controller first encounters that plan.
        """
        with self.committer._lock, self.tracker._lock:
            if self._read_plan(step_run_id) is not None:
                self.recover(step_run_id)
                return self.tracker.current_state(step_run_id)
            state = self.tracker.current_state(step_run_id)
            return self.tracker.record_cancellation(step_run_id, state.status, outcome)

    def lookup(
        self, step_run_id: str, cache_policy: CachePolicy
    ) -> WholeStepCacheHit | None:
        """Prefer the newest eligible whole completion to independent unit hits."""
        admission = self.tracker.read_admission(step_run_id)
        states = sorted(
            self.tracker.load_all_current_states(),
            key=lambda state: state.timestamp,
            reverse=True,
        )
        candidates = []
        for state in states:
            if (
                state.step_number != admission.step_number
                or state.step_run_id == step_run_id
            ):
                continue
            if state.status not in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
                continue
            plan = self._read_plan(state.step_run_id)
            if plan is not None:
                candidates.append(plan)
        return lookup_completed_step(
            admission,
            cache_policy,
            candidates,
            reader=self.reader,
            verify_source=self._verify_completed_source,
        )

    def reuse(
        self, hit: WholeStepCacheHit, *, duration_seconds: float | None = None
    ) -> StepResult:
        """Fence current units and publish one immutable historical selection."""
        step_run_id = hit.admission.step_run_id
        with self.committer._lock, self.tracker._lock:
            if self._read_plan(step_run_id) is not None:
                return self.recover(step_run_id)
            admission = self.tracker.read_admission(step_run_id)
            state = self.tracker.current_state(step_run_id)
            if state.cancellation_status is not None:
                raise CancelledError()
            if state.status != StepStatus.RUNNING or admission != hit.admission:
                msg = "Whole-step reuse requires its exact running admission"
                raise StoreIntegrityError(msg)
            source = hit.source
            if (
                not isinstance(source.selection, CurrentStepUnits)
                or source.step_number != admission.step_number
                or source.step_run_id == step_run_id
                or not eligible_step_status(source.result.status, hit.cache_policy)
            ):
                msg = "Whole-step reuse requires a distinct eligible original source"
                raise StoreIntegrityError(msg)
            self._verify_completed_source(source)
            verify_step_inputs(
                admission.assignments, source.decision_admissions, self.reader
            )
            decisions = self.committer.journal.states(
                tuple(item.attempt.assignment for item in admission.assignments)
            )
            for reference in admission.assignments:
                selected = decisions[reference.attempt.assignment]
                if (
                    selected is None
                    or selected.attempt != reference.attempt
                    or selected.admission_digest != reference.record_digest
                    or selected.cancelled
                    or selected.winner is not None
                    or selected.reuse is not None
                ):
                    msg = "Whole-step reuse cannot replace a current decision or fence"
                    raise StoreIntegrityError(msg)
            fences = tuple(
                StepUnitFence(
                    admission=reference,
                    decision_sequence=self.committer.journal.cancel(
                        reference.attempt
                    ).sequence,
                )
                for reference in admission.assignments
            )
            self._checkpoint("reuse_fenced")
            selection = WholeStepReuse(
                source_step_run_id=source.step_run_id,
                source_plan_digest=source.plan_digest,
                cache_policy=hit.cache_policy,
                fences=fences,
            )
            result = reused_step_result(
                source,
                admission,
                step_name=state.step_name,
                duration_seconds=duration_seconds,
            )
            terminal = self.tracker.prepare_terminal_candidate(
                step_run_id,
                StepStatus.RUNNING,
                result.status,
                step_spec_id=admission.digest,
                result=result,
            )
            plan = publish_step_result_plan(
                admission,
                selection,
                result,
                terminal,
                delta_root=self.committer.delta_root,
                fs=self.committer.fs,
            )
            verify_step_reuse_source(
                plan,
                self.committer._controls(),
                {source.step_run_id: source},
                self.committer,
            )
            self._checkpoint("plan")
            return self.recover(step_run_id)

    def _verify_completed_source(self, plan: StepResultPlan) -> None:
        with self.committer._read_snapshot() as read:
            self._verify_source_completion(plan, read)

    def _verify_source_completion(
        self, plan: StepResultPlan, read: RetainedReadSnapshot
    ) -> None:
        read.require_committer(self.committer)
        if self._read_plan(plan.step_run_id) != plan:
            msg = "Whole-step cache source changed its immutable plan"
            raise StoreIntegrityError(msg)
        control = control_for(plan, read.controls)
        if control is None or control["state"] != "complete":
            msg = "Whole-step cache source is not complete"
            raise StoreIntegrityError(msg)
        _, state = self.tracker._read_state(plan.step_run_id, read)
        self._verify_sources(plan, read, state)
        self._verify_terminal(plan, read, state)

    def finish(
        self,
        step_run_id: str,
        *,
        failures: Mapping[int, StepUnitFailure] | None = None,
        duration_seconds: float | None = None,
        failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
        infrastructure_error: str | None = None,
    ) -> StepResult:
        """Fence observed failures and publish one current, complete step summary."""
        with self.committer._lock, self.tracker._lock:
            if self._read_plan(step_run_id) is not None:
                return self.recover(step_run_id)
            with self.committer._read_snapshot() as read:
                _, state = self.tracker._read_state(step_run_id, read)
                admission = self.tracker._admission_for(state)
            if not admission.assignments:
                return state.to_step_result()
            if state.status != StepStatus.RUNNING:
                msg = "Only a running step may publish terminal results"
                raise StoreIntegrityError(msg)
            if state.cancellation_status == CancellationStatus.REQUESTED:
                msg = "Step completion requires a final cancellation acknowledgement"
                raise ValueError(msg)
            units = self._resolve(admission, failures or {})
            succeeded = sum(unit.receipt is not None for unit in units)
            status = classify_step_status(
                succeeded,
                len(units) - succeeded,
                failure_policy,
                infrastructure_error=infrastructure_error is not None,
                cancellation_status=state.cancellation_status,
            )
            usable = status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
            disposition = (
                (
                    StepDisposition.CACHE_HIT
                    if all(unit.reused for unit in units)
                    else StepDisposition.EXECUTED
                )
                if usable
                else None
            )
            error = (
                infrastructure_error
                or "; ".join(unit.error for unit in units if unit.error)
                or state.error
            )
            if state.cancellation_status == CancellationStatus.UNKNOWN:
                error = error or "Assignment cleanup could not be confirmed"
            cancelled = status == StepStatus.CANCELLED
            result = StepResult(
                step_run_id=step_run_id,
                step_number=state.step_number,
                step_name=state.step_name,
                status=status,
                disposition=disposition,
                cancellation_status=state.cancellation_status,
                error=error or None,
                total_count=0 if cancelled else len(units),
                succeeded_count=0 if cancelled else succeeded,
                failed_count=0 if cancelled else len(units) - succeeded,
                output_roles=state.output_roles if usable else frozenset(),
                output_types=state.output_types if usable else {},
                duration_seconds=duration_seconds,
                metadata={**state.metadata, ADMISSION_DIGEST: admission.digest},
            )
            with self.committer._read_snapshot() as read:
                rows, current = self.tracker._read_state(step_run_id, read)
                if self.tracker._admission_for(current) != admission:
                    msg = "Step admission changed during unit resolution"
                    raise StoreIntegrityError(msg)
                for receipt in dict.fromkeys(
                    unit.receipt for unit in units if unit.receipt is not None
                ):
                    self.reader._read(receipt, read)
                terminal = self.tracker._terminal_candidate(
                    step_run_id,
                    StepStatus.RUNNING,
                    status,
                    step_spec_id=admission.digest,
                    result=result,
                    rows=rows,
                    controls=read.controls,
                )
            publish_step_result_plan(
                admission,
                CurrentStepUnits(
                    units=units,
                    failure_policy=failure_policy,
                    infrastructure_error=infrastructure_error,
                ),
                result,
                terminal,
                delta_root=self.committer.delta_root,
                fs=self.committer.fs,
            )
            self._checkpoint("plan")
            return self.recover(step_run_id)

    def recover(self, step_run_id: str) -> StepResult:
        """Complete only the frozen selected units; never accept an orphan delivery."""
        with self.committer._lock, self.tracker._lock:
            plan = self._read_plan(step_run_id)
            if plan is None:
                msg = "Step has no frozen terminal result plan"
                raise StoreIntegrityError(msg)
            with ExitStack() as captured:
                with self.committer._read_snapshot() as read:
                    rows, state = self.tracker._read_state(step_run_id, read)
                    self._verify_sources(plan, read, state)
                    controls = read.controls
                    control = control_for(plan, controls)
                    if control is not None and control["state"] == "complete":
                        self._verify_terminal(plan, read, state)
                        return plan.result
                    if control is not None and control["state"] != "planned":
                        msg = "Step result plan is not eligible for recovery"
                        raise StoreIntegrityError(msg)
                    terminal = captured.enter_context(
                        capture_step_terminal(
                            plan,
                            delta_root=self.committer.delta_root,
                            fs=self.committer.fs,
                        )
                    )
                    self._validate_terminal(plan, terminal, read, rows, state)
                if control is None:
                    append_table_files(
                        self.committer._snapshot(TablePath.LOGICAL_COMMITS.value),
                        [planned_control(plan)],
                        step_number=plan.step_number,
                        statistics_columns=["logical_commit_id"],
                    )
                    controls = self.committer._controls()
                    self._checkpoint("planned")
                apply_table_effect(
                    plan,
                    plan.terminal,
                    terminal,
                    controls,
                    read_snapshot=self.committer._snapshot,
                )
                self._checkpoint("terminal")
            with self.committer._read_snapshot() as read:
                _, state = self.tracker._read_state(step_run_id, read)
                self._verify_sources(plan, read, state)
            complete_control(
                plan,
                uri_join(self.committer.delta_root, TablePath.LOGICAL_COMMITS.value),
                storage_options=self.committer.storage_options,
            )
            with self.committer._read_snapshot() as read:
                _, state = self.tracker._read_state(step_run_id, read)
                self._verify_terminal(plan, read, state)
            self._checkpoint("complete")
            return plan.result

    def outputs(self, step_run_id: str) -> tuple[AcceptedRole, ...]:
        """Read only completed current selections in unit and role occurrence order."""
        plan = self._read_plan(step_run_id)
        if plan is None:
            state = self.tracker.current_state(step_run_id)
            if state.status == StepStatus.SKIPPED:
                return ()
            msg = "Step has no completed result membership"
            raise StoreIntegrityError(msg)
        with self.committer._read_snapshot() as read:
            control = control_for(plan, read.controls)
            if control is None or control["state"] != "complete":
                msg = "Step result membership is not complete"
                raise StoreIntegrityError(msg)
            _, state = self.tracker._read_state(step_run_id, read)
            self._verify_sources(plan, read, state)
            self._verify_terminal(plan, read, state)
        if plan.result.status not in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
            return ()
        source = canonical_step_source(plan, self.committer)
        selected = self.reader.read_many(source.selected_receipts)
        return (
            tuple(
                AcceptedRole(
                    output.role,
                    tuple(
                        artifact
                        for result in selected
                        for group in result.outputs
                        if group.role == output.role
                        for artifact in group.occurrences
                    ),
                )
                for output in plan.admission.recipe.outputs
            )
            if selected
            else ()
        )

    def _resolve(
        self, admission: StepAdmission, failures: Mapping[int, StepUnitFailure]
    ) -> tuple[StepUnitResolution, ...]:
        if any(index not in range(len(admission.assignments)) for index in failures):
            msg = "Failure refers to an unadmitted unit"
            raise ValueError(msg)
        resolutions = []
        for index, reference in enumerate(admission.assignments):
            state = self.committer.journal.state(reference.attempt.assignment)
            if state is None:
                msg = "Step unit admission changed before completion"
                raise StoreIntegrityError(msg)
            selected_admission = AdmissionReference(
                attempt=state.attempt, record_digest=state.admission_digest
            )
            validate_unit_admission(reference, selected_admission)
            record = read_admission_record(
                selected_admission,
                delta_root=self.committer.delta_root,
                fs=self.committer.fs,
            )
            if record.step_number != admission.step_number:
                msg = "Retried unit has another outer step number"
                raise StoreIntegrityError(msg)
            if index in failures and state.winner is None and state.reuse is None:
                delivery = failures[index].failed_delivery
                if delivery is not None and delivery.attempt != state.attempt:
                    msg = "Failed delivery differs from the current unit admission"
                    raise StoreIntegrityError(msg)
                state = self.committer.journal.cancel(selected_admission.attempt)
            selected = state.reuse or state.winner
            if selected is not None:
                resolutions.append(
                    StepUnitResolution(
                        admission=selected_admission,
                        decision_sequence=selected.decision_sequence,
                        receipt=selected.receipt,
                        reused=state.reuse is not None,
                    )
                )
            elif index in failures:
                resolutions.append(
                    StepUnitResolution(
                        admission=selected_admission,
                        decision_sequence=state.sequence,
                        error=failures[index].error,
                        failed_delivery=failures[index].failed_delivery,
                    )
                )
            else:
                msg = "Step unit has no accepted result or observed failure"
                raise StoreIntegrityError(msg)
        return tuple(resolutions)

    def _verify_sources(
        self, plan: StepResultPlan, read: RetainedReadSnapshot, state: StepState
    ) -> None:
        read.require_committer(self.committer)
        if self.tracker._admission_for(state) != plan.admission:
            msg = "Step result changed its finite admitted work"
            raise StoreIntegrityError(msg)
        states = self.committer.journal.states(
            tuple(item.attempt.assignment for item in plan.decision_admissions)
        )
        winners = self.committer.journal.require_winners(plan.selected_receipts)
        verify_step_decisions(plan, states, winners)
        if isinstance(plan.selection, WholeStepReuse):
            source = canonical_step_source(plan, self.committer)
            verify_step_reuse_source(
                plan, read.controls, {source.step_run_id: source}, self.committer
            )
            self._verify_source_completion(source, read)
            verify_step_inputs(
                plan.decision_admissions,
                source.decision_admissions,
                self.reader,
                _read=read,
            )
        else:
            for receipt in dict.fromkeys(plan.selected_receipts):
                self.reader._read(receipt, read)

    def _validate_terminal(
        self,
        plan: StepResultPlan,
        terminal: DiskTable,
        read: RetainedReadSnapshot,
        rows: pl.DataFrame,
        current: StepState,
    ) -> None:
        row = terminal.get_row(plan.terminal.row_keys[0])
        saved = self.tracker._validated_snapshot(row)
        _validate_identity(saved, plan.admission)
        if (
            saved.to_step_result() != plan.result
            or saved.step_spec_id != plan.admission.digest
        ):
            msg = "Terminal snapshot disagrees with current unit summary"
            raise StoreIntegrityError(msg)
        expected = self.tracker._terminal_candidate(
            plan.step_run_id,
            StepStatus.RUNNING,
            plan.result.status,
            step_spec_id=plan.admission.digest,
            result=plan.result,
            rows=rows,
            controls=read.controls,
        ).row(0, named=True)
        ignored = {"timestamp", "state_sequence"}
        if saved.model_dump(exclude=ignored) != self.tracker._validated_snapshot(
            expected
        ).model_dump(exclude=ignored):
            msg = "Terminal snapshot changed its pending step owner"
            raise StoreIntegrityError(msg)
        if saved.state_sequence != current.state_sequence + 1:
            msg = "Terminal snapshot has a stale lifecycle sequence"
            raise StoreIntegrityError(msg)

    def _verify_terminal(
        self, plan: StepResultPlan, read: RetainedReadSnapshot, state: StepState
    ) -> None:
        verify_snapshot_effect(
            plan,
            TablePath.STEPS.value,
            read.snapshot(TablePath.STEPS.value),
            read.controls,
        )
        if state.to_step_result() != plan.result:
            msg = "Visible step summary differs from its terminal plan"
            raise StoreIntegrityError(msg)

    def _read_plan(self, step_run_id: str) -> StepResultPlan | None:
        return read_step_result_plan(
            self.committer.delta_root, step_run_id, self.committer.fs
        )

    def _checkpoint(self, event: str) -> None:
        if self.checkpoint is not None:
            self.checkpoint(event)
