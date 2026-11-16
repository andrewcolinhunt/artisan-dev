"""Use the existing step lifecycle with finite retained-assignment admission."""

from __future__ import annotations

from concurrent.futures import CancelledError
from typing import Literal

import polars as pl

from artisan.errors import PersistenceIntegrityError, StoreIntegrityError
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.step_admission import ADMISSION_DIGEST, StepAdmission
from artisan.schemas.orchestration.step_lifecycle import (
    TERMINAL_STEP_STATUSES,
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.core.committed_scan import filter_committed_rows
from artisan.storage.core.curator_result import dynamic_output_roles
from artisan.storage.core.store_format import assert_retained_store
from artisan.storage.io.admission import read_admission_record
from artisan.storage.io.curator_plan import CuratorCommitPlan, read_curator_plan
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_metadata import (
    read_curator_source_metadata,
    read_terminal_metadata,
)
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.step_admission import (
    publish_step_admission,
    read_step_admission,
)
from artisan.storage.io.step_result_plan import StepResultPlan, read_step_result_plan


class RetainedStepTracker(StepTracker):
    """Validate lifecycle transitions against accepted result authority."""

    def __init__(self, committer: RetainedCommitter, pipeline_run_id: str) -> None:
        self.committer = committer
        super().__init__(
            committer.delta_root,
            pipeline_run_id=pipeline_run_id,
            storage_options=committer.storage_options,
            fs=committer.fs,
        )

    def _validate_store(self) -> None:
        assert_retained_store(self._delta_root, self._fs)

    def _read_controls(self) -> pl.DataFrame:
        return self.committer._controls()

    def _read_result_evidence(self, step_run_id: str) -> dict[str, pl.DataFrame]:
        from artisan.storage.core.curator_evidence import read_curator_result_evidence

        return read_curator_result_evidence(self.committer, step_run_id)

    def record_cancellation(
        self,
        step_run_id: str,
        expected: StepStatus,
        outcome: CancellationAcknowledgement,
    ) -> StepState:
        """Reject acknowledgements that would invalidate a frozen terminal result."""
        with self.committer._lock, self._lock:
            state = self.current_state(step_run_id)
            if state.status not in TERMINAL_STEP_STATUSES and (
                read_step_result_plan(self._delta_root, step_run_id, self._fs)
                is not None
                or read_curator_plan(self._delta_root, step_run_id, self._fs)
                is not None
            ):
                msg = (
                    "Recover the frozen terminal result before requesting cancellation"
                )
                raise PersistenceIntegrityError(msg)
            return super().record_cancellation(step_run_id, expected, outcome)

    def skip_pending(
        self,
        step_run_id: str,
        *,
        reason: Literal["empty_inputs", "pipeline_stopped"],
    ) -> StepResult:
        """Persist pipeline no-work before admission or provider resolution."""
        with self.committer._lock, self._lock:
            state = self.current_state(step_run_id)
            if state.cancellation_status not in (None, CancellationStatus.REJECTED):
                raise CancelledError()
            result = StepResult(
                step_run_id=step_run_id,
                step_number=state.step_number,
                step_name=state.step_name,
                status=StepStatus.SKIPPED,
                metadata={"skip_reason": reason},
            )
            return self.transition(
                step_run_id, StepStatus.PENDING, StepStatus.SKIPPED, result=result
            ).to_step_result()

    def require_prework_skip(self, step_run_id: str, result: StepResult) -> StepResult:
        """Accept only this owner's already durable skipped result from preflight."""
        state = self.current_state(step_run_id)
        if (
            state.status != StepStatus.SKIPPED
            or result.step_run_id != step_run_id
            or state.to_step_result() != result
        ):
            msg = "Pre-work skip differs from the current persisted step owner"
            raise StoreIntegrityError(msg)
        return result

    def admit(self, admission: StepAdmission) -> StepState:
        """Bind complete unit membership before either cache lookup or dispatch."""
        admission = StepAdmission.model_validate(admission)
        with self._lock:
            state = self.current_state(admission.step_run_id)
            _validate_identity(state, admission)
            if state.status in {StepStatus.RUNNING, StepStatus.SKIPPED}:
                if self.read_admission(admission.step_run_id) == admission:
                    return state
                msg = "Step admission changed after dispatch became eligible"
                raise StoreIntegrityError(msg)
            if state.status != StepStatus.PENDING:
                msg = "Only a pending step can admit assignments"
                raise StoreIntegrityError(msg)
            self._validate_assignments(admission)
            digest = publish_step_admission(
                admission, delta_root=self._delta_root, fs=self._fs
            )
            metadata = {ADMISSION_DIGEST: digest}
            if not admission.assignments:
                metadata["skip_reason"] = "no_outer_work"
                return self.transition(
                    admission.step_run_id,
                    StepStatus.PENDING,
                    StepStatus.SKIPPED,
                    result=StepResult(
                        step_name=state.step_name,
                        step_number=state.step_number,
                        step_run_id=state.step_run_id,
                        status=StepStatus.SKIPPED,
                        metadata=metadata,
                    ),
                )
            return self.transition(
                admission.step_run_id,
                StepStatus.PENDING,
                StepStatus.RUNNING,
                metadata=metadata,
            )

    def read_admission(self, step_run_id: str) -> StepAdmission:
        """Read membership against the recorded lifecycle digest, never a listing."""
        return self._admission_for(self.current_state(step_run_id))

    def _read_lifecycle(
        self, read: RetainedReadSnapshot
    ) -> tuple[pl.DataFrame, list[StepState]]:
        """Validate all attempt histories at this read's fixed versions and controls."""
        from artisan.storage.core.curator_evidence import read_curator_result_evidence

        read.require_committer(self.committer)
        physical = read.snapshot(TablePath.STEPS.value)
        frames = list(physical.iter_frames())
        rows = pl.concat(frames) if frames else pl.DataFrame(schema=physical.schema)
        self._validate_physical_rows(rows)
        self._validate_completed_terminals(read)
        attempts = []
        for group in rows.partition_by("step_run_id", maintain_order=True):
            visible = filter_committed_rows(group, TablePath.STEPS.value, read.controls)
            evidence = None
            if (
                not visible.is_empty()
                and dynamic_output_roles(visible["compute_options_json"][0])
                and visible["status"].is_in(["succeeded", "partial"]).any()
            ):
                evidence = read_curator_result_evidence(
                    self.committer, group["step_run_id"][0], _read=read
                )
            attempts.append(
                self._validate_attempt_rows(
                    group, result_evidence=evidence, controls=read.controls
                )
            )
        return rows, attempts

    @staticmethod
    def _validate_completed_terminals(read: RetainedReadSnapshot) -> None:
        """Bind all selected terminals, including an entirely missing history row."""
        owners = read.controls.filter(
            (pl.col("commit_kind") == "step_result") & (pl.col("state") == "complete")
        )["logical_commit_id"]
        for owner in owners:
            plan = read.completed_control_plan(owner)
            if not isinstance(
                plan, (StepResultPlan, CuratorCommitPlan, CuratorReusePlan)
            ):
                msg = "Completed step has an incompatible terminal selection"
                raise StoreIntegrityError(msg)
            terminal = read_terminal_metadata(read, plan)
            if (
                isinstance(plan, CuratorCommitPlan)
                and plan.table(TablePath.EXECUTIONS.value) is None
                and terminal.status in {StepStatus.FAILED, StepStatus.CANCELLED}
            ):
                continue
            if isinstance(plan, (CuratorCommitPlan, CuratorReusePlan)):
                read_curator_source_metadata(read, plan, terminal)

    def _read_state(
        self, step_run_id: str, read: RetainedReadSnapshot
    ) -> tuple[pl.DataFrame, StepState]:
        """Read one attempt's physical history at the finite proof's steps version."""
        from artisan.storage.core.curator_evidence import read_curator_result_evidence

        read.require_committer(self.committer)
        frames = []
        for frame in read.snapshot(TablePath.STEPS.value).iter_frames():
            self._validate_physical_rows(frame)
            selected = frame.filter(pl.col("step_run_id") == step_run_id)
            if not selected.is_empty():
                frames.append(selected)
        if not frames:
            msg = f"Unknown step attempt {step_run_id}"
            raise PersistenceIntegrityError(msg)
        rows = pl.concat(frames)
        visible = filter_committed_rows(rows, TablePath.STEPS.value, read.controls)
        evidence = None
        if (
            not visible.is_empty()
            and dynamic_output_roles(visible["compute_options_json"][0])
            and visible["status"].is_in(["succeeded", "partial"]).any()
        ):
            evidence = read_curator_result_evidence(
                self.committer, step_run_id, _read=read
            )
        return rows, self._validate_attempt_rows(
            rows, result_evidence=evidence, controls=read.controls
        )

    def _admission_for(self, state: StepState) -> StepAdmission:
        """Verify frozen membership against an already validated lifecycle state."""
        digest = state.metadata.get(ADMISSION_DIGEST)
        if not isinstance(digest, str):
            msg = "Step has no durable finite assignment admission"
            raise StoreIntegrityError(msg)
        admission = read_step_admission(
            state.step_run_id, digest, delta_root=self._delta_root, fs=self._fs
        )
        _validate_identity(state, admission)
        return admission

    def _validate_assignments(self, admission: StepAdmission) -> None:
        for reference in admission.assignments:
            state = self.committer.journal.state(reference.attempt.assignment)
            if (
                state is None
                or (state.attempt, state.admission_digest)
                != (
                    reference.attempt,
                    reference.record_digest,
                )
                or state.cancelled
                or state.winner is not None
                or state.reuse is not None
            ):
                msg = "Step requires exact active unclaimed unit admissions"
                raise StoreIntegrityError(msg)
            record = read_admission_record(
                reference, delta_root=self._delta_root, fs=self._fs
            )
            if record.step_number != admission.step_number:
                msg = "Assignment admission has another outer step number"
                raise StoreIntegrityError(msg)


def _validate_identity(state: StepState, admission: StepAdmission) -> None:
    identity = admission.recipe.definition
    outputs = {item.role: item.artifact_type for item in admission.recipe.outputs}
    has_output_contract = state.status not in {
        StepStatus.FAILED,
        StepStatus.CANCELLED,
        StepStatus.SKIPPED,
    }
    if (
        state.step_number != admission.step_number
        or state.operation_class != f"{identity.module}.{identity.qualname}"
        or state.params_json != admission.recipe.params_json
        or (
            has_output_contract
            and (
                state.output_roles != frozenset(outputs)
                or state.output_types != outputs
            )
        )
        or dynamic_output_roles(state.compute_options_json)
        or any(
            reference.attempt.replay_of_execution_run_id
            != state.replay_of_execution_run_id
            for reference in admission.assignments
        )
    ):
        msg = "Step admission changed its pending scientific owner"
        raise StoreIntegrityError(msg)
