"""Explicit offline recovery of frozen retained-store publication selections."""

from __future__ import annotations

from collections.abc import Iterator, Mapping

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_step_results import RetainedStepResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.io.admission import read_admission_record
from artisan.storage.io.assignment_journal import AssignmentState
from artisan.storage.io.curator_control import CandidatePlan
from artisan.storage.io.curator_plan import CuratorCommitPlan, read_curator_plan
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan
from artisan.storage.io.input_registration import recover_input_registration
from artisan.storage.io.input_registration_plan import (
    InputRegistrationPlan,
    read_input_registration,
)
from artisan.storage.io.logical_control import control_for
from artisan.storage.io.repair_report import RepairItem, RepairReport
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_repair import (
    _EVIDENCE_ERRORS,
    _plans,
    inspect_retained_store,
)
from artisan.storage.io.step_result_plan import (
    StepResultPlan,
    read_step_result_plan,
    validate_unit_admission,
)

type _Selection = (
    InputRegistrationPlan
    | AssignmentState
    | StepResultPlan
    | CuratorCommitPlan
    | CuratorReusePlan
)


def apply_retained_repair(
    committer: RetainedCommitter,
    *,
    staging_root: str,
    preserve_staging: bool = False,
) -> RepairReport:
    """Finish existing selections and return a fresh inventory under exclusive ownership.

    A blocking initial audit prevents every write. Otherwise each selected result
    uses its ordinary exact recovery and cleanup path. A later failure stops
    further application; independently completed results remain committed. No
    staged seal is claimed, unfinished step is inferred, or worker is retried.
    Abandoned selections and uncertain worker ownership remain unchanged.
    """
    with committer._lock:
        before = inspect_retained_store(committer, staging_root=staging_root)
        if before.blocking:
            return before
        owner = "retained-store"
        try:
            selections, states = _capture_selections(committer)
            for selection in selections:
                owner = _owner(selection)
                _validate_step_owner(committer, selection, states)
            for selection in selections:
                owner = _owner(selection)
                _recover(committer, selection, states, staging_root, preserve_staging)
        except _EVIDENCE_ERRORS as error:
            return _failure_report(committer, staging_root, before, owner, error)
        return inspect_retained_store(committer, staging_root=staging_root)


def _capture_selections(
    committer: RetainedCommitter,
) -> tuple[tuple[_Selection, ...], dict[str, StepState]]:
    states = {
        state.step_run_id: state
        for state in RetainedStepTracker(
            committer, "offline-recovery"
        ).load_all_current_states()
    }
    with committer._read_snapshot() as snapshot:
        findings: list[RepairItem] = []
        plans = _plans(committer, snapshot, findings)
        if findings:
            message = "Selected publication evidence changed after the initial audit"
            raise StoreIntegrityError(message)
        abandoned = {
            plan.logical_commit_id
            for plan in plans.values()
            if (control := control_for(plan, snapshot.controls)) is not None
            and control["state"] == "abandoned"
        }
        return tuple(
            _ordered_selections(plans, committer.journal.all_states(), abandoned)
        ), states


def _ordered_selections(
    plans: Mapping[str, CandidatePlan],
    assignments: tuple[AssignmentState, ...],
    abandoned: set[str],
) -> Iterator[_Selection]:
    ordered = [plans[key] for key in sorted(plans) if key not in abandoned]
    yield from (plan for plan in ordered if isinstance(plan, InputRegistrationPlan))
    yield from (
        state
        for state in assignments
        if state.winner is not None and _owner(state) not in abandoned
    )
    yield from (
        plan
        for plan in ordered
        if isinstance(plan, (StepResultPlan, CuratorCommitPlan, CuratorReusePlan))
    )


def _owner(selection: _Selection) -> str:
    return (
        f"retained_result:{selection.attempt.assignment.assignment_id}"
        if isinstance(selection, AssignmentState)
        else selection.logical_commit_id
    )


def _validate_step_owner(
    committer: RetainedCommitter, selection: _Selection, states: Mapping[str, StepState]
) -> None:
    step_run_id = (
        selection.attempt.assignment.step_run_id
        if isinstance(selection, AssignmentState)
        else selection.step_run_id
    )
    state = states.get(step_run_id)
    if isinstance(selection, InputRegistrationPlan):
        if state is not None and (selection.step_number, selection.operation_name) != (
            state.step_number,
            state.step_name,
        ):
            message = "Input registration differs from its pipeline step owner"
            raise StoreIntegrityError(message)
    elif isinstance(selection, AssignmentState):
        if state is None:
            return
        admission = RetainedStepTracker(
            committer, state.pipeline_run_id
        ).read_admission(step_run_id)
        index = selection.attempt.assignment.unit_index
        if index >= len(admission.assignments):
            message = "Selected winner is outside its outer step membership"
            raise StoreIntegrityError(message)
        reference = AdmissionReference(
            attempt=selection.attempt, record_digest=selection.admission_digest
        )
        validate_unit_admission(admission.assignments[index], reference)
        record = read_admission_record(
            reference, delta_root=committer.delta_root, fs=committer.fs
        )
        if record.step_number != admission.step_number:
            message = "Selected winner has another outer step number"
            raise StoreIntegrityError(message)
    elif state is None:
        message = "Selected terminal result has no recorded lifecycle owner"
        raise StoreIntegrityError(message)


def _recover(
    committer: RetainedCommitter,
    selection: _Selection,
    states: Mapping[str, StepState],
    staging_root: str,
    preserve_staging: bool,
) -> None:
    if isinstance(selection, AssignmentState):
        current = committer.journal.state(selection.attempt.assignment)
        if current is None or (
            current.attempt,
            current.admission_digest,
            current.winner,
        ) != (selection.attempt, selection.admission_digest, selection.winner):
            message = "Winner selection changed during offline recovery"
            raise StoreIntegrityError(message)
        committer.recover(
            selection.attempt.assignment, preserve_staging=preserve_staging
        )
        return
    if isinstance(selection, InputRegistrationPlan):
        current_plan: CandidatePlan | None = read_input_registration(
            committer.delta_root, selection.step_run_id, committer.fs
        )
    elif isinstance(selection, StepResultPlan):
        current_plan = read_step_result_plan(
            committer.delta_root, selection.step_run_id, committer.fs
        )
    else:
        current_plan = read_curator_plan(
            committer.delta_root, selection.step_run_id, committer.fs
        )
    if current_plan != selection:
        message = "Selected publication plan changed during offline recovery"
        raise StoreIntegrityError(message)
    if isinstance(selection, InputRegistrationPlan):
        recover_input_registration(committer, selection.step_run_id)
        return
    tracker = RetainedStepTracker(
        committer, states[selection.step_run_id].pipeline_run_id
    )
    if isinstance(selection, StepResultPlan):
        RetainedStepResults(tracker).recover(selection.step_run_id)
    else:
        CuratorResults(
            tracker, staging_root, preserve_staging=preserve_staging
        ).recover(selection.step_run_id)


def _failure_report(
    committer: RetainedCommitter,
    staging_root: str,
    before: RepairReport,
    owner: str,
    error: Exception,
) -> RepairReport:
    try:
        report = inspect_retained_store(committer, staging_root=staging_root)
    except _EVIDENCE_ERRORS as inspection_error:
        report = RepairReport(
            items=(
                *before.items,
                RepairItem(
                    evidence_id="repair-inspection",
                    classification="corrupt",
                    detail=str(inspection_error),
                ),
            )
        )
    items = [item for item in report.items if item.evidence_id != owner]
    items.append(
        RepairItem(evidence_id=owner, classification="corrupt", detail=str(error))
    )
    return RepairReport(
        items=tuple(
            sorted(items, key=lambda item: (item.evidence_id, item.classification))
        )
    )
