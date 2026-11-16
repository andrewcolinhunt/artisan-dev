"""Complete selected pipeline publications without retrying scientific work."""

from __future__ import annotations

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_step_results import RetainedStepResults
from artisan.orchestration.engine.retained_step_tracker import (
    RetainedStepTracker,
)
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.step_admission import ADMISSION_DIGEST, StepAdmission
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.io.admission import read_admission_record
from artisan.storage.io.curator_plan import read_curator_plan
from artisan.storage.io.input_registration import recover_input_registration
from artisan.storage.io.input_registration_plan import read_input_registration
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.step_result_plan import (
    read_step_result_plan,
    validate_unit_admission,
)


def recover_pipeline_results(
    committer: RetainedCommitter,
    *,
    pipeline_run_id: str,
    staging_root: str,
    preserve_staging: bool = False,
) -> tuple[StepState, ...]:
    """Finish this run's durable selections under exclusive controller ownership.

    Recover input registrations, already claimed deliveries, and selected terminal
    plans through their existing authorities. Unselected seals confer no authority.
    An unfinished step without a terminal plan remains unfinished, including after
    its independently selected unit results finish publication. The caller must
    resolve that ownership before resuming submissions.

    Returns:
        Every current attempt in the selected run, including older unresolved
        attempts hidden by a newer attempt at the same step number.

    Raises:
        ValueError: If the run identity is empty or has no recorded attempts.
        StoreIntegrityError: If selected evidence conflicts or fails verification.
    """
    if not pipeline_run_id.strip():
        message = "Pipeline recovery requires an explicit run identity"
        raise ValueError(message)
    tracker = RetainedStepTracker(committer, pipeline_run_id)
    with committer._lock:
        states = _run_attempts(tracker, pipeline_run_id)
        if not states:
            message = "No step attempts found for the selected pipeline run"
            raise ValueError(message)
        for state in states:
            _recover_attempt(committer, tracker, state, staging_root, preserve_staging)
        return _run_attempts(tracker, pipeline_run_id)


def _run_attempts(
    tracker: RetainedStepTracker, pipeline_run_id: str
) -> tuple[StepState, ...]:
    return tuple(
        state
        for state in tracker.load_all_current_states()
        if state.pipeline_run_id == pipeline_run_id
    )


def _recover_attempt(
    committer: RetainedCommitter,
    tracker: RetainedStepTracker,
    state: StepState,
    staging_root: str,
    preserve_staging: bool,
) -> None:
    step_run_id = state.step_run_id
    scientific = read_step_result_plan(committer.delta_root, step_run_id, committer.fs)
    curator = read_curator_plan(committer.delta_root, step_run_id, committer.fs)
    if scientific is not None and curator is not None:
        message = "Step has conflicting scientific and curator result plans"
        raise StoreIntegrityError(message)
    registration = read_input_registration(
        committer.delta_root, step_run_id, committer.fs
    )
    if registration is not None:
        if (registration.step_number, registration.operation_name) != (
            state.step_number,
            state.step_name,
        ):
            message = "Input registration differs from its pipeline step owner"
            raise StoreIntegrityError(message)
        recover_input_registration(committer, step_run_id)
    if ADMISSION_DIGEST in state.metadata:
        _recover_winners(
            committer, tracker.read_admission(step_run_id), preserve_staging
        )
    if scientific is not None:
        RetainedStepResults(tracker).recover(step_run_id)
    elif curator is not None:
        CuratorResults(
            tracker, staging_root, preserve_staging=preserve_staging
        ).recover(step_run_id)


def _recover_winners(
    committer: RetainedCommitter, admission: StepAdmission, preserve_staging: bool
) -> None:
    assignments = tuple(item.attempt.assignment for item in admission.assignments)
    states = committer.journal.states(assignments)
    for initial in admission.assignments:
        identity = initial.attempt.assignment
        selected = states[identity]
        if selected is None:
            message = "Pipeline admission has no matching assignment journal owner"
            raise StoreIntegrityError(message)
        reference = AdmissionReference(
            attempt=selected.attempt, record_digest=selected.admission_digest
        )
        validate_unit_admission(initial, reference)
        record = read_admission_record(
            reference, delta_root=committer.delta_root, fs=committer.fs
        )
        if record.step_number != admission.step_number:
            message = "Selected assignment has another outer step number"
            raise StoreIntegrityError(message)
        if selected.winner is not None:
            committer.recover(identity, preserve_staging=preserve_staging)
