"""Authenticate an anchored replay's complete ordinary input selection."""

from __future__ import annotations

from dataclasses import dataclass

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.composites.grouping import AnchoredNameGrouping
from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.execution.admission import AdmissionRecord, AdmissionReference
from artisan.schemas.execution.step_admission import StepAdmission
from artisan.schemas.orchestration.step_lifecycle import TERMINAL_STEP_STATUSES
from artisan.storage.cache.retained_identity import _input_semantics
from artisan.storage.core.execution_source import (
    FailedScientificExecutionSource,
    ScientificExecutionSource,
    _read_execution_source,
)
from artisan.storage.io.admission import read_admission_record
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_metadata import read_terminal_metadata
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.step_result_plan import StepResultPlan, validate_unit_admission

type _ScientificSource = ScientificExecutionSource | FailedScientificExecutionSource


@dataclass(frozen=True)
class OriginalSelectionProof:
    """Verified step membership and exact selected units, newest replay first.

    This call-owned proof is compared on recheck; it is never persisted as an
    alternative to the lifecycle and immutable admission records.
    """

    steps: tuple[StepAdmission, ...]
    selected_admissions: tuple[AdmissionReference, ...]

    @property
    def origin_step(self) -> StepAdmission:
        """Return the ordinary step whose full selection supports the replay."""
        return self.steps[-1]


def resolve_original_selection(
    committer: RetainedCommitter,
    execution_run_id: str,
    recipe: ExecutionRecipe,
) -> OriginalSelectionProof:
    """Verify original selection authority without reading scientific payloads.

    Every diagnostic link must retain the original unit's exact input semantics
    and anchored grouping shape. Current nonempty requirements are checked on
    the ordinary origin's entire admitted inventory, not the sparse replay unit.
    Callers separately validate the captured unit against its current bound.
    """
    if not isinstance(recipe.policy.group_by, AnchoredNameGrouping):
        msg = "Original selection proof requires anchored grouping"
        raise ValueError(msg)
    with committer._read_snapshot() as snapshot:
        return _resolve_original_selection(snapshot, execution_run_id, recipe)


def _resolve_original_selection(
    snapshot: RetainedReadSnapshot,
    execution_run_id: str,
    recipe: ExecutionRecipe,
) -> OriginalSelectionProof:
    seen = set()
    steps = []
    references = []
    previous: AdmissionRecord | None = None
    tracker = RetainedStepTracker(snapshot.committer, "")
    while execution_run_id not in seen:
        seen.add(execution_run_id)
        source = _read_execution_source(snapshot, execution_run_id)
        if not isinstance(
            source, (ScientificExecutionSource, FailedScientificExecutionSource)
        ):
            msg = "Original selection requires completed scientific replay sources"
            raise StoreIntegrityError(msg)
        _require_grouping_shape(recipe, source.admission.recipe)
        reference = _source_reference(source)
        step, records = _verified_step(snapshot, tracker, source, reference)
        if previous is not None:
            _require_same_inputs(previous, source.admission)
        steps.append(step)
        references.append(reference)
        previous = source.admission
        parent_id = reference.attempt.replay_of_execution_run_id
        if parent_id is None:
            _require_nonempty_inventory(recipe, records)
            return OriginalSelectionProof(tuple(steps), tuple(references))
        execution_run_id = parent_id
    msg = "Original selection replay chain contains a cycle"
    raise StoreIntegrityError(msg)


def _verified_step(
    snapshot: RetainedReadSnapshot,
    tracker: RetainedStepTracker,
    source: _ScientificSource,
    reference: AdmissionReference,
) -> tuple[StepAdmission, tuple[AdmissionRecord, ...]]:
    identity = reference.attempt.assignment
    _, state = tracker._read_state(identity.step_run_id, snapshot)
    step = tracker._admission_for(state)
    index = identity.unit_index
    if index >= len(step.assignments):
        msg = "Replay source is absent from its complete step admission"
        raise StoreIntegrityError(msg)
    if state.status in TERMINAL_STEP_STATUSES:
        terminal = snapshot.completed_control_plan(f"step_result:{step.step_run_id}")
        if (
            not isinstance(terminal, StepResultPlan)
            or terminal.admission != step
            or read_terminal_metadata(snapshot, terminal) != state.to_step_result()
            or terminal.decision_admissions[index] != reference
        ):
            msg = "Replay source differs from its completed step membership"
            raise StoreIntegrityError(msg)
    validate_unit_admission(step.assignments[index], reference)
    committer = snapshot.committer
    records = tuple(
        read_admission_record(item, delta_root=committer.delta_root, fs=committer.fs)
        for item in step.assignments
    )
    if any(
        record.recipe != step.recipe or record.step_number != step.step_number
        for record in (*records, source.admission)
    ):
        msg = "Original selection changed its admitted recipe or step number"
        raise StoreIntegrityError(msg)
    _require_same_inputs(records[index], source.admission)
    return step, records


def _source_reference(source: _ScientificSource) -> AdmissionReference:
    return (
        source.plan.admission
        if isinstance(source, ScientificExecutionSource)
        else source.unit.admission
    )


def _require_same_inputs(first: AdmissionRecord, second: AdmissionRecord) -> None:
    if _input_semantics(first.boundary) != _input_semantics(second.boundary):
        msg = "Replay source chain changed exact admitted input semantics"
        raise StoreIntegrityError(msg)


def _require_grouping_shape(
    current: ExecutionRecipe, captured: ExecutionRecipe
) -> None:
    if (
        not isinstance(captured.policy.group_by, AnchoredNameGrouping)
        or current.policy.group_by != captured.policy.group_by
        or current.policy.shared_input_roles != captured.policy.shared_input_roles
    ):
        msg = "Anchored replay cannot change grouping or shared input roles"
        raise StoreIntegrityError(msg)


def _require_nonempty_inventory(
    recipe: ExecutionRecipe, records: tuple[AdmissionRecord, ...]
) -> None:
    nonempty = {
        role
        for record in records
        for role, addresses in record.boundary.input_roles().items()
        if addresses
    }
    for role in recipe.policy.required_nonempty_inputs:
        if role not in nonempty:
            msg = f"Original complete selection requires nonempty input role {role!r}"
            raise StoreIntegrityError(msg)
