"""Read-only inventory of retained selections, completed effects and orphan seals."""

from __future__ import annotations

import posixpath
from collections.abc import Iterator

from deltalake.exceptions import DeltaError

from artisan.errors import (
    ArtifactIntegrityError,
    PersistenceIntegrityError,
    StoreIntegrityError,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus, StepStatus
from artisan.storage.core.committed_scan import (
    audit_table_owners,
    verify_snapshot_effect,
)
from artisan.storage.core.table_schemas import FRAMEWORK_SCHEMAS
from artisan.storage.io.assignment_journal import AssignmentState
from artisan.storage.io.curator_carrier_effects import verify_curator_carriers
from artisan.storage.io.curator_control import CandidatePlan
from artisan.storage.io.curator_plan import CuratorCommitPlan, read_curator_plan
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan
from artisan.storage.io.input_registration import _verify_content
from artisan.storage.io.input_registration_plan import (
    InputRegistrationPlan,
    read_input_registration,
    registration_directory,
)
from artisan.storage.io.logical_control import control_for
from artisan.storage.io.published_effects import capture_table_effects
from artisan.storage.io.repair_report import RepairItem, RepairReport
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_effects import read_retained_plan
from artisan.storage.io.retained_metadata import (
    read_curator_source_metadata,
    read_terminal_metadata,
)
from artisan.storage.io.retained_plan import RetainedCommitPlan, retained_table_paths
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.retained_seal import RETAINED_SEAL_FILENAME
from artisan.storage.io.step_result_plan import StepResultPlan, read_step_result_plan
from artisan.storage.io.worker_seal import EXECUTION_SEAL_FILENAME
from artisan.utils.path import uri_join

_EVIDENCE_ERRORS = (
    StoreIntegrityError,
    ArtifactIntegrityError,
    PersistenceIntegrityError,
    ValueError,
    OSError,
    DeltaError,
)


def inspect_retained_store(
    committer: RetainedCommitter, *, staging_root: str
) -> RepairReport:
    """Audit existing evidence without accepting, repairing or deleting anything.

    Completed owners require exact table effects and managed carrier bytes.
    Selected but incomplete plans remain incomplete: this inventory does not
    promise that their remaining evidence is replayable. Only explicit recovery
    can finish those decisions. Unclaimed worker seals are diagnostic evidence,
    including after cancellation, and never confer acceptance or worker closure.
    The caller must own the offline store inspection exclusively.
    """
    if not staging_root.strip():
        message = "Store inspection requires an explicit staging root"
        raise ValueError(message)
    items: list[RepairItem] = []
    selected_seals: set[str] = set()
    with committer._lock:
        try:
            states = committer.journal.all_states()
            committer.journal.require_winners(
                tuple(
                    selection.receipt
                    for state in states
                    for selection in (state.winner, state.reuse)
                    if selection is not None
                )
            )
            selected_seals = {
                committer.fs._strip_protocol(state.winner.receipt.seal_uri)
                for state in states
                if state.winner is not None
            }
            with committer._read_snapshot() as snapshot:
                plans = _plans(committer, snapshot, items)
                selected_seals.update(_curator_seals(committer, staging_root, plans))
                _audit_tables(snapshot, items)
                for plan in plans.values():
                    items.append(_plan_item(snapshot, plan))
                resolved = {
                    admission.attempt.assignment.assignment_id
                    for plan in plans.values()
                    if isinstance(plan, StepResultPlan)
                    and (control := control_for(plan, snapshot.controls)) is not None
                    and control["state"] == "complete"
                    for admission in plan.decision_admissions
                }
                items.extend(_assignment_items(states, plans, resolved))
                items.extend(_lifecycle_items(committer, plans))
        except _EVIDENCE_ERRORS as error:
            items.append(_corrupt("retained-store", error))
        items.extend(_orphan_seals(committer, staging_root, selected_seals))
    return RepairReport(
        items=tuple(
            sorted(items, key=lambda item: (item.evidence_id, item.classification))
        )
    )


def _plans(
    committer: RetainedCommitter,
    snapshot: RetainedReadSnapshot,
    items: list[RepairItem],
) -> dict[str, CandidatePlan]:
    plans = dict(snapshot._plans)
    for namespace in (
        "retained-commits",
        "input-registrations",
        "step-results",
        "curator-commits",
    ):
        pattern = uri_join(
            committer.delta_root, "_artisan", namespace, "*", "plan.json"
        )
        for path in sorted(committer.fs.glob(pattern)):
            try:
                plan = _read_plan(committer, namespace, path)
                previous = plans.get(plan.logical_commit_id)
                if previous is not None and previous != plan:
                    message = "Logical owner has conflicting immutable plans"
                    raise StoreIntegrityError(message)
                plans[plan.logical_commit_id] = plan
            except _EVIDENCE_ERRORS as error:
                items.append(_corrupt(path, error))
    return plans


def _read_plan(
    committer: RetainedCommitter, namespace: str, path: str
) -> CandidatePlan:
    owner = posixpath.basename(posixpath.dirname(path))
    if namespace == "retained-commits":
        with committer.fs.open(path, "rb", cache_type="none") as stream:
            candidate = RetainedCommitPlan.model_validate_json(stream.read())
        assignment = candidate.receipt.delivery.attempt.assignment
        if assignment.assignment_id != owner:
            message = "Retained plan is stored under another assignment"
            raise StoreIntegrityError(message)
        plan: CandidatePlan | None = read_retained_plan(
            committer.delta_root, assignment, committer.fs
        )
        if plan != candidate:
            message = "Retained plan differs from its canonical owner"
            raise StoreIntegrityError(message)
        committer._require_plan_owner(candidate, candidate.receipt, candidate.admission)
    elif namespace == "input-registrations":
        plan = read_input_registration(committer.delta_root, owner, committer.fs)
    elif namespace == "step-results":
        plan = read_step_result_plan(committer.delta_root, owner, committer.fs)
    else:
        plan = read_curator_plan(committer.delta_root, owner, committer.fs)
    if plan is None:
        message = "Inventoried plan disappeared during offline inspection"
        raise StoreIntegrityError(message)
    return plan


def _table_paths() -> tuple[str, ...]:
    return tuple(
        sorted(
            {*retained_table_paths(), *(table.value for table in FRAMEWORK_SCHEMAS)}
            - {TablePath.LOGICAL_COMMITS.value}
        )
    )


def _audit_tables(snapshot: RetainedReadSnapshot, items: list[RepairItem]) -> None:
    for path in _table_paths():
        try:
            for frame in snapshot.snapshot(path).iter_frames():
                audit_table_owners(frame, path, snapshot.controls)
        except _EVIDENCE_ERRORS as error:
            items.append(_corrupt(path, error))


def _plan_item(snapshot: RetainedReadSnapshot, plan: CandidatePlan) -> RepairItem:
    owner = plan.logical_commit_id
    try:
        control = control_for(plan, snapshot.controls)
        if control is None or control["state"] == "planned":
            return RepairItem(
                evidence_id=owner,
                classification="incomplete",
                detail="Selected plan is not complete; explicit recovery must verify its remaining evidence",
            )
        if control["state"] == "abandoned":
            return RepairItem(
                evidence_id=owner,
                classification="abandoned",
                detail="Publication was explicitly abandoned; original selections remain unchanged",
            )
        for path in _table_paths():
            verify_snapshot_effect(
                plan, path, snapshot.snapshot(path), snapshot.controls
            )
        if isinstance(plan, (StepResultPlan, CuratorCommitPlan, CuratorReusePlan)):
            terminal = read_terminal_metadata(snapshot, plan)
            if isinstance(plan, (CuratorCommitPlan, CuratorReusePlan)):
                read_curator_source_metadata(snapshot, plan, terminal)
        _verify_carriers(snapshot.committer, plan)
        return RepairItem(
            evidence_id=owner,
            classification="complete",
            detail="Completed selection, exact table effects and managed carrier bytes verified",
        )
    except _EVIDENCE_ERRORS as error:
        return _corrupt(owner, error)


def _verify_carriers(committer: RetainedCommitter, plan: CandidatePlan) -> None:
    if isinstance(plan, RetainedCommitPlan):
        committer._verify_carriers(plan)
    elif isinstance(plan, CuratorCommitPlan):
        verify_curator_carriers(
            plan.carrier_inventory,
            plan.managed_locations,
            files_root=committer.files_root,
            fs=committer.fs,
        )
    elif isinstance(plan, InputRegistrationPlan):
        with capture_table_effects(
            plan,
            plan.tables,
            directory=registration_directory(committer.delta_root, plan.step_run_id),
            fs=committer.fs,
        ) as tables:
            _verify_content(committer, plan, tables)


def _assignment_items(
    states: tuple[AssignmentState, ...],
    plans: dict[str, CandidatePlan],
    resolved: set[str],
) -> Iterator[RepairItem]:
    for state in states:
        owner = f"retained_result:{state.attempt.assignment.assignment_id}"
        if state.winner is not None:
            if owner not in plans:
                yield RepairItem(
                    evidence_id=owner,
                    classification="incomplete",
                    detail="Durable winner has no publication plan; explicit recovery may finish only this selection",
                )
        elif state.reuse is not None:
            source = f"retained_result:{state.reuse.receipt.delivery.attempt.assignment.assignment_id}"
            if source not in plans:
                yield RepairItem(
                    evidence_id=owner,
                    classification="corrupt",
                    detail="Reused result has no original publication plan",
                )
        elif state.attempt.assignment.assignment_id not in resolved:
            yield RepairItem(
                evidence_id=f"assignment:{state.attempt.assignment.assignment_id}",
                classification="ineligible" if state.cancelled else "incomplete",
                detail="Assignment is fenced without a selected result; late seals remain ineligible"
                if state.cancelled
                else "Assignment has no selected result; inspection cannot infer worker exit or authorize retry",
            )


def _lifecycle_items(
    committer: RetainedCommitter, plans: dict[str, CandidatePlan]
) -> Iterator[RepairItem]:
    tracker = RetainedStepTracker(committer, "offline-inspection")
    for state in tracker.load_all_current_states():
        if state.cancellation_status == CancellationStatus.UNKNOWN:
            yield RepairItem(
                evidence_id=f"step:{state.step_run_id}",
                classification="unknown_owner",
                detail="Recorded worker cleanup is unresolved; inspection cannot authorize retry",
            )
        elif state.status in {StepStatus.PENDING, StepStatus.RUNNING} and (
            f"step_result:{state.step_run_id}" not in plans
        ):
            yield RepairItem(
                evidence_id=f"step:{state.step_run_id}",
                classification="incomplete",
                detail="Step has no selected terminal result; inspection cannot infer completion or worker exit",
            )


def _orphan_seals(
    committer: RetainedCommitter, staging_root: str, selected: set[str]
) -> Iterator[RepairItem]:
    if not committer.fs.exists(staging_root):
        return
    for path in sorted(committer.fs.find(staging_root)):
        if (
            posixpath.basename(path)
            in {RETAINED_SEAL_FILENAME, EXECUTION_SEAL_FILENAME}
            and committer.fs._strip_protocol(path) not in selected
        ):
            yield RepairItem(
                evidence_id=path,
                classification="unplanned",
                detail="Unclaimed worker seal is diagnostic only; discovery does not authorize acceptance or cleanup",
            )


def _curator_seals(
    committer: RetainedCommitter, staging_root: str, plans: dict[str, CandidatePlan]
) -> Iterator[str]:
    for plan in plans.values():
        if isinstance(plan, CuratorCommitPlan):
            execution = plan.effects.table(TablePath.EXECUTIONS.value)
            if execution is not None:
                for item in execution.files:
                    yield committer.fs._strip_protocol(
                        uri_join(staging_root, item.relative_path)
                    )


def _corrupt(owner: str, error: Exception) -> RepairItem:
    return RepairItem(evidence_id=owner, classification="corrupt", detail=str(error))
