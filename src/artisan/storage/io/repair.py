"""Report and deliberately repair immutable logical-commit evidence."""

from __future__ import annotations

import posixpath
from typing import Literal, cast

from deltalake import DeltaTable
from fsspec import AbstractFileSystem
from pydantic import BaseModel, ConfigDict

from artisan.errors import (
    ArtifactIntegrityError,
    IncompatibleStoreError,
    PersistenceIntegrityError,
    StoreIntegrityError,
)
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import (
    TERMINAL_STEP_STATUSES,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.core.committed_scan import read_logical_commits
from artisan.storage.core.store_format import assert_store_format
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import (
    CommitKind,
    CommitPlan,
    prepare_commit_plan,
    publish_commit_plan,
    read_commit_plan,
    verify_plan_files,
)
from artisan.storage.io.publication import is_publication_temporary
from artisan.storage.io.staging import StagingManager
from artisan.storage.io.worker_seal import verify_worker_seal
from artisan.utils.path import shard_uri, uri_join

RepairClassification = Literal[
    "complete",
    "replayable",
    "conflict",
    "corrupt",
    "unplanned",
    "legacy_unverifiable",
    "abandoned",
    "recoverable",
    "incomplete",
    "ineligible",
    "unknown_owner",
]


class RepairItem(BaseModel):
    """One independently actionable piece of store evidence."""

    evidence_id: str
    classification: RepairClassification
    detail: str

    model_config = ConfigDict(frozen=True)


class RepairReport(BaseModel):
    """Deterministically ordered offline repair report."""

    items: tuple[RepairItem, ...]

    model_config = ConfigDict(frozen=True)

    @property
    def blocking_items(self) -> tuple[RepairItem, ...]:
        """Integrity findings that forbid recovery or new pipeline work."""
        return tuple(
            item
            for item in self.items
            if item.classification in {"corrupt", "conflict", "legacy_unverifiable"}
        )

    @property
    def blocking(self) -> bool:
        """Whether integrity findings forbid recovery or new pipeline work."""
        return bool(self.blocking_items)

    @property
    def unresolved(self) -> bool:
        """Whether the report contains evidence requiring operator action."""
        return any(
            item.classification
            not in {
                "complete",
                "abandoned",
            }
            for item in self.items
        )


def repair_store(
    *,
    delta_root: str,
    staging_root: str,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
    apply: bool = False,
    abandon: str | None = None,
    reason: str | None = None,
    recover_staging: bool = False,
    preserve_staging: bool = False,
    files_root: str | None = None,
) -> RepairReport:
    """Inspect commit evidence or perform one explicit repair action.

    With neither action selected, report without modifying either root.
    ``apply=True`` retries validated plans and removes staging for completed
    plans. ``abandon`` instead marks one planned commit abandoned and reconciles
    its step's failure; it requires a reason and retains staging evidence.

    Raises:
        ValueError: If apply and abandon are combined, abandonment has no
            reason, or a reason is supplied without abandonment.
        IncompatibleStoreError: If a mutation targets an unsupported store.
        StoreIntegrityError: If evidence cannot authorize the requested action.
    """
    _validate_action(apply=apply, abandon=abandon, reason=reason)
    if recover_staging and abandon is not None:
        msg = "Recovery and abandonment are separate actions"
        raise ValueError(msg)
    options = storage_options or {}
    try:
        assert_store_format(delta_root, fs, options)
    except IncompatibleStoreError as exc:
        if apply or abandon is not None:
            raise
        return RepairReport(
            items=(
                RepairItem(
                    evidence_id=delta_root,
                    classification="legacy_unverifiable",
                    detail=str(exc),
                ),
            )
        )

    committer = DeltaCommitter(
        delta_root,
        StagingManager(staging_root, fs),
        fs=fs,
        storage_options=options,
        files_root=files_root,
    )
    staged_paths = (
        tuple(fs.find(staging_root))
        if recover_staging and fs.exists(staging_root)
        else ()
    )
    if not apply and abandon is None:
        return _build_report(
            committer, recover_staging=recover_staging, staged_paths=staged_paths
        )[0]
    report, plans = _build_report(committer)
    if abandon is not None:
        _abandon(committer, report, plans, abandon, reason or "")
        return _build_report(committer)[0]
    if report.blocking:
        return report
    _apply_report(committer, report, plans, preserve_staging)
    if recover_staging:
        report, plans = _build_report(
            committer, recover_staging=True, staged_paths=staged_paths
        )
        if report.blocking:
            return report
        _apply_report(committer, report, plans, preserve_staging, standalone_only=True)
    return _build_report(
        committer, recover_staging=recover_staging, staged_paths=staged_paths
    )[0]


def _apply_report(
    committer: DeltaCommitter,
    report: RepairReport,
    plans: dict[str, CommitPlan],
    preserve_staging: bool,
    *,
    standalone_only: bool = False,
) -> None:
    """Apply exact plans first, then independently owned successful workers."""
    allowed = {"recoverable"} if standalone_only else {"complete", "replayable"}
    for item in report.items:
        if item.classification not in allowed:
            continue
        plan = plans[item.evidence_id]
        if item.classification == "recoverable":
            plan = publish_commit_plan(committer.delta_base_path, committer._fs, plan)
        committer.commit_logical(plan, preserve_staging=preserve_staging)


def _validate_action(
    *,
    apply: bool,
    abandon: str | None,
    reason: str | None,
) -> None:
    if apply and abandon is not None:
        msg = "Repair apply and abandonment are separate actions"
        raise ValueError(msg)
    if abandon is not None and not reason:
        msg = "Abandonment requires a non-empty reason"
        raise ValueError(msg)
    if abandon is None and reason is not None:
        msg = "A reason is valid only with abandonment"
        raise ValueError(msg)


def _build_report(
    committer: DeltaCommitter,
    *,
    recover_staging: bool = False,
    staged_paths: tuple[str, ...] = (),
) -> tuple[RepairReport, dict[str, CommitPlan]]:
    controls = read_logical_commits(
        committer.delta_base_path,
        fs=committer._fs,
        storage_options=committer._storage_options,
    )
    plans, plan_items = _read_all_plans(committer)
    control_by_id = {
        row["logical_commit_id"]: row for row in controls.iter_rows(named=True)
    }
    items = list(plan_items)
    unreadable_plan_ids = {item.evidence_id for item in plan_items}
    for logical_commit_id in sorted(set(plans) | set(control_by_id)):
        plan = plans.get(logical_commit_id)
        control = control_by_id.get(logical_commit_id)
        if plan is None:
            if logical_commit_id in unreadable_plan_ids:
                continue
            items.append(
                RepairItem(
                    evidence_id=logical_commit_id,
                    classification="corrupt",
                    detail="Control row has no readable immutable plan",
                )
            )
        elif control is None:
            items.append(_classify_pending(committer, plan))
        else:
            items.append(_classify_controlled(committer, plan, control))
    if recover_staging and not any(
        item.classification in {"conflict", "corrupt"} for item in items
    ):
        worker_items, candidates = _worker_staging(committer, plans, staged_paths)
        items.extend(worker_items)
        plans.update(candidates)
    else:
        items.extend(_unplanned_staging(committer, plans))
    ordered = tuple(
        sorted(items, key=lambda item: (item.evidence_id, item.classification))
    )
    return RepairReport(items=ordered), plans


def _read_all_plans(
    committer: DeltaCommitter,
) -> tuple[dict[str, CommitPlan], list[RepairItem]]:
    root = uri_join(committer.delta_base_path, "_artisan", "commit_plans")
    plans: dict[str, CommitPlan] = {}
    items: list[RepairItem] = []
    if not committer._fs.exists(root):
        return plans, items
    for path in sorted(committer._fs.glob(f"{root}/**/*.json")):
        relative = _relative_path(root, path, committer._fs)
        pieces = relative.split("/")
        execution_id = None
        if len(pieces) == 2 and pieces[1] in {
            "step_result.json",
            "input_registration.json",
        }:
            step_run_id, filename = pieces
            kind = cast(CommitKind, filename.removesuffix(".json"))
        elif len(pieces) == 3 and pieces[1] == "execution_recovery":
            step_run_id = pieces[0]
            kind = "execution_recovery"
            execution_id = pieces[2].removesuffix(".json")
        else:
            items.append(
                RepairItem(
                    evidence_id=f"plan:{relative}",
                    classification="corrupt",
                    detail="Unknown commit-plan path",
                )
            )
            continue
        evidence_id = f"{kind}:{execution_id or step_run_id}"
        try:
            plan = read_commit_plan(
                committer.delta_base_path,
                committer._fs,
                step_run_id,
                kind,
                execution_id,
            )
        except (StoreIntegrityError, ValueError) as exc:
            items.append(
                RepairItem(
                    evidence_id=evidence_id, classification="corrupt", detail=str(exc)
                )
            )
            continue
        if plan.logical_commit_id in plans:
            items.append(
                RepairItem(
                    evidence_id=plan.logical_commit_id,
                    classification="conflict",
                    detail="Multiple immutable plans claim the same commit ID",
                )
            )
            continue
        plans[plan.logical_commit_id] = plan
    return plans, items


def _classify_controlled(
    committer: DeltaCommitter,
    plan: CommitPlan,
    control: dict[str, object],
) -> RepairItem:
    if (
        control["commit_kind"] != plan.commit_kind
        or control["step_run_id"] != plan.step_run_id
        or control["plan_digest"] != plan.plan_digest
        or control["execution_run_id"] != plan.execution_run_id
    ):
        return RepairItem(
            evidence_id=plan.logical_commit_id,
            classification="conflict",
            detail="Control row disagrees with the immutable plan",
        )
    state = str(control["state"])
    if state == "abandoned":
        return RepairItem(
            evidence_id=plan.logical_commit_id,
            classification="abandoned",
            detail=f"Explicitly abandoned: {control['abandon_reason']}",
        )
    if state == "complete":
        try:
            committer._validate_complete(plan)
        except StoreIntegrityError as exc:
            return RepairItem(
                evidence_id=plan.logical_commit_id,
                classification=_integrity_classification(exc),
                detail=str(exc),
            )
        return RepairItem(
            evidence_id=plan.logical_commit_id,
            classification="complete",
            detail="Control, plan, and all planned effects agree",
        )
    return _classify_pending(committer, plan)


def _classify_pending(committer: DeltaCommitter, plan: CommitPlan) -> RepairItem:
    """Validate retry even when a crash preceded control-row insertion."""
    try:
        committer._reject_terminal_attempt(plan)
    except (StoreIntegrityError, PersistenceIntegrityError) as exc:
        return RepairItem(
            evidence_id=plan.logical_commit_id,
            classification="conflict",
            detail=str(exc),
        )
    try:
        frames = verify_plan_files(
            plan,
            committer.staging_manager.staging_dir,
            committer._fs,
        )
    except StoreIntegrityError as exc:
        return RepairItem(
            evidence_id=plan.logical_commit_id,
            classification="corrupt",
            detail=str(exc),
        )
    try:
        committer._validate_recovery(plan, frames)
        controls = committer._controls()
        for table in plan.tables:
            committer._missing_rows(
                plan,
                table,
                frames[table.table_path],
                committer._read_physical(table.table_path),
                controls,
            )
    except (
        StoreIntegrityError,
        ArtifactIntegrityError,
        PersistenceIntegrityError,
        ValueError,
    ) as exc:
        return RepairItem(
            evidence_id=plan.logical_commit_id,
            classification=_integrity_classification(exc),
            detail=str(exc),
        )
    return RepairItem(
        evidence_id=plan.logical_commit_id,
        classification="replayable",
        detail="Valid planned commit can be replayed exactly",
    )


def _integrity_classification(exc: Exception) -> RepairClassification:
    text = str(exc).lower()
    if "unreadable" in text or "missing" in text:
        return "corrupt"
    return "conflict"


def _worker_staging(
    committer: DeltaCommitter,
    plans: dict[str, CommitPlan],
    staged_paths: tuple[str, ...],
) -> tuple[list[RepairItem], dict[str, CommitPlan]]:
    """Snapshot worker shards without claiming anything owned by a saved plan."""
    root = committer.staging_manager.staging_dir
    if not committer._fs.exists(root):
        return [], {}
    referenced = {
        file.relative_path
        for plan in plans.values()
        for table in plan.tables
        for file in table.files
    }
    claimed = {posixpath.dirname(path) for path in referenced}
    directories: dict[str, list[str]] = {}
    for path in sorted(staged_paths):
        if is_publication_temporary(posixpath.basename(path)):
            continue
        relative = _relative_path(root, path, committer._fs)
        directories.setdefault(posixpath.dirname(relative), []).append(relative)
    tracker = StepTracker(
        committer.delta_base_path,
        fs=committer._fs,
        storage_options=committer._storage_options,
    )
    owners = {state.step_run_id: state for state in tracker.load_all_current_states()}
    items: list[RepairItem] = []
    candidates: dict[str, CommitPlan] = {}
    for directory, paths in directories.items():
        if directory in claimed or "_orchestrator" in directory.split("/"):
            items.extend(
                RepairItem(
                    evidence_id=f"staging:{path}",
                    classification="unplanned",
                    detail="Retained object outside immutable plans",
                )
                for path in paths
                if path not in referenced
            )
            continue
        item, plan = _classify_worker(committer, directory, paths, owners)
        items.append(item)
        if plan is not None:
            candidates[plan.logical_commit_id] = plan
    return items, candidates


def _classify_worker(
    committer: DeltaCommitter,
    directory: str,
    paths: list[str],
    owners: dict[str, StepState],
) -> tuple[RepairItem, CommitPlan | None]:
    """Classify one sealed unit, retaining incomplete and ineligible evidence."""
    root = committer.staging_manager.staging_dir
    evidence_id = f"staging:{directory}"
    seal = f"{directory}/executions.parquet"

    def retained(
        classification: RepairClassification, detail: str
    ) -> tuple[RepairItem, None]:
        return RepairItem(
            evidence_id=evidence_id, classification=classification, detail=detail
        ), None

    if seal not in paths:
        return retained("incomplete", "No published execution seal")
    try:
        frame = verify_worker_seal(uri_join(root, directory), committer._fs)
        record = frame.row(0, named=True)
        if (
            record["success"] is not True
            or record["replay_of_execution_run_id"] is not None
        ):
            return retained("ineligible", "Failed or diagnostic execution retained")
        owner = owners.get(record["step_run_id"])
        if owner is None:
            return retained("unknown_owner", "Execution has no known source attempt")
        if owner.replay_of_execution_run_id is not None:
            return retained("conflict", "Ordinary execution claims a diagnostic source")
        if owner.status in {StepStatus.PENDING, StepStatus.SKIPPED}:
            return retained("ineligible", "Source attempt did not execute")
        if owner.status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
            return retained(
                "conflict", "Unplanned execution would alter an accepted source attempt"
            )
        expected = shard_uri(
            root,
            record["execution_run_id"],
            step_number=record["origin_step_number"],
            operation_name=record["operation_name"],
        )
        if _relative_path(root, expected, committer._fs) != directory:
            return retained("conflict", "Execution seal is outside its canonical shard")
        plan = prepare_commit_plan(
            staging_root=root,
            fs=committer._fs,
            commit_kind="execution_recovery",
            step_run_id=record["step_run_id"],
            step_number=record["origin_step_number"],
            operation_name=record["operation_name"],
            execution_run_id=record["execution_run_id"],
        )
        item = _classify_pending(committer, plan)
        if item.classification != "replayable":
            return item, None
        return RepairItem(
            evidence_id=plan.logical_commit_id,
            classification="recoverable",
            detail="Validated successful worker execution can be recovered",
        ), plan
    except (
        StoreIntegrityError,
        ArtifactIntegrityError,
        PersistenceIntegrityError,
        ValueError,
        KeyError,
        TypeError,
    ) as exc:
        detail = (
            str(exc)
            if isinstance(
                exc,
                (
                    StoreIntegrityError,
                    ArtifactIntegrityError,
                    PersistenceIntegrityError,
                ),
            )
            else f"Invalid worker evidence ({type(exc).__name__})"
        )
        return retained("corrupt", detail)


def _unplanned_staging(
    committer: DeltaCommitter,
    plans: dict[str, CommitPlan],
) -> list[RepairItem]:
    root = committer.staging_manager.staging_dir
    if not committer._fs.exists(root):
        return []
    referenced = {
        file.relative_path
        for plan in plans.values()
        for table in plan.tables
        for file in table.files
    }
    found = committer._fs.glob(f"{root}/**/*.parquet")
    items: list[RepairItem] = []
    for path in sorted(found):
        relative = _relative_path(root, path, committer._fs)
        if relative not in referenced:
            items.append(
                RepairItem(
                    evidence_id=f"staging:{relative}",
                    classification="unplanned",
                    detail="Staged object is not named by any immutable plan",
                )
            )
    return items


def _abandon(
    committer: DeltaCommitter,
    report: RepairReport,
    plans: dict[str, CommitPlan],
    logical_commit_id: str,
    reason: str,
) -> None:
    matches = [item for item in report.items if item.evidence_id == logical_commit_id]
    if len(matches) != 1 or logical_commit_id not in plans:
        msg = f"Cannot abandon unresolved evidence {logical_commit_id}"
        raise StoreIntegrityError(msg)
    item = matches[0]
    controls = committer._controls()
    control = committer._control_for(plans[logical_commit_id], controls)
    if control is None:
        msg = f"Cannot abandon unplanned commit {logical_commit_id}"
        raise StoreIntegrityError(msg)
    if item.classification == "abandoned":
        if control["abandon_reason"] != reason:
            msg = f"Commit {logical_commit_id} was abandoned for a different reason"
            raise StoreIntegrityError(msg)
        _reconcile_abandonment(committer, plans[logical_commit_id], reason)
        return
    if control["state"] != "planned":
        msg = f"Cannot abandon {item.classification} commit {logical_commit_id}"
        raise StoreIntegrityError(msg)
    _preflight_abandonment(committer, plans[logical_commit_id])
    escaped_reason = reason.replace("'", "''")
    table = DeltaTable(
        uri_join(committer.delta_base_path, TablePath.LOGICAL_COMMITS),
        storage_options=committer._storage_options,
    )
    metrics = table.update(
        predicate=(f"logical_commit_id = '{logical_commit_id}' AND state = 'planned'"),
        updates={
            "state": "'abandoned'",
            "abandon_reason": f"'{escaped_reason}'",
        },
    )
    if metrics.get("num_updated_rows") != 1:
        msg = f"Abandonment was not conditional for {logical_commit_id}"
        raise StoreIntegrityError(msg)
    updated = committer._control_for(plans[logical_commit_id], committer._controls())
    if (
        updated is None
        or updated["state"] != "abandoned"
        or updated["abandon_reason"] != reason
    ):
        msg = f"Abandonment was not durable for {logical_commit_id}"
        raise StoreIntegrityError(msg)
    _reconcile_abandonment(committer, plans[logical_commit_id], reason)


def _preflight_abandonment(
    committer: DeltaCommitter,
    plan: CommitPlan,
) -> None:
    """Prove lifecycle reconciliation is possible before changing control state."""
    if plan.commit_kind != "step_result":
        return
    tracker = StepTracker(
        committer.delta_base_path,
        storage_options=committer._storage_options,
        fs=committer._fs,
    )
    current = tracker.current_state(plan.step_run_id)
    if current.status in TERMINAL_STEP_STATUSES:
        return
    if current.status != StepStatus.RUNNING:
        msg = (
            f"Cannot abandon {plan.logical_commit_id}: step attempt has not "
            "reached running"
        )
        raise StoreIntegrityError(msg)


def _reconcile_abandonment(
    committer: DeltaCommitter,
    plan: CommitPlan,
    reason: str,
) -> None:
    """Record failure without replacing an authoritative terminal outcome."""
    if plan.commit_kind != "step_result":
        return
    tracker = StepTracker(
        committer.delta_base_path,
        storage_options=committer._storage_options,
        fs=committer._fs,
    )
    current = tracker.current_state(plan.step_run_id)
    if current.status in TERMINAL_STEP_STATUSES:
        return
    message = f"Logical commit {plan.logical_commit_id} abandoned: {reason}"
    result = StepResult(
        step_name=current.step_name,
        step_number=current.step_number,
        step_run_id=current.step_run_id,
        status=StepStatus.FAILED,
        error=message,
    )
    tracker.transition(
        current.step_run_id,
        current.status,
        StepStatus.FAILED,
        result=result,
        error=message,
    )


def _relative_path(root: str, path: str, fs: AbstractFileSystem) -> str:
    stripped_root = str(fs._strip_protocol(root)).rstrip("/")
    stripped_path = str(fs._strip_protocol(path))
    return posixpath.relpath(stripped_path, stripped_root)
