"""Report and deliberately repair immutable logical-commit evidence."""

from __future__ import annotations

import posixpath
from typing import Literal

from deltalake import DeltaTable
from fsspec import AbstractFileSystem
from pydantic import BaseModel, ConfigDict

from artisan.errors import IncompatibleStoreError, StoreIntegrityError
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.core.committed_scan import read_logical_commits
from artisan.storage.core.store_format import assert_store_format
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import (
    CommitPlan,
    read_commit_plan,
    verify_plan_files,
)
from artisan.storage.io.staging import StagingManager
from artisan.utils.path import uri_join

RepairClassification = Literal[
    "complete",
    "replayable",
    "conflict",
    "corrupt",
    "unplanned",
    "legacy_unverifiable",
    "abandoned",
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
) -> RepairReport:
    """Report by default, replay validated plans, or abandon one planned commit."""
    _validate_action(apply=apply, abandon=abandon, reason=reason)
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
    )
    report, plans = _build_report(committer)
    if abandon is not None:
        _abandon(committer, report, plans, abandon, reason or "")
        return _build_report(committer)[0]
    if apply:
        actionable = {
            item.evidence_id
            for item in report.items
            if item.classification in {"complete", "replayable"}
        }
        for logical_commit_id in sorted(actionable):
            committer.commit_logical(plans[logical_commit_id])
        return _build_report(committer)[0]
    return report


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
            items.append(
                RepairItem(
                    evidence_id=logical_commit_id,
                    classification="unplanned",
                    detail="Immutable plan exists without a planned control row",
                )
            )
        else:
            items.append(_classify_controlled(committer, plan, control))
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
    for path in sorted(committer._fs.glob(f"{root}/*/*.json")):
        kind_text = posixpath.basename(path).removesuffix(".json")
        step_run_id = posixpath.basename(posixpath.dirname(path))
        evidence_id = f"{kind_text}:{step_run_id}"
        if kind_text not in {"step_result", "input_registration"}:
            items.append(
                RepairItem(
                    evidence_id=evidence_id,
                    classification="corrupt",
                    detail="Unknown commit-plan kind",
                )
            )
            continue
        try:
            plan = read_commit_plan(
                committer.delta_base_path,
                committer._fs,
                step_run_id,
                kind_text,
            )
        except StoreIntegrityError as exc:
            items.append(
                RepairItem(
                    evidence_id=evidence_id,
                    classification="corrupt",
                    detail=str(exc),
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
        controls = committer._controls()
        for table in plan.tables:
            committer._missing_rows(
                plan,
                table,
                frames[table.table_path],
                committer._read_physical(table.table_path),
                controls,
            )
    except StoreIntegrityError as exc:
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


def _integrity_classification(exc: StoreIntegrityError) -> RepairClassification:
    text = str(exc).lower()
    if "unreadable" in text or "missing" in text:
        return "corrupt"
    return "conflict"


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
    if item.classification != "replayable":
        msg = f"Cannot abandon {item.classification} commit {logical_commit_id}"
        raise StoreIntegrityError(msg)
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


def _reconcile_abandonment(
    committer: DeltaCommitter,
    plan: CommitPlan,
    reason: str,
) -> None:
    """Append one guarded D4 failure without replacing a terminal winner."""
    if plan.commit_kind != "step_result":
        return
    tracker = StepTracker(
        committer.delta_base_path,
        storage_options=committer._storage_options,
        fs=committer._fs,
    )
    current = tracker.current_state(plan.step_run_id)
    if current.status in {
        StepStatus.SUCCEEDED,
        StepStatus.PARTIAL,
        StepStatus.FAILED,
        StepStatus.CANCELLED,
        StepStatus.SKIPPED,
    }:
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
