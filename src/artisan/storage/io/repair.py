"""Report and deliberately repair immutable logical-commit evidence."""

from __future__ import annotations

import posixpath
from dataclasses import dataclass
from typing import Any, Literal, cast

import polars as pl
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
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import (
    TERMINAL_STEP_STATUSES,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.core.committed_scan import (
    audit_table_owners,
    filter_committed_rows,
    read_logical_commits,
    verify_plan_effect,
)
from artisan.storage.core.store_format import assert_store_format
from artisan.storage.io.commit import DeltaCommitter, PreparedCommitEvidence
from artisan.storage.io.commit_plan import (
    CommitKind,
    CommitPlan,
    prepare_commit_evidence,
    publish_commit_plan,
    read_commit_plan,
)
from artisan.storage.io.publication import is_publication_temporary
from artisan.storage.io.staging import StagingManager
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
    staged_paths = tuple(fs.find(staging_root)) if fs.exists(staging_root) else ()
    staged_relative = {_relative_path(staging_root, path, fs) for path in staged_paths}
    controls = read_logical_commits(delta_root, fs=fs, storage_options=options)
    plans, items = _read_all_plans(committer)
    items.extend(_plan_ownership_conflicts(plans))
    control_by_id = {
        row["logical_commit_id"]: row for row in controls.iter_rows(named=True)
    }
    items.extend(_inventory_items(plans, control_by_id, items))
    if abandon is not None:
        _abandon(committer, _report(items), plans, abandon, reason or "")
        items = [item for item in items if item.evidence_id != abandon]
        items.append(
            RepairItem(
                evidence_id=abandon,
                classification="abandoned",
                detail=f"Explicitly abandoned: {reason}",
            )
        )
        return _report(items + _unplanned_staging(committer, plans, staged_paths))
    audited_frames: dict[str, pl.DataFrame] = {}
    if not apply:
        items, audited_frames = _audit_store(committer, plans, controls, items)
    if apply and _report(items).blocking:
        return _report(items + _unplanned_staging(committer, plans, staged_paths))

    visible_frames = {
        path: _visible_rows(frame, path, controls)
        for path, frame in audited_frames.items()
    }
    source_rows: pl.DataFrame | None = audited_frames.get(TablePath.STEPS.value)
    for logical_id, plan in plans.items():
        if any(
            item.evidence_id == logical_id
            and item.classification in {"corrupt", "conflict"}
            for item in items
        ):
            continue
        control = control_by_id.get(logical_id)
        state = control["state"] if control is not None else "planned"
        if state == "abandoned":
            continue
        if state == "complete":
            if (
                apply
                and not preserve_staging
                and _has_staged_files(plan, staged_relative)
            ):
                try:
                    committer.commit_logical(plan)
                    _replace_item(items, _complete_item(plan, audited=True))
                except _EVIDENCE_ERRORS as exc:
                    _replace_item(items, _failure_item(logical_id, exc))
            continue
        try:
            if (
                not apply
                and source_rows is None
                and plan.commit_kind != "input_registration"
            ):
                msg = "Unreadable source attempt table during audit"
                raise StoreIntegrityError(msg)
            if plan.commit_kind == "execution_recovery" and source_rows is None:
                source_rows = _source_rows(committer)
            prepared = committer.prepare_logical(
                plan,
                source_rows=(
                    source_rows
                    if plan.commit_kind == "execution_recovery"
                    else visible_frames.get(TablePath.STEPS.value)
                ),
                committed_frames=None if apply else visible_frames,
            )
            if apply:
                committer.commit_logical(
                    plan, preserve_staging=preserve_staging, prepared=prepared
                )
                _replace_item(items, _complete_item(plan, audited=True))
                if plan.commit_kind == "step_result":
                    source_rows = None
            else:
                _check_pending_effects(
                    committer, plan, prepared, controls, audited_frames
                )
                _replace_item(
                    items,
                    RepairItem(
                        evidence_id=logical_id,
                        classification="replayable",
                        detail="Valid planned commit can be replayed exactly",
                    ),
                )
        except _EVIDENCE_ERRORS as exc:
            _replace_item(items, _failure_item(logical_id, exc))
    if _report(items).blocking:
        return _report(items + _unplanned_staging(committer, plans, staged_paths))
    if not recover_staging:
        return _report(items + _unplanned_staging(committer, plans, staged_paths))

    # Existing plans are resolved before reading source lifecycle or adopting shards.
    candidates, retained = _worker_candidates(committer, plans, staged_paths)
    items.extend(retained)
    if candidates:
        source_rows = _source_rows(committer) if source_rows is None else source_rows
        batches, candidate_items = _prepare_batches(
            committer, candidates, source_rows, None if apply else visible_frames
        )
        items.extend(candidate_items)
        if apply and not _report(items).blocking:
            for plan, prepared in batches:
                publish_commit_plan(delta_root, fs, plan)
                try:
                    committer.commit_logical(
                        plan, preserve_staging=preserve_staging, prepared=prepared
                    )
                    _replace_item(items, _complete_item(plan, audited=True))
                except _EVIDENCE_ERRORS as exc:
                    _replace_item(items, _failure_item(plan.logical_commit_id, exc))
                    break
    return _report(items)


_EVIDENCE_ERRORS = (
    StoreIntegrityError,
    ArtifactIntegrityError,
    PersistenceIntegrityError,
    ValueError,
)


def _report(items: list[RepairItem]) -> RepairReport:
    return RepairReport(
        items=tuple(
            sorted(items, key=lambda item: (item.evidence_id, item.classification))
        )
    )


def _replace_item(items: list[RepairItem], replacement: RepairItem) -> None:
    items[:] = [item for item in items if item.evidence_id != replacement.evidence_id]
    items.append(replacement)


def _failure_item(evidence_id: str, exc: Exception) -> RepairItem:
    return RepairItem(
        evidence_id=evidence_id,
        classification=_integrity_classification(exc),
        detail=str(exc),
    )


def _complete_item(plan: CommitPlan, *, audited: bool) -> RepairItem:
    return RepairItem(
        evidence_id=plan.logical_commit_id,
        classification="complete",
        detail=(
            "Control, plan, and all planned effects agree"
            if audited
            else "Completion recorded; historical effects not audited"
        ),
    )


def _source_rows(committer: DeltaCommitter) -> pl.DataFrame:
    return committer._read_physical(TablePath.STEPS.value)


def _has_staged_files(plan: CommitPlan, present: set[str]) -> bool:
    return any(
        file.relative_path in present for table in plan.tables for file in table.files
    )


def _visible_rows(
    physical: pl.DataFrame, table_path: str, controls: pl.DataFrame
) -> pl.DataFrame:
    visible = filter_committed_rows(physical, table_path, controls)
    return (
        visible.drop("logical_commit_id") if "logical_commit_id" in visible else visible
    )


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


def _inventory_items(
    plans: dict[str, CommitPlan],
    controls: dict[str, dict[str, Any]],
    findings: list[RepairItem],
) -> list[RepairItem]:
    items: list[RepairItem] = []
    unreadable = {item.evidence_id for item in findings}
    for logical_id in sorted(set(plans) | set(controls)):
        plan, control = plans.get(logical_id), controls.get(logical_id)
        if plan is None:
            if logical_id not in unreadable:
                items.append(
                    RepairItem(
                        evidence_id=logical_id,
                        classification="corrupt",
                        detail="Control row has no readable immutable plan",
                    )
                )
        elif control is not None and any(
            control[key] != getattr(plan, key)
            for key in (
                "commit_kind",
                "step_run_id",
                "plan_digest",
                "recovery_batch_id",
            )
        ):
            items.append(
                RepairItem(
                    evidence_id=logical_id,
                    classification="conflict",
                    detail="Control row disagrees with the immutable plan",
                )
            )
        elif control is not None and control["state"] == "abandoned":
            items.append(
                RepairItem(
                    evidence_id=logical_id,
                    classification="abandoned",
                    detail=f"Explicitly abandoned: {control['abandon_reason']}",
                )
            )
        elif control is not None and control["state"] == "complete":
            items.append(_complete_item(plan, audited=False))
        else:
            items.append(
                RepairItem(
                    evidence_id=logical_id,
                    classification="replayable",
                    detail="Immutable plan awaits validation and replay",
                )
            )
    return items


def _plan_ownership_conflicts(plans: dict[str, CommitPlan]) -> list[RepairItem]:
    files: dict[str, str] = {}
    executions: dict[str, str] = {}
    findings: list[RepairItem] = []
    for logical_id, plan in plans.items():
        claims = [
            (files, file.relative_path) for table in plan.tables for file in table.files
        ]
        execution_table = plan.table(TablePath.EXECUTIONS.value)
        if execution_table is not None:
            claims.extend((executions, str(key[0])) for key in execution_table.row_keys)
        for owners, key in claims:
            previous = owners.setdefault(key, logical_id)
            if previous != logical_id:
                findings.append(
                    RepairItem(
                        evidence_id=logical_id,
                        classification="conflict",
                        detail=f"Immutable plans {previous} and {logical_id} both claim {key}",
                    )
                )
    return findings


def _audit_store(
    committer: DeltaCommitter,
    plans: dict[str, CommitPlan],
    controls: pl.DataFrame,
    items: list[RepairItem],
) -> tuple[list[RepairItem], dict[str, pl.DataFrame]]:
    """Audit every registered physical table once, including undeclared effects."""
    complete = set(controls.filter(pl.col("state") == "complete")["logical_commit_id"])
    paths = {table.value for table in TablePath} | {
        definition.table_path for definition in ArtifactTypeDef.get_all().values()
    }
    failures: dict[str, Exception] = {}
    frames: dict[str, pl.DataFrame] = {}
    for path in sorted(paths - {TablePath.LOGICAL_COMMITS.value}):
        try:
            physical = committer._read_physical(path)
            frames[path] = physical
            audit_table_owners(physical, path, controls)
        except _EVIDENCE_ERRORS as exc:
            items.append(_failure_item(f"table:{path}", exc))
            for logical_id in complete & plans.keys():
                if plans[logical_id].table(path) is not None:
                    failures.setdefault(logical_id, exc)
            continue
        for logical_id in complete & plans.keys():
            try:
                verify_plan_effect(plans[logical_id], path, physical, controls)
            except _EVIDENCE_ERRORS as exc:
                failures.setdefault(logical_id, exc)
    for logical_id in complete & plans.keys():
        existing = next(item for item in items if item.evidence_id == logical_id)
        if existing.classification in {"corrupt", "conflict"}:
            continue
        replacement = (
            _failure_item(logical_id, failures[logical_id])
            if logical_id in failures
            else _complete_item(plans[logical_id], audited=True)
        )
        _replace_item(items, replacement)
    return items, frames


def _check_pending_effects(
    committer: DeltaCommitter,
    plan: CommitPlan,
    prepared: PreparedCommitEvidence,
    controls: pl.DataFrame,
    physical_frames: dict[str, pl.DataFrame],
) -> None:
    for table in plan.tables:
        physical = physical_frames.get(table.table_path)
        if physical is None:
            msg = f"Unreadable planned table {table.table_path!r}"
            raise StoreIntegrityError(msg)
        committer._missing_rows(
            plan,
            table,
            prepared.staged.frames[table.table_path],
            physical,
            controls,
        )


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
        batch_id = None
        if len(pieces) == 2 and pieces[1] in {
            "step_result.json",
            "input_registration.json",
        }:
            step_run_id, filename = pieces
            kind = cast(CommitKind, filename.removesuffix(".json"))
        elif len(pieces) == 3 and pieces[1] == "execution_recovery":
            step_run_id = pieces[0]
            kind = "execution_recovery"
            batch_id = pieces[2].removesuffix(".json")
        else:
            items.append(
                RepairItem(
                    evidence_id=f"plan:{relative}",
                    classification="corrupt",
                    detail="Unknown commit-plan path",
                )
            )
            continue
        evidence_id = f"{kind}:{batch_id or step_run_id}"
        try:
            plan = read_commit_plan(
                committer.delta_base_path,
                committer._fs,
                step_run_id,
                kind,
                batch_id,
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


def _integrity_classification(exc: Exception) -> RepairClassification:
    text = str(exc).lower()
    if "unreadable" in text or "missing" in text:
        return "corrupt"
    return "conflict"


@dataclass(frozen=True)
class _WorkerCandidate:
    directory: str
    record: dict[str, Any]


def _worker_candidates(
    committer: DeltaCommitter,
    plans: dict[str, CommitPlan],
    staged_paths: tuple[str, ...],
) -> tuple[list[_WorkerCandidate], list[RepairItem]]:
    """Discover unclaimed seals in the finite snapshot without reading payloads."""
    root = committer.staging_manager.staging_dir
    referenced = {
        file.relative_path
        for plan in plans.values()
        for table in plan.tables
        for file in table.files
    }
    claimed = {posixpath.dirname(path) for path in referenced}
    execution_owners = {
        str(key[0]): plan.logical_commit_id
        for plan in plans.values()
        if (table := plan.table(TablePath.EXECUTIONS.value)) is not None
        for key in table.row_keys
    }
    directories: dict[str, list[str]] = {}
    for path in sorted(staged_paths):
        if not is_publication_temporary(posixpath.basename(path)):
            relative = _relative_path(root, path, committer._fs)
            directories.setdefault(posixpath.dirname(relative), []).append(relative)
    candidates: list[_WorkerCandidate] = []
    items: list[RepairItem] = []
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
        evidence_id = f"staging:{directory}"
        seal = f"{directory}/executions.parquet"
        if seal not in paths:
            items.append(
                RepairItem(
                    evidence_id=evidence_id,
                    classification="incomplete",
                    detail="No published execution seal",
                )
            )
            continue
        try:
            with committer._fs.open(uri_join(root, seal), "rb") as stream:
                frame = pl.read_parquet(stream)
            if frame.height != 1:
                msg = "Worker seal requires exactly one execution"
                raise StoreIntegrityError(msg)
            record = frame.row(0, named=True)
            execution_id = record["execution_run_id"]
            if execution_id in execution_owners:
                msg = "Execution is already claimed by an immutable plan"
                raise StoreIntegrityError(msg)
            if (
                record["success"] is not True
                or record["replay_of_execution_run_id"] is not None
            ):
                items.append(
                    RepairItem(
                        evidence_id=evidence_id,
                        classification="ineligible",
                        detail="Failed or diagnostic execution retained",
                    )
                )
                continue
            expected = shard_uri(
                root,
                execution_id,
                step_number=record["origin_step_number"],
                operation_name=record["operation_name"],
            )
            if _relative_path(root, expected, committer._fs) != directory:
                msg = "Execution seal is outside its canonical shard"
                raise StoreIntegrityError(msg)
            candidates.append(_WorkerCandidate(directory, record))
        except (
            *_EVIDENCE_ERRORS,
            KeyError,
            TypeError,
            pl.exceptions.PolarsError,
        ) as exc:
            detail = (
                str(exc)
                if isinstance(exc, _EVIDENCE_ERRORS)
                else f"Invalid worker evidence ({type(exc).__name__})"
            )
            items.append(
                RepairItem(
                    evidence_id=evidence_id, classification="corrupt", detail=detail
                )
            )
    return candidates, items


def _prepare_batches(
    committer: DeltaCommitter,
    candidates: list[_WorkerCandidate],
    source_rows: pl.DataFrame,
    committed_frames: dict[str, pl.DataFrame] | None,
) -> tuple[list[tuple[CommitPlan, PreparedCommitEvidence]], list[RepairItem]]:
    visible_sources = filter_committed_rows(
        source_rows, TablePath.STEPS.value, committer._controls()
    )
    owners = {
        group["step_run_id"][0]: group.sort("state_sequence").row(-1, named=True)
        for group in visible_sources.partition_by("step_run_id")
    }
    groups: dict[str, list[_WorkerCandidate]] = {}
    items: list[RepairItem] = []
    batches: list[tuple[CommitPlan, PreparedCommitEvidence]] = []
    for candidate in candidates:
        owner = owners.get(candidate.record["step_run_id"])
        classification: RepairClassification | None = None
        detail = ""
        if owner is None:
            classification, detail = (
                "unknown_owner",
                "Execution has no known source attempt",
            )
        elif owner["replay_of_execution_run_id"] is not None:
            classification, detail = (
                "conflict",
                "Ordinary execution claims a diagnostic source",
            )
        elif owner["status"] in {"pending", "skipped"}:
            classification, detail = "ineligible", "Source attempt did not execute"
        elif owner["status"] in {"succeeded", "partial"}:
            classification, detail = (
                "conflict",
                "Unplanned execution would alter an accepted source attempt",
            )
        if classification is not None:
            items.append(
                RepairItem(
                    evidence_id=f"staging:{candidate.directory}",
                    classification=classification,
                    detail=detail,
                )
            )
        else:
            groups.setdefault(candidate.record["step_run_id"], []).append(candidate)
    for step_id, workers in sorted(groups.items()):
        first = workers[0].record
        evidence_id = f"staging:{workers[0].directory}"
        try:
            if any(
                (worker.record["origin_step_number"], worker.record["operation_name"])
                != (first["origin_step_number"], first["operation_name"])
                for worker in workers
            ):
                msg = "Recovery source batch has conflicting worker ownership"
                raise StoreIntegrityError(msg)
            staged = prepare_commit_evidence(
                staging_root=committer.staging_manager.staging_dir,
                fs=committer._fs,
                commit_kind="execution_recovery",
                step_run_id=step_id,
                step_number=first["origin_step_number"],
                operation_name=first["operation_name"],
                execution_run_ids=[
                    worker.record["execution_run_id"] for worker in workers
                ],
            )
            evidence_id = staged.plan.logical_commit_id
            prepared = committer.prepare_logical(
                staged.plan,
                staged=staged,
                source_rows=source_rows,
                committed_frames=committed_frames,
            )
            batches.append((staged.plan, prepared))
            items.append(
                RepairItem(
                    evidence_id=evidence_id,
                    classification="recoverable",
                    detail=f"Validated batch of {len(workers)} successful worker executions",
                )
            )
        except _EVIDENCE_ERRORS as exc:
            items.append(_failure_item(evidence_id, exc))
    return batches, items


def _unplanned_staging(
    committer: DeltaCommitter,
    plans: dict[str, CommitPlan],
    staged_paths: tuple[str, ...],
) -> list[RepairItem]:
    root = committer.staging_manager.staging_dir
    referenced = {
        file.relative_path
        for plan in plans.values()
        for table in plan.tables
        for file in table.files
    }
    return [
        RepairItem(
            evidence_id=f"staging:{relative}",
            classification="unplanned",
            detail="Staged object is not named by any immutable plan",
        )
        for path in sorted(staged_paths)
        if (relative := _relative_path(root, path, committer._fs)) not in referenced
        and path.endswith(".parquet")
        and not is_publication_temporary(posixpath.basename(path))
    ]


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
