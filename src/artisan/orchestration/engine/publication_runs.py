"""Portable run inspection using verified lifecycle and original source selections."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import polars as pl
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.retained_execution_query import (
    ExecutionMembership,
    _step_members,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.orchestration.run_status import RunStatus, _iso, _step_status
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import (
    TERMINAL_STEP_STATUSES,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_state import StepState
from artisan.schemas.provenance.projection import ProjectedOccurrence
from artisan.storage._publication_filesystem import _PublicationFileSystem
from artisan.storage._publication_policy import PublicationPolicy
from artisan.storage._publication_runs import (
    InspectedCuratorSelection,
    InspectedCurrentUnits,
    InspectedFailedResult,
    InspectedRedaction,
    InspectedRuns,
    InspectedSelection,
    InspectedStep,
    InspectedUnit,
    InspectedWholeReuse,
)
from artisan.storage._retained_publication import (
    PublicationExport,
    RetainedInspectionManifest,
    _finish_inspection,
    _inspection_digest,
    _owned_destination,
    _write_inspection,
)
from artisan.storage.core.artifact_query import ArtifactRef, _parse_metadata
from artisan.storage.core.curator_view import capture_curator_view
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    ExecutionSource,
    FailedScientificExecutionSource,
    _read_execution_source,
)
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.core.table_schemas import STEPS_SCHEMA
from artisan.storage.io.curator_control import CandidatePlan
from artisan.storage.io.curator_plan import CuratorCommitPlan
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan
from artisan.storage.io.repair_report import RepairItem
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_repair import _assignment_items, _plans
from artisan.storage.io.step_result_plan import (
    CurrentStepUnits,
    StepResultPlan,
    StepUnitResolution,
    WholeStepReuse,
)
from artisan.storage.io.table_payload import capture_table_payload, stage_table_payload
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


@dataclass(frozen=True)
class _CapturedRuns:
    rows: pl.DataFrame
    selections: tuple[InspectedStep, ...]
    failed_sources: tuple[InspectedFailedResult, ...]
    redactions: tuple[InspectedRedaction, ...]
    fingerprint: str


def export_retained_runs(
    reader: RetainedReader, destination: Path, *, redact: tuple[str, ...] = ()
) -> PublicationExport:
    """Copy a quiescent source with historical run selections, never control authority."""
    policy = PublicationPolicy.create(redact)
    captured = _capture_runs(reader.committer, policy)
    with _owned_destination(reader, destination) as target:
        manifest = _write_inspection(reader, target, policy)
        with TemporaryDirectory(prefix="artisan-publication-runs-") as scratch:
            lifecycle = stage_table_payload(
                TablePath.STEPS.value,
                "run-lifecycle.parquet",
                captured.rows.iter_rows(named=True),
                STEPS_SCHEMA,
                str(target),
                Path(scratch),
                LocalFileSystem(),
            )
        runs = InspectedRuns(
            lifecycle=lifecycle,
            selections=captured.selections,
            failed_sources=captured.failed_sources,
            redactions=captured.redactions,
        )
        fresh = _capture_runs(reader.committer, policy)
        if fresh.fingerprint != captured.fingerprint:
            msg = "Source changed during run inspection export"
            raise StoreIntegrityError(msg)
        payload = manifest.model_dump(mode="json", exclude={"inspection_digest"})
        payload["runs"] = runs.model_dump(mode="json")
        manifest = RetainedInspectionManifest.model_validate(
            {**payload, "inspection_digest": _inspection_digest(payload)}
        )
        publication = _finish_inspection(target, manifest)
        _read_runs(publication)
        return publication


def _capture_runs(
    committer: RetainedCommitter, policy: PublicationPolicy
) -> _CapturedRuns:
    with committer._read_snapshot() as read:
        tracker = RetainedStepTracker(committer, "")
        rows, attempts = tracker._read_lifecycle(read)
        if read.controls.filter(pl.col("state") != "complete").height or any(
            state.status not in TERMINAL_STEP_STATUSES
            or state.cancellation_status
            in {CancellationStatus.REQUESTED, CancellationStatus.UNKNOWN}
            for state in attempts
        ):
            msg = "Run publication requires all owners and attempts quiescent"
            raise StoreIntegrityError(msg)
        issues: list[RepairItem] = []
        plans = _plans(committer, read, issues)
        if issues or any(
            read.completed_control_plan(owner) != plan for owner, plan in plans.items()
        ):
            msg = "Run publication has incomplete selected evidence"
            raise StoreIntegrityError(msg)
        assignments = committer.journal.all_states()
        resolved = {
            reference.attempt.assignment.assignment_id
            for plan in plans.values()
            if isinstance(plan, StepResultPlan)
            for reference in plan.decision_admissions
        }
        if tuple(_assignment_items(assignments, plans, resolved)):
            msg = "Run publication has unresolved assignment ownership"
            raise StoreIntegrityError(msg)
        sources: dict[str, ExecutionSource] = {}

        def resolve(identity: str) -> ExecutionSource:
            if identity not in sources:
                source = _read_execution_source(read, identity)
                if source is None:
                    msg = "Inspection selection has no original execution"
                    raise StoreIntegrityError(msg)
                sources[identity] = source
            return sources[identity]

        selections = []
        redactions: list[InspectedRedaction] = []
        owners = set(read.controls["logical_commit_id"])
        for state in sorted(attempts, key=lambda item: item.step_run_id):
            owner = f"step_result:{state.step_run_id}"
            if owner not in owners:
                continue
            plan = read.completed_control_plan(owner)
            # The completed terminal remains proven by _read_lifecycle; an
            # infrastructure-only curator failure has no execution to export.
            if (
                isinstance(plan, CuratorCommitPlan)
                and plan.table(TablePath.EXECUTIONS.value) is None
            ):
                continue
            members = tuple(_step_members(read, state, resolve))
            selections.append(_selection(plan, state, members, policy, redactions))
        failed = tuple(
            _failed_source(source)
            for _, source in sorted(sources.items())
            if isinstance(source, FailedScientificExecutionSource)
        )
        rows = (
            rows.select(list(STEPS_SCHEMA))
            .unique()
            .sort("step_run_id", "state_sequence")
        )
        view = capture_curator_view(committer, _read=read)
        fingerprint = compute_content_digest(
            canonical_json_bytes(
                {
                    "view": view.model_dump(mode="json"),
                    "rows": rows.with_columns(
                        pl.col("timestamp").cast(pl.Int64)
                    ).to_dicts(),
                    "selections": [item.model_dump(mode="json") for item in selections],
                    "failed_sources": [item.model_dump(mode="json") for item in failed],
                    "assignments": [
                        (
                            state.attempt.assignment.assignment_id,
                            state.sequence,
                            state.admission_digest,
                        )
                        for state in assignments
                    ],
                }
            )
        )
        sanitized = []
        for row in rows.iter_rows(named=True):
            copied = policy.row(TablePath.STEPS.value, row, {})
            for field in ("error", "input_refs_json"):
                before, after = row[field], copied[field]
                if field == "input_refs_json":
                    before, after = json.loads(before), json.loads(after)
                if before != after:
                    redactions.append(
                        InspectedRedaction(
                            step_run_id=row["step_run_id"],
                            location="lifecycle",
                            index=row["state_sequence"],
                            field=field,
                        )
                    )
            sanitized.append(copied)
        for source in failed:
            policy.protected(
                source.model_dump(mode="json"), "failed execution assertions"
            )
        return _CapturedRuns(
            pl.DataFrame(sanitized, schema=STEPS_SCHEMA),
            tuple(selections),
            failed,
            tuple(
                sorted(
                    redactions,
                    key=lambda item: (
                        item.step_run_id,
                        item.location,
                        item.index,
                        item.field,
                    ),
                )
            ),
            fingerprint,
        )


def _failed_source(source: FailedScientificExecutionSource) -> InspectedFailedResult:
    assert source.unit.failed_delivery is not None
    return InspectedFailedResult(
        delivery=source.unit.failed_delivery,
        step_run_id=source.plan.step_run_id,
        plan_digest=source.plan.plan_digest,
        admission_digest=source.unit.admission.record_digest,
        recipe=source.admission.recipe,
    )


def _diagnostic(
    value: str | None,
    policy: PublicationPolicy,
    redactions: list[InspectedRedaction],
    *,
    step_run_id: str,
    location: Literal["result", "unit", "selection"],
    index: int = 0,
    field: Literal["error", "infrastructure_error"] = "error",
) -> str | None:
    copied = policy.text(value) if value is not None else None
    if copied != value:
        redactions.append(
            InspectedRedaction(
                step_run_id=step_run_id,
                location=location,
                index=index,
                field=field,
            )
        )
    return copied


def _selection(
    plan: CandidatePlan,
    state: StepState,
    members: tuple[ExecutionMembership, ...],
    policy: PublicationPolicy,
    redactions: list[InspectedRedaction],
) -> InspectedStep:
    selection: InspectedSelection
    result = state.to_step_result().model_dump(mode="json")
    result["error"] = _diagnostic(
        state.error,
        policy,
        redactions,
        step_run_id=state.step_run_id,
        location="result",
    )
    if isinstance(plan, StepResultPlan):
        if isinstance(plan.selection, WholeStepReuse):
            selection = InspectedWholeReuse(
                source_step_run_id=plan.selection.source_step_run_id,
                source_plan_digest=plan.selection.source_plan_digest,
                cache_policy=plan.selection.cache_policy,
            )
        else:
            assert isinstance(plan.selection, CurrentStepUnits)
            selection = InspectedCurrentUnits(
                units=tuple(
                    InspectedUnit(
                        unit_index=index,
                        execution_run_id=_unit_execution_id(unit),
                        reused=unit.reused,
                        error=_diagnostic(
                            unit.error,
                            policy,
                            redactions,
                            step_run_id=state.step_run_id,
                            location="unit",
                            index=index,
                        ),
                    )
                    for index, unit in enumerate(plan.selection.units)
                ),
                failure_policy=plan.selection.failure_policy,
                infrastructure_error=_diagnostic(
                    plan.selection.infrastructure_error,
                    policy,
                    redactions,
                    step_run_id=state.step_run_id,
                    location="selection",
                    field="infrastructure_error",
                ),
            )
    else:
        if not isinstance(plan, (CuratorCommitPlan, CuratorReusePlan)):
            msg = "Run selection has an incompatible source plan"
            raise StoreIntegrityError(msg)
        if len(members) != 1 or not isinstance(
            members[0].source, CuratorExecutionSource
        ):
            msg = "Curator inspection lacks one original execution"
            raise StoreIntegrityError(msg)
        source = members[0].source
        selection = InspectedCuratorSelection(
            source_step_run_id=source.plan.step_run_id,
            source_plan_digest=source.plan.plan_digest,
            execution_run_id=str(source.execution_record["execution_run_id"]),
        )
    selected = InspectedStep(
        step_run_id=state.step_run_id,
        plan_digest=plan.plan_digest,
        result=StepResult.model_validate(result),
        selection=selection,
    )
    policy.protected(selected.model_dump(mode="json"), "run selection assertions")
    return selected


def _unit_execution_id(unit: StepUnitResolution) -> str | None:
    if unit.receipt is not None:
        return unit.receipt.delivery.delivery_id
    if unit.failed_delivery is not None:
        return unit.failed_delivery.delivery_id
    return None


@dataclass(frozen=True)
class _ReadRuns:
    manifest: RetainedInspectionManifest
    rows: pl.DataFrame
    attempts: list[StepState]
    index: pl.DataFrame


def _read_runs(publication: PublicationExport) -> _ReadRuns:
    manifest = publication.metadata()
    runs = manifest.runs
    if runs is None:
        msg = "Inspection bundle has no run inventory"
        raise ValueError(msg)
    with TemporaryDirectory(prefix="artisan-publication-runs-") as scratch:
        captured = capture_table_payload(
            runs.lifecycle,
            directory=str(publication.directory),
            scratch=Path(scratch),
            expected_schema=STEPS_SCHEMA,
            fs=_PublicationFileSystem(publication.directory),
        )
        rows = pl.read_parquet(captured.path)
    attempts = _validate_run_rows(manifest, rows)
    return _ReadRuns(manifest, rows, attempts, publication._artifact_index(manifest))


def _validate_run_rows(
    manifest: RetainedInspectionManifest, rows: pl.DataFrame
) -> list[StepState]:
    """Validate one already captured inspection lifecycle without reopening files."""
    runs = manifest.runs
    if runs is None:
        msg = "Inspection bundle has no run inventory"
        raise ValueError(msg)
    if not rows.equals(rows.unique().sort("step_run_id", "state_sequence")):
        msg = "Inspection lifecycle must be unique and canonically ordered"
        raise StoreIntegrityError(msg)
    attempts = []
    for group in rows.partition_by("step_run_id", maintain_order=True):
        if group["state_sequence"].to_list() != list(range(group.height)):
            msg = "Inspection lifecycle has a sequence gap"
            raise StoreIntegrityError(msg)
        snapshots = group.to_dicts()
        StepTracker._validate_history(snapshots)
        states = [StepTracker._validated_snapshot(row) for row in snapshots]
        if (
            states[-1].status not in TERMINAL_STEP_STATUSES
            or states[-1].cancellation_status == CancellationStatus.UNKNOWN
        ):
            msg = "Inspection lifecycle is not quiescent"
            raise StoreIntegrityError(msg)
        attempts.append(states[-1])
    _validate_terminals(runs, attempts)
    return attempts


def _validate_terminals(runs: InspectedRuns, attempts: list[StepState]) -> None:
    states = {state.step_run_id: state for state in attempts}
    selected = {item.step_run_id: item for item in runs.selections}
    if selected.keys() - states.keys():
        msg = "Inspection selection has no lifecycle attempt"
        raise StoreIntegrityError(msg)
    for owner, state in states.items():
        step = selected.get(owner)
        if step is not None and step.result != state.to_step_result():
            msg = "Inspection terminal differs from its selection"
            raise StoreIntegrityError(msg)
        if step is None and state.status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
            msg = "Successful inspection attempt lacks a selection"
            raise StoreIntegrityError(msg)
    for marker in runs.redactions:
        if marker.step_run_id not in states:
            msg = "Redaction observation names an unknown attempt"
            raise StoreIntegrityError(msg)
        if marker.location == "lifecycle":
            if (
                marker.field not in {"error", "input_refs_json"}
                or marker.index > states[marker.step_run_id].state_sequence
            ):
                msg = "Invalid lifecycle redaction observation"
                raise StoreIntegrityError(msg)
        elif marker.step_run_id not in selected:
            msg = "Redaction observation has no selection"
            raise StoreIntegrityError(msg)
        else:
            selection = selected[marker.step_run_id].selection
            valid = (
                marker.location == "result"
                and marker.field == "error"
                and marker.index == 0
            ) or (
                isinstance(selection, InspectedCurrentUnits)
                and (
                    (
                        marker.location == "unit"
                        and marker.field == "error"
                        and marker.index < len(selection.units)
                    )
                    or (
                        marker.location == "selection"
                        and marker.field == "infrastructure_error"
                        and marker.index == 0
                    )
                )
            )
            if not valid:
                msg = "Invalid selection redaction observation"
                raise StoreIntegrityError(msg)


def list_publication_runs(publication: PublicationExport) -> pl.DataFrame:
    """Read the existing history shape without live controls or artifact hydration."""
    read = _read_runs(publication)
    return StepTracker._summarize_runs(read.rows, read.attempts)


def publication_run_status(
    publication: PublicationExport, pipeline_run_id: str
) -> RunStatus:
    """Read current names, counts and timing from one verified bundle snapshot."""
    if not pipeline_run_id:
        msg = "Run inspection requires an explicit pipeline run"
        raise ValueError(msg)
    read = _read_runs(publication)
    states = StepTracker._select_current_states(read.attempts, pipeline_run_id)
    if not states:
        return RunStatus(
            pipeline_run_id=pipeline_run_id,
            last_status=None,
            step_count=0,
            started_at=None,
            ended_at=None,
            steps=[],
        )
    counts: dict[int, Counter[str]] = {}
    for state in states:
        unique = {
            (item.artifact_id, item.artifact_type)
            for item in _exports(read.manifest, state)
        }
        counts[state.step_number] = Counter(kind for _, kind in unique)
    row = (
        StepTracker._summarize_runs(read.rows, read.attempts)
        .filter(pl.col("pipeline_run_id") == pipeline_run_id)
        .row(0, named=True)
    )
    return RunStatus(
        pipeline_run_id=pipeline_run_id,
        last_status=row["last_status"],
        step_count=row["step_count"],
        started_at=_iso(row["started_at"]),
        ended_at=_iso(row["ended_at"]),
        steps=[
            _step_status(state, counts.get(state.step_number, {})) for state in states
        ],
    )


def _exports(
    manifest: RetainedInspectionManifest, state: StepState
) -> tuple[ProjectedOccurrence, ...]:
    if state.status not in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
        return ()
    assert manifest.runs is not None
    steps = {item.step_run_id: item for item in manifest.runs.selections}
    selected = steps[state.step_run_id].selection
    if isinstance(selected, InspectedWholeReuse):
        selected = steps[selected.source_step_run_id].selection
    if isinstance(selected, InspectedCuratorSelection):
        source = next(
            item
            for item in manifest.curator_sources
            if item.execution_run_id == selected.execution_run_id
        )
        inventory = {
            item.occurrence.address: item.occurrence
            for item in source.projection.occurrences
        }
        return tuple(
            inventory[address]
            for group in source.projection.outputs
            for address in group.occurrences
        )
    assert isinstance(selected, InspectedCurrentUnits)
    sources = {item.delivery.delivery_id: item for item in manifest.sources}
    exported: list[ProjectedOccurrence] = []
    for unit in selected.units:
        if unit.error is None:
            assert unit.execution_run_id is not None
            scientific = sources[unit.execution_run_id]
            inventory = {
                item.address: item for item in scientific.projection.occurrences
            }
            exported.extend(
                inventory[address]
                for group in scientific.projection.exports
                for address in group.occurrences
            )
    return tuple(exported)


def query_publication_artifacts(
    publication: PublicationExport, *, pipeline_run_id: str
) -> list[ArtifactRef]:
    """Read metadata-only current-step references, deduplicated within each step."""
    if not pipeline_run_id:
        msg = "Artifact inspection requires an explicit pipeline run"
        raise ValueError(msg)
    read = _read_runs(publication)
    states = StepTracker._select_current_states(read.attempts, pipeline_run_id)
    selected = {
        (state.step_number, item.artifact_id): item.artifact_type
        for state in states
        for item in _exports(read.manifest, state)
    }
    index = {row["artifact_id"]: row for row in read.index.iter_rows(named=True)}
    return [
        ArtifactRef(
            artifact_id=key,
            artifact_type=kind,
            origin_step_number=index[key]["origin_step_number"],
            current_step_number=number,
            pipeline_run_id=pipeline_run_id,
            metadata=_parse_metadata(index[key]["metadata"]),
        )
        for (number, key), kind in sorted(selected.items())
    ]
