"""Current-run artifact references from completed retained metadata, not payloads."""

from __future__ import annotations

from collections.abc import Iterator

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_state import StepState
from artisan.schemas.provenance.projection import ProjectedOccurrence
from artisan.storage.core.artifact_query import ArtifactRef, _parse_metadata
from artisan.storage.core.committed_scan import verify_snapshot_effect
from artisan.storage.io.curator_plan import (
    CuratorCommitPlan,
)
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_metadata import (
    read_curator_source_metadata,
    read_terminal_metadata,
)
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.step_result_plan import StepResultPlan, WholeStepReuse


def query_retained_artifacts(
    committer: RetainedCommitter,
    *,
    pipeline_run_id: str,
    artifact_type: str | None = None,
) -> list[ArtifactRef]:
    """List latest-step exported content using exact index and membership metadata.

    Repeated exports collapse only within each current step. Protected origins
    remain unchanged across reuse. This inspection does not establish payload
    availability or cache eligibility, and never repairs an incomplete result.
    """
    if not pipeline_run_id:
        msg = "Artifact inspection requires an explicit pipeline run"
        raise ValueError(msg)
    with committer._read_snapshot() as snapshot:
        tracker = RetainedStepTracker(committer, pipeline_run_id)
        _, attempts = tracker._read_lifecycle(snapshot)
        states = tracker._select_current_states(attempts, pipeline_run_id)
        return _references_for_states(
            snapshot,
            states,
            pipeline_run_id=pipeline_run_id,
            artifact_type=artifact_type,
        )


def _references_for_states(
    snapshot: RetainedReadSnapshot,
    states: list[StepState],
    *,
    pipeline_run_id: str,
    artifact_type: str | None = None,
) -> list[ArtifactRef]:
    """Use a caller's captured lifecycle selection with this call's metadata proof."""
    selected: dict[tuple[int, str], str] = {}
    for state in states:
        if state.status not in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
            continue
        for item in _step_exports(snapshot, state):
            if artifact_type is None or item.artifact_type == artifact_type:
                key = state.step_number, item.artifact_id
                if key in selected and selected[key] != item.artifact_type:
                    msg = "Exported content has conflicting artifact types"
                    raise StoreIntegrityError(msg)
                selected[key] = item.artifact_type
    return _references(snapshot, selected, pipeline_run_id)


def _step_exports(
    snapshot: RetainedReadSnapshot, state: StepState, *, role: str | None = None
) -> Iterator[ProjectedOccurrence]:
    plan = snapshot.completed_control_plan(f"step_result:{state.step_run_id}")
    if not isinstance(plan, (StepResultPlan, CuratorCommitPlan, CuratorReusePlan)):
        msg = "Current step has no matching terminal selection plan"
        raise StoreIntegrityError(msg)
    terminal = read_terminal_metadata(snapshot, plan)
    if terminal != state.to_step_result():
        msg = "Current step differs from its exact terminal selection"
        raise StoreIntegrityError(msg)
    if isinstance(plan, StepResultPlan):
        source = plan
        if isinstance(plan.selection, WholeStepReuse):
            selected_source = snapshot.completed_control_plan(
                f"step_result:{plan.selection.source_step_run_id}"
            )
            if not isinstance(selected_source, StepResultPlan):
                msg = "Whole-step selection has no scientific source"
                raise StoreIntegrityError(msg)
            source = selected_source
            read_terminal_metadata(snapshot, source)
        for receipt in source.selected_receipts:
            projection = snapshot.projection_metadata(receipt)
            occurrences = {item.address: item for item in projection.occurrences}
            for group in projection.exports:
                if role is None or group.role == role:
                    yield from (occurrences[address] for address in group.occurrences)
        return
    curator, _ = read_curator_source_metadata(snapshot, plan, terminal)
    occurrences = {
        item.occurrence.address: item.occurrence
        for item in curator.projection.occurrences
    }
    for group in curator.projection.outputs:
        if role is None or group.role == role:
            yield from (occurrences[address] for address in group.occurrences)


def _references(
    snapshot: RetainedReadSnapshot,
    selected: dict[tuple[int, str], str],
    pipeline_run_id: str,
) -> list[ArtifactRef]:
    if not selected:
        return []
    ids = {artifact_id for _, artifact_id in selected}
    path = TablePath.ARTIFACT_INDEX.value
    physical = snapshot.snapshot(path)
    frames = [
        frame.filter(pl.col("artifact_id").is_in(ids))
        for frame in physical.iter_frames()
    ]
    rows = pl.concat(frames) if frames else pl.DataFrame(schema=physical.schema)
    if set(rows["artifact_id"]) != ids or rows.height != len(ids):
        msg = "Exported content has missing or duplicate artifact index metadata"
        raise StoreIntegrityError(msg)
    for owner in rows["logical_commit_id"].unique():
        plan = snapshot.completed_control_plan(owner)
        verify_snapshot_effect(plan, path, physical, snapshot.controls)
    index = {row["artifact_id"]: row for row in rows.iter_rows(named=True)}
    references = []
    for (step_number, artifact_id), kind in sorted(selected.items()):
        row = index[artifact_id]
        if row["artifact_type"] != kind:
            msg = "Exported occurrence type differs from registered index metadata"
            raise StoreIntegrityError(msg)
        references.append(
            ArtifactRef(
                artifact_id=artifact_id,
                artifact_type=kind,
                origin_step_number=row["origin_step_number"],
                current_step_number=step_number,
                pipeline_run_id=pipeline_run_id,
                metadata=_parse_metadata(row["metadata"]),
            )
        )
    return references
