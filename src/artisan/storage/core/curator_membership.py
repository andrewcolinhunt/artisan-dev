"""Accepted step exports at a curator's fixed visibility boundary."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.storage.io.retained_read import RetainedControlProof, RetainedReadSnapshot

if TYPE_CHECKING:
    from artisan.schemas.execution.curator_admission import CuratorAdmission
    from artisan.schemas.orchestration.step_state import StepState
    from artisan.storage.core.curator_view import CuratorView
    from artisan.storage.io.delta_files import DeltaSnapshot


ACCEPTED_STEP_VIEW = "curator/accepted_step_artifacts"
_SCHEMA = {
    "pipeline_run_id": pl.String,
    "step_number": pl.Int64,
    "step_name": pl.String,
    "artifact_type": pl.String,
    "artifact_id": pl.String,
}


class _FrozenMetadataRead(RetainedReadSnapshot):
    """Reuse exact terminal verification without opening newer table versions."""

    def __init__(self, view: CuratorView) -> None:
        self._view = view
        plans = {
            row["logical_commit_id"]: (
                view.committer._control_plan(row)
                if view._read is None
                else view._read.completed_control_plan(row["logical_commit_id"])
            )
            for row in view.controls.iter_rows(named=True)
        }
        super().__init__(view.committer, RetainedControlProof(view.controls, plans))

    def snapshot(self, table_path: str, *, version: int | None = None) -> DeltaSnapshot:
        captured = self._view.snapshot.table(table_path)
        if captured is None or (version is not None and version != captured.version):
            msg = "Accepted membership requires its exact captured table version"
            raise StoreIntegrityError(msg)
        return super().snapshot(table_path, version=captured.version)


def owning_pipeline_run(view: CuratorView, admission: CuratorAdmission) -> str:
    """Resolve immutable invocation ownership, never current result membership."""
    from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker

    tracker = RetainedStepTracker(view.committer, "")

    def read_owner(step_run_id: str) -> StepState:
        return (
            tracker.current_state(step_run_id)
            if view._read is None
            else tracker._read_state(step_run_id, view._read)[1]
        )

    state = read_owner(admission.record.step_run_id)
    if state.step_spec_id != admission.digest:
        msg = "Curator query owner differs from its admitted invocation"
        raise StoreIntegrityError(msg)
    seen = {state.step_run_id}
    while state.replay_of_execution_run_id is not None:
        source = (
            view._visible(TablePath.EXECUTIONS.value)
            .filter(pl.col("execution_run_id") == state.replay_of_execution_run_id)
            .select("step_run_id")
            .collect()
        )
        if source.height != 1 or source.item() in seen:
            msg = "Curator replay has no unique captured source execution ancestry"
            raise StoreIntegrityError(msg)
        state = read_owner(source.item())
        seen.add(state.step_run_id)
    return state.pipeline_run_id


def current_states(view: CuratorView, pipeline_run_id: str | None) -> list[StepState]:
    """Validate complete terminal ownership before selecting current attempts."""
    from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
    from artisan.storage.core.curator_evidence import curator_result_evidence
    from artisan.storage.core.curator_result import dynamic_output_roles

    tracker = RetainedStepTracker(view.committer, "")
    rows = view.physical(TablePath.STEPS.value).collect()
    tracker._validate_physical_rows(rows)
    tracker._validate_completed_terminals(_FrozenMetadataRead(view))
    states = []
    evidence_view = view._at(view.snapshot)
    for group in rows.partition_by("step_run_id", maintain_order=True):
        evidence = None
        if (
            dynamic_output_roles(group["compute_options_json"][0])
            and group["status"].is_in(["succeeded", "partial"]).any()
        ):
            evidence = curator_result_evidence(evidence_view, group["step_run_id"][0])
        states.append(
            tracker._validate_attempt_rows(
                group,
                result_evidence=evidence,
                controls=view.controls,
            )
        )
    return tracker._select_current_states(states, pipeline_run_id)


def accepted_step_frame(view: CuratorView) -> pl.DataFrame:
    """Include ownership and empty steps in the complete cache dependency."""
    from artisan.orchestration.engine.retained_artifact_query import _step_exports

    run_id = view._pipeline_run_id
    states = current_states(view, run_id)
    if run_id is None and states:
        run_id = states[0].pipeline_run_id
    rows: list[tuple[str | None, int | None, str | None, str | None, str | None]] = [
        (run_id, None, None, None, None)
    ]
    read = _FrozenMetadataRead(view)
    for state in states:
        rows.append((run_id, state.step_number, state.step_name, None, None))
        if state.status not in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
            continue
        rows.extend(
            (
                run_id,
                state.step_number,
                state.step_name,
                item.artifact_type,
                item.artifact_id,
            )
            for item in _step_exports(read, state)
        )
    return pl.DataFrame(rows, schema=_SCHEMA, orient="row").unique()
