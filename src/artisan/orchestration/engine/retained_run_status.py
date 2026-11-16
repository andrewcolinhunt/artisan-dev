"""Read current retained lifecycle and names without payload hydration or recovery."""

from __future__ import annotations

from collections import Counter

import polars as pl

from artisan.orchestration.engine.retained_artifact_query import (
    _references_for_states,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.run_status import RunStatus, _iso, _step_status
from artisan.storage.io.retained_commit import RetainedCommitter


def retained_run_status(
    committer: RetainedCommitter, pipeline_run_id: str
) -> RunStatus:
    """Preserve current status, timing and summaries using metadata-only counts.

    The existing lifecycle reader owns rollup and attempt selection. Pending and
    running attempts remain visible, including unresolved cancellation. Inspection
    neither recovers a selected plan nor establishes artifact availability.
    """
    _require_run(pipeline_run_id)
    with committer._read_snapshot() as snapshot:
        tracker = RetainedStepTracker(committer, pipeline_run_id)
        lifecycle, attempts = tracker._read_lifecycle(snapshot)
        states = tracker._select_current_states(attempts, pipeline_run_id)
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
        for reference in _references_for_states(
            snapshot, states, pipeline_run_id=pipeline_run_id
        ):
            assert reference.current_step_number is not None
            counts.setdefault(reference.current_step_number, Counter())[
                reference.artifact_type
            ] += 1
        rollup = tracker._summarize_runs(lifecycle, attempts).filter(
            pl.col("pipeline_run_id") == pipeline_run_id
        )
        row = rollup.row(0, named=True)
        return RunStatus(
            pipeline_run_id=pipeline_run_id,
            last_status=row["last_status"],
            step_count=int(row["step_count"]),
            started_at=_iso(row["started_at"]),
            ended_at=_iso(row["ended_at"]),
            steps=[
                _step_status(state, counts.get(state.step_number, {}))
                for state in states
            ],
        )


def resolve_retained_step_number(
    committer: RetainedCommitter, pipeline_run_id: str, step_name: str
) -> int | None:
    """Resolve the first current step with this name in ascending logical order."""
    _require_run(pipeline_run_id)
    with committer._read_snapshot() as snapshot:
        tracker = RetainedStepTracker(committer, pipeline_run_id)
        _, attempts = tracker._read_lifecycle(snapshot)
        states = tracker._select_current_states(attempts, pipeline_run_id)
        return next(
            (state.step_number for state in states if state.step_name == step_name),
            None,
        )


def list_retained_runs(committer: RetainedCommitter) -> pl.DataFrame:
    """Roll up retained runs from one validated lifecycle snapshot.

    Preserve the existing history columns, timestamps and newest-started ordering.
    Reading never hydrates artifacts, recovers selections or infers worker closure.
    """
    with committer._read_snapshot() as snapshot:
        tracker = RetainedStepTracker(committer, "")
        rows, attempts = tracker._read_lifecycle(snapshot)
        return tracker._summarize_runs(rows, attempts)


def _require_run(pipeline_run_id: str) -> None:
    if not pipeline_run_id:
        msg = "Run inspection requires an explicit pipeline run"
        raise ValueError(msg)
