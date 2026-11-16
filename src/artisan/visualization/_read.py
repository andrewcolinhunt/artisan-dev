"""Call-owned metadata selection shared by inspection and graph readers."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.retained_artifact_query import _references_for_states
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_read import RetainedReadSnapshot


@contextmanager
def read_view(
    committer: RetainedCommitter,
) -> Iterator[tuple[RetainedReadSnapshot, CuratorView]]:
    """Bind table visibility and finite source proofs to the same fresh read."""
    with committer._read_snapshot() as snapshot:
        yield (
            snapshot,
            CuratorView(
                committer,
                capture_curator_view(committer, _read=snapshot),
                _read=snapshot,
            ),
        )


def select_attempts(
    attempts: list[StepState],
    *,
    pipeline_run_id: str | None,
    step_run_id: str | None = None,
    step_number: int | None = None,
) -> list[StepState]:
    """Select exact history or current attempts, validating explicit coordinates."""
    if step_run_id is None:
        selected = RetainedStepTracker._select_current_states(attempts, pipeline_run_id)
    else:
        selected = [state for state in attempts if state.step_run_id == step_run_id]
        if not selected:
            msg = f"Unknown step attempt {step_run_id!r}"
            raise ValueError(msg)
        state = selected[0]
        if pipeline_run_id is not None and state.pipeline_run_id != pipeline_run_id:
            msg = "Step attempt does not belong to the selected run"
            raise ValueError(msg)
        if step_number is not None and state.step_number != step_number:
            msg = "Step attempt does not match the selected step number"
            raise ValueError(msg)
    return [
        state
        for state in selected
        if step_number is None or state.step_number == step_number
    ]


def registered_rows(
    snapshot: RetainedReadSnapshot,
    table: str,
    artifact_ids: set[str],
    *,
    columns: list[str] | None = None,
) -> pl.DataFrame:
    """Verify selected physical rows without hydrating artifacts or carriers."""
    physical = snapshot.snapshot(table)
    if not artifact_ids:
        return pl.DataFrame(schema=physical.schema).drop("logical_commit_id")
    selected_columns = (
        list(dict.fromkeys(["artifact_id", "logical_commit_id", *columns]))
        if columns is not None
        else None
    )
    frames = [
        frame.filter(pl.col("artifact_id").is_in(artifact_ids))
        for frame in physical.iter_frames(columns=selected_columns)
    ]
    rows = pl.concat(frames) if frames else pl.DataFrame(schema=physical.schema)
    if rows.height != len(artifact_ids) or set(rows["artifact_id"]) != artifact_ids:
        msg = "Selected artifacts have missing or duplicate registered rows"
        raise StoreIntegrityError(msg)
    for owner in rows["logical_commit_id"].unique():
        snapshot.verify_effect(snapshot.completed_control_plan(owner), table)
    return rows.drop("logical_commit_id")


def artifact_membership(
    snapshot: RetainedReadSnapshot,
    view: CuratorView,
    *,
    pipeline_run_id: str | None = None,
    step_run_id: str | None = None,
    step_number: int | None = None,
    artifact_type: str | None = None,
) -> pl.DataFrame:
    """Select output metadata or global protected origins without content reads."""
    if pipeline_run_id is None and step_run_id is None:
        index = view.scan(TablePath.ARTIFACT_INDEX)
        if step_number is not None:
            index = index.filter(pl.col("origin_step_number") == step_number)
        if artifact_type is not None:
            index = index.filter(pl.col("artifact_type") == artifact_type)
        ids = set(index.select("artifact_id").collect()["artifact_id"])
        rows = registered_rows(snapshot, TablePath.ARTIFACT_INDEX.value, ids)
        return rows.select(
            "artifact_id",
            "artifact_type",
            "origin_step_number",
            pl.col("origin_step_number").alias("current_step_number"),
        )
    tracker = RetainedStepTracker(snapshot.committer, "")
    _, attempts = tracker._read_lifecycle(snapshot)
    states = select_attempts(
        attempts,
        pipeline_run_id=pipeline_run_id,
        step_run_id=step_run_id,
        step_number=step_number,
    )
    references = _references_for_states(
        snapshot,
        states,
        pipeline_run_id=pipeline_run_id
        or (states[0].pipeline_run_id if states else ""),
        artifact_type=artifact_type,
    )
    return pl.DataFrame(
        [
            reference.model_dump(
                include={
                    "artifact_id",
                    "artifact_type",
                    "origin_step_number",
                    "current_step_number",
                }
            )
            for reference in references
        ],
        schema={
            "artifact_id": pl.String,
            "artifact_type": pl.String,
            "origin_step_number": pl.Int64,
            "current_step_number": pl.Int64,
        },
    )
