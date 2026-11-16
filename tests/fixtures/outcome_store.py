"""Retained, identity-valid metric populations for outcome and comparison tests."""

from __future__ import annotations

from pathlib import Path

from artisan.schemas.artifact.base import Artifact
from artisan.storage.core.retained_reader import RetainedReader
from artisan.visualization.outcomes import OutcomeSelection
from fixtures.retained_inspection import commit_inspection_attempt, inspection_store


def append_metric_attempt(
    root: Path,
    artifacts: list[Artifact],
    *,
    pipeline_run_id: str = "run",
    step_name: str = "score",
    step_number: int = 0,
    role: str = "metrics",
    declared_type: str | None = "metric",
    status: str = "succeeded",
    step_run_id: str | None = None,
    reuse_execution_ids: list[str] | None = None,
    replay_of: str | None = None,
) -> OutcomeSelection:
    """Append one finite attempt with exact accepted output membership."""
    if reuse_execution_ids is not None and len(reuse_execution_ids) != 1:
        msg = "This single-unit fixture requires one exact reuse source"
        raise ValueError(msg)
    step_run_id, _ = commit_inspection_attempt(
        root,
        run_id=pipeline_run_id,
        number=step_number,
        name=step_name,
        outputs={role: artifacts},
        output_types={role: declared_type},
        status=status,
        step_run_id=step_run_id,
        reused_execution_id=None
        if reuse_execution_ids is None
        else reuse_execution_ids[0],
        replay_of=replay_of,
    )
    return OutcomeSelection(
        pipeline_run_id=pipeline_run_id,
        step_name=step_name,
        role=role,
        step_number=step_number,
        step_run_id=step_run_id,
    )


def metric_execution(root: Path, selection: OutcomeSelection) -> str:
    """Return the fixture's real accepted UUID rather than manufacturing an ID."""
    return next(
        plan.receipt.delivery.delivery_id
        for plan in RetainedReader(inspection_store(root)).completed_plans()
        if plan.receipt.delivery.attempt.assignment.step_run_id == selection.step_run_id
    )
