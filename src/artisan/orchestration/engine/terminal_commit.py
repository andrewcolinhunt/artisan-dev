"""Shared terminal publication for running pipeline and interactive attempts."""

from __future__ import annotations

from typing import TYPE_CHECKING

from artisan.errors import PersistenceIntegrityError
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import (
    prepare_commit_evidence,
    publish_commit_plan,
)
from artisan.storage.io.staging import StagingManager

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

    from artisan.orchestration.engine.step_tracker import StepTracker
    from artisan.schemas.orchestration.step_result import StepResult


def commit_terminal_result(
    *,
    tracker: StepTracker,
    result: StepResult,
    execution_run_ids: tuple[str, ...],
    delta_root: str,
    staging_root: str,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None,
    step_spec_id: str,
    operation_name: str,
    preserve_staging: bool,
) -> StepResult:
    """Commit exact execution evidence and reread the durable terminal result.

    The caller owns policy, cancellation recording, and result sanitization.
    Existing cache-reuse staging participates even when execution IDs are empty.
    """
    step_run_id = result.step_run_id
    if step_run_id is None:
        msg = "Persistence-bearing step result has no step_run_id"
        raise PersistenceIntegrityError(msg)
    candidate = tracker.prepare_terminal_candidate(
        step_run_id,
        StepStatus.RUNNING,
        result.status,
        step_spec_id=step_spec_id,
        result=result,
    )
    staging = StagingManager(staging_root, fs)
    staging.stage_orchestrator_dataframe(
        candidate,
        TablePath.STEPS.value,
        commit_kind="step_result",
        step_run_id=step_run_id,
        step_number=result.step_number,
        operation_name=operation_name,
    )
    staged = prepare_commit_evidence(
        staging_root=staging_root,
        fs=fs,
        commit_kind="step_result",
        step_run_id=step_run_id,
        step_number=result.step_number,
        operation_name=operation_name,
        execution_run_ids=execution_run_ids,
    )
    committer = DeltaCommitter(
        delta_root,
        staging,
        fs=fs,
        storage_options=storage_options,
    )
    prepared = committer.prepare_logical(staged.plan, staged=staged)
    publish_commit_plan(delta_root, fs, staged.plan)
    committer.commit_logical(
        staged.plan,
        prepared=prepared,
        preserve_staging=preserve_staging,
    )
    return tracker.current_state(step_run_id).to_step_result()
