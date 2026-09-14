"""Cache, resume, and rollup tests for StepTracker."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from artisan.errors import PersistenceIntegrityError
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.enums import CachePolicy
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import build_commit_plan
from artisan.storage.io.staging import StagingManager


def _record(run_id: str, step: int = 0, spec: str | None = None) -> StepStartRecord:
    return StepStartRecord(
        step_run_id=run_id,
        step_spec_id=spec,
        step_number=step,
        step_name=f"op-{step}",
        operation_class="tests.Op",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json="{}",
        output_roles_json='["data"]',
        output_types_json='{"data":"data"}',
    )


def _terminal(
    status: StepStatus,
    succeeded: int,
    failed: int,
    step_run_id: str,
) -> StepResult:
    return StepResult(
        step_name="op-0",
        step_number=0,
        status=status,
        disposition=StepDisposition.EXECUTED,
        total_count=succeeded + failed,
        succeeded_count=succeeded,
        failed_count=failed,
        output_roles=frozenset({"data"}),
        output_types={"data": "data"},
        step_run_id=step_run_id,
    )


def _write_terminal(
    tracker: StepTracker,
    tmp_path: Path,
    run_id: str,
    status: StepStatus,
    *,
    spec: str = "b" * 32,
) -> None:
    tracker.create_attempt(_record(run_id))
    tracker.transition(run_id, StepStatus.PENDING, StepStatus.RUNNING)
    counts = (2, 1) if status is StepStatus.PARTIAL else (3, 0)
    _commit_terminal(
        tracker,
        tmp_path,
        run_id,
        _terminal(status, *counts, run_id),
        spec,
    )


def _commit_terminal(
    tracker: StepTracker,
    tmp_path: Path,
    run_id: str,
    result: StepResult,
    spec: str = "b" * 32,
) -> None:
    candidate = tracker.prepare_terminal_candidate(
        run_id,
        StepStatus.RUNNING,
        result.status,
        step_spec_id=spec,
        result=result,
    )
    staging = StagingManager(str(tmp_path / "staging"), tracker._fs)
    staging.stage_orchestrator_dataframe(
        candidate,
        "orchestration/steps",
        commit_kind="step_result",
        step_run_id=run_id,
        step_number=0,
        operation_name="op-0",
    )
    plan = build_commit_plan(
        delta_root=str(tmp_path),
        staging_root=staging.staging_dir,
        fs=tracker._fs,
        commit_kind="step_result",
        step_run_id=run_id,
        step_number=0,
        operation_name="op-0",
    )
    DeltaCommitter(str(tmp_path), staging, fs=tracker._fs).commit_logical(plan)


@patch("artisan.orchestration.engine.step_tracker.load_execution_membership")
def test_cache_policy_uses_explicit_status(mock_membership, tmp_path) -> None:
    mock_membership.return_value = pl.DataFrame({"execution_run_id": ["c" * 32]})
    tracker = StepTracker(str(tmp_path), "run")
    _write_terminal(tracker, tmp_path, "a" * 32, StepStatus.PARTIAL)
    assert tracker.check_cache("b" * 32, CachePolicy.ALL_SUCCEEDED) is None
    hit = tracker.check_cache("b" * 32, CachePolicy.STEP_COMPLETED)
    assert hit is not None
    assert hit.result.status is StepStatus.PARTIAL


def test_resume_restores_explicit_statuses(tmp_path) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    _write_terminal(tracker, tmp_path, "a" * 32, StepStatus.SUCCEEDED)
    states = tracker.load_resumable_steps("run")
    assert [state.status for state in states] == [StepStatus.SUCCEEDED]


@pytest.mark.parametrize("status", [StepStatus.PENDING, StepStatus.RUNNING])
def test_resume_refuses_nonterminal_attempt(tmp_path, status: StepStatus) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    tracker.create_attempt(_record("a" * 32))
    if status == StepStatus.RUNNING:
        tracker.transition("a" * 32, StepStatus.PENDING, StepStatus.RUNNING)
    with pytest.raises(PersistenceIntegrityError, match="unresolved"):
        tracker.load_resumable_steps("run")


@pytest.mark.parametrize("status", list(StepStatus))
def test_current_reader_exposes_every_authoritative_status(
    tmp_path,
    status: StepStatus,
) -> None:
    """Current-state reads preserve each lifecycle value without derivation."""
    tracker = StepTracker(str(tmp_path), "run")
    step_run_id = "a" * 32
    tracker.create_attempt(_record(step_run_id))
    if status == StepStatus.RUNNING:
        tracker.transition(step_run_id, StepStatus.PENDING, StepStatus.RUNNING)
    elif status == StepStatus.SKIPPED:
        tracker.transition(
            step_run_id,
            StepStatus.PENDING,
            status,
            result=StepResult(
                step_name="op-0",
                step_number=0,
                status=status,
                step_run_id=step_run_id,
            ),
        )
    elif status == StepStatus.CANCELLED:
        tracker.record_cancellation(
            step_run_id,
            StepStatus.PENDING,
            CancellationAcknowledgement(CancellationStatus.REQUESTED),
        )
        tracker.record_cancellation(
            step_run_id,
            StepStatus.PENDING,
            CancellationAcknowledgement(CancellationStatus.CONFIRMED),
        )
        tracker.transition(
            step_run_id,
            StepStatus.PENDING,
            status,
            result=StepResult(
                step_name="op-0",
                step_number=0,
                status=status,
                cancellation_status=CancellationStatus.CONFIRMED,
                step_run_id=step_run_id,
            ),
        )
    elif status != StepStatus.PENDING:
        tracker.transition(step_run_id, StepStatus.PENDING, StepStatus.RUNNING)
        if status == StepStatus.SUCCEEDED:
            result = StepResult(
                step_name="op-0",
                step_number=0,
                status=status,
                disposition=StepDisposition.EXECUTED,
                output_roles=frozenset({"data"}),
                output_types={"data": "data"},
                step_run_id=step_run_id,
            )
        elif status == StepStatus.PARTIAL:
            result = StepResult(
                step_name="op-0",
                step_number=0,
                status=status,
                disposition=StepDisposition.EXECUTED,
                total_count=2,
                succeeded_count=1,
                failed_count=1,
                output_roles=frozenset({"data"}),
                output_types={"data": "data"},
                step_run_id=step_run_id,
            )
        else:
            result = StepResult(
                step_name="op-0",
                step_number=0,
                status=status,
                error="test failure",
                total_count=1,
                failed_count=1,
                step_run_id=step_run_id,
            )
        if status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
            _commit_terminal(tracker, tmp_path, step_run_id, result)
        else:
            tracker.transition(
                step_run_id,
                StepStatus.RUNNING,
                status,
                step_spec_id="b" * 32,
                result=result,
            )

    states = tracker.load_current_states("run")
    assert len(states) == 1
    assert states[0].status == status


def test_run_rollup_uses_authoritative_status_and_active_end(tmp_path) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    tracker.create_attempt(_record("a" * 32))
    row = tracker.list_runs().row(0, named=True)
    assert row["last_status"] == "pending"
    assert row["step_count"] == 1
    assert row["ended_at"] is None


def test_run_rollup_started_at_uses_first_pending_snapshot(tmp_path) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    tracker.create_attempt(_record("a" * 32))
    pending_at = tracker.current_state("a" * 32).timestamp
    tracker.transition("a" * 32, StepStatus.PENDING, StepStatus.RUNNING)
    rows = pl.read_delta(str(tmp_path / "orchestration/steps")).with_columns(
        pl.when(pl.col("status") == "running")
        .then(pl.lit(pending_at - timedelta(days=1)))
        .otherwise(pl.col("timestamp"))
        .alias("timestamp")
    )
    rows.write_delta(str(tmp_path / "orchestration/steps"), mode="overwrite")

    row = tracker.list_runs().row(0, named=True)

    assert row["started_at"] == pending_at


def test_legacy_completed_status_is_rejected(tmp_path) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    tracker.create_attempt(_record("a" * 32))
    rows = pl.read_delta(str(tmp_path / "orchestration/steps"))
    rows = rows.with_columns(pl.lit("completed").alias("status"))
    rows.write_delta(str(tmp_path / "orchestration/steps"), mode="overwrite")
    with pytest.raises(PersistenceIntegrityError, match="legacy status"):
        tracker.load_current_states("run")
