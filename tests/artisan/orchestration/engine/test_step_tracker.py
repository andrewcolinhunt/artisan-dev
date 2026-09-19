"""Tests for StepTracker lifecycle, cache eligibility, resume, and run rollups."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
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


def _record(
    step_run_id: str = "a" * 32, step_spec_id: str | None = None
) -> StepStartRecord:
    return StepStartRecord(
        step_run_id=step_run_id,
        step_spec_id=step_spec_id,
        step_number=0,
        step_name="op",
        operation_class="tests.Op",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json="{}",
        output_roles_json='["data"]',
        output_types_json='{"data":"data"}',
    )


def _result(
    status: StepStatus = StepStatus.SUCCEEDED,
    step_run_id: str = "a" * 32,
) -> StepResult:
    if status == StepStatus.SUCCEEDED:
        return StepResult(
            step_name="op",
            step_number=0,
            status=status,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset({"data"}),
            output_types={"data": "data"},
            step_run_id=step_run_id,
        )
    if status == StepStatus.CANCELLED:
        return StepResult(
            step_name="op",
            step_number=0,
            status=status,
            cancellation_status=CancellationStatus.CONFIRMED,
            step_run_id=step_run_id,
        )
    raise AssertionError(status)


def test_create_attempt_is_pending_and_idempotent(tmp_path) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    record = _record()
    first = tracker.create_attempt(record)
    second = tracker.create_attempt(record)
    assert first.status is StepStatus.PENDING
    assert second.state_sequence == 0
    assert pl.read_delta(str(tmp_path / "orchestration/steps")).height == 1


def test_running_to_succeeded_uses_monotonic_sequences(tmp_path) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    tracker.create_attempt(_record())
    tracker.transition("a" * 32, StepStatus.PENDING, StepStatus.RUNNING)
    terminal, _, _ = _commit_terminal(tracker, tmp_path, _result())
    assert terminal.status is StepStatus.SUCCEEDED
    assert terminal.state_sequence == 2
    rows = pl.read_delta(str(tmp_path / "orchestration/steps"))
    assert rows.sort("state_sequence")["status"].to_list() == [
        "pending",
        "running",
        "succeeded",
    ]


def test_stale_transition_fails_closed(tmp_path) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    tracker.create_attempt(_record())
    tracker.transition("a" * 32, StepStatus.PENDING, StepStatus.RUNNING)
    with pytest.raises(PersistenceIntegrityError, match="Stale"):
        tracker.transition("a" * 32, StepStatus.PENDING, StepStatus.SKIPPED)


def test_terminal_retry_is_idempotent_but_conflict_fails(tmp_path) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    tracker.create_attempt(_record())
    tracker.transition("a" * 32, StepStatus.PENDING, StepStatus.RUNNING)
    result = _result()
    _, committer, plan = _commit_terminal(tracker, tmp_path, result)
    committer.commit_logical(plan)
    retry = tracker.current_state("a" * 32)
    assert retry.status is StepStatus.SUCCEEDED
    with pytest.raises(PersistenceIntegrityError, match="already succeeded"):
        tracker.transition(
            "a" * 32,
            StepStatus.RUNNING,
            StepStatus.CANCELLED,
            result=_result(StepStatus.CANCELLED),
        )


def test_terminal_result_must_name_current_attempt(tmp_path) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    tracker.create_attempt(_record())
    tracker.transition("a" * 32, StepStatus.PENDING, StepStatus.RUNNING)

    with pytest.raises(PersistenceIntegrityError, match="current step attempt"):
        tracker.prepare_terminal_candidate(
            "a" * 32,
            StepStatus.RUNNING,
            StepStatus.SUCCEEDED,
            step_spec_id="b" * 32,
            result=_result(step_run_id="c" * 32),
        )


def test_cancellation_snapshots_do_not_change_lifecycle(tmp_path) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    tracker.create_attempt(_record())
    tracker.transition("a" * 32, StepStatus.PENDING, StepStatus.RUNNING)
    requested = tracker.record_cancellation(
        "a" * 32,
        StepStatus.RUNNING,
        CancellationAcknowledgement(CancellationStatus.REQUESTED),
    )
    confirmed = tracker.record_cancellation(
        "a" * 32,
        StepStatus.RUNNING,
        CancellationAcknowledgement(CancellationStatus.CONFIRMED, "stopped"),
    )
    terminal = tracker.transition(
        "a" * 32,
        StepStatus.RUNNING,
        StepStatus.CANCELLED,
        result=_result(StepStatus.CANCELLED),
    )
    assert requested.status is StepStatus.RUNNING
    assert confirmed.cancellation_status is CancellationStatus.CONFIRMED
    assert terminal.status is StepStatus.CANCELLED
    assert terminal.state_sequence == 4


def test_cancelled_transition_requires_persisted_confirmation(tmp_path) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    tracker.create_attempt(_record())

    with pytest.raises(
        PersistenceIntegrityError,
        match="previously persisted acknowledgement",
    ):
        tracker.transition(
            "a" * 32,
            StepStatus.PENDING,
            StepStatus.CANCELLED,
            result=_result(StepStatus.CANCELLED),
        )


def test_conflicting_maximum_sequence_is_rejected(tmp_path) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    tracker.create_attempt(_record())
    current = tracker.current_state("a" * 32)
    row = tracker._state_to_row(current)
    row.update(status="running", timestamp=datetime.now(UTC))
    tracker._write_row(row)
    with pytest.raises(PersistenceIntegrityError, match="conflicting snapshots"):
        tracker.current_state("a" * 32)


def test_reader_rejects_combined_status_and_cancellation_change(tmp_path) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    tracker.create_attempt(_record())
    current = tracker.current_state("a" * 32)
    row = tracker._state_to_row(current)
    row.update(
        status="cancelled",
        state_sequence=1,
        cancellation_status="confirmed",
        output_roles_json="[]",
        output_types_json="{}",
        timestamp=datetime.now(UTC),
    )
    tracker._write_row(row)

    with pytest.raises(PersistenceIntegrityError, match="cannot change status"):
        tracker.current_state("a" * 32)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("output_roles_json", '["other"]'),
        ("output_types_json", '{"other":"metric"}'),
    ],
)
def test_reader_rejects_changed_output_contract(
    tmp_path, field: str, value: str
) -> None:
    tracker = StepTracker(str(tmp_path), "run")
    tracker.create_attempt(_record())
    tracker.transition("a" * 32, StepStatus.PENDING, StepStatus.RUNNING)
    rows = pl.read_delta(str(tmp_path / "orchestration/steps")).with_columns(
        pl.when(pl.col("status") == "running")
        .then(pl.lit(value))
        .otherwise(pl.col(field))
        .alias(field)
    )
    rows.write_delta(str(tmp_path / "orchestration/steps"), mode="overwrite")

    with pytest.raises(PersistenceIntegrityError, match="changed output contract"):
        tracker.current_state("a" * 32)


def _commit_terminal(
    tracker: StepTracker,
    tmp_path: Path,
    result: StepResult,
    spec: str = "b" * 32,
):
    step_run_id = result.step_run_id
    assert step_run_id is not None
    candidate = tracker.prepare_terminal_candidate(
        step_run_id,
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
        step_run_id=step_run_id,
        step_number=0,
        operation_name=result.step_name,
    )
    plan = build_commit_plan(
        delta_root=str(tmp_path),
        staging_root=staging.staging_dir,
        fs=tracker._fs,
        commit_kind="step_result",
        step_run_id=step_run_id,
        step_number=0,
        operation_name=result.step_name,
    )
    committer = DeltaCommitter(str(tmp_path), staging, fs=tracker._fs)
    committer.commit_logical(plan)
    return tracker.current_state(step_run_id), committer, plan


def _terminal(
    status: StepStatus,
    succeeded: int,
    failed: int,
    step_run_id: str,
) -> StepResult:
    return StepResult(
        step_name="op",
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
        _terminal(status, *counts, run_id),
        spec,
    )


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
    tracker.create_attempt(_record(step_run_id, step_spec_id="b" * 32))
    if status == StepStatus.RUNNING:
        tracker.transition(step_run_id, StepStatus.PENDING, StepStatus.RUNNING)
    elif status == StepStatus.SKIPPED:
        tracker.transition(
            step_run_id,
            StepStatus.PENDING,
            status,
            result=StepResult(
                step_name="op",
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
                step_name="op",
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
                step_name="op",
                step_number=0,
                status=status,
                disposition=StepDisposition.EXECUTED,
                output_roles=frozenset({"data"}),
                output_types={"data": "data"},
                step_run_id=step_run_id,
            )
        elif status == StepStatus.PARTIAL:
            result = StepResult(
                step_name="op",
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
                step_name="op",
                step_number=0,
                status=status,
                error="test failure",
                total_count=1,
                failed_count=1,
                step_run_id=step_run_id,
            )
        if status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
            _commit_terminal(tracker, tmp_path, result)
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

    with patch(
        "artisan.orchestration.engine.step_tracker.load_execution_membership"
    ) as membership:
        membership.return_value = pl.DataFrame({"execution_run_id": ["c" * 32]})
        for policy in CachePolicy:
            hit = tracker.check_cache("b" * 32, policy)
            eligible = status is StepStatus.SUCCEEDED or (
                status is StepStatus.PARTIAL and policy is CachePolicy.STEP_COMPLETED
            )
            assert (hit is not None) is eligible


@patch("artisan.orchestration.engine.step_tracker.load_execution_membership")
def test_cache_selects_newest_eligible_status(mock_membership, tmp_path) -> None:
    mock_membership.return_value = pl.DataFrame({"execution_run_id": ["c" * 32]})
    older = StepTracker(str(tmp_path), "older")
    newer = StepTracker(str(tmp_path), "newer")
    _write_terminal(older, tmp_path, "a" * 32, StepStatus.SUCCEEDED)
    _write_terminal(newer, tmp_path, "d" * 32, StepStatus.PARTIAL)
    strict = newer.check_cache("b" * 32, CachePolicy.ALL_SUCCEEDED)
    partial = newer.check_cache("b" * 32, CachePolicy.STEP_COMPLETED)
    assert strict is not None
    assert strict.result.step_run_id == "a" * 32
    assert partial is not None
    assert partial.result.step_run_id == "d" * 32


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
