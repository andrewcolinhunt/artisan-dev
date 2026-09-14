"""State-machine tests for StepTracker."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from artisan.errors import PersistenceIntegrityError
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord


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
    terminal = tracker.transition(
        "a" * 32,
        StepStatus.RUNNING,
        StepStatus.SUCCEEDED,
        step_spec_id="b" * 32,
        result=_result(),
    )
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
    tracker.transition(
        "a" * 32,
        StepStatus.RUNNING,
        StepStatus.SUCCEEDED,
        step_spec_id="b" * 32,
        result=result,
    )
    retry = tracker.transition(
        "a" * 32,
        StepStatus.RUNNING,
        StepStatus.SUCCEEDED,
        result=result,
    )
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
        tracker.transition(
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
