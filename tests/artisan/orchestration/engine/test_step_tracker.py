"""Tests for StepTracker lifecycle, cache eligibility, resume, and run rollups."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from fixtures.candidate_outputs import prepare_scientific
from fixtures.retained_results import retained_case

from artisan.errors import PersistenceIntegrityError
from artisan.orchestration.engine.retained_run_status import list_retained_runs
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.enums import CachePolicy
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.io.retained_store import create_retained_store
from artisan.storage.io.step_result_plan import StepUnitFailure


def _tracker(root: Path, pipeline_run_id: str = "run") -> RetainedStepTracker:
    committer = create_retained_store(
        str(root), str(root / "files"), fs=StorageConfig().filesystem()
    )
    return RetainedStepTracker(committer, pipeline_run_id)


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
        output_types_json='{"data":"csv"}',
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
            output_types={"data": "csv"},
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
    tracker = _tracker(tmp_path)
    record = _record()
    first = tracker.create_attempt(record)
    second = tracker.create_attempt(record)
    assert first.status is StepStatus.PENDING
    assert second.state_sequence == 0
    assert pl.read_delta(str(tmp_path / "orchestration/steps")).height == 1


def test_stale_transition_fails_closed(tmp_path) -> None:
    tracker = _tracker(tmp_path)
    tracker.create_attempt(_record())
    tracker.transition("a" * 32, StepStatus.PENDING, StepStatus.RUNNING)
    with pytest.raises(PersistenceIntegrityError, match="Stale"):
        tracker.transition("a" * 32, StepStatus.PENDING, StepStatus.SKIPPED)


def test_terminal_result_must_name_current_attempt(tmp_path) -> None:
    tracker = _tracker(tmp_path)
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


def test_terminal_candidate_reuses_explicit_history_without_reopening(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tracker = _tracker(tmp_path)
    tracker.create_attempt(_record())
    tracker.transition("a" * 32, StepStatus.PENDING, StepStatus.RUNNING)
    rows = tracker._rows_for_attempt("a" * 32)
    controls = tracker._read_controls()

    def unexpected_read(*args: object, **kwargs: object) -> None:
        msg = "Explicit finite evidence must not reopen lifecycle I/O"
        raise AssertionError(msg)

    monkeypatch.setattr(tracker, "_rows_for_attempt", unexpected_read)
    monkeypatch.setattr(tracker, "_read_controls", unexpected_read)
    terminal = tracker._terminal_candidate(
        "a" * 32,
        StepStatus.RUNNING,
        StepStatus.SUCCEEDED,
        step_spec_id="f" * 32,
        result=_result(),
        rows=rows,
        controls=controls,
    )
    assert terminal["state_sequence"].to_list() == [2]
    assert terminal["status"].to_list() == ["succeeded"]
    with pytest.raises(PersistenceIntegrityError, match="another step"):
        tracker._terminal_candidate(
            "b" * 32,
            StepStatus.RUNNING,
            StepStatus.SUCCEEDED,
            step_spec_id="f" * 32,
            result=_result(step_run_id="b" * 32),
            rows=rows,
            controls=controls,
        )


def test_cancellation_snapshots_do_not_change_lifecycle(tmp_path) -> None:
    tracker = _tracker(tmp_path)
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
    tracker = _tracker(tmp_path)
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
    tracker = _tracker(tmp_path)
    tracker.create_attempt(_record())
    current = tracker.current_state("a" * 32)
    row = tracker._state_to_row(current)
    row.update(status="running", timestamp=datetime.now(UTC))
    tracker._write_row(row)
    with pytest.raises(PersistenceIntegrityError, match="conflicting snapshots"):
        tracker.current_state("a" * 32)


def test_reader_rejects_combined_status_and_cancellation_change(tmp_path) -> None:
    tracker = _tracker(tmp_path)
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
    tracker = _tracker(tmp_path)
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


def test_legacy_completed_status_is_rejected(tmp_path) -> None:
    tracker = _tracker(tmp_path)
    tracker.create_attempt(_record("a" * 32))
    rows = pl.read_delta(str(tmp_path / "orchestration/steps"))
    rows = rows.with_columns(pl.lit("completed").alias("status"))
    rows.write_delta(str(tmp_path / "orchestration/steps"), mode="overwrite")
    with pytest.raises(PersistenceIntegrityError, match="legacy status"):
        tracker.load_current_states("run")


@pytest.mark.parametrize("marker", [1, 0, "true", None, [], {}])
def test_dynamic_output_marker_requires_boolean(tmp_path, marker) -> None:
    import json

    tracker = _tracker(tmp_path)
    record = _record().model_copy(
        update={
            "compute_options_json": json.dumps({"dynamic_output_roles": marker}),
            "output_roles_json": "[]",
            "output_types_json": "{}",
        }
    )
    with pytest.raises(PersistenceIntegrityError, match="Boolean"):
        tracker.create_attempt(record)


def test_dynamic_output_marker_rejects_static_declaration(tmp_path) -> None:
    tracker = _tracker(tmp_path)
    record = _record().model_copy(
        update={
            "compute_options_json": '{"dynamic_output_roles": true}',
        }
    )
    with pytest.raises(PersistenceIntegrityError, match="contradicts declared outputs"):
        tracker.create_attempt(record)


@pytest.mark.parametrize("new_marker", [False, True])
def test_dynamic_output_marker_cannot_change_during_attempt(
    tmp_path, new_marker
) -> None:
    import json

    tracker = _tracker(tmp_path)
    record = _record().model_copy(
        update={
            "compute_options_json": json.dumps(
                {"dynamic_output_roles": not new_marker}
            ),
            "output_roles_json": "[]",
            "output_types_json": "{}",
        }
    )
    tracker.create_attempt(record)
    tracker.transition(record.step_run_id, StepStatus.PENDING, StepStatus.RUNNING)
    path = str(tmp_path / "orchestration/steps")
    pl.read_delta(path).with_columns(
        pl.when(pl.col("status") == "running")
        .then(pl.lit(json.dumps({"dynamic_output_roles": new_marker})))
        .otherwise(pl.col("compute_options_json"))
        .alias("compute_options_json")
    ).write_delta(path, mode="overwrite")
    with pytest.raises(PersistenceIntegrityError, match="changed owner metadata"):
        tracker.current_state(record.step_run_id)


def test_unmarked_empty_declaration_retains_static_history_contract(tmp_path) -> None:
    tracker = _tracker(tmp_path)
    record = _record().model_copy(
        update={
            "output_roles_json": "[]",
            "output_types_json": "{}",
        }
    )
    tracker.create_attempt(record)
    tracker.transition(record.step_run_id, StepStatus.PENDING, StepStatus.RUNNING)
    candidate = tracker.prepare_terminal_candidate(
        "a" * 32,
        StepStatus.RUNNING,
        StepStatus.SUCCEEDED,
        step_spec_id="b" * 32,
        result=_result(),
    )
    history = tracker._rows_for_attempt("a" * 32).sort("state_sequence")
    with pytest.raises(PersistenceIntegrityError, match="changed output contract"):
        tracker._validate_history([*history.to_dicts(), candidate.row(0, named=True)])


@pytest.mark.parametrize(
    "status", [StepStatus.SUCCEEDED, StepStatus.PARTIAL, StepStatus.FAILED]
)
def test_selected_results_preserve_status_counts_and_terminal_idempotency(
    tmp_path: Path,
    status: StepStatus,
) -> None:
    committer = _tracker(tmp_path).committer
    case = retained_case(tmp_path / "payload", kind="alias")
    results, admission, receipts = prepare_scientific(committer, case, count=2)
    succeeded = {StepStatus.SUCCEEDED: 2, StepStatus.PARTIAL: 1, StepStatus.FAILED: 0}[
        status
    ]
    for receipt in receipts[:succeeded]:
        committer.accept(receipt)
    result = results.finish(
        admission.step_run_id,
        failures={i: StepUnitFailure(error="unit failed") for i in range(succeeded, 2)},
    )
    assert result.status is status
    assert (result.succeeded_count, result.failed_count) == (succeeded, 2 - succeeded)
    assert results.recover(admission.step_run_id) == result
    state = results.tracker.current_state(admission.step_run_id)
    assert state.status is status
    assert state.state_sequence == 2
    rows = pl.read_delta(str(tmp_path / "orchestration/steps")).sort("state_sequence")
    assert rows["status"].to_list() == ["pending", "running", status.value]
    with pytest.raises(PersistenceIntegrityError, match="already"):
        results.tracker.transition(
            admission.step_run_id,
            StepStatus.RUNNING,
            StepStatus.CANCELLED,
            result=_result(StepStatus.CANCELLED),
        )


def test_cache_policy_selects_newest_eligible_completed_result(tmp_path: Path) -> None:
    committer = _tracker(tmp_path).committer
    case = retained_case(tmp_path / "payload", kind="alias")
    first, admission, receipts = prepare_scientific(committer, case, count=2)
    for receipt in receipts:
        committer.accept(receipt)
    first.finish(admission.step_run_id)
    second, admission, receipts = prepare_scientific(
        committer, case, step_run_id="b" * 32, count=2
    )
    committer.accept(receipts[0])
    second.finish(
        admission.step_run_id, failures={1: StepUnitFailure(error="unit failed")}
    )
    current, admission, _ = prepare_scientific(
        committer, case, step_run_id="d" * 32, count=2
    )
    strict = current.lookup(admission.step_run_id, CachePolicy.ALL_SUCCEEDED)
    partial = current.lookup(admission.step_run_id, CachePolicy.STEP_COMPLETED)
    assert strict is not None
    assert partial is not None
    assert strict.source.result.step_run_id == "a" * 32
    assert partial.source.result.step_run_id == "b" * 32
    assert partial.source.result.status is StepStatus.PARTIAL


def test_run_rollup_keeps_first_pending_time_and_active_end(tmp_path: Path) -> None:
    tracker = _tracker(tmp_path)
    tracker.create_attempt(_record())
    pending_at = tracker.current_state("a" * 32).timestamp
    row = list_retained_runs(tracker.committer).row(0, named=True)
    assert row["last_status"] == "pending"
    assert row["step_count"] == 1
    assert row["ended_at"] is None
    tracker.transition("a" * 32, StepStatus.PENDING, StepStatus.RUNNING)
    path = str(tmp_path / "orchestration/steps")
    pl.read_delta(path).with_columns(
        pl.when(pl.col("status") == "running")
        .then(pl.lit(pending_at - timedelta(days=1)))
        .otherwise(pl.col("timestamp"))
        .alias("timestamp")
    ).write_delta(path, mode="overwrite")
    row = list_retained_runs(tracker.committer).row(0, named=True)
    assert row["started_at"] == pending_at
    assert row["last_status"] == "running"
    assert row["ended_at"] is None
