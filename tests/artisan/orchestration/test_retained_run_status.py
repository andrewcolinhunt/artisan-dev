"""Retained status and names preserve lifecycle semantics without reading payloads."""

from __future__ import annotations

from functools import partial
from pathlib import Path

import polars as pl
import pytest
from fixtures.candidate_outputs import external_case, prepare_scientific
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_curators import admit_curator
from fixtures.retained_results import retained_case

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.retained_run_status import (
    resolve_retained_step_number,
    retained_run_status,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.run_status import RunStatus, _step_status
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import CachePolicy, TablePath
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.delta_files import DeltaSnapshot
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.retained_store import create_retained_store
from artisan.storage.io.step_result_plan import StepUnitFailure
from artisan.utils.path import uri_join


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    return create_retained_store(
        str(tmp_path / "store"),
        str(tmp_path / "files"),
        fs=StorageConfig().filesystem(),
    )


def _pending(
    tracker: RetainedStepTracker, *, owner: str, number: int, name: str
) -> StepStartRecord:
    record = StepStartRecord(
        step_run_id=owner * 32,
        step_number=number,
        step_name=name,
        operation_class="synthetic.Operation",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json="{}",
        output_roles_json="[]",
        output_types_json="{}",
    )
    tracker.create_attempt(record)
    return record


def test_empty_or_unknown_run_uses_existing_empty_response(
    committer: RetainedCommitter,
) -> None:
    empty = RunStatus(
        pipeline_run_id="unknown",
        last_status=None,
        step_count=0,
        started_at=None,
        ended_at=None,
        steps=[],
    )
    assert retained_run_status(committer, "unknown") == empty
    assert resolve_retained_step_number(committer, "unknown", "missing") is None
    _pending(
        RetainedStepTracker(committer, "other"), owner="a", number=1, name="exists"
    )
    assert retained_run_status(committer, "unknown") == empty
    with pytest.raises(ValueError, match="explicit pipeline"):
        retained_run_status(committer, "")
    with pytest.raises(ValueError, match="explicit pipeline"):
        resolve_retained_step_number(committer, "", "exists")


def test_live_unknown_and_rollup_follow_authoritative_timestamps(
    committer: RetainedCommitter,
) -> None:
    tracker = RetainedStepTracker(committer, "run")
    running = _pending(tracker, owner="a", number=20, name="running")
    first_time = tracker.current_state(running.step_run_id).timestamp.isoformat()
    tracker.transition(running.step_run_id, StepStatus.PENDING, StepStatus.RUNNING)
    for status in (CancellationStatus.REQUESTED, CancellationStatus.UNKNOWN):
        tracker.record_cancellation(
            running.step_run_id, StepStatus.RUNNING, CancellationAcknowledgement(status)
        )
    pending = _pending(tracker, owner="b", number=1, name="pending")
    result = retained_run_status(committer, "run")
    assert result.last_status == StepStatus.PENDING
    assert result.step_count == 2
    assert result.started_at == first_time
    assert result.ended_at is None
    assert [step.status for step in result.steps] == [
        StepStatus.PENDING,
        StepStatus.RUNNING,
    ]
    assert all(step.produced == step.duration == "-" for step in result.steps)
    assert (
        tracker.current_state(running.step_run_id).cancellation_status
        == CancellationStatus.UNKNOWN
    )
    tracker.skip_pending(pending.step_run_id, reason="empty_inputs")
    tracker.transition(
        running.step_run_id,
        StepStatus.RUNNING,
        StepStatus.FAILED,
        result=StepResult(
            step_run_id=running.step_run_id,
            step_number=20,
            step_name="running",
            status=StepStatus.FAILED,
            cancellation_status=CancellationStatus.UNKNOWN,
            error="cleanup remains unknown",
        ),
    )
    terminal = retained_run_status(committer, "run")
    assert terminal.last_status == StepStatus.FAILED
    assert (
        terminal.ended_at
        == tracker.current_state(running.step_run_id).timestamp.isoformat()
    )
    assert terminal.started_at == first_time


def test_names_follow_current_attempts_and_preserve_first_match_and_run_scope(
    committer: RetainedCommitter,
) -> None:
    tracker = RetainedStepTracker(committer, "run")
    _pending(tracker, owner="a", number=1, name="same")
    _pending(tracker, owner="b", number=3, name="same")
    assert resolve_retained_step_number(committer, "run", "same") == 1
    _pending(tracker, owner="c", number=1, name="renamed")
    assert resolve_retained_step_number(committer, "run", "same") == 3
    assert resolve_retained_step_number(committer, "run", "renamed") == 1
    _pending(
        RetainedStepTracker(committer, "other"), owner="d", number=1, name="other-name"
    )
    assert resolve_retained_step_number(committer, "run", "other-name") is None
    assert [step.name for step in retained_run_status(committer, "run").steps] == [
        "renamed",
        "same",
    ]


@pytest.mark.parametrize("mode", ["direct", "unit", "whole_partial"])
def test_scientific_reuse_counts_unique_current_exports_and_formats_duration(
    committer: RetainedCommitter, tmp_path: Path, mode: str
) -> None:
    case = retained_case(tmp_path, kind="alias")
    count = 2 if mode == "whole_partial" else 1
    results, admission, receipts = prepare_scientific(committer, case, count=count)
    committer.accept(receipts[0])
    results.finish(
        admission.step_run_id,
        failures={1: StepUnitFailure(error="tool failed")} if count == 2 else {},
        duration_seconds=1.26,
    )
    if mode != "direct":
        current, selected, _ = prepare_scientific(
            committer, case, step_run_id="b" * 32, count=count
        )
        if mode == "unit":
            committer.journal.reuse(
                selected.assignments[0],
                receipts[0],
                verify_source=partial(
                    verify_cache_source, reader=RetainedReader(committer)
                ),
            )
            current.finish(selected.step_run_id, duration_seconds=2.34)
        else:
            hit = current.lookup(selected.step_run_id, CachePolicy.STEP_COMPLETED)
            assert hit is not None
            current.reuse(hit, duration_seconds=2.34)
    status = retained_run_status(committer, "synthetic-pipeline")
    assert status.step_count == 1
    assert status.last_status == (
        StepStatus.PARTIAL if count == 2 else StepStatus.SUCCEEDED
    )
    assert status.steps[0].produced == "1 csv"
    assert status.steps[0].duration == ("1.3s" if mode == "direct" else "2.3s")
    assert status.ended_at is not None
    assert resolve_retained_step_number(committer, "synthetic-pipeline", "produce") == 7


@pytest.mark.parametrize("empty", [False, True])
def test_curator_reuse_counts_current_steps_without_payload_hydration(
    committer: RetainedCommitter, empty: bool
) -> None:
    operation = DynamicCurator()
    operation.params.empty = empty
    results, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(results, admission, operation)
    results.finish(reference, staged.execution_run_id, terminal)
    _, _, current = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, step_number=9, **STAMPS
    )
    hit = lookup_curator(current, reader=CuratorReader(committer))
    assert hit is not None
    results.reuse(hit)
    committer.fs.rm(
        uri_join(committer.delta_root, ArtifactTypeDef.get_table_path("csv")),
        recursive=True,
    )
    result = retained_run_status(committer, "synthetic-pipeline")
    assert [step.step_number for step in result.steps] == [8, 9]
    assert [step.produced for step in result.steps] == (
        ["-", "-"] if empty else ["1 csv", "1 csv"]
    )
    assert result.last_status == StepStatus.SUCCEEDED
    assert result.ended_at is not None


def test_missing_carrier_does_not_hide_metadata_status(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, external_case(tmp_path)
    )
    plan = committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    expected = retained_run_status(committer, "synthetic-pipeline")
    committer.fs.rm(uri_join(committer.files_root, plan.carriers[0].relative_path))
    assert retained_run_status(committer, "synthetic-pipeline") == expected


def test_selected_incomplete_plan_remains_running_without_recovery(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])

    def interrupt(event: str) -> None:
        if event == "planned":
            raise InterruptedError(event)

    results.checkpoint = interrupt
    with pytest.raises(InterruptedError):
        results.finish(admission.step_run_id)
    before = committer._controls()
    result = retained_run_status(committer, "synthetic-pipeline")
    assert result.last_status == result.steps[0].status == StepStatus.RUNNING
    assert result.ended_at is None
    assert result.steps[0].produced == "-"
    assert committer._controls().equals(before)


def test_status_counts_reject_tampered_metadata_and_shared_formatter_keeps_rules(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    state = results.tracker.current_state(admission.step_run_id)
    assert _step_status(state, {"metric": 2, "csv": 1}).produced == "1 csv, 2 metric"
    filtered = state.model_copy(
        update={"operation_class": "Filter", "succeeded_count": 3}
    )
    assert _step_status(filtered, {"csv": 1}).produced == "3 passed"
    assert (
        retained_run_status(committer, "synthetic-pipeline").steps[0].produced
        == "1 csv"
    )
    path = uri_join(committer.delta_root, TablePath.ARTIFACT_INDEX.value)
    pl.read_delta(path).with_columns(
        pl.lit('{"changed": true}').alias("metadata")
    ).write_delta(path, mode="overwrite")
    with pytest.raises(StoreIntegrityError, match="conflicting"):
        retained_run_status(committer, "synthetic-pipeline")


def test_pending_insertion_does_not_splice_lifecycle_counts_and_rollup(
    committer: RetainedCommitter, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    snapshot = RetainedReadSnapshot.snapshot
    inserted = False

    def insert_after_capture(
        read: RetainedReadSnapshot, table: str, *, version: int | None = None
    ) -> DeltaSnapshot:
        nonlocal inserted
        physical = snapshot(read, table, version=version)
        if table == TablePath.STEPS.value and not inserted:
            inserted = True
            _pending(results.tracker, owner="b", number=7, name="queued_retry")
        return physical

    monkeypatch.setattr(RetainedReadSnapshot, "snapshot", insert_after_capture)
    captured = retained_run_status(committer, "synthetic-pipeline")
    assert inserted
    assert captured.last_status == captured.steps[0].status == StepStatus.SUCCEEDED
    assert captured.steps[0].name == "produce"
    assert captured.steps[0].produced == "1 csv"
    assert captured.ended_at is not None
    current = retained_run_status(committer, "synthetic-pipeline")
    assert current.last_status == current.steps[0].status == StepStatus.PENDING
    assert current.steps[0].name == "queued_retry"
    assert current.steps[0].produced == "-"
    assert current.ended_at is None
