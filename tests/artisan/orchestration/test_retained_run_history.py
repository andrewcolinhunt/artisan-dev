"""Retained history shares existing lifecycle rules and one fresh metadata proof."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from fixtures.candidate_outputs import external_case, prepare_scientific
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_curators import admit_curator
from fixtures.retained_results import retained_case

from artisan.errors import PersistenceIntegrityError, StoreIntegrityError
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.retained_run_status import (
    list_retained_runs,
    resolve_retained_step_number,
    retained_run_status,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.io.delta_files import DeltaSnapshot
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_read import RetainedControlProof, RetainedReadSnapshot
from artisan.storage.io.retained_store import create_retained_store
from artisan.utils.path import uri_join


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    return create_retained_store(
        str(tmp_path / "store"),
        str(tmp_path / "files"),
        fs=StorageConfig().filesystem(),
    )


def _pending(
    tracker: RetainedStepTracker, owner: str, *, number: int = 1
) -> StepStartRecord:
    record = StepStartRecord(
        step_run_id=owner * 32,
        step_number=number,
        step_name=f"step-{owner}",
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


def test_empty_history_preserves_existing_columns_and_dtypes(
    committer: RetainedCommitter,
) -> None:
    history = list_retained_runs(committer)
    assert history.is_empty()
    assert history.schema == {
        "pipeline_run_id": pl.String,
        "step_count": pl.UInt32,
        "last_status": pl.String,
        "started_at": pl.Datetime("us", "UTC"),
        "ended_at": pl.Datetime("us", "UTC"),
    }


def test_history_keeps_latest_positions_start_times_and_unknown_ownership(
    committer: RetainedCommitter,
) -> None:
    first = RetainedStepTracker(committer, "first")
    initial = _pending(first, "a")
    started = first.current_state(initial.step_run_id).timestamp
    first.skip_pending(initial.step_run_id, reason="empty_inputs")
    replacement = _pending(first, "b")
    first.transition(replacement.step_run_id, StepStatus.PENDING, StepStatus.RUNNING)
    for status in (CancellationStatus.REQUESTED, CancellationStatus.UNKNOWN):
        first.record_cancellation(
            replacement.step_run_id,
            StepStatus.RUNNING,
            CancellationAcknowledgement(status),
        )
    second = RetainedStepTracker(committer, "second")
    later = _pending(second, "c")
    second.skip_pending(later.step_run_id, reason="empty_inputs")
    history = list_retained_runs(committer)
    assert history["pipeline_run_id"].to_list() == ["second", "first"]
    assert history["step_count"].to_list() == [1, 1]
    assert history["last_status"].to_list() == ["skipped", "running"]
    assert history["started_at"][1] == started
    assert history["ended_at"][1] is None
    assert history["ended_at"][0] == second.current_state(later.step_run_id).timestamp
    assert first.current_state(replacement.step_run_id).cancellation_status == (
        CancellationStatus.UNKNOWN
    )


def test_equal_timestamps_preserve_distinct_logical_positions(
    committer: RetainedCommitter,
) -> None:
    tracker = RetainedStepTracker(committer, "run")
    first = _pending(tracker, "a", number=2)
    _pending(tracker, "b", number=1)
    timestamp = tracker.current_state(first.step_run_id).timestamp
    path = uri_join(committer.delta_root, TablePath.STEPS.value)
    pl.read_delta(path).with_columns(pl.lit(timestamp).alias("timestamp")).write_delta(
        path, mode="overwrite"
    )
    history = list_retained_runs(committer)
    assert history["step_count"].to_list() == [2]
    assert history["started_at"][0] == timestamp
    assert history["ended_at"][0] is None
    assert [step.name for step in retained_run_status(committer, "run").steps] == [
        "step-b",
        "step-a",
    ]


def test_mixed_scientific_and_curator_history_does_not_read_carrier_bytes(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    scientific, admitted, receipts = prepare_scientific(
        committer, external_case(tmp_path)
    )
    plan = committer.accept(receipts[0])
    scientific.finish(admitted.step_run_id)
    operation = DynamicCurator()
    curator, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(curator, admission, operation)
    curator.finish(reference, staged.execution_run_id, terminal)
    expected = list_retained_runs(committer)
    assert expected["step_count"].to_list() == [2]
    assert expected["last_status"].to_list() == ["succeeded"]
    assert expected["ended_at"][0] is not None
    committer.fs.rm(uri_join(committer.files_root, plan.carriers[0].relative_path))
    assert list_retained_runs(committer).equals(expected)


@pytest.mark.parametrize(
    "acknowledgement", [None, CancellationStatus.CONFIRMED, CancellationStatus.UNKNOWN]
)
def test_curator_without_execution_preserves_verified_terminal_history(
    committer: RetainedCommitter, acknowledgement: CancellationStatus | None
) -> None:
    curator, _, reference = admit_curator(committer, Merge(), {}, step_run_id="a" * 32)
    if acknowledgement is not None:
        for status in (CancellationStatus.REQUESTED, acknowledgement):
            curator.record_cancellation(
                reference.step_run_id, CancellationAcknowledgement(status)
            )
    terminal = curator.finish_without_execution(
        reference, error="worker ended before producing a result"
    )
    assert list_retained_runs(committer)["last_status"].to_list() == [
        terminal.status.value
    ]
    status = retained_run_status(committer, "synthetic-pipeline")
    assert status.steps[0].status == terminal.status
    assert resolve_retained_step_number(committer, "synthetic-pipeline", "merge") == 8

    path = uri_join(committer.delta_root, TablePath.STEPS.value)
    pl.read_delta(path).filter(pl.col("status") != terminal.status.value).write_delta(
        path, mode="overwrite"
    )
    with pytest.raises(StoreIntegrityError):
        list_retained_runs(committer)


@pytest.mark.parametrize("query", ["history", "status", "name"])
def test_each_query_uses_one_global_proof_and_next_call_is_fresh(
    committer: RetainedCommitter,
    monkeypatch: pytest.MonkeyPatch,
    query: str,
) -> None:
    operation = DynamicCurator()
    curator, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(curator, admission, operation)
    curator.finish(reference, staged.execution_run_id, terminal)
    calls = 0
    prove = committer._control_proof

    def counted() -> RetainedControlProof:
        nonlocal calls
        calls += 1
        return prove()

    def inspect() -> object:
        if query == "history":
            return list_retained_runs(committer)
        if query == "status":
            return retained_run_status(committer, "synthetic-pipeline")
        return resolve_retained_step_number(
            committer, "synthetic-pipeline", operation.name
        )

    monkeypatch.setattr(committer, "_control_proof", counted)
    inspect()
    assert calls == 1
    inspect()
    assert calls == 2


@pytest.mark.parametrize("query", ["history", "name"])
def test_captured_lifecycle_does_not_splice_a_new_attempt(
    committer: RetainedCommitter, monkeypatch: pytest.MonkeyPatch, query: str
) -> None:
    tracker = RetainedStepTracker(committer, "run")
    first = _pending(tracker, "a")
    tracker.skip_pending(first.step_run_id, reason="empty_inputs")
    snapshot = RetainedReadSnapshot.snapshot
    inserted = False

    def capture_then_submit(
        read: RetainedReadSnapshot, table: str, *, version: int | None = None
    ) -> DeltaSnapshot:
        nonlocal inserted
        physical = snapshot(read, table, version=version)
        if table == TablePath.STEPS.value and not inserted:
            inserted = True
            _pending(tracker, "b")
        return physical

    monkeypatch.setattr(RetainedReadSnapshot, "snapshot", capture_then_submit)
    if query == "history":
        old = list_retained_runs(committer)
        current = list_retained_runs(committer)
        assert old["last_status"].to_list() == ["skipped"]
        assert old["ended_at"][0] is not None
        assert current["last_status"].to_list() == ["pending"]
        assert current["ended_at"][0] is None
    else:
        assert resolve_retained_step_number(committer, "run", "step-a") == 1
        assert resolve_retained_step_number(committer, "run", "step-a") is None
        assert resolve_retained_step_number(committer, "run", "step-b") == 1
    assert inserted


def test_lifecycle_tamper_is_rejected_by_the_next_history_read(
    committer: RetainedCommitter,
) -> None:
    tracker = RetainedStepTracker(committer, "run")
    record = _pending(tracker, "a")
    tracker.skip_pending(record.step_run_id, reason="empty_inputs")
    assert list_retained_runs(committer)["last_status"].to_list() == ["skipped"]
    path = uri_join(committer.delta_root, TablePath.STEPS.value)
    pl.read_delta(path).with_columns(
        pl.when(pl.col("status") == "skipped")
        .then(pl.lit("different-owner-name"))
        .otherwise(pl.col("step_name"))
        .alias("step_name")
    ).write_delta(path, mode="overwrite")
    with pytest.raises(PersistenceIntegrityError, match="changed owner"):
        list_retained_runs(committer)


@pytest.mark.parametrize("kind", ["scientific", "curator"])
@pytest.mark.parametrize("changed", ["timestamp", "counts", "removed"])
def test_completed_terminal_requires_exact_effect_before_history_or_names(
    committer: RetainedCommitter, tmp_path: Path, kind: str, changed: str
) -> None:
    if kind == "scientific":
        results, admitted, receipts = prepare_scientific(
            committer, retained_case(tmp_path, kind="alias")
        )
        committer.accept(receipts[0])
        results.finish(admitted.step_run_id)
    else:
        operation = DynamicCurator()
        curator, admission, reference = admit_curator(
            committer, operation, {}, step_run_id="a" * 32, **STAMPS
        )
        staged, terminal, _ = stage_observed(curator, admission, operation)
        curator.finish(reference, staged.execution_run_id, terminal)
    assert list_retained_runs(committer)["last_status"].to_list() == ["succeeded"]
    path = uri_join(committer.delta_root, TablePath.STEPS.value)
    rows = pl.read_delta(path)
    if changed == "removed":
        rows = rows.filter(pl.col("status") != "succeeded")
    elif changed == "timestamp":
        rows = rows.with_columns(
            pl.when(pl.col("status") == "succeeded")
            .then(pl.col("timestamp") + pl.duration(days=1))
            .otherwise(pl.col("timestamp"))
            .alias("timestamp")
        )
    else:
        rows = rows.with_columns(
            pl.when(pl.col("status") == "succeeded")
            .then(pl.col(column) + 1)
            .otherwise(pl.col(column))
            .alias(column)
            for column in ("total_count", "succeeded_count")
        )
    rows.write_delta(path, mode="overwrite")
    with pytest.raises(StoreIntegrityError):
        list_retained_runs(committer)
    with pytest.raises(StoreIntegrityError):
        resolve_retained_step_number(committer, "synthetic-pipeline", "irrelevant")


def test_lifecycle_read_rejects_a_snapshot_from_another_store(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    other = create_retained_store(
        str(tmp_path / "other-store"),
        str(tmp_path / "other-files"),
        fs=StorageConfig().filesystem(),
    )
    with (
        other._read_snapshot() as snapshot,
        pytest.raises(StoreIntegrityError, match="another store"),
    ):
        RetainedStepTracker(committer, "run")._read_lifecycle(snapshot)


def test_public_inspection_refreshes_retry_membership_and_keeps_protected_origins(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    from artisan.orchestration.run_history import list_runs
    from artisan.orchestration.run_status import resolve_step_number, run_status
    from artisan.storage.core.artifact_query import query_artifacts

    case = retained_case(tmp_path, kind="alias")
    results, admission, receipts = prepare_scientific(committer, case)
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    root = committer.delta_root
    run_id = "synthetic-pipeline"
    assert list_runs(root)["pipeline_run_id"].to_list() == [run_id]
    assert run_status(root, run_id).last_status == StepStatus.SUCCEEDED
    name = results.tracker.current_state(admission.step_run_id).step_name
    assert resolve_step_number(root, run_id, name) == 7
    selected = query_artifacts(root, pipeline_run_id=run_id)
    assert len(selected) == 1
    assert selected[0].current_step_number == 7
    assert selected[0].origin_step_number == 1
    global_before = query_artifacts(root)
    assert global_before
    assert all(item.current_step_number is None for item in global_before)

    previous = results.tracker.current_state(admission.step_run_id)
    retry = StepStartRecord(
        step_run_id="b" * 32,
        step_number=7,
        step_name=name,
        operation_class=previous.operation_class,
        params_json=previous.params_json,
        input_refs_json=previous.input_refs_json,
        compute_backend=previous.compute_backend,
        compute_options_json=previous.compute_options_json,
        output_roles_json='["data"]',
        output_types_json='{"data": "csv"}',
    )
    results.tracker.create_attempt(retry)
    assert run_status(root, run_id).last_status == StepStatus.PENDING
    assert list_runs(root)["step_count"].to_list() == [1]
    assert query_artifacts(root, pipeline_run_id=run_id) == []
    assert query_artifacts(root) == global_before
    assert query_artifacts(root, pipeline_run_id="unknown") == []
