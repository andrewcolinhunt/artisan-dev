"""Curator replay preserves confirmed and uncertain cancellation evidence."""

from __future__ import annotations

import polars as pl
import pytest

from artisan.execution.recording.parquet_writer import StagingResult
from artisan.operations.curator.ingest_data import IngestData
from artisan.orchestration import PipelineManager, replay_execution
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.core.committed_scan import read_committed

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [
        (CancellationStatus.UNKNOWN, StepStatus.FAILED),
        (CancellationStatus.CONFIRMED, StepStatus.CANCELLED),
    ],
)
def test_curator_replay_cancellation_commits_no_worker_outputs(
    tmp_path, monkeypatch, evidence, expected
):
    source_file = tmp_path / "input.csv"
    source_file.write_text("x,y\n1,2\n")
    with PipelineManager.create(
        "source", str(tmp_path / "delta"), str(tmp_path / "staging")
    ) as source:
        original = source.run(IngestData, inputs=[str(source_file)], compact=False)
        assert original.status is StepStatus.SUCCEEDED
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "debug" / "staging"),
        working_root=str(tmp_path / "debug" / "work"),
        files_root=str(tmp_path / "debug" / "files"),
        failure_logs_root=str(tmp_path / "debug" / "logs"),
    )
    fs = runtime.storage.filesystem()
    before = read_committed(runtime.delta_root, TablePath.EXECUTIONS, fs=fs)

    def cancelled_curator(unit, runtime, event):
        event.set()
        return StagingResult(
            success=False,
            error="Curator cancellation evidence",
            cancellation_acknowledgement=CancellationAcknowledgement(
                evidence, "Curator cancellation evidence"
            ),
        )

    monkeypatch.setattr(
        "artisan.orchestration.engine.step_executor._run_curator_in_subprocess",
        cancelled_curator,
    )
    result = replay_execution(before["execution_run_id"][0], runtime=runtime)
    assert result.step_result.status is expected
    assert result.step_result.cancellation_status is evidence
    assert result.execution_run_id is None
    assert result.diagnostic_status == "unavailable"
    assert read_committed(runtime.delta_root, TablePath.EXECUTIONS, fs=fs).equals(
        before
    )
    snapshots = (
        read_committed(runtime.delta_root, TablePath.STEPS, fs=fs)
        .filter(pl.col("step_run_id") == result.step_run_id)
        .sort("state_sequence")
    )
    assert snapshots["status"][-1] == expected.value
    assert snapshots["cancellation_status"].to_list() == [
        None,
        None,
        "requested",
        evidence.value,
        evidence.value,
    ]
    assert set(snapshots["replay_of_execution_run_id"]) == {
        result.source_execution_run_id
    }
    assert not list((tmp_path / "debug" / "staging").rglob("*.parquet"))


@pytest.mark.parametrize("prepared", [False, True])
@pytest.mark.parametrize("race_at", ["requested", "unknown", "failed"])
def test_unknown_cancellation_preserves_terminal_winner_during_persistence(
    tmp_path, monkeypatch, prepared, race_at
):
    source_file = tmp_path / "input.csv"
    source_file.write_text("x,y\n1,2\n")
    roots = {
        "delta_root": str(tmp_path / "delta"),
        "staging_root": str(tmp_path / "staging"),
    }
    with PipelineManager.create("source", **roots) as source:
        source.run(IngestData, inputs=[str(source_file)], compact=False)
    runtime = RuntimeEnvironment(
        delta_root=roots["delta_root"],
        staging_root=str(tmp_path / "debug" / "staging"),
        working_root=str(tmp_path / "debug" / "work"),
        files_root=str(tmp_path / "debug" / "files"),
        failure_logs_root=str(tmp_path / "debug" / "logs"),
    )
    before = read_committed(
        runtime.delta_root, TablePath.EXECUTIONS, fs=runtime.storage.filesystem()
    )
    record_cancellation = StepTracker.record_cancellation
    transition = StepTracker.transition
    winners = []

    def finish_attempt(tracker, step_run_id):
        current = tracker.current_state(step_run_id)
        evidence = current.cancellation_status
        status = StepStatus.FAILED
        if evidence is not CancellationStatus.UNKNOWN:
            if evidence is None:
                record_cancellation(
                    tracker,
                    step_run_id,
                    StepStatus.RUNNING,
                    CancellationAcknowledgement(CancellationStatus.REQUESTED),
                )
            evidence = CancellationStatus.CONFIRMED
            record_cancellation(
                tracker,
                step_run_id,
                StepStatus.RUNNING,
                CancellationAcknowledgement(evidence),
            )
            status = StepStatus.CANCELLED
        winner = StepResult(
            step_name=current.step_name,
            step_number=current.step_number,
            step_run_id=step_run_id,
            status=status,
            cancellation_status=evidence,
            error="Terminal result recorded first",
            duration_seconds=3.25,
        )
        transition(tracker, step_run_id, StepStatus.RUNNING, status, result=winner)
        winners.append(winner)

    def racing_evidence(tracker, step_run_id, expected, acknowledgement):
        if acknowledgement.status.value == race_at:
            finish_attempt(tracker, step_run_id)
        return record_cancellation(tracker, step_run_id, expected, acknowledgement)

    def racing_transition(tracker, step_run_id, expected, target, **kwargs):
        if target is StepStatus.FAILED and race_at == "failed":
            finish_attempt(tracker, step_run_id)
        return transition(tracker, step_run_id, expected, target, **kwargs)

    def uncertain_curator(unit, runtime, event):
        event.set()
        return StagingResult(
            success=False,
            error="Late unknown outcome",
            cancellation_acknowledgement=CancellationAcknowledgement(
                CancellationStatus.UNKNOWN, "Late unknown outcome"
            ),
        )

    monkeypatch.setattr(StepTracker, "record_cancellation", racing_evidence)
    monkeypatch.setattr(StepTracker, "transition", racing_transition)
    monkeypatch.setattr(
        "artisan.orchestration.engine.step_executor._run_curator_in_subprocess",
        uncertain_curator,
    )
    if prepared:
        replay = replay_execution(before["execution_run_id"][0], runtime=runtime)
        result = replay.step_result
    else:
        with PipelineManager.create("ordinary", **roots) as pipeline:
            future = pipeline.submit(IngestData, name="custom", compact=False)
            result = future.result(timeout=10)
            assert future.status is result.status
            assert len(pipeline) == 1
    assert winners == [result]
    assert (
        StepTracker(runtime.delta_root)
        .current_state(result.step_run_id)
        .to_step_result()
        == result
    )
    assert read_committed(
        runtime.delta_root, TablePath.EXECUTIONS, fs=runtime.storage.filesystem()
    ).equals(before)
