"""Integration tests for pipeline cancellation.

Tests cancel() during execution, finalize() after cancel, and
skip cascade behaviour with real operations.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners import LocalRunner
from artisan.schemas.orchestration.step_lifecycle import StepStatus


def test_cancel_before_any_steps(pipeline_env: dict[str, str]):
    """cancel() before running steps confirms queued cancellation."""
    pipeline = PipelineManager.create(
        name="test_cancel_before",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    pipeline.cancel()

    result = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        step_runner=LocalRunner(),
    )

    assert result.status is StepStatus.CANCELLED
    assert result.succeeded_count == 0

    summary = pipeline.finalize()
    assert "pipeline_name" in summary
    assert "overall_success" in summary


def test_cancel_marks_downstream_steps_cancelled(pipeline_env: dict[str, str]):
    """Cancelling after step 0 marks the subsequent step cancelled."""
    pipeline = PipelineManager.create(
        name="test_cancel_downstream",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        step_runner=LocalRunner(),
    )
    assert step0.status is StepStatus.SUCCEEDED

    pipeline.cancel()

    step1 = pipeline.run(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
        step_runner=LocalRunner(),
    )

    assert step1.status is StepStatus.CANCELLED

    summary = pipeline.finalize()
    assert summary["total_steps"] == 2


def test_cancel_before_submit(pipeline_env: dict[str, str]):
    """Cancelling before submission returns a cancelled future."""
    pipeline = PipelineManager.create(
        name="test_cancel_during",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        step_runner=LocalRunner(),
    )

    pipeline.cancel()

    future = pipeline.submit(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
        step_runner=LocalRunner(),
    )

    result = future.result(timeout=10)
    assert result.status is StepStatus.CANCELLED

    summary = pipeline.finalize()
    assert "pipeline_name" in summary


def test_finalize_after_cancel_returns_cleanly(pipeline_env: dict[str, str]):
    """finalize() after cancel returns a valid summary dict."""
    pipeline = PipelineManager.create(
        name="test_finalize_cancel",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        step_runner=LocalRunner(),
    )

    pipeline.cancel()

    summary = pipeline.finalize()

    assert summary["pipeline_name"] == "test_finalize_cancel"
    assert summary["total_steps"] == 1
    assert "overall_success" in summary
    assert "steps" in summary
    assert len(summary["steps"]) == 1


def test_cancel_idempotent(pipeline_env: dict[str, str]):
    """Calling cancel() multiple times is safe."""
    pipeline = PipelineManager.create(
        name="test_cancel_idempotent",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    pipeline.cancel()
    pipeline.cancel()
    pipeline.cancel()

    summary = pipeline.finalize()
    assert "pipeline_name" in summary


def test_cancel_event_reaches_lifecycle_router(pipeline_env: dict[str, str]):
    """cancel() during a running step flows through handle.run(cancel_event).

    Submits a slow Wait step (30s), cancels after 1s, and verifies the
    pipeline finishes quickly — proving the cancel_event reached the
    lifecycle router's run() poll loop rather than blocking for 30s.
    """
    import threading
    import time

    from artisan.operations.examples import Wait

    pipeline = PipelineManager.create(
        name="test_cancel_reaches_handle",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    def _cancel_after_delay():
        time.sleep(1.0)
        pipeline.cancel()

    threading.Thread(target=_cancel_after_delay, daemon=True).start()

    start = time.monotonic()
    pipeline.submit(
        Wait,
        params={"duration": 30},
        step_runner=LocalRunner(),
    )
    summary = pipeline.finalize()
    elapsed = time.monotonic() - start

    # Should finish much faster than 30s — cancel interrupted the wait
    assert elapsed < 15.0
    assert "pipeline_name" in summary


def test_cancel_after_terminal_selection_preserves_completed_result(
    pipeline_env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Late cancellation finishes the exact selected result before replying."""
    from artisan.orchestration.engine.retained_step_results import RetainedStepResults
    from artisan.schemas.execution.worker_ownership import OwnershipState
    from artisan.schemas.orchestration.step_lifecycle import CancellationStatus
    from artisan.storage.core.execution_source import (
        ScientificExecutionSource,
        read_execution_source,
    )

    pipeline = PipelineManager.create(name="test_late_cancel", **pipeline_env)
    observations: list[StepStatus] = []

    def cancel_after_selection(results: RetainedStepResults, event: str) -> None:
        if event == "plan":
            pipeline.cancel()
            observations.append(
                pipeline.tracker.current_state(
                    pipeline.executions[0].record.step_run_id
                ).status
            )

    monkeypatch.setattr(RetainedStepResults, "_checkpoint", cancel_after_selection)
    result = pipeline.run(DataGenerator, params={"count": 2, "seed": 42})
    pipeline.finalize()

    assert observations == [StepStatus.SUCCEEDED]
    assert result.status is StepStatus.SUCCEEDED, result.error
    assert pipeline.ownership_state() is OwnershipState.CLOSED
    execution = pipeline.executions[0]
    assert execution.cancel().status is CancellationStatus.REJECTED
    receipt = execution.handles[0].result()
    source = read_execution_source(pipeline.committer, receipt.delivery.delivery_id)
    assert isinstance(source, ScientificExecutionSource)
    assert source.plan.receipt == receipt


def test_finalize_terminalizes_running_and_queued_cancellations(
    pipeline_env: dict[str, str],
) -> None:
    """Bounded finalization leaves no started step permanently running."""
    import time
    from pathlib import Path

    import polars as pl

    from artisan.operations.examples import Wait

    pipeline = PipelineManager.create(
        name="test_finalize_terminal_cancellation",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )
    running = pipeline.submit(
        Wait,
        name="running",
        params={"duration": 6.0},
        step_runner=LocalRunner(),
    )

    working_root = Path(pipeline_env["working_root"])
    deadline = time.monotonic() + 30
    while not any(working_root.rglob("execute")):
        assert not running.done, running.result() if running.done else None
        if time.monotonic() >= deadline:
            pipeline.cancel()
            pipeline.close()
            pytest.fail("Qualified Wait worker did not reach its execute phase")
        time.sleep(0.05)

    queued = pipeline.submit(
        Wait,
        name="queued",
        params={"duration": 0.0},
        step_runner=LocalRunner(),
    )
    pipeline.cancel()

    summary = pipeline.finalize()

    assert summary["total_steps"] == 2
    assert [step["name"] for step in summary["steps"]] == ["running", "queued"]
    assert all(result.status is StepStatus.CANCELLED for result in pipeline)
    assert queued.status is StepStatus.CANCELLED
    assert queued.result().status is StepStatus.CANCELLED

    steps = pl.read_delta(Path(pipeline_env["delta_root"]) / "orchestration/steps")
    for step_number in (0, 1):
        statuses = steps.filter(pl.col("step_number") == step_number)[
            "status"
        ].to_list()
        assert "cancelled" in statuses

    running.result(timeout=5)
    assert pipeline.finalize() is summary
    assert len(pipeline) == 2
