"""Integration tests for pipeline cancellation.

Tests cancel() during execution, finalize() after cancel, and
skip cascade behaviour with real operations.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners import Runner


def test_cancel_before_any_steps(pipeline_env: dict[str, str]):
    """cancel() before running steps causes subsequent steps to skip."""
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
        step_runner=Runner.LOCAL,
    )

    assert (
        result.metadata.get("skipped") is True
        or result.metadata.get("cancelled") is True
    )
    assert result.succeeded_count == 0

    summary = pipeline.finalize()
    assert "pipeline_name" in summary
    assert "overall_success" in summary


def test_cancel_skips_downstream_steps(pipeline_env: dict[str, str]):
    """Cancelling after step 0 causes step 1 to be skipped."""
    pipeline = PipelineManager.create(
        name="test_cancel_downstream",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        step_runner=Runner.LOCAL,
    )
    assert step0.success is True

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
        step_runner=Runner.LOCAL,
    )

    assert step1.metadata.get("skipped") is True
    assert step1.metadata.get("skip_reason") == "cancelled"

    summary = pipeline.finalize()
    assert summary["total_steps"] == 2


def test_cancel_during_submit(pipeline_env: dict[str, str]):
    """cancel() after submit() causes the queued step to skip."""
    pipeline = PipelineManager.create(
        name="test_cancel_during",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        step_runner=Runner.LOCAL,
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
        step_runner=Runner.LOCAL,
    )

    result = future.result(timeout=10)
    assert (
        result.metadata.get("skipped") is True
        or result.metadata.get("cancelled") is True
    )

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
        step_runner=Runner.LOCAL,
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
        step_runner=Runner.LOCAL,
    )
    summary = pipeline.finalize()
    elapsed = time.monotonic() - start

    # Should finish much faster than 30s — cancel interrupted the wait
    assert elapsed < 15.0
    assert "pipeline_name" in summary


def test_cancelled_creator_staging_is_not_recovered_or_cached(
    pipeline_env: dict[str, str],
):
    """Successful in-flight work from a cancelled step stays invisible."""
    import time
    from pathlib import Path

    import polars as pl

    from artisan.operations.examples import Wait

    pipeline = PipelineManager.create(
        name="test_cancelled_staging",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )
    future = pipeline.submit(
        Wait,
        params={"duration": 1.0},
        step_runner=Runner.LOCAL,
    )

    working_root = Path(pipeline_env["working_root"])
    deadline = time.monotonic() + 10
    while not any(working_root.rglob("execute")):
        assert time.monotonic() < deadline, "Wait worker did not start"
        time.sleep(0.05)

    pipeline.cancel()
    cancelled = future.result(timeout=10)
    pipeline.finalize()

    executions_path = Path(pipeline_env["delta_root"]) / "orchestration/executions"
    assert cancelled.metadata["cancelled"] is True
    assert not executions_path.exists()
    assert not list(Path(pipeline_env["staging_root"]).rglob("*.parquet"))

    rerun = PipelineManager.create(
        name="test_cancelled_staging",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )
    result = rerun.run(
        Wait,
        params={"duration": 1.0},
        step_runner=Runner.LOCAL,
    )
    rerun.finalize()

    executions = pl.read_delta(executions_path)
    assert result.succeeded_count == 1
    assert executions.height == 1
    assert executions.item(0, "step_run_id") == rerun._step_run_ids[0]


def test_finalize_terminalizes_running_and_queued_cancellations(
    pipeline_env: dict[str, str],
) -> None:
    """Bounded finalization leaves no started step permanently running."""
    import time
    from concurrent.futures import CancelledError
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
        step_runner=Runner.LOCAL,
    )

    working_root = Path(pipeline_env["working_root"])
    deadline = time.monotonic() + 10
    while not any(working_root.rglob("execute")):
        assert time.monotonic() < deadline, "Wait worker did not start"
        time.sleep(0.05)

    queued = pipeline.submit(
        Wait,
        name="queued",
        params={"duration": 0.0},
        step_runner=Runner.LOCAL,
    )
    pipeline.cancel()

    summary = pipeline.finalize()

    assert summary["total_steps"] == 2
    assert [step["name"] for step in summary["steps"]] == ["running", "queued"]
    assert all(result.metadata["cancelled"] is True for result in pipeline)
    assert queued.status == "cancelled"
    with pytest.raises(CancelledError):
        queued.result()

    steps = pl.read_delta(Path(pipeline_env["delta_root"]) / "orchestration/steps")
    for step_number in (0, 1):
        statuses = steps.filter(pl.col("step_number") == step_number)[
            "status"
        ].to_list()
        assert "cancelled" in statuses

    running.result(timeout=5)
    assert pipeline.finalize() is summary
    assert len(pipeline) == 2
