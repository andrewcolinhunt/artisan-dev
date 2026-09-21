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
        step_runner=Runner.LOCAL,
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
        step_runner=Runner.LOCAL,
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
        step_runner=Runner.LOCAL,
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


@pytest.mark.parametrize("preserve_staging", [False, True])
@pytest.mark.parametrize("recover_staging", [False, True])
def test_cancelled_creator_retains_sealed_work_until_verified_commit(
    pipeline_env: dict[str, str],
    monkeypatch,
    preserve_staging,
    recover_staging,
):
    """Cancel at a real worker's seal boundary, then prove optional reuse."""
    from pathlib import Path

    import polars as pl
    from fsspec.implementations.local import LocalFileSystem

    from artisan.orchestration.engine import step_executor
    from artisan.schemas.enums import TablePath
    from artisan.schemas.orchestration.step_lifecycle import StepDisposition
    from artisan.storage.core.committed_scan import read_committed
    from artisan.visualization import inspect_step

    pipeline = PipelineManager.create(
        name="test_cancelled_staging",
        **pipeline_env,
        recover_staging=False,
        preserve_staging=preserve_staging,
    )
    original_capture = step_executor.persist_worker_logs
    snapshots = {}

    def cancel_after_worker_sealed(*args, **kwargs):
        original_capture(*args, **kwargs)
        root = Path(pipeline_env["staging_root"])
        snapshots.update({path: path.read_bytes() for path in root.rglob("*.parquet")})
        assert any(path.name == "executions.parquet" for path in snapshots)
        pipeline.cancel()

    monkeypatch.setattr(
        step_executor, "persist_worker_logs", cancel_after_worker_sealed
    )
    cancelled = pipeline.run(DataGenerator, params={"count": 2, "seed": 42})
    pipeline.finalize()
    monkeypatch.setattr(step_executor, "persist_worker_logs", original_capture)

    fs = LocalFileSystem()
    root = pipeline_env["delta_root"]
    assert cancelled.status is StepStatus.CANCELLED
    assert read_committed(root, TablePath.EXECUTIONS, fs=fs).is_empty()
    assert {path: path.read_bytes() for path in snapshots} == snapshots
    assert inspect_step(
        root, 0, pipeline_run_id=pipeline.config.pipeline_run_id
    ).is_empty()

    rerun = PipelineManager.create(
        name="test_cancelled_staging",
        **pipeline_env,
        recover_staging=recover_staging,
        preserve_staging=preserve_staging,
    )
    result = rerun.run(DataGenerator, params={"count": 2, "seed": 42})
    rerun.finalize()

    expected = (
        StepDisposition.CACHE_HIT if recover_staging else StepDisposition.EXECUTED
    )
    assert result.disposition is expected
    assert (
        inspect_step(root, 0, pipeline_run_id=rerun.config.pipeline_run_id).height == 2
    )
    assert inspect_step(
        root, 0, pipeline_run_id=pipeline.config.pipeline_run_id
    ).is_empty()
    history = (
        read_committed(root, TablePath.STEPS, fs=fs)
        .filter(pl.col("step_run_id") == cancelled.step_run_id)
        .sort("state_sequence")
    )
    assert history.item(-1, "status") == "cancelled"
    executions = read_committed(root, TablePath.EXECUTIONS, fs=fs)
    owner = cancelled.step_run_id if recover_staging else result.step_run_id
    assert executions["step_run_id"].to_list() == [owner]
    if not recover_staging or preserve_staging:
        assert {path: path.read_bytes() for path in snapshots} == snapshots
    else:
        assert not any(path.exists() for path in snapshots)


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
    assert all(result.status is StepStatus.CANCELLED for result in pipeline)
    assert queued.status is StepStatus.CANCELLED
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
