"""Integration tests for pipeline cancellation.

Tests cancel() during execution, finalize() after cancel, and
skip cascade behaviour with real operations.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.slow

from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend


def test_cancel_before_any_steps(pipeline_env: dict[str, Path]):
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
        backend=Backend.LOCAL,
    )

    assert (
        result.metadata.get("skipped") is True
        or result.metadata.get("cancelled") is True
    )
    assert result.succeeded_count == 0

    summary = pipeline.finalize()
    assert "pipeline_name" in summary
    assert "overall_success" in summary


def test_cancel_skips_downstream_steps(pipeline_env: dict[str, Path]):
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
        backend=Backend.LOCAL,
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
        backend=Backend.LOCAL,
    )

    assert step1.metadata.get("skipped") is True
    assert step1.metadata.get("skip_reason") == "cancelled"

    summary = pipeline.finalize()
    assert summary["total_steps"] == 2


def test_cancel_during_submit(pipeline_env: dict[str, Path]):
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
        backend=Backend.LOCAL,
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
        backend=Backend.LOCAL,
    )

    result = future.result(timeout=10)
    assert (
        result.metadata.get("skipped") is True
        or result.metadata.get("cancelled") is True
    )

    summary = pipeline.finalize()
    assert "pipeline_name" in summary


def test_finalize_after_cancel_returns_cleanly(pipeline_env: dict[str, Path]):
    """finalize() after cancel returns a valid summary dict."""
    pipeline = PipelineManager.create(
        name="test_finalize_cancel",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )

    pipeline.cancel()

    summary = pipeline.finalize()

    assert summary["pipeline_name"] == "test_finalize_cancel"
    assert summary["total_steps"] == 1
    assert "overall_success" in summary
    assert "steps" in summary
    assert len(summary["steps"]) == 1


def test_cancel_idempotent(pipeline_env: dict[str, Path]):
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


def test_cancel_event_reaches_dispatch_handle(pipeline_env: dict[str, Path]):
    """cancel() during a running step flows through handle.run(cancel_event).

    Submits a slow Wait step (30s), cancels after 1s, and verifies the
    pipeline finishes quickly — proving the cancel_event reached the
    dispatch handle's run() poll loop rather than blocking for 30s.
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
    future = pipeline.submit(
        Wait,
        params={"duration": 30},
        backend=Backend.LOCAL,
    )
    summary = pipeline.finalize()
    elapsed = time.monotonic() - start

    # Should finish much faster than 30s — cancel interrupted the wait
    assert elapsed < 15.0
    assert "pipeline_name" in summary
