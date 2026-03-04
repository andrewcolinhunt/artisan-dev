"""Integration tests for step persistence, caching, and async submit.

Tests the full steps Delta Lake table lifecycle with real operations:
1. Cache hit: second run with same params skips execution
2. Cache miss: different params causes re-execution
3. Upstream invalidation: changed upstream causes downstream re-execution
4. Resume: reconstruct pipeline state from delta
5. Async submit: non-blocking step execution with StepFuture
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

pytestmark = pytest.mark.slow

from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend

from .conftest import read_table

# =============================================================================
# Test 1: Cache Hit
# =============================================================================


def test_cache_hit(pipeline_env: dict[str, Path]) -> None:
    """Second identical run skips execution via cache."""
    delta = pipeline_env["delta_root"]
    staging = pipeline_env["staging_root"]
    working = pipeline_env["working_root"]

    # First run
    p1 = PipelineManager.create(
        name="test_cache",
        delta_root=delta,
        staging_root=staging,
        working_root=working,
    )
    result1 = p1.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )
    p1.finalize()

    # Count steps rows after first run
    steps_df1 = read_table(delta, "orchestration/steps")
    first_run_rows = len(steps_df1)
    assert first_run_rows >= 2  # at least running + completed

    # Second run — same operation, same params, same step position
    p2 = PipelineManager.create(
        name="test_cache",
        delta_root=delta,
        staging_root=staging,
        working_root=working,
    )
    result2 = p2.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )
    p2.finalize()

    # Cache hit: same step name, same success
    assert result2.step_name == result1.step_name
    assert result2.success is True

    # Cache hit should NOT add new running/completed rows
    steps_df2 = read_table(delta, "orchestration/steps")
    assert len(steps_df2) == first_run_rows


# =============================================================================
# Test 2: Cache Miss (different params)
# =============================================================================


def test_cache_miss_different_params(pipeline_env: dict[str, Path]) -> None:
    """Different params cause re-execution."""
    delta = pipeline_env["delta_root"]
    staging = pipeline_env["staging_root"]
    working = pipeline_env["working_root"]

    # First run
    p1 = PipelineManager.create(
        name="test_miss",
        delta_root=delta,
        staging_root=staging,
        working_root=working,
    )
    p1.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )
    p1.finalize()

    # Second run with different params
    p2 = PipelineManager.create(
        name="test_miss",
        delta_root=delta,
        staging_root=staging,
        working_root=working,
    )
    result = p2.run(
        DataGenerator,
        params={"count": 3, "seed": 99},
        backend=Backend.LOCAL,
    )
    p2.finalize()

    assert result.success is True
    assert result.total_count >= 1


# =============================================================================
# Test 3: Upstream Invalidation
# =============================================================================


def test_upstream_invalidation(pipeline_env: dict[str, Path]) -> None:
    """Changed upstream params invalidate downstream cache."""
    delta = pipeline_env["delta_root"]
    staging = pipeline_env["staging_root"]
    working = pipeline_env["working_root"]

    # First run: Generate -> Transform
    p1 = PipelineManager.create(
        name="test_invalidate",
        delta_root=delta,
        staging_root=staging,
        working_root=working,
    )
    step0 = p1.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )
    p1.run(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.1,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )
    p1.finalize()

    steps_df1 = read_table(delta, "orchestration/steps")
    completed_1 = steps_df1.filter(pl.col("status") == "completed")
    count_1 = len(completed_1)

    # Second run: different seed on step 0 → step 1 should also re-execute
    p2 = PipelineManager.create(
        name="test_invalidate",
        delta_root=delta,
        staging_root=staging,
        working_root=working,
    )
    step0b = p2.run(
        DataGenerator,
        params={"count": 2, "seed": 99},  # different seed
        backend=Backend.LOCAL,
    )
    result = p2.run(
        DataTransformer,
        inputs={"dataset": step0b.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.1,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )
    p2.finalize()

    # Both steps re-executed → new completed rows
    steps_df2 = read_table(delta, "orchestration/steps")
    completed_2 = steps_df2.filter(pl.col("status") == "completed")
    assert len(completed_2) > count_1
    assert result.success is True


# =============================================================================
# Test 4: Resume
# =============================================================================


def test_resume(pipeline_env: dict[str, Path]) -> None:
    """Resume reconstructs pipeline state from delta."""
    delta = pipeline_env["delta_root"]
    staging = pipeline_env["staging_root"]
    working = pipeline_env["working_root"]

    # Run a 2-step pipeline
    p1 = PipelineManager.create(
        name="test_resume",
        delta_root=delta,
        staging_root=staging,
        working_root=working,
    )
    run_id = p1.config.pipeline_run_id
    step0 = p1.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )
    p1.run(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.1,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )
    p1.finalize()

    # Resume
    p2 = PipelineManager.resume(
        delta_root=delta,
        staging_root=staging,
        pipeline_run_id=run_id,
    )
    assert p2.current_step == 2
    assert len(p2._step_results) == 2

    # Can chain from resumed state
    ref = p2[1].output("dataset")
    assert ref.source_step == 1
    assert ref.role == "dataset"


# =============================================================================
# Test 5: Async Submit
# =============================================================================


def test_async_submit(pipeline_env: dict[str, Path]) -> None:
    """submit() returns StepFuture that resolves to StepResult."""
    delta = pipeline_env["delta_root"]
    staging = pipeline_env["staging_root"]
    working = pipeline_env["working_root"]

    pipeline = PipelineManager.create(
        name="test_submit",
        delta_root=delta,
        staging_root=staging,
        working_root=working,
    )

    future = pipeline.submit(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )

    # output() returns immediately (never blocks)
    ref = future.output("datasets")
    assert ref.source_step == 0
    assert ref.role == "datasets"

    # result() blocks until done
    result = future.result()
    assert result.success is True
    assert result.step_name == "data_generator"

    pipeline.finalize()


# =============================================================================
# Test 6: List Runs
# =============================================================================


def test_list_runs(pipeline_env: dict[str, Path]) -> None:
    """list_runs() returns DataFrame with completed runs."""
    delta = pipeline_env["delta_root"]
    staging = pipeline_env["staging_root"]
    working = pipeline_env["working_root"]

    # Create a pipeline run first
    p = PipelineManager.create(
        name="test_list",
        delta_root=delta,
        staging_root=staging,
        working_root=working,
    )
    p.run(
        DataGenerator,
        params={"count": 1, "seed": 42},
        backend=Backend.LOCAL,
    )
    p.finalize()

    runs = PipelineManager.list_runs(delta)
    assert isinstance(runs, pl.DataFrame)
    assert "pipeline_run_id" in runs.columns
    assert len(runs) >= 1
