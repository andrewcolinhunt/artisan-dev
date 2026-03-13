"""Integration tests for failure policies and resume from failure.

Tests FAIL_FAST and CONTINUE policies with the FailingTransformer operation,
and verifies pipeline resume after a failed run.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.slow

from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend
from artisan.schemas.enums import FailurePolicy

from .conftest import (
    FailingTransformer,
    count_artifacts_by_step,
    get_step_status,
)


def test_fail_fast_policy(pipeline_env: dict[str, Path]):
    """FAIL_FAST: step fails immediately, step status is 'failed'."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_fail_fast",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
        failure_policy=FailurePolicy.FAIL_FAST,
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        backend=Backend.LOCAL,
    )

    step1 = pipeline.run(
        FailingTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={"fail_on_index": 1},
        backend=Backend.LOCAL,
    )

    assert step1.success is False

    # Step status recorded as "failed" in delta
    status = get_step_status(delta_root, 1)
    assert status == "failed"

    result = pipeline.finalize()
    assert result["overall_success"] is False


def test_continue_policy(pipeline_env: dict[str, Path]):
    """CONTINUE: partial failures proceed, successful artifacts persisted."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_continue",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
        failure_policy=FailurePolicy.CONTINUE,
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        backend=Backend.LOCAL,
    )

    step1 = pipeline.run(
        FailingTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={"fail_on_index": 1},
        backend=Backend.LOCAL,
    )

    assert step1.succeeded_count == 2
    assert step1.failed_count == 1

    status = get_step_status(delta_root, 1)
    assert status == "completed"

    # 2 artifacts persisted from the successful executions
    assert count_artifacts_by_step(delta_root, 1) == 2

    # Step 2 processes the 2 successful artifacts
    step2 = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step1.output("dataset")},
        backend=Backend.LOCAL,
    )

    assert step2.success is True
    assert step2.succeeded_count == 2

    result = pipeline.finalize()
    # overall_success is False because step 1 has failed_count > 0
    # (StepResult.success = failed_count == 0)
    assert result["overall_success"] is False


def test_all_executions_fail_continue(pipeline_env: dict[str, Path]):
    """CONTINUE with all failures: step completes, downstream skipped."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_all_fail_continue",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
        failure_policy=FailurePolicy.CONTINUE,
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        backend=Backend.LOCAL,
    )

    step1 = pipeline.run(
        FailingTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={"fail_on_all": True},
        backend=Backend.LOCAL,
    )

    assert step1.succeeded_count == 0
    assert step1.failed_count == 3

    # Step 2: no inputs available → skipped
    step2 = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step1.output("dataset")},
        backend=Backend.LOCAL,
    )

    assert step2.metadata.get("skipped") is True

    result = pipeline.finalize()
    assert result is not None


def test_resume_from_failed_pipeline(pipeline_env: dict[str, Path]):
    """Resume re-executes failed steps while reusing cached successful steps."""
    delta_root = pipeline_env["delta_root"]
    staging_root = pipeline_env["staging_root"]
    working_root = pipeline_env["working_root"]

    # Run 1: FAIL_FAST, step 1 fails
    p1 = PipelineManager.create(
        name="test_resume_fail",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
        failure_policy=FailurePolicy.FAIL_FAST,
    )
    run_id = p1.config.pipeline_run_id

    step0 = p1.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )
    assert step0.success is True

    step1 = p1.run(
        FailingTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={"fail_on_all": True},
        backend=Backend.LOCAL,
    )
    assert step1.success is False
    p1.finalize()

    # Run 2: resume, replace step 1 with DataTransformer, add step 2
    p2 = PipelineManager.resume(
        delta_root=delta_root,
        staging_root=staging_root,
        pipeline_run_id=run_id,
    )

    # Step 0 was restored from cache
    assert p2.current_step >= 1

    # Re-run step 1 with a working operation
    step1b = p2.run(
        DataTransformer,
        inputs={"dataset": p2[0].output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )
    assert step1b.success is True

    # Step 2: MetricCalculator
    step2 = p2.run(
        MetricCalculator,
        inputs={"dataset": step1b.output("dataset")},
        backend=Backend.LOCAL,
    )
    assert step2.success is True

    result = p2.finalize()
    assert result["overall_success"] is True
