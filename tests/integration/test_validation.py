"""Integration tests for fail-fast input/override validation.

Validates that PipelineManager.run()/submit() raise ValueError
for bad params, resources, execution, input roles, required inputs,
and input type mismatches — all BEFORE any predecessor wait or execution.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
from artisan.schemas.orchestration.step_lifecycle import StepStatus

from .conftest import create_native_pipeline


def test_invalid_params_raises(pipeline_env: dict[str, str]):
    """Unknown param keys raise ValueError before execution."""
    pipeline = create_native_pipeline(
        name="test_invalid_params",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    with pytest.raises(ValueError, match="nonexistent_param"):
        pipeline.run(
            DataGenerator,
            params={"count": 2, "nonexistent_param": 42},
        )

    summary = pipeline.finalize()
    assert summary["total_steps"] == 0


def test_invalid_resources_raises(pipeline_env: dict[str, str]):
    """Unknown resource keys raise ValueError before execution."""
    pipeline = create_native_pipeline(
        name="test_invalid_resources",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    with pytest.raises(ValueError, match="bogus_resource"):
        pipeline.run(
            DataGenerator,
            params={"count": 2},
            runner_resources={"bogus_resource": 99},
        )


def test_invalid_execution_raises(pipeline_env: dict[str, str]):
    """Unknown execution keys raise ValueError before execution."""
    pipeline = create_native_pipeline(
        name="test_invalid_execution",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    with pytest.raises(ValueError, match="nonexistent_key"):
        pipeline.run(
            DataGenerator,
            params={"count": 2},
            batch_strategy={"nonexistent_key": True},
        )


def test_invalid_input_role_raises(pipeline_env: dict[str, str]):
    """Unknown input role raises ValueError before execution."""
    pipeline = create_native_pipeline(
        name="test_invalid_input_role",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
    )

    with pytest.raises(ValueError, match="undeclared roles"):
        pipeline.run(
            DataTransformer,
            inputs={"nonexistent_role": step0.output("datasets")},
        )


def test_missing_required_input_raises(pipeline_env: dict[str, str]):
    """Missing required input role raises ValueError before execution."""
    pipeline = create_native_pipeline(
        name="test_missing_required",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    with pytest.raises(ValueError, match="missing required"):
        pipeline.run(
            DataTransformer,
            inputs={},
        )


def test_input_type_mismatch_raises(pipeline_env: dict[str, str]):
    """Type mismatch between upstream output and downstream input raises ValueError."""
    pipeline = create_native_pipeline(
        name="test_type_mismatch",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
    )

    step1 = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
    )

    with pytest.raises(ValueError, match="incompatible artifact type"):
        pipeline.run(
            DataTransformer,
            inputs={"dataset": step1.output("metrics")},
        )


def test_valid_overrides_accepted(pipeline_env: dict[str, str]):
    """Valid overrides don't raise and pipeline succeeds."""
    pipeline = create_native_pipeline(
        name="test_valid_overrides",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        runner_resources={"cpus": 2, "memory_gb": 8},
        batch_strategy={"artifacts_per_unit": 1},
    )

    assert step0.status is StepStatus.SUCCEEDED

    summary = pipeline.finalize()
    assert summary["overall_success"] is True


def test_known_role_error_rejects_before_waiting_for_pending_predecessor(
    pipeline_env: dict[str, str],
) -> None:
    """A known contract mistake cannot wait on or publish beside a blocked source."""
    with create_native_pipeline(
        name="predecessor-preflight", **pipeline_env
    ) as pipeline:
        runner = pipeline.runner
        runner.release.clear()
        try:
            pending = pipeline.submit(DataGenerator, params={"count": 1})
            assert runner.entered.wait(2)
            with pytest.raises(ValueError, match="undeclared roles"):
                pipeline.submit(
                    DataTransformer, inputs={"unknown": pending.output("datasets")}
                )
            assert len(pipeline.executions) == 1
        finally:
            runner.release.set()
        assert pending.result().status == StepStatus.SUCCEEDED
