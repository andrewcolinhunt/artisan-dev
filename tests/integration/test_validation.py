"""Integration tests for fail-fast input/override validation.

Validates that PipelineManager.run()/submit() raise ValueError
for bad params, resources, execution, input roles, required inputs,
and input type mismatches — all BEFORE any predecessor wait or execution.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.slow

from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend


def test_invalid_params_raises(pipeline_env: dict[str, Path]):
    """Unknown param keys raise ValueError before execution."""
    pipeline = PipelineManager.create(
        name="test_invalid_params",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    with pytest.raises(ValueError, match="Unknown params"):
        pipeline.run(
            DataGenerator,
            params={"count": 2, "nonexistent_param": 42},
            backend=Backend.LOCAL,
        )

    summary = pipeline.finalize()
    assert summary["total_steps"] == 0


def test_invalid_resources_raises(pipeline_env: dict[str, Path]):
    """Unknown resource keys raise ValueError before execution."""
    pipeline = PipelineManager.create(
        name="test_invalid_resources",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    with pytest.raises(ValueError, match="Unknown resource keys"):
        pipeline.run(
            DataGenerator,
            params={"count": 2},
            resources={"bogus_resource": 99},
            backend=Backend.LOCAL,
        )


def test_invalid_execution_raises(pipeline_env: dict[str, Path]):
    """Unknown execution keys raise ValueError before execution."""
    pipeline = PipelineManager.create(
        name="test_invalid_execution",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    with pytest.raises(ValueError, match="Unknown execution keys"):
        pipeline.run(
            DataGenerator,
            params={"count": 2},
            execution={"nonexistent_key": True},
            backend=Backend.LOCAL,
        )


def test_invalid_input_role_raises(pipeline_env: dict[str, Path]):
    """Unknown input role raises ValueError before execution."""
    pipeline = PipelineManager.create(
        name="test_invalid_input_role",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )

    with pytest.raises(ValueError, match="Unknown input roles"):
        pipeline.run(
            DataTransformer,
            inputs={"nonexistent_role": step0.output("datasets")},
            backend=Backend.LOCAL,
        )


def test_missing_required_input_raises(pipeline_env: dict[str, Path]):
    """Missing required input role raises ValueError before execution."""
    pipeline = PipelineManager.create(
        name="test_missing_required",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    with pytest.raises(ValueError, match="Missing required input"):
        pipeline.run(
            DataTransformer,
            inputs={},
            backend=Backend.LOCAL,
        )


def test_input_type_mismatch_raises(pipeline_env: dict[str, Path]):
    """Type mismatch between upstream output and downstream input raises ValueError."""
    pipeline = PipelineManager.create(
        name="test_type_mismatch",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )

    step1 = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        backend=Backend.LOCAL,
    )

    # MetricCalculator outputs "metrics" (type=metric), but DataTransformer
    # expects "dataset" (type=data). Wiring metric output to data input
    # should fail type validation.
    with pytest.raises(ValueError, match="Type mismatch"):
        pipeline.run(
            DataTransformer,
            inputs={"dataset": step1.output("metrics")},
            backend=Backend.LOCAL,
        )


def test_valid_overrides_accepted(pipeline_env: dict[str, Path]):
    """Valid overrides don't raise and pipeline succeeds."""
    pipeline = PipelineManager.create(
        name="test_valid_overrides",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        resources={"cpus": 2, "memory_gb": 8},
        execution={"artifacts_per_unit": 1},
        backend=Backend.LOCAL,
    )

    assert step0.success is True

    summary = pipeline.finalize()
    assert summary["overall_success"] is True
