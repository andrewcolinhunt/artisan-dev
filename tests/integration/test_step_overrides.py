"""Integration tests for per-step parameter and execution overrides.

Verifies that step-level overrides (params, execution config) work correctly
and remain isolated between steps.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.slow

from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend

from .conftest import (
    count_artifacts_by_step,
    count_executions_by_step,
    get_execution_outputs,
)


def test_param_override_produces_different_outputs(pipeline_env: dict[str, str]):
    """Different param overrides on the same operation produce different artifacts."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_param_override",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: scale_factor=2.0
    step1 = pipeline.run(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 2.0,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )

    # Step 2: scale_factor=5.0
    step2 = pipeline.run(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 5.0,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # Different params → different artifact IDs
    step1_ids = set(get_execution_outputs(delta_root, 1, "dataset"))
    step2_ids = set(get_execution_outputs(delta_root, 2, "dataset"))
    assert (
        step1_ids != step2_ids
    ), "Different scale_factors should produce different artifacts"
    assert len(step1_ids) == 2
    assert len(step2_ids) == 2


def test_execution_override_batching(pipeline_env: dict[str, str]):
    """Execution override controls batch size independently per step."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_exec_override",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 6, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: 3 artifacts per unit → ceil(6/3) = 2 executions
    pipeline.run(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={"scale_factor": 2.0, "noise_amplitude": 0.0, "variants": 1, "seed": 1},
        execution={"artifacts_per_unit": 3},
        backend=Backend.LOCAL,
    )

    # Step 2: 2 artifacts per unit → ceil(6/2) = 3 executions
    pipeline.run(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={"scale_factor": 3.0, "noise_amplitude": 0.0, "variants": 1, "seed": 2},
        execution={"artifacts_per_unit": 2},
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    assert count_executions_by_step(delta_root, 1) == 2
    assert count_executions_by_step(delta_root, 2) == 3

    # Both produce 6 artifacts
    assert count_artifacts_by_step(delta_root, 1) == 6
    assert count_artifacts_by_step(delta_root, 2) == 6


def test_override_isolation(pipeline_env: dict[str, str]):
    """Execution override on one step doesn't affect the next step's defaults."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_override_isolation",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 4, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: all 4 in a single batch
    pipeline.run(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={"scale_factor": 2.0, "noise_amplitude": 0.0, "variants": 1, "seed": 1},
        execution={"artifacts_per_unit": 4},
        backend=Backend.LOCAL,
    )

    # Step 2: default batching (artifacts_per_unit=1 for DataTransformer)
    pipeline.run(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={"scale_factor": 3.0, "noise_amplitude": 0.0, "variants": 1, "seed": 2},
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    assert count_executions_by_step(delta_root, 1) == 1, "Override: all 4 in 1 batch"
    assert count_executions_by_step(delta_root, 2) == 4, "Default: 1 per batch"
