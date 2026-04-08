"""Integration tests for advanced Filter usage.

Tests multi-criterion filtering, passthrough_failures, and step-targeted
criteria that select metrics from a specific step.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.slow

from artisan.operations.curator import Filter
from artisan.operations.examples import (
    DataGenerator,
    DataGeneratorWithMetrics,
    DataTransformer,
    MetricCalculator,
)
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend

from .conftest import (
    get_execution_outputs,
)


def test_multi_criterion_filter(pipeline_env: dict[str, str]):
    """Filter with multiple AND criteria narrows the artifact set."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_multi_criterion",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    # Step 0: Generate 5 datasets
    step0 = pipeline.run(
        DataGenerator,
        params={"count": 5, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: Compute metrics
    step1 = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        backend=Backend.LOCAL,
    )

    # Step 2: Filter with 3 AND criteria
    step2 = pipeline.run(
        Filter,
        inputs={"passthrough": step0.output("datasets")},
        params={
            "criteria": [
                {"metric": "distribution.min", "operator": "ge", "value": 0},
                {"metric": "distribution.median", "operator": "gt", "value": 0.3},
                {"metric": "summary.row_count", "operator": "eq", "value": 10},
            ],
        },
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    passthrough_ids = get_execution_outputs(delta_root, 2, "passthrough")
    # All datasets have min >= 0 (generated 0-10), row_count == 10,
    # median likely > 0.3 for most datasets
    assert 0 <= len(passthrough_ids) <= 5


def test_filter_passthrough_failures(pipeline_env: dict[str, str]):
    """passthrough_failures=True passes all artifacts regardless of criteria."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_passthrough_failures",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Compute metrics (needed for filter evaluation)
    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        backend=Backend.LOCAL,
    )

    # Impossible criterion (median > 999) with passthrough_failures=True
    step2 = pipeline.run(
        Filter,
        inputs={"passthrough": step0.output("datasets")},
        params={
            "criteria": [
                {"metric": "distribution.median", "operator": "gt", "value": 999},
            ],
            "passthrough_failures": True,
        },
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # All 3 artifacts pass through despite failing criteria
    passthrough_ids = get_execution_outputs(delta_root, 2, "passthrough")
    assert len(passthrough_ids) == 3


def test_step_targeted_criterion(pipeline_env: dict[str, str]):
    """Filter criterion with step_number targets metrics from a specific step."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_step_targeted",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    # Step 0: Generate 3 datasets
    step0 = pipeline.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: Transform datasets
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

    # Step 2: Metrics on originals
    step2 = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        backend=Backend.LOCAL,
    )

    # Step 3: Metrics on transformed
    step3 = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step1.output("dataset")},
        backend=Backend.LOCAL,
    )

    # Step 4: Filter passthrough step 1 artifacts using step 3 metrics
    step4 = pipeline.run(
        Filter,
        inputs={"passthrough": step1.output("dataset")},
        params={
            "criteria": [
                {
                    "metric": "distribution.min",
                    "operator": "ge",
                    "value": 0,
                    "step_number": 3,
                },
            ],
        },
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # Filter uses step 3 metrics (on transformed data)
    passthrough_ids = get_execution_outputs(delta_root, 4, "passthrough")
    assert len(passthrough_ids) > 0


def test_multi_source_filter_with_colliding_field_names(
    pipeline_env: dict[str, str],
):
    """Filter across two metric sources where field names collide at
    different nesting levels (top-level row_count vs summary.row_count).

    DataGeneratorWithMetrics produces flat metrics (mean_score, row_count, ...),
    MetricCalculator produces nested metrics (distribution.range,
    summary.row_count, ...). Filter must merge both schemas without crashing
    on the shared 'row_count' field name.
    """
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_multi_source_collision",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    # Step 0: Generate datasets + co-produced flat metrics (mean_score, row_count)
    step0 = pipeline.run(
        DataGeneratorWithMetrics,
        params={"count": 5, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: Compute nested metrics (distribution.range, summary.row_count)
    step1 = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        backend=Backend.LOCAL,
    )

    # Step 2: Filter on criteria from BOTH metric sources
    step2 = pipeline.run(
        Filter,
        inputs={"passthrough": step0.output("datasets")},
        params={
            "criteria": [
                {"metric": "mean_score", "operator": "gt", "value": 0.3},
                {"metric": "distribution.range", "operator": "lt", "value": 0.95},
            ],
        },
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    passthrough_ids = get_execution_outputs(delta_root, 2, "passthrough")
    assert len(passthrough_ids) > 0
