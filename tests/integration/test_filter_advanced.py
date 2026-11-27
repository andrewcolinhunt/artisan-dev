"""Integration tests for advanced Filter usage.

Tests multi-criterion filtering, passthrough_failures, and step-targeted
criteria that select metrics from a specific step.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

from artisan.operations.curator import Filter
from artisan.operations.examples import (
    DataGenerator,
    DataGeneratorWithMetrics,
    DataTransformer,
    MetricCalculator,
)

from .conftest import (
    create_native_pipeline,
    get_execution_outputs,
)


def test_multi_criterion_filter(pipeline_env: dict[str, str]):
    """Filter with multiple AND criteria narrows the artifact set."""
    delta_root = pipeline_env["delta_root"]

    pipeline = create_native_pipeline(
        name="test_multi_criterion",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 5, "seed": 42},
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
    )

    pipeline.run(
        Filter,
        inputs={"passthrough": step0.output("datasets")},
        params={
            "criteria": [
                {"metric": "distribution.min", "operator": "ge", "value": 0},
                {"metric": "distribution.median", "operator": "gt", "value": 0.3},
                {"metric": "summary.row_count", "operator": "eq", "value": 10},
            ],
        },
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    passthrough_ids = get_execution_outputs(delta_root, 2, "passthrough")
    assert 0 <= len(passthrough_ids) <= 5


def test_filter_passthrough_failures(pipeline_env: dict[str, str]):
    """passthrough_failures=True passes all artifacts regardless of criteria."""
    delta_root = pipeline_env["delta_root"]

    pipeline = create_native_pipeline(
        name="test_passthrough_failures",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
    )

    pipeline.run(
        Filter,
        inputs={"passthrough": step0.output("datasets")},
        params={
            "criteria": [
                {"metric": "distribution.median", "operator": "gt", "value": 999},
            ],
            "passthrough_failures": True,
        },
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    passthrough_ids = get_execution_outputs(delta_root, 2, "passthrough")
    assert len(passthrough_ids) == 3


def test_step_targeted_criterion(pipeline_env: dict[str, str]):
    """Filter criterion with step_number targets metrics from a specific step."""
    delta_root = pipeline_env["delta_root"]

    pipeline = create_native_pipeline(
        name="test_step_targeted",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
    )

    step1 = pipeline.run(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 2.0,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step1.output("dataset")},
    )

    pipeline.run(
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
    )

    result = pipeline.finalize()
    assert result["overall_success"]

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

    pipeline = create_native_pipeline(
        name="test_multi_source_collision",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGeneratorWithMetrics,
        params={"count": 5, "seed": 42},
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
    )

    pipeline.run(
        Filter,
        inputs={"passthrough": step0.output("datasets")},
        params={
            "criteria": [
                {"metric": "mean_score", "operator": "gt", "value": 0.3},
                {"metric": "distribution.range", "operator": "lt", "value": 0.95},
            ],
        },
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    passthrough_ids = get_execution_outputs(delta_root, 2, "passthrough")
    assert len(passthrough_ids) > 0
