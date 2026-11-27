"""Integration tests for composed pipeline topologies.

Tests topology patterns identified as gaps in tutorial coverage:
filter-then-creator, empty filter cascade, iterative refinement,
diamond DAG, composite-then-downstream, and passthrough_failures downstream.
"""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar

import polars as pl
import pytest

pytestmark = pytest.mark.integration

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.operations.curator import Filter, Merge
from artisan.operations.examples import (
    DataGenerator,
    DataGeneratorWithMetrics,
    DataTransformer,
    MetricCalculator,
)
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.specs.output_spec import OutputSpec

from .conftest import (
    count_artifacts_by_step,
    create_native_pipeline,
    get_execution_outputs,
    load_artifact_edges,
)


class _GenerateAndTransform(CompositeDefinition):
    """Generate data then transform it (for topology gap tests)."""

    name = "topology_gen_and_transform"
    description = "Generate and transform data"

    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "dataset": OutputSpec(artifact_type="csv"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        gen = ctx.call(
            DataGenerator, name="generate", inputs={}, params={"count": 2, "seed": 42}
        )
        transformed = ctx.call(
            DataTransformer,
            name="transform",
            inputs={"dataset": gen.output("datasets")},
            params={
                "scale_factor": 1.5,
                "noise_amplitude": 0.1,
                "variants": 1,
                "seed": 100,
            },
        )
        ctx.output("dataset", transformed.output("dataset"))


def test_filter_then_creator(pipeline_env: dict[str, str]) -> None:
    """Downstream creator receives only filter-passed artifacts with correct provenance."""
    delta_root = pipeline_env["delta_root"]

    pipeline = create_native_pipeline(
        name="test_filter_then_creator",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
    )

    step1 = pipeline.run(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step1.output("dataset")},
    )

    step3 = pipeline.run(
        Filter,
        inputs={"passthrough": step1.output("dataset")},
        params={
            "criteria": [
                {"metric": "distribution.min", "operator": "ge", "value": 0},
            ],
        },
    )

    pipeline.run(
        DataTransformer,
        inputs={"dataset": step3.output("passthrough")},
        params={
            "scale_factor": 1.1,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 200,
        },
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    passthrough_ids = get_execution_outputs(delta_root, 3, "passthrough")
    assert len(passthrough_ids) == 2

    assert count_artifacts_by_step(delta_root, 4) == 2

    step1_outputs = set(get_execution_outputs(delta_root, 1, "dataset"))
    assert set(passthrough_ids) == step1_outputs

    step4_outputs = get_execution_outputs(delta_root, 4, "dataset")
    df_edges = load_artifact_edges(delta_root, None)
    for out_id in step4_outputs:
        sources = df_edges.filter(pl.col("target_artifact_id") == out_id)[
            "source_artifact_id"
        ].to_list()
        assert any(s in step1_outputs for s in sources)


def test_empty_filter_cascade(pipeline_env: dict[str, str]) -> None:
    """Impossible filter causes skip cascade with correct skip reasons."""
    delta_root = pipeline_env["delta_root"]

    pipeline = create_native_pipeline(
        name="test_empty_filter_cascade",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGeneratorWithMetrics,
        params={"count": 3, "seed": 42},
    )

    step1 = pipeline.run(
        Filter,
        inputs={"passthrough": step0.output("datasets")},
        params={
            "criteria": [
                {"metric": "mean_score", "operator": "gt", "value": 99999},
            ],
        },
    )

    step2_result = pipeline.run(
        DataTransformer,
        inputs={"dataset": step1.output("passthrough")},
        params={
            "scale_factor": 1.0,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
    )

    step3_result = pipeline.run(
        MetricCalculator,
        inputs={"dataset": pipeline.output("data_transformer", "dataset")},
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    passthrough_ids = get_execution_outputs(delta_root, 1, "passthrough")
    assert len(passthrough_ids) == 0

    assert step2_result.status is StepStatus.SKIPPED
    assert step3_result.status is StepStatus.SKIPPED

    assert count_artifacts_by_step(delta_root, 2) == 0
    assert count_artifacts_by_step(delta_root, 3) == 0


def test_iterative_refinement(pipeline_env: dict[str, str]) -> None:
    """Multi-round refine loop with OutputReference reassignment."""
    delta_root = pipeline_env["delta_root"]

    pipeline = create_native_pipeline(
        name="test_iterative_refinement",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 5, "seed": 42},
    )

    current_data = step0.output("datasets")
    round_counts = []

    for round_idx in range(2):
        pipeline.run(
            MetricCalculator,
            inputs={"dataset": current_data},
        )

        filter_step = pipeline.run(
            Filter,
            inputs={"passthrough": current_data},
            params={
                "criteria": [
                    {"metric": "distribution.median", "operator": "gt", "value": 0.3},
                ],
            },
        )

        transform_step = pipeline.run(
            DataTransformer,
            inputs={"dataset": filter_step.output("passthrough")},
            params={
                "scale_factor": 1.1,
                "noise_amplitude": 0.0,
                "variants": 1,
                "seed": 100 + round_idx,
            },
        )

        current_data = transform_step.output("dataset")
        passed = get_execution_outputs(
            delta_root, filter_step.step_number, "passthrough"
        )
        round_counts.append(len(passed))

    result = pipeline.finalize()
    assert result["overall_success"]

    assert round_counts[1] <= round_counts[0]

    final_step_number = transform_step.step_number
    final_outputs = get_execution_outputs(delta_root, final_step_number, "dataset")
    gen_outputs = set(get_execution_outputs(delta_root, 0, "datasets"))

    df_edges = load_artifact_edges(delta_root, None)
    for out_id in final_outputs:
        visited = set()
        frontier = {out_id}
        while frontier:
            current = frontier.pop()
            if current in visited:
                continue
            visited.add(current)
            sources = df_edges.filter(pl.col("target_artifact_id") == current)[
                "source_artifact_id"
            ].to_list()
            frontier.update(sources)
        assert visited & gen_outputs


def test_diamond_dag(pipeline_env: dict[str, str]) -> None:
    """Diamond: gen -> two branches -> merge -> downstream."""
    delta_root = pipeline_env["delta_root"]

    pipeline = create_native_pipeline(
        name="test_diamond_dag",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
    )

    shared_ref = step0.output("datasets")

    step1 = pipeline.run(
        DataTransformer,
        inputs={"dataset": shared_ref},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
            "output_prefix": "A",
        },
    )

    step2 = pipeline.run(
        DataTransformer,
        inputs={"dataset": shared_ref},
        params={
            "scale_factor": 2.0,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 200,
            "output_prefix": "B",
        },
    )

    step3 = pipeline.run(
        Merge,
        inputs={
            "branch_a": step1.output("dataset"),
            "branch_b": step2.output("dataset"),
        },
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step3.output("merged")},
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    merged_ids = get_execution_outputs(delta_root, 3, "merged")
    assert len(merged_ids) == 6

    branch_a_ids = set(get_execution_outputs(delta_root, 1, "dataset"))
    branch_b_ids = set(get_execution_outputs(delta_root, 2, "dataset"))
    assert set(merged_ids) == branch_a_ids | branch_b_ids

    assert count_artifacts_by_step(delta_root, 4) == 6

    df_edges = load_artifact_edges(delta_root, None)
    metric_outputs = get_execution_outputs(delta_root, 4, "metrics")
    source_data_ids = set()
    for m_id in metric_outputs:
        sources = df_edges.filter(pl.col("target_artifact_id") == m_id)[
            "source_artifact_id"
        ].to_list()
        source_data_ids.update(sources)
    assert source_data_ids & branch_a_ids
    assert source_data_ids & branch_b_ids


def test_composite_then_downstream(pipeline_env: dict[str, str]) -> None:
    """Composite output consumed by a regular downstream step."""
    delta_root = pipeline_env["delta_root"]

    pipeline = create_native_pipeline(
        name="test_composite_then_downstream",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    composite_step = pipeline.run(
        _GenerateAndTransform,
    )

    assert composite_step.status is StepStatus.SUCCEEDED, composite_step.error

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": composite_step.output("dataset")},
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    assert result["total_steps"] == 2
    assert count_artifacts_by_step(delta_root, 0) == 2
    assert count_artifacts_by_step(delta_root, 1) == 2

    metric_ids = get_execution_outputs(delta_root, 1, "metrics")
    composite_ids = set(get_execution_outputs(delta_root, 0, "dataset"))
    assert len(metric_ids) == 2

    df_edges = load_artifact_edges(delta_root, None)
    for m_id in metric_ids:
        sources = df_edges.filter(pl.col("target_artifact_id") == m_id)[
            "source_artifact_id"
        ].to_list()
        assert any(s in composite_ids for s in sources)


def test_passthrough_failures_downstream(pipeline_env: dict[str, str]) -> None:
    """passthrough_failures=True passes all artifacts to downstream."""
    delta_root = pipeline_env["delta_root"]

    pipeline = create_native_pipeline(
        name="test_passthrough_failures_downstream",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGeneratorWithMetrics,
        params={"count": 3, "seed": 42},
    )

    step1 = pipeline.run(
        Filter,
        inputs={"passthrough": step0.output("datasets")},
        params={
            "criteria": [
                {"metric": "mean_score", "operator": "gt", "value": 99999},
            ],
            "passthrough_failures": True,
        },
    )

    pipeline.run(
        DataTransformer,
        inputs={"dataset": step1.output("passthrough")},
        params={
            "scale_factor": 1.1,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    passthrough_ids = get_execution_outputs(delta_root, 1, "passthrough")
    assert len(passthrough_ids) == 3

    assert count_artifacts_by_step(delta_root, 2) == 3
