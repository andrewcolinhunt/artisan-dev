"""Integration tests for composed pipeline topologies.

Tests topology patterns identified as gaps in tutorial coverage:
filter-then-creator, empty filter cascade, iterative refinement,
diamond DAG, composite-then-downstream, and passthrough_failures downstream.
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest

pytestmark = pytest.mark.slow

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.operations.curator import Filter, Merge
from artisan.operations.examples import (
    DataGenerator,
    DataGeneratorWithMetrics,
    DataTransformer,
    MetricCalculator,
)
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend
from artisan.schemas.specs.output_spec import OutputSpec

from .conftest import (
    count_artifacts_by_step,
    get_execution_outputs,
    read_table,
)


class _GenerateAndTransform(CompositeDefinition):
    """Generate data then transform it (for topology gap tests)."""

    name = "topology_gen_and_transform"
    description = "Generate and transform data"

    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "dataset": OutputSpec(artifact_type="data"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        gen = ctx.run(DataGenerator, params={"count": 2, "seed": 42})
        transformed = ctx.run(
            DataTransformer,
            inputs={"dataset": gen.output("datasets")},
            params={
                "scale_factor": 1.5,
                "noise_amplitude": 0.1,
                "variants": 1,
                "seed": 100,
            },
        )
        ctx.output("dataset", transformed.output("dataset"))


def test_filter_then_creator(pipeline_env: dict[str, Path]) -> None:
    """Downstream creator receives only filter-passed artifacts with correct provenance."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_filter_then_creator",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    # Step 0: Generate 2 datasets
    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: Transform (no noise to ensure distribution.min stays >= 0)
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

    # Step 2: Metrics on transformed data
    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step1.output("dataset")},
        backend=Backend.LOCAL,
    )

    # Step 3: Filter (distribution.min >= 0 should pass all; data is 0-10)
    step3 = pipeline.run(
        Filter,
        inputs={"passthrough": step1.output("dataset")},
        params={
            "criteria": [
                {"metric": "distribution.min", "operator": "ge", "value": 0},
            ],
        },
        backend=Backend.LOCAL,
    )

    # Step 4: Downstream creator on filter output
    pipeline.run(
        DataTransformer,
        inputs={"dataset": step3.output("passthrough")},
        params={
            "scale_factor": 1.1,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 200,
        },
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # Filter passed all 2 artifacts
    passthrough_ids = get_execution_outputs(delta_root, 3, "passthrough")
    assert len(passthrough_ids) == 2

    # Downstream received exactly the passed artifacts
    assert count_artifacts_by_step(delta_root, 4) == 2

    # Provenance: downstream artifacts trace back through filter to generator
    # Step 1 outputs -> step 3 passthrough (same IDs) -> step 4 inputs
    step1_outputs = set(get_execution_outputs(delta_root, 1, "dataset"))
    assert set(passthrough_ids) == step1_outputs

    # Artifact edges: each step 4 output traces to a step 1 output
    step4_outputs = get_execution_outputs(delta_root, 4, "dataset")
    df_edges = read_table(delta_root, "provenance/artifact_edges")
    for out_id in step4_outputs:
        sources = df_edges.filter(pl.col("target_artifact_id") == out_id)[
            "source_artifact_id"
        ].to_list()
        assert any(s in step1_outputs for s in sources)


def test_empty_filter_cascade(pipeline_env: dict[str, Path]) -> None:
    """Impossible filter causes skip cascade with correct skip reasons."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_empty_filter_cascade",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    # Step 0: Generate 3 datasets with co-produced metrics
    step0 = pipeline.run(
        DataGeneratorWithMetrics,
        params={"count": 3, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: Filter with impossible criterion on co-produced metrics
    step1 = pipeline.run(
        Filter,
        inputs={"passthrough": step0.output("datasets")},
        params={
            "criteria": [
                {"metric": "mean_score", "operator": "gt", "value": 99999},
            ],
        },
        backend=Backend.LOCAL,
    )

    # Step 2: DataTransformer (should be skipped — empty inputs)
    step2_result = pipeline.run(
        DataTransformer,
        inputs={"dataset": step1.output("passthrough")},
        params={
            "scale_factor": 1.0,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )

    # Step 3: MetricCalculator (should be skipped — pipeline stopped)
    step3_result = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step2_result.output("dataset")},
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # Filter passed 0 artifacts
    passthrough_ids = get_execution_outputs(delta_root, 1, "passthrough")
    assert len(passthrough_ids) == 0

    # Post-filter steps are skipped (check via StepResult metadata)
    assert step2_result.metadata.get("skipped") is True
    assert step3_result.metadata.get("skipped") is True

    # Skipped steps produce 0 artifacts
    assert count_artifacts_by_step(delta_root, 2) == 0
    assert count_artifacts_by_step(delta_root, 3) == 0


def test_iterative_refinement(pipeline_env: dict[str, Path]) -> None:
    """Multi-round refine loop with OutputReference reassignment."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_iterative_refinement",
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

    current_data = step0.output("datasets")
    round_counts = []

    for round_idx in range(2):
        # Score
        pipeline.run(
            MetricCalculator,
            inputs={"dataset": current_data},
            backend=Backend.LOCAL,
        )

        # Filter: lenient threshold so we keep some but not all
        filter_step = pipeline.run(
            Filter,
            inputs={"passthrough": current_data},
            params={
                "criteria": [
                    {"metric": "distribution.median", "operator": "gt", "value": 0.3},
                ],
            },
            backend=Backend.LOCAL,
        )

        # Transform survivors
        transform_step = pipeline.run(
            DataTransformer,
            inputs={"dataset": filter_step.output("passthrough")},
            params={
                "scale_factor": 1.1,
                "noise_amplitude": 0.0,
                "variants": 1,
                "seed": 100 + round_idx,
            },
            backend=Backend.LOCAL,
        )

        # Reassign for next round
        current_data = transform_step.output("dataset")
        passed = get_execution_outputs(
            delta_root, filter_step.step_number, "passthrough"
        )
        round_counts.append(len(passed))

    result = pipeline.finalize()
    assert result["overall_success"]

    # Artifact count is non-increasing across rounds
    assert round_counts[1] <= round_counts[0]

    # Final outputs trace back to the original generator
    final_step_number = transform_step.step_number
    final_outputs = get_execution_outputs(delta_root, final_step_number, "dataset")
    gen_outputs = set(get_execution_outputs(delta_root, 0, "datasets"))

    df_edges = read_table(delta_root, "provenance/artifact_edges")
    for out_id in final_outputs:
        # Walk backward through artifact edges
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
        # Should reach at least one generator output
        assert visited & gen_outputs


def test_diamond_dag(pipeline_env: dict[str, Path]) -> None:
    """Diamond: gen -> two branches -> merge -> downstream."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_diamond_dag",
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

    shared_ref = step0.output("datasets")

    # Step 1: Branch A (scale=1.5)
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
        backend=Backend.LOCAL,
    )

    # Step 2: Branch B (scale=2.0)
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
        backend=Backend.LOCAL,
    )

    # Step 3: Merge
    step3 = pipeline.run(
        Merge,
        inputs={
            "branch_a": step1.output("dataset"),
            "branch_b": step2.output("dataset"),
        },
        backend=Backend.LOCAL,
    )

    # Step 4: Metrics on merged set
    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step3.output("merged")},
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # Merge output contains artifacts from both branches (3 + 3 = 6)
    merged_ids = get_execution_outputs(delta_root, 3, "merged")
    assert len(merged_ids) == 6

    branch_a_ids = set(get_execution_outputs(delta_root, 1, "dataset"))
    branch_b_ids = set(get_execution_outputs(delta_root, 2, "dataset"))
    assert set(merged_ids) == branch_a_ids | branch_b_ids

    # Downstream metrics step processes all 6
    assert count_artifacts_by_step(delta_root, 4) == 6

    # Provenance: metric artifacts have paths through both branches
    df_edges = read_table(delta_root, "provenance/artifact_edges")
    metric_outputs = get_execution_outputs(delta_root, 4, "metrics")
    source_data_ids = set()
    for m_id in metric_outputs:
        sources = df_edges.filter(pl.col("target_artifact_id") == m_id)[
            "source_artifact_id"
        ].to_list()
        source_data_ids.update(sources)
    # Sources should span both branches
    assert source_data_ids & branch_a_ids
    assert source_data_ids & branch_b_ids


def test_composite_then_downstream(pipeline_env: dict[str, Path]) -> None:
    """Composite output consumed by a regular downstream step."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_composite_then_downstream",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    # Step 0: collapsed composite (DataGenerator -> DataTransformer)
    composite_step = pipeline.run(
        _GenerateAndTransform,
        backend=Backend.LOCAL,
    )

    # Step 1: MetricCalculator on composite output
    pipeline.run(
        MetricCalculator,
        inputs={"dataset": composite_step.output("dataset")},
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # Composite produced 2 data artifacts
    assert count_artifacts_by_step(delta_root, 0) == 2

    # MetricCalculator produced 2 metric artifacts
    assert count_artifacts_by_step(delta_root, 1) == 2

    # Metric artifacts have provenance to composite outputs
    metric_ids = get_execution_outputs(delta_root, 1, "metrics")
    composite_ids = set(get_execution_outputs(delta_root, 0, "dataset"))
    assert len(metric_ids) == 2

    df_edges = read_table(delta_root, "provenance/artifact_edges")
    for m_id in metric_ids:
        sources = df_edges.filter(pl.col("target_artifact_id") == m_id)[
            "source_artifact_id"
        ].to_list()
        assert any(s in composite_ids for s in sources)


def test_passthrough_failures_downstream(pipeline_env: dict[str, Path]) -> None:
    """passthrough_failures=True passes all artifacts to downstream."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_passthrough_failures_downstream",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    # Step 0: Generate 3 datasets with co-produced metrics
    step0 = pipeline.run(
        DataGeneratorWithMetrics,
        params={"count": 3, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: Filter with impossible criterion + passthrough_failures
    step1 = pipeline.run(
        Filter,
        inputs={"passthrough": step0.output("datasets")},
        params={
            "criteria": [
                {"metric": "mean_score", "operator": "gt", "value": 99999},
            ],
            "passthrough_failures": True,
        },
        backend=Backend.LOCAL,
    )

    # Step 2: Downstream transformer (scale != 1.0 to ensure distinct content)
    pipeline.run(
        DataTransformer,
        inputs={"dataset": step1.output("passthrough")},
        params={
            "scale_factor": 1.1,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # All 3 artifacts pass through
    passthrough_ids = get_execution_outputs(delta_root, 1, "passthrough")
    assert len(passthrough_ids) == 3

    # Downstream processes all 3
    assert count_artifacts_by_step(delta_root, 2) == 3
