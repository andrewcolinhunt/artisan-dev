"""Data flow pattern integration tests.

These tests verify the core data flow patterns supported by the pipeline framework:
1. Linear Chain: Sequential operation execution
2. Fan-Out (Execution): Same input consumed by multiple operations
3. Fan-Out (Data): Single input producing multiple outputs (variants)
4. Fan-In (Merge): Union of multiple data streams
5. Passthrough: Artifact identity preservation through Filter (direct-input)
6. Batch Processing: Multi-item batch execution
7. Comprehensive: Complex multi-pattern pipeline

Reference: docs/design/design_data_flow_integration_tests.md
"""

from __future__ import annotations

import math
from pathlib import Path

import polars as pl
import pytest

# Mark all tests in this module as slow (integration tests)
pytestmark = pytest.mark.slow

from artisan.operations.curator import Filter, IngestData, Merge
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend

from .conftest import (
    count_artifacts_by_step,
    count_artifacts_by_type,
    count_executions_by_step,
    get_execution_inputs,
    get_execution_outputs,
    read_table,
)

# =============================================================================
# Test 1: Linear Chain
# =============================================================================


def linear_chain_pipeline(
    delta_root: Path,
    staging_root: Path,
    working_root: Path,
) -> dict:
    """Pipeline for Test 1: Linear Chain.

    Pattern: DataGenerator -> DataTransformer -> MetricCalculator
    """
    pipeline = PipelineManager.create(
        name="test_linear_chain",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    # Step 0: Generate 2 datasets
    step0 = pipeline.run(
        operation=DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: Transform datasets (1 variant each)
    step1 = pipeline.run(
        operation=DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.1,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )

    # Step 2: Calculate metrics
    pipeline.run(
        operation=MetricCalculator,
        inputs={"dataset": step1.output("dataset")},
        backend=Backend.LOCAL,
    )

    return pipeline.finalize()


def test_linear_chain(pipeline_env: dict[str, Path]):
    """Test 1: Linear chain data flow.

    Verifies:
    - Sequential operation execution
    - Correct artifact counts at each step
    - Proper provenance tracking
    """
    delta_root = pipeline_env["delta_root"]

    result = linear_chain_pipeline(
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    assert result["overall_success"], "Pipeline should complete successfully"

    # Artifact counts at each step
    assert count_artifacts_by_step(delta_root, 0) == 2, "Generator should produce 2"
    assert count_artifacts_by_step(delta_root, 1) == 2, "Transformer should produce 2"
    assert count_artifacts_by_step(delta_root, 2) == 2, "MetricCalc should produce 2"

    # Type totals
    assert count_artifacts_by_type(delta_root, "data") == 4, "2 gen + 2 transform"
    assert count_artifacts_by_type(delta_root, "metric") == 2

    # Execution records
    assert count_executions_by_step(delta_root, 0) == 1, "1 generative execution"
    assert count_executions_by_step(delta_root, 1) == 2, "2 creator executions"
    assert count_executions_by_step(delta_root, 2) == 1, "1 batched execution"


# =============================================================================
# Test 2: Fan-Out (Execution)
# =============================================================================


def fan_out_execution_pipeline(
    delta_root: Path,
    staging_root: Path,
    working_root: Path,
) -> dict:
    """Pipeline for Test 2: Fan-Out (Execution).

    Pattern: DataGenerator -> [DataTransformer A, DataTransformer B]
    Same input consumed by two parallel branches.
    """
    pipeline = PipelineManager.create(
        name="test_fan_out_execution",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    # Step 0: Generate 2 datasets
    step0 = pipeline.run(
        operation=DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Shared reference for both branches
    shared_ref = step0.output("datasets")

    # Step 1: Transform with seed 100 (Branch A)
    pipeline.run(
        operation=DataTransformer,
        inputs={"dataset": shared_ref},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.1,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )

    # Step 2: Transform with seed 200 (Branch B)
    pipeline.run(
        operation=DataTransformer,
        inputs={"dataset": shared_ref},
        params={
            "scale_factor": 2.0,
            "noise_amplitude": 0.2,
            "variants": 1,
            "seed": 200,
        },
        backend=Backend.LOCAL,
    )

    return pipeline.finalize()


def test_fan_out_execution(pipeline_env: dict[str, Path]):
    """Test 2: Fan-out at execution level.

    Verifies:
    - Same input feeds multiple operations
    - Both branches receive identical input IDs
    - Independent outputs from each branch
    """
    delta_root = pipeline_env["delta_root"]

    result = fan_out_execution_pipeline(
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    assert result["overall_success"], "Pipeline should complete successfully"

    # Both branches receive same input IDs
    step1_input_ids = get_execution_inputs(delta_root, 1, "dataset")
    step2_input_ids = get_execution_inputs(delta_root, 2, "dataset")
    assert set(step1_input_ids) == set(
        step2_input_ids
    ), "Both branches should receive identical input artifact IDs"

    # Total: 2 (gen) + 2 (A) + 2 (B) = 6
    assert count_artifacts_by_type(delta_root, "data") == 6


# =============================================================================
# Test 3: Fan-Out (Data)
# =============================================================================


def fan_out_data_pipeline(
    delta_root: Path,
    staging_root: Path,
    working_root: Path,
) -> dict:
    """Pipeline for Test 3: Fan-Out (Data).

    Pattern: DataGenerator (1) -> DataTransformer (variants=3)
    Single input produces multiple outputs.
    """
    pipeline = PipelineManager.create(
        name="test_fan_out_data",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    # Step 0: Generate 1 dataset
    step0 = pipeline.run(
        operation=DataGenerator,
        params={"count": 1, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: Create 3 variants
    pipeline.run(
        operation=DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.1,
            "variants": 3,
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )

    return pipeline.finalize()


def test_fan_out_data(pipeline_env: dict[str, Path]):
    """Test 3: Fan-out at data level.

    Verifies:
    - Single input produces multiple outputs via variants
    - Correct lineage: all outputs trace to single input
    """
    delta_root = pipeline_env["delta_root"]

    result = fan_out_data_pipeline(
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    assert result["overall_success"], "Pipeline should complete successfully"

    # Single input produces multiple outputs
    assert count_artifacts_by_step(delta_root, 0) == 1
    assert count_artifacts_by_step(delta_root, 1) == 3

    # Verify lineage: all 3 outputs derive from single input
    input_ids = get_execution_outputs(delta_root, 0, "datasets")
    assert len(input_ids) == 1

    df_prov = read_table(delta_root, "provenance/artifact_edges")
    edges = df_prov.filter(pl.col("source_artifact_id") == input_ids[0])
    assert edges.height == 3, "3 edges from 1 source"


# =============================================================================
# Test 4: Fan-In (Merge)
# =============================================================================


def fan_in_merge_pipeline(
    delta_root: Path,
    staging_root: Path,
    working_root: Path,
) -> dict:
    """Pipeline for Test 4: Fan-In (Merge).

    Pattern: [DataGenerator A, DataGenerator B] -> Merge
    Union of multiple data streams.
    """
    pipeline = PipelineManager.create(
        name="test_fan_in_merge",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    # Step 0: Generate 2 datasets (Branch A)
    step0 = pipeline.run(
        operation=DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: Generate 2 datasets (Branch B)
    step1 = pipeline.run(
        operation=DataGenerator,
        params={"count": 2, "seed": 100},
        backend=Backend.LOCAL,
    )

    # Step 2: Merge both branches
    pipeline.run(
        operation=Merge,
        inputs={
            "branch_a": step0.output("datasets"),
            "branch_b": step1.output("datasets"),
        },
        backend=Backend.LOCAL,
    )

    return pipeline.finalize()


def test_fan_in_merge(pipeline_env: dict[str, Path]):
    """Test 4: Fan-in via Merge curator.

    Verifies:
    - Merge doesn't create new artifacts (passthrough)
    - Merged output references all 4 input artifacts
    - Passthrough IDs match original IDs
    """
    delta_root = pipeline_env["delta_root"]

    result = fan_in_merge_pipeline(
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    assert result["overall_success"], "Pipeline should complete successfully"

    # Merge doesn't create new artifacts in artifact_index
    assert (
        count_artifacts_by_step(delta_root, 2) == 0
    ), "Merge is passthrough, no new artifacts"

    # But merged output references 4 artifacts
    merged_outputs = get_execution_outputs(delta_root, 2, "merged")
    assert len(merged_outputs) == 4

    # Verify passthrough: merged IDs match original IDs
    step0_ids = set(get_execution_outputs(delta_root, 0, "datasets"))
    step1_ids = set(get_execution_outputs(delta_root, 1, "datasets"))
    assert set(merged_outputs) == step0_ids | step1_ids


# =============================================================================
# Test 5: Passthrough
# =============================================================================


def passthrough_pipeline(
    delta_root: Path,
    staging_root: Path,
    working_root: Path,
) -> dict:
    """Pipeline for Test 6: Passthrough.

    Pattern: DataGenerator -> MetricCalculator -> Filter(direct) -> DataTransformer
    Tests artifact identity preservation through curator operations.
    """
    pipeline = PipelineManager.create(
        name="test_passthrough",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    # Step 0: Generate 2 datasets
    step0 = pipeline.run(
        operation=DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: Calculate metrics
    step1 = pipeline.run(
        operation=MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        backend=Backend.LOCAL,
    )

    # Step 2: Filter (forward-walk metric discovery, always pass with min >= 0)
    step2 = pipeline.run(
        operation=Filter,
        inputs={
            "passthrough": step0.output("datasets"),
        },
        params={
            "criteria": [
                {"metric": "distribution.min", "operator": "ge", "value": 0},
            ],
        },
        backend=Backend.LOCAL,
    )

    # Step 3: Transform the passthrough datasets
    pipeline.run(
        operation=DataTransformer,
        inputs={"dataset": step2.output("passthrough")},
        params={
            "scale_factor": 1.1,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )

    return pipeline.finalize()


def test_passthrough(pipeline_env: dict[str, Path]):
    """Test 6: Passthrough identity preservation.

    Verifies:
    - Filter passthrough IDs match original step 0 IDs
    - Downstream receives correct count
    - No artifact duplication through utilities
    """
    delta_root = pipeline_env["delta_root"]

    result = passthrough_pipeline(
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    assert result["overall_success"], "Pipeline should complete successfully"

    # Filter output IDs must match original step 0 IDs
    passthrough_ids = get_execution_outputs(delta_root, 2, "passthrough")
    step0_ids = get_execution_outputs(delta_root, 0, "datasets")

    for pt_id in passthrough_ids:
        assert (
            pt_id in step0_ids
        ), "Passthrough IDs must reference original artifacts, not copies"

    # Downstream receives correct count
    assert count_artifacts_by_step(delta_root, 3) == len(passthrough_ids)


# =============================================================================
# Test 7: Batch Processing
# =============================================================================


def batch_processing_pipeline(
    delta_root: Path,
    staging_root: Path,
    working_root: Path,
) -> dict:
    """Pipeline for Test 7: Batch Processing.

    Pattern: DataGenerator (5) -> DataTransformer (artifacts_per_unit=2)
    Tests execution unit batching.
    """
    pipeline = PipelineManager.create(
        name="test_batch_processing",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    # Step 0: Generate 5 datasets
    step0 = pipeline.run(
        operation=DataGenerator,
        params={"count": 5, "seed": 42},
        backend=Backend.LOCAL,
    )

    # Step 1: Batch transform with artifacts_per_unit=2 via execution override
    pipeline.run(
        operation=DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.1,
            "variants": 1,
            "seed": 100,
        },
        execution={"artifacts_per_unit": 2},
        backend=Backend.LOCAL,
    )

    return pipeline.finalize()


def test_batch_processing(pipeline_env: dict[str, Path]):
    """Test 7: Batch execution unit creation.

    Verifies:
    - ceil(5/2) = 3 execution units for DataTransformer with artifacts_per_unit=2
    - No data loss: still 5 outputs
    - Batch composition: [2, 2, 1]
    """
    delta_root = pipeline_env["delta_root"]

    result = batch_processing_pipeline(
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    assert result["overall_success"], "Pipeline should complete successfully"

    # Generator: 1 ExecutionUnit (generative)
    assert count_executions_by_step(delta_root, 0) == 1

    # DataTransformer (artifacts_per_unit=2): ceil(5/2) = 3 ExecutionUnits
    assert count_executions_by_step(delta_root, 1) == math.ceil(5 / 2)

    # No data loss: still 5 outputs
    assert count_artifacts_by_step(delta_root, 1) == 5

    # Verify batch composition: [2, 2, 1]
    df_exec = read_table(delta_root, "orchestration/executions")
    df_prov = read_table(delta_root, "provenance/execution_edges")

    step1_exec_ids = df_exec.filter(pl.col("origin_step_number") == 1)[
        "execution_run_id"
    ].to_list()

    batch_sizes = []
    for exec_id in step1_exec_ids:
        n_inputs = df_prov.filter(
            (pl.col("execution_run_id") == exec_id)
            & (pl.col("direction") == "input")
            & (pl.col("role") == "dataset")
        ).height
        batch_sizes.append(n_inputs)

    batch_sizes.sort()
    assert batch_sizes == [1, 2, 2]


# =============================================================================
# Test 8: Comprehensive
# =============================================================================


def comprehensive_pipeline(
    source_files: list[Path],
    delta_root: Path,
    staging_root: Path,
    working_root: Path,
) -> dict:
    """Pipeline for Test 8: Comprehensive.

    9-step pipeline combining all patterns:
    - Ingest + Generation
    - Merge
    - Parallel branches
    - Metrics calculation
    - Filter (direct-input with criteria)
    - Final fan-out
    """
    pipeline = PipelineManager.create(
        name="test_comprehensive",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    # Stage 1: Sources
    # Step 0: Ingest 3 external CSV files as data artifacts
    step0 = pipeline.run(
        operation=IngestData,
        inputs=[str(f) for f in source_files],
        backend=Backend.LOCAL,
    )

    # Step 1: Generate 2 additional datasets
    step1 = pipeline.run(
        operation=DataGenerator,
        params={"count": 2, "seed": 9999},
        backend=Backend.LOCAL,
    )

    # Step 2: Merge ingest + generated
    step2 = pipeline.run(
        operation=Merge,
        inputs={
            "ingested": step0.output("data"),
            "generated": step1.output("datasets"),
        },
        backend=Backend.LOCAL,
    )

    # Stage 2: Parallel Branches
    # Step 3: DataTransformer A
    step3 = pipeline.run(
        operation=DataTransformer,
        inputs={"dataset": step2.output("merged")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.1,
            "variants": 1,
            "seed": 100,
            "output_prefix": "A",
        },
        backend=Backend.LOCAL,
    )

    # Step 4: DataTransformer B
    step4 = pipeline.run(
        operation=DataTransformer,
        inputs={"dataset": step2.output("merged")},
        params={
            "scale_factor": 2.0,
            "noise_amplitude": 0.2,
            "variants": 1,
            "seed": 200,
            "output_prefix": "B",
        },
        backend=Backend.LOCAL,
    )

    # Step 5: Merge both branches
    step5 = pipeline.run(
        operation=Merge,
        inputs={
            "branch_a": step3.output("dataset"),
            "branch_b": step4.output("dataset"),
        },
        backend=Backend.LOCAL,
    )

    # Stage 3: Metrics & Filter
    # Step 6: Calculate metrics
    step6 = pipeline.run(
        operation=MetricCalculator,
        inputs={"dataset": step5.output("merged")},
        backend=Backend.LOCAL,
    )

    # Step 7: Filter based on median_score (forward-walk metric discovery)
    step7 = pipeline.run(
        operation=Filter,
        inputs={
            "passthrough": step5.output("merged"),
        },
        params={
            "criteria": [
                {
                    "metric": "distribution.median",
                    "operator": "gt",
                    "value": 0.3,
                },
            ],
        },
        backend=Backend.LOCAL,
    )

    # Stage 4: Final Fan-Out
    # Step 8: Create variants of filtered datasets
    pipeline.run(
        operation=DataTransformer,
        inputs={"dataset": step7.output("passthrough")},
        params={
            "scale_factor": 1.1,
            "noise_amplitude": 0.0,
            "variants": 2,
            "seed": 300,
        },
        backend=Backend.LOCAL,
    )

    return pipeline.finalize()


def test_comprehensive(pipeline_env: dict[str, Path], sample_csv_files: list[Path]):
    """Test 8: Comprehensive multi-pattern pipeline.

    Verifies:
    - All patterns work together
    - Correct artifact counts at each stage
    - Data integrity through complex flow
    """
    delta_root = pipeline_env["delta_root"]

    result = comprehensive_pipeline(
        source_files=sample_csv_files,
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    assert result["overall_success"], "Pipeline should complete successfully"

    # Stage 1
    # Ingest: 3 data artifacts + 3 file_refs = 6 artifacts
    assert count_artifacts_by_step(delta_root, 0) == 6
    assert count_artifacts_by_step(delta_root, 1) == 2  # Generator
    merged_step2 = get_execution_outputs(delta_root, 2, "merged")
    assert len(merged_step2) == 5  # 3 ingest + 2 generated

    # Stage 2
    assert count_artifacts_by_step(delta_root, 3) == 5  # Branch A
    assert count_artifacts_by_step(delta_root, 4) == 5  # Branch B
    merged_step5 = get_execution_outputs(delta_root, 5, "merged")
    assert len(merged_step5) == 10  # 5 + 5

    # Stage 3 (no Group step — Filter is direct-input)
    assert count_artifacts_by_step(delta_root, 6) == 10  # Metrics
    passthrough_ids = get_execution_outputs(delta_root, 7, "passthrough")
    n_passed = len(passthrough_ids)
    assert 0 <= n_passed <= 10  # Some may be filtered out

    # Stage 4: Final fan-out with variants=2
    assert count_artifacts_by_step(delta_root, 8) == n_passed * 2

    # Integrity checks
    assert count_artifacts_by_type(delta_root, "file_ref") == 3
    assert count_artifacts_by_type(delta_root, "metric") == 10

    # Total data artifacts: 3 (ingest) + 2 (gen) + 5 (A) + 5 (B) + n_passed*2 (final)
    expected_data = 3 + 2 + 5 + 5 + n_passed * 2
    assert count_artifacts_by_type(delta_root, "data") == expected_data
