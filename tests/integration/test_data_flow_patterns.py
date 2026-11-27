"""Data flow pattern integration tests.

These tests verify the core data flow patterns supported by the pipeline framework:
1. Linear Chain: Sequential operation execution
2. Fan-Out (Execution): Same input consumed by multiple operations
3. Fan-Out (Data): Single input producing multiple outputs (variants)
4. Fan-In (Merge): Union of multiple data streams
5. Passthrough: Artifact identity preservation through Filter (direct-input)
6. Batch Processing: Multi-item batch execution
7. Comprehensive: Complex multi-pattern pipeline
"""

from __future__ import annotations

import math
from pathlib import Path

import polars as pl
import pytest

pytestmark = pytest.mark.integration

from artisan.operations.curator import Filter, IngestCSV, Merge
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator

from .conftest import (
    _step_read,
    count_artifacts_by_step,
    count_artifacts_by_type,
    count_executions_by_step,
    create_native_pipeline,
    get_execution_inputs,
    get_execution_outputs,
    load_artifact_edges,
    read_table,
)


def linear_chain_pipeline(
    delta_root: Path,
    staging_root: Path,
    working_root: Path,
) -> dict:
    """Pipeline for Test 1: Linear Chain.

    Pattern: DataGenerator -> DataTransformer -> MetricCalculator
    """
    pipeline = create_native_pipeline(
        name="test_linear_chain",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    step0 = pipeline.run(
        definition=DataGenerator,
        params={"count": 2, "seed": 42},
    )

    step1 = pipeline.run(
        definition=DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.1,
            "variants": 1,
            "seed": 100,
        },
    )

    pipeline.run(
        definition=MetricCalculator,
        inputs={"dataset": step1.output("dataset")},
    )

    return pipeline.finalize()


def test_linear_chain(pipeline_env: dict[str, str]):
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

    assert count_artifacts_by_step(delta_root, 0) == 2, "Generator should produce 2"
    assert count_artifacts_by_step(delta_root, 1) == 2, "Transformer should produce 2"
    assert count_artifacts_by_step(delta_root, 2) == 2, "MetricCalc should produce 2"

    assert count_artifacts_by_type(delta_root, "csv") == 4, "2 gen + 2 transform"
    assert count_artifacts_by_type(delta_root, "metric") == 2

    assert count_executions_by_step(delta_root, 0) == 1, "1 generative execution"
    assert count_executions_by_step(delta_root, 1) == 2, "2 creator executions"
    assert count_executions_by_step(delta_root, 2) == 1, "1 batched execution"


def fan_out_execution_pipeline(
    delta_root: Path,
    staging_root: Path,
    working_root: Path,
) -> dict:
    """Pipeline for Test 2: Fan-Out (Execution).

    Pattern: DataGenerator -> [DataTransformer A, DataTransformer B]
    Same input consumed by two parallel branches.
    """
    pipeline = create_native_pipeline(
        name="test_fan_out_execution",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    step0 = pipeline.run(
        definition=DataGenerator,
        params={"count": 2, "seed": 42},
    )

    shared_ref = step0.output("datasets")

    pipeline.run(
        definition=DataTransformer,
        inputs={"dataset": shared_ref},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.1,
            "variants": 1,
            "seed": 100,
        },
    )

    pipeline.run(
        definition=DataTransformer,
        inputs={"dataset": shared_ref},
        params={
            "scale_factor": 2.0,
            "noise_amplitude": 0.2,
            "variants": 1,
            "seed": 200,
        },
    )

    return pipeline.finalize()


def test_fan_out_execution(pipeline_env: dict[str, str]):
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

    step1_input_ids = get_execution_inputs(delta_root, 1, "dataset")
    step2_input_ids = get_execution_inputs(delta_root, 2, "dataset")
    assert set(step1_input_ids) == set(step2_input_ids), (
        "Both branches should receive identical input artifact IDs"
    )

    assert count_artifacts_by_type(delta_root, "csv") == 6


def fan_out_data_pipeline(
    delta_root: Path,
    staging_root: Path,
    working_root: Path,
) -> dict:
    """Pipeline for Test 3: Fan-Out (Data).

    Pattern: DataGenerator (1) -> DataTransformer (variants=3)
    Single input produces multiple outputs.
    """
    pipeline = create_native_pipeline(
        name="test_fan_out_data",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    step0 = pipeline.run(
        definition=DataGenerator,
        params={"count": 1, "seed": 42},
    )

    pipeline.run(
        definition=DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.1,
            "variants": 3,
            "seed": 100,
        },
    )

    return pipeline.finalize()


def test_fan_out_data(pipeline_env: dict[str, str]):
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

    assert count_artifacts_by_step(delta_root, 0) == 1
    assert count_artifacts_by_step(delta_root, 1) == 3

    input_ids = get_execution_outputs(delta_root, 0, "datasets")
    assert len(input_ids) == 1

    df_prov = load_artifact_edges(delta_root, None)
    edges = df_prov.filter(pl.col("source_artifact_id") == input_ids[0])
    assert edges.height == 3, "3 edges from 1 source"


def fan_in_merge_pipeline(
    delta_root: Path,
    staging_root: Path,
    working_root: Path,
) -> dict:
    """Pipeline for Test 4: Fan-In (Merge).

    Pattern: [DataGenerator A, DataGenerator B] -> Merge
    Union of multiple data streams.
    """
    pipeline = create_native_pipeline(
        name="test_fan_in_merge",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    step0 = pipeline.run(
        definition=DataGenerator,
        params={"count": 2, "seed": 42},
    )

    step1 = pipeline.run(
        definition=DataGenerator,
        params={"count": 2, "seed": 100},
    )

    pipeline.run(
        definition=Merge,
        inputs={
            "branch_a": step0.output("datasets"),
            "branch_b": step1.output("datasets"),
        },
    )

    return pipeline.finalize()


def test_fan_in_merge(pipeline_env: dict[str, str]):
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

    assert count_artifacts_by_step(delta_root, 2) == 0, (
        "Merge is passthrough, no new artifacts"
    )

    merged_outputs = get_execution_outputs(delta_root, 2, "merged")
    assert len(merged_outputs) == 4

    step0_ids = set(get_execution_outputs(delta_root, 0, "datasets"))
    step1_ids = set(get_execution_outputs(delta_root, 1, "datasets"))
    assert set(merged_outputs) == step0_ids | step1_ids


def passthrough_pipeline(
    delta_root: Path,
    staging_root: Path,
    working_root: Path,
) -> dict:
    """Pipeline for Test 6: Passthrough.

    Pattern: DataGenerator -> MetricCalculator -> Filter(direct) -> DataTransformer
    Tests artifact identity preservation through curator operations.
    """
    pipeline = create_native_pipeline(
        name="test_passthrough",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    step0 = pipeline.run(
        definition=DataGenerator,
        params={"count": 2, "seed": 42},
    )

    pipeline.run(
        definition=MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
    )

    step2 = pipeline.run(
        definition=Filter,
        inputs={
            "passthrough": step0.output("datasets"),
        },
        params={
            "criteria": [
                {"metric": "distribution.min", "operator": "ge", "value": 0},
            ],
        },
    )

    pipeline.run(
        definition=DataTransformer,
        inputs={"dataset": step2.output("passthrough")},
        params={
            "scale_factor": 1.1,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
    )

    return pipeline.finalize()


def test_passthrough(pipeline_env: dict[str, str]):
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

    passthrough_ids = get_execution_outputs(delta_root, 2, "passthrough")
    step0_ids = get_execution_outputs(delta_root, 0, "datasets")

    for pt_id in passthrough_ids:
        assert pt_id in step0_ids, (
            "Passthrough IDs must reference original artifacts, not copies"
        )

    assert count_artifacts_by_step(delta_root, 3) == len(passthrough_ids)


def batch_processing_pipeline(
    delta_root: Path,
    staging_root: Path,
    working_root: Path,
) -> dict:
    """Pipeline for Test 7: Batch Processing.

    Pattern: DataGenerator (5) -> DataTransformer (artifacts_per_unit=2)
    Tests execution unit batching.
    """
    pipeline = create_native_pipeline(
        name="test_batch_processing",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    step0 = pipeline.run(
        definition=DataGenerator,
        params={"count": 5, "seed": 42},
    )

    pipeline.run(
        definition=DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.1,
            "variants": 1,
            "seed": 100,
        },
        batch_strategy={"artifacts_per_unit": 2},
    )

    return pipeline.finalize()


def test_batch_processing(pipeline_env: dict[str, str]):
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

    assert count_executions_by_step(delta_root, 0) == 1

    assert count_executions_by_step(delta_root, 1) == math.ceil(5 / 2)

    assert count_artifacts_by_step(delta_root, 1) == 5

    from artisan.orchestration.engine.retained_execution_query import (
        _execution_membership,
    )
    from artisan.storage.core.execution_source import ScientificExecutionSource

    with _step_read(delta_root, 1, fs=None, storage_options=None, files_root=None) as (
        read,
        states,
    ):
        members = _execution_membership(read, states)
    assert all(
        isinstance(member.source, ScientificExecutionSource) for member in members
    )
    assert sorted(
        len(member.source.admission.boundary.input_roles()["dataset"])
        for member in members
    ) == [1, 2, 2]


def comprehensive_pipeline(
    source_files: list[Path],
    delta_root: str | Path,
    staging_root: str | Path,
    working_root: str | Path,
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
    pipeline = create_native_pipeline(
        name="test_comprehensive",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )

    step0 = pipeline.run(
        definition=IngestCSV,
        inputs=[str(f) for f in source_files],
    )

    step1 = pipeline.run(
        definition=DataGenerator,
        params={"count": 1, "rows_per_file": 1, "seed": 1},
    )

    step2 = pipeline.run(
        definition=Merge,
        inputs={
            "ingested": step0.output("csv"),
            "generated": step1.output("datasets"),
        },
    )

    step3 = pipeline.run(
        definition=DataTransformer,
        inputs={"dataset": step2.output("merged")},
        params={
            "scale_factor": 0.1,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
            "output_prefix": "A",
        },
    )

    step4 = pipeline.run(
        definition=DataTransformer,
        inputs={"dataset": step2.output("merged")},
        params={
            "scale_factor": 2.0,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 200,
            "output_prefix": "B",
        },
    )

    step5 = pipeline.run(
        definition=Merge,
        inputs={
            "branch_a": step3.output("dataset"),
            "branch_b": step4.output("dataset"),
        },
    )

    pipeline.run(
        definition=MetricCalculator,
        inputs={"dataset": step5.output("merged")},
    )

    step7 = pipeline.run(
        definition=Filter,
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
    )

    pipeline.run(
        definition=DataTransformer,
        inputs={"dataset": step7.output("passthrough")},
        params={
            "scale_factor": 1.1,
            "noise_amplitude": 0.0,
            "variants": 2,
            "seed": 300,
        },
    )

    return pipeline.finalize()


def test_comprehensive(pipeline_env: dict[str, str], tmp_path: Path) -> None:
    """Combine two source kinds, branches, filtering, and nonempty fan-out."""
    delta_root = pipeline_env["delta_root"]
    source = tmp_path / "source.csv"
    source.write_text("id,x,y,z,score\n0,1.0,2.0,3.0,0.8\n")

    result = comprehensive_pipeline(
        source_files=[source],
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )
    assert result["overall_success"], "Pipeline should complete successfully"

    artifacts = read_table(delta_root, "artifacts/index")
    by_step = dict(artifacts.group_by("origin_step_number").len().iter_rows())
    assert by_step == {0: 2, 1: 1, 3: 2, 4: 2, 6: 4, 8: 4}
    by_type = dict(artifacts.group_by("artifact_type").len().iter_rows())
    assert by_type == {"file_ref": 1, "csv": 10, "metric": 4}

    merged_sources = get_execution_outputs(delta_root, 2, "merged")
    branch_a = get_execution_outputs(delta_root, 3, "dataset")
    branch_b = get_execution_outputs(delta_root, 4, "dataset")
    merged_branches = get_execution_outputs(delta_root, 5, "merged")
    assert len(merged_sources) == len(branch_a) == len(branch_b) == 2
    assert set(merged_branches) == set(branch_a) | set(branch_b)
    assert len(merged_branches) == 4

    passthrough = get_execution_outputs(delta_root, 7, "passthrough")
    assert set(passthrough) == set(branch_b)
    assert len(passthrough) == 2
    assert set(passthrough).isdisjoint(branch_a)
    assert len(get_execution_outputs(delta_root, 8, "dataset")) == 4
