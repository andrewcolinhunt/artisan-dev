"""Integration tests for per-artifact batch execute through the local backend.

Verifies that the split lifecycle (prep_unit → route_execute_batch → post_unit)
produces correct artifacts and lineage when run through real operations with
real Delta Lake tables.
"""

from __future__ import annotations

import pytest

from artisan.execution.compute.local import LocalComputeRouter
from artisan.execution.executors.creator import run_creator_lifecycle
from artisan.execution.executors.creator_phases import post_unit, prep_unit
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment

from .conftest import count_artifacts_by_step, get_execution_outputs

pytestmark = pytest.mark.slow


def test_split_lifecycle_matches_monolithic(pipeline_env):
    """Per-artifact split path produces same artifact count and edges as monolithic."""
    # --- generate source data via pipeline ---
    pipeline = PipelineManager.create(
        name="batch_test",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )
    pipeline.run(
        operation=DataGenerator,
        name="generate",
        params={"count": 3, "seed": 42},
    )
    result = pipeline.finalize()
    assert result["overall_success"]

    gen_artifacts = count_artifacts_by_step(pipeline_env["delta_root"], 0)
    assert gen_artifacts == 3

    # Get the generated artifact IDs for transform input
    source_ids = get_execution_outputs(pipeline_env["delta_root"], 0, "datasets")
    assert len(source_ids) == 3

    runtime_env = RuntimeEnvironment(
        delta_root=pipeline_env["delta_root"],
        working_root=pipeline_env["working_root"],
        staging_root=pipeline_env["staging_root"],
    )

    # --- monolithic path: run_creator_lifecycle (no splitting) ---
    unit_mono = ExecutionUnit(
        operation=DataTransformer(params={"scale_factor": 0.5, "variants": 1}),
        inputs={"dataset": source_ids},
        execution_spec_id="spec_mo" + "0" * 26,
        step_number=1,
    )
    mono_result = run_creator_lifecycle(unit_mono, runtime_env)

    # --- split path: prep_unit → route_execute_batch → post_unit ---
    unit_split = ExecutionUnit(
        operation=DataTransformer(params={"scale_factor": 0.5, "variants": 1}),
        inputs={"dataset": source_ids},
        execution_spec_id="spec_sp" + "0" * 26,
        step_number=2,
    )

    prepped = prep_unit(unit_split, runtime_env)

    router = LocalComputeRouter()
    raw_results = list(
        router.route_execute_batch(
            prepped.operation,
            prepped.artifact_execute_inputs,
            prepped.sandbox_path,
        )
    )

    split_result = post_unit(prepped, raw_results, runtime_env)

    # --- compare ---
    mono_artifact_count = sum(len(arts) for arts in mono_result.artifacts.values())
    split_artifact_count = sum(len(arts) for arts in split_result.artifacts.values())
    assert split_artifact_count == mono_artifact_count
    assert len(split_result.edges) == len(mono_result.edges)
    assert set(split_result.artifacts.keys()) == set(mono_result.artifacts.keys())


def test_pipeline_with_batched_transform(pipeline_env):
    """Full pipeline with batched transform step runs correctly."""
    pipeline = PipelineManager.create(
        name="batched_pipeline",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )
    output = pipeline.output

    pipeline.run(
        operation=DataGenerator,
        name="generate",
        params={"count": 4, "seed": 42},
    )

    pipeline.run(
        operation=DataTransformer,
        name="transform",
        inputs={"dataset": output("generate", "datasets")},
        params={"scale_factor": 0.5, "variants": 1},
    )

    result = pipeline.finalize()

    assert result["overall_success"]
    assert count_artifacts_by_step(pipeline_env["delta_root"], 0) == 4
    assert count_artifacts_by_step(pipeline_env["delta_root"], 1) == 4
