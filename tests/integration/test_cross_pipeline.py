"""Integration tests for cross-pipeline operations.

Tests IngestPipelineStep (importing artifacts from another pipeline's Delta
store) and ExecutionConfigArtifact with $artifact references.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.slow

from artisan.operations.curator import IngestPipelineStep
from artisan.operations.examples import (
    DataGenerator,
    DataTransformer,
    DataTransformerConfig,
    MetricCalculator,
)
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend

from .conftest import (
    count_artifacts_by_step,
    count_artifacts_by_type,
    get_execution_outputs,
    read_table,
)


def test_ingest_pipeline_step_basic(
    dual_pipeline_env: dict[str, dict[str, Path]],
):
    """IngestPipelineStep imports artifacts from another pipeline."""
    env_a = dual_pipeline_env["a"]
    env_b = dual_pipeline_env["b"]

    # Pipeline A: Gen(3) → Transform(3)
    pa = PipelineManager.create(
        name="pipeline_a",
        delta_root=env_a["delta_root"],
        staging_root=env_a["staging_root"],
        working_root=env_a["working_root"],
    )
    step0 = pa.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        backend=Backend.LOCAL,
    )
    step1 = pa.run(
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
    pa.finalize()

    # Pipeline B: Ingest from A step 1
    pb = PipelineManager.create(
        name="pipeline_b",
        delta_root=env_b["delta_root"],
        staging_root=env_b["staging_root"],
        working_root=env_b["working_root"],
    )
    step0b = pb.run(
        IngestPipelineStep,
        params={
            "source_delta_root": str(env_a["delta_root"]),
            "source_step": 1,
        },
        backend=Backend.LOCAL,
    )
    result = pb.finalize()

    assert result["overall_success"]

    # 3 imported artifacts
    assert count_artifacts_by_step(env_b["delta_root"], 0) == 3

    # Same artifact IDs (content-addressed, same content)
    a_ids = set(get_execution_outputs(env_a["delta_root"], 1, "dataset"))
    b_ids = set(get_execution_outputs(env_b["delta_root"], 0, "data"))
    assert a_ids == b_ids


def test_ingest_pipeline_step_type_filter(
    dual_pipeline_env: dict[str, dict[str, Path]],
):
    """IngestPipelineStep with artifact_type filter imports only matching type."""
    env_a = dual_pipeline_env["a"]
    env_b = dual_pipeline_env["b"]

    # Pipeline A: Gen(2) → MetricCalc(2) — produces data + metric artifacts
    pa = PipelineManager.create(
        name="pipeline_a_filter",
        delta_root=env_a["delta_root"],
        staging_root=env_a["staging_root"],
        working_root=env_a["working_root"],
    )
    step0 = pa.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )
    pa.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        backend=Backend.LOCAL,
    )
    pa.finalize()

    # Pipeline B: Ingest only metrics from A step 1
    pb = PipelineManager.create(
        name="pipeline_b_filter",
        delta_root=env_b["delta_root"],
        staging_root=env_b["staging_root"],
        working_root=env_b["working_root"],
    )
    step0b = pb.run(
        IngestPipelineStep,
        params={
            "source_delta_root": str(env_a["delta_root"]),
            "source_step": 1,
            "artifact_type": "metric",
        },
        backend=Backend.LOCAL,
    )
    result = pb.finalize()

    assert result["overall_success"]

    # Only metric artifacts imported
    assert count_artifacts_by_type(env_b["delta_root"], "metric") == 2
    assert count_artifacts_by_type(env_b["delta_root"], "data") == 0


def test_execution_config_artifact_references(pipeline_env: dict[str, Path]):
    """DataTransformerConfig embeds $artifact references to input artifact IDs."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_config_refs",
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

    # Step 1: Generate configs with $artifact references
    step1 = pipeline.run(
        DataTransformerConfig,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factors": [1.0, 2.0],
            "noise_amplitudes": [0.0],
            "seed": 42,
        },
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # Read config artifacts from delta
    df_configs = read_table(delta_root, "artifacts/configs")
    assert not df_configs.is_empty()

    # Get step 0 artifact IDs for validation
    step0_ids = set(get_execution_outputs(delta_root, 0, "datasets"))
    assert len(step0_ids) == 2

    # Verify $artifact references in config content
    for row in df_configs.iter_rows(named=True):
        raw = row["content"]
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        content = json.loads(raw)
        if "input" in content and isinstance(content["input"], dict):
            ref = content["input"].get("$artifact")
            if ref is not None:
                assert (
                    ref in step0_ids
                ), f"$artifact reference {ref} should be a valid step 0 artifact ID"
