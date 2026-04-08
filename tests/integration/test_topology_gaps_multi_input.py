"""Integration tests for topology gaps requiring custom operations or complex setup.

Tests: co-produced metrics filter, 1:N LINEAGE grouping, resume-and-extend.
"""

from __future__ import annotations

import csv
import os
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import pytest

pytestmark = pytest.mark.slow

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator import Filter
from artisan.operations.examples import (
    DataGenerator,
    DataGeneratorWithMetrics,
    DataTransformer,
    DataTransformerConfig,
    MetricCalculator,
)
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

from .conftest import (
    count_artifacts_by_step,
    count_executions_by_step,
    get_execution_outputs,
    get_step_status,
)

# =============================================================================
# Custom Test Operation for 1:N LINEAGE
# =============================================================================


class DualInputConfigConsumer(OperationDefinition):
    """Consumes data + config inputs with LINEAGE grouping.

    Reads the primary CSV and writes a copy with a marker column.
    Config inputs are acknowledged but not materially used, since
    ExecutionConfigArtifact content is JSON (not a file path).
    """

    name = "dual_input_config_consumer"
    description = "Consume data and config inputs via LINEAGE grouping"

    class InputRole(StrEnum):
        dataset = "dataset"
        config = "config"

    class OutputRole(StrEnum):
        result = "result"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.dataset: InputSpec(artifact_type="data"),
        InputRole.config: InputSpec(artifact_type="config"),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["dataset", "config"]},
        ),
    }
    group_by: ClassVar[GroupByStrategy | None] = GroupByStrategy.LINEAGE

    resources: ResourceConfig = ResourceConfig(time_limit="00:10:00")
    execution: ExecutionConfig = ExecutionConfig(job_name="dual_input_config_consumer")

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        output_dir = inputs.execute_dir
        os.makedirs(output_dir, exist_ok=True)

        dataset_files = inputs.inputs.get("dataset", [])
        if isinstance(dataset_files, (str, Path)):
            dataset_files = [dataset_files]

        for df_path in dataset_files:
            df_path = Path(df_path)
            with open(df_path) as fh:
                reader = csv.DictReader(fh)
                headers = list(reader.fieldnames or []) + ["consumed"]
                rows = list(reader)

            out_path = os.path.join(output_dir, f"{df_path.stem}_0.csv")
            with open(out_path, "w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=headers)
                writer.writeheader()
                for row in rows:
                    row["consumed"] = "1"
                    writer.writerow(row)

        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts = []
        for f in inputs.file_outputs:
            if f.endswith(".csv"):
                with open(f, "rb") as fh:
                    content = fh.read()
                drafts.append(
                    DataArtifact.draft(
                        content=content,
                        original_name=os.path.basename(f),
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"result": drafts})


# =============================================================================
# Tests
# =============================================================================


def test_co_produced_metrics_filter(pipeline_env: dict[str, str]) -> None:
    """Filter auto-discovers co-produced metrics from same step."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_co_produced_metrics_filter",
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

    # Step 1: Filter using co-produced metrics (mean_score >= 0)
    # The filter should auto-discover metrics from step 0's output-to-output lineage
    step1 = pipeline.run(
        Filter,
        inputs={"passthrough": step0.output("datasets")},
        params={
            "criteria": [
                {"metric": "mean_score", "operator": "ge", "value": 0},
            ],
        },
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # Scores are uniform(0, 1), so mean_score >= 0 should pass all
    passthrough_ids = get_execution_outputs(delta_root, 1, "passthrough")
    assert len(passthrough_ids) == 3

    # Verify correct artifacts passed (same IDs as step 0 datasets)
    gen_dataset_ids = set(get_execution_outputs(delta_root, 0, "datasets"))
    assert set(passthrough_ids) == gen_dataset_ids


def test_lineage_grouping_one_to_many(pipeline_env: dict[str, str]) -> None:
    """1:N LINEAGE expansion: one dataset fans out to multiple configs."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_lineage_1_to_n",
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

    # Step 1: Generate configs (2 scale_factors per dataset = 4 configs total)
    step1 = pipeline.run(
        DataTransformerConfig,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factors": [1.0, 2.0],
            "noise_amplitudes": [0.0],
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )

    # Step 2: Consume with LINEAGE grouping
    # Each dataset has 2 configs descended from it -> 2 groups
    # Each group: 1 dataset + 2 configs -> product gives 2 execution units per group
    step2 = pipeline.run(
        DualInputConfigConsumer,
        inputs={
            "dataset": step0.output("datasets"),
            "config": step1.output("config"),
        },
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # 2 datasets, each with 2 configs = 4 execution units (product expansion)
    assert count_executions_by_step(delta_root, 2) == 4

    # Each execution unit produces 1 result artifact = 4 total
    assert count_artifacts_by_step(delta_root, 2) == 4


def test_resume_and_extend(pipeline_env: dict[str, str]) -> None:
    """Resume a successful pipeline and extend with new steps."""
    delta_root = pipeline_env["delta_root"]
    staging_root = pipeline_env["staging_root"]
    working_root = pipeline_env["working_root"]

    # Run 1: Generate -> Transform -> Metrics
    p1 = PipelineManager.create(
        name="test_resume_extend",
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )
    run_id = p1.config.pipeline_run_id

    step0 = p1.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )
    step1 = p1.run(
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
    p1.run(
        MetricCalculator,
        inputs={"dataset": step1.output("dataset")},
        backend=Backend.LOCAL,
    )
    p1.finalize()

    # Verify initial run
    assert count_artifacts_by_step(delta_root, 0) == 2
    assert count_artifacts_by_step(delta_root, 1) == 2
    assert count_artifacts_by_step(delta_root, 2) == 2

    # Run 2: Resume and extend with filter + transform
    p2 = PipelineManager.resume(
        delta_root=delta_root,
        staging_root=staging_root,
        pipeline_run_id=run_id,
    )

    # Resumed steps are restored
    assert p2.current_step >= 3

    # Extend: Filter on step 2 metrics, passthrough step 1 data
    step3 = p2.run(
        Filter,
        inputs={"passthrough": p2[1].output("dataset")},
        params={
            "criteria": [
                {"metric": "distribution.min", "operator": "ge", "value": 0},
            ],
        },
        backend=Backend.LOCAL,
    )

    # Step 4: Transform the filtered output
    p2.run(
        DataTransformer,
        inputs={"dataset": step3.output("passthrough")},
        params={
            "scale_factor": 2.0,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 200,
        },
        backend=Backend.LOCAL,
    )

    result = p2.finalize()
    assert result["overall_success"]

    # New steps executed
    assert get_step_status(delta_root, 3) == "completed"
    assert get_step_status(delta_root, 4) == "completed"

    # New steps produced artifacts
    passthrough_ids = get_execution_outputs(delta_root, 3, "passthrough")
    assert len(passthrough_ids) > 0
    assert count_artifacts_by_step(delta_root, 4) == len(passthrough_ids)
