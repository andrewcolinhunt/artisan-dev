"""Integration tests for LINEAGE grouping and with_associated.

Defines custom operations for LINEAGE grouping and with_associated specs,
then tests each pattern end-to-end.
"""

from __future__ import annotations

import csv
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import pytest

pytestmark = pytest.mark.slow

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
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
)

# =============================================================================
# Custom Test Operations
# =============================================================================


class DualInputLineage(OperationDefinition):
    """Dual-input operation using LINEAGE grouping."""

    name = "dual_input_lineage"
    description = "Copy primary CSV inputs (lineage matching)"

    class InputRole(StrEnum):
        primary = "primary"
        secondary = "secondary"

    class OutputRole(StrEnum):
        result = "result"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.primary: InputSpec(artifact_type="data"),
        InputRole.secondary: InputSpec(artifact_type="metric"),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["primary", "secondary"]},
        ),
    }
    group_by: ClassVar[GroupByStrategy | None] = GroupByStrategy.LINEAGE

    resources: ResourceConfig = ResourceConfig(time_limit="00:10:00")
    execution: ExecutionConfig = ExecutionConfig(job_name="dual_input_lineage")

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        output_dir = inputs.execute_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        primary_files = inputs.inputs.get("primary", [])
        if isinstance(primary_files, (str, Path)):
            primary_files = [primary_files]

        for pf in primary_files:
            pf = Path(pf)
            # Read CSV and add a marker column to produce unique content
            with pf.open() as fh:
                reader = csv.DictReader(fh)
                headers = list(reader.fieldnames or []) + ["lineage_marker"]
                rows = list(reader)

            out_path = output_dir / f"{pf.stem}_0.csv"
            with out_path.open("w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=headers)
                writer.writeheader()
                for row in rows:
                    row["lineage_marker"] = "1"
                    writer.writerow(row)

        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts = [
            DataArtifact.draft(
                content=f.read_bytes(),
                original_name=f.name,
                step_number=inputs.step_number,
            )
            for f in inputs.file_outputs
            if f.suffix == ".csv"
        ]
        return ArtifactResult(success=True, artifacts={"result": drafts})


class AssociatedMetricConsumer(OperationDefinition):
    """Single-input operation that reads associated metrics."""

    name = "associated_metric_consumer"
    description = "Read associated metrics for input data artifacts"

    class InputRole(StrEnum):
        primary = "primary"

    class OutputRole(StrEnum):
        result = "result"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.primary: InputSpec(
            artifact_type="data",
            with_associated=("metric",),
        ),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["primary"]},
        ),
    }

    resources: ResourceConfig = ResourceConfig(time_limit="00:10:00")
    execution: ExecutionConfig = ExecutionConfig(job_name="associated_metric_consumer")

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        result = {}
        for i, artifact in enumerate(inputs.input_artifacts["primary"]):
            associated = inputs.associated_artifacts(artifact, "metric")
            result[f"primary_{i}_path"] = str(artifact.materialized_path)
            result[f"primary_{i}_assoc_count"] = len(associated)
        result["count"] = len(inputs.input_artifacts["primary"])
        return result

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        output_dir = inputs.execute_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        count = inputs.inputs.get("count", 0)
        for i in range(count):
            assoc_count = inputs.inputs.get(f"primary_{i}_assoc_count", 0)
            primary_path = Path(inputs.inputs[f"primary_{i}_path"])

            # Use {stem}_0.csv naming for lineage inference
            # Add assoc_count as a column to produce unique content
            with primary_path.open() as fh:
                reader = csv.DictReader(fh)
                headers = list(reader.fieldnames or []) + ["assoc_count"]
                rows = list(reader)

            out_path = output_dir / f"{primary_path.stem}_0.csv"
            with out_path.open("w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=headers)
                writer.writeheader()
                for row in rows:
                    row["assoc_count"] = str(assoc_count)
                    writer.writerow(row)

        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts = [
            DataArtifact.draft(
                content=f.read_bytes(),
                original_name=f.name,
                step_number=inputs.step_number,
            )
            for f in inputs.file_outputs
            if f.suffix == ".csv"
        ]
        return ArtifactResult(success=True, artifacts={"result": drafts})


# =============================================================================
# Tests
# =============================================================================


def test_lineage_grouping(pipeline_env: dict[str, Path]):
    """LINEAGE grouping: match by shared ancestry."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_lineage",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    # Gen(2) → Transform(2) → MetricCalc(2)
    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )
    step1 = pipeline.run(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.1,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
    )
    step2 = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step1.output("dataset")},
        backend=Backend.LOCAL,
    )

    # LINEAGE matches: B1↔M1 (share ancestor A1), B2↔M2 (share ancestor A2)
    step3 = pipeline.run(
        DualInputLineage,
        inputs={
            "primary": step1.output("dataset"),
            "secondary": step2.output("metrics"),
        },
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    assert count_artifacts_by_step(delta_root, 3) == 2
    assert count_executions_by_step(delta_root, 3) == 2


def test_with_associated(pipeline_env: dict[str, Path]):
    """with_associated resolves associated metrics for each input artifact."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_associated",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )
    # MetricCalc creates artifact_edges: D1→M1, D2→M2
    step1 = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        backend=Backend.LOCAL,
    )

    step2 = pipeline.run(
        AssociatedMetricConsumer,
        inputs={"primary": step0.output("datasets")},
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    assert count_artifacts_by_step(delta_root, 2) == 2

    # Verify association count is encoded in output filenames
    from .conftest import get_execution_outputs

    output_ids = get_execution_outputs(delta_root, 2, "result")
    assert len(output_ids) == 2
