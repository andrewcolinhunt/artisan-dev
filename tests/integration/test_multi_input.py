"""Integration tests for LINEAGE grouping and with_associated.

Defines custom operations for LINEAGE grouping and with_associated specs,
then tests each pattern end-to-end.
"""

from __future__ import annotations

import csv
import os
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import pytest

pytestmark = pytest.mark.integration

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners import Runner
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.runner_resources import RunnerResources
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
    load_artifact_edges,
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
    group_by: GroupByStrategy | None = GroupByStrategy.LINEAGE

    runner_resources: RunnerResources = RunnerResources(time_limit="00:10:00")
    batch_strategy: BatchStrategy = BatchStrategy(job_name="dual_input_lineage")

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        output_dir = inputs.execute_dir
        os.makedirs(output_dir, exist_ok=True)

        primary_files = inputs.inputs.get("primary", [])
        if isinstance(primary_files, (str, Path)):
            primary_files = [primary_files]

        for pf in primary_files:
            pf = Path(pf)
            with open(pf) as fh:
                reader = csv.DictReader(fh)
                headers = [*list(reader.fieldnames or []), "lineage_marker"]
                rows = list(reader)

            out_path = os.path.join(output_dir, f"{pf.stem}_0.csv")
            with open(out_path, "w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=headers)
                writer.writeheader()
                for row in rows:
                    row["lineage_marker"] = "1"
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

    runner_resources: RunnerResources = RunnerResources(time_limit="00:10:00")
    batch_strategy: BatchStrategy = BatchStrategy(job_name="associated_metric_consumer")

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
        os.makedirs(output_dir, exist_ok=True)

        count = inputs.inputs.get("count", 0)
        for i in range(count):
            assoc_count = inputs.inputs.get(f"primary_{i}_assoc_count", 0)
            primary_path = Path(inputs.inputs[f"primary_{i}_path"])

            with open(primary_path) as fh:
                reader = csv.DictReader(fh)
                headers = [*list(reader.fieldnames or []), "assoc_count"]
                rows = list(reader)

            out_path = os.path.join(output_dir, f"{primary_path.stem}_0.csv")
            with open(out_path, "w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=headers)
                writer.writeheader()
                for row in rows:
                    row["assoc_count"] = str(assoc_count)
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


def test_lineage_grouping(pipeline_env: dict[str, str]):
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
        step_runner=Runner.LOCAL,
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
        step_runner=Runner.LOCAL,
    )
    step2 = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step1.output("dataset")},
        step_runner=Runner.LOCAL,
    )

    # LINEAGE matches: B1↔M1 (share ancestor A1), B2↔M2 (share ancestor A2)
    pipeline.run(
        DualInputLineage,
        inputs={
            "primary": step1.output("dataset"),
            "secondary": step2.output("metrics"),
        },
        step_runner=Runner.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    assert count_artifacts_by_step(delta_root, 3) == 2
    assert count_executions_by_step(delta_root, 3) == 2


def test_with_associated(pipeline_env: dict[str, str]):
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
        step_runner=Runner.LOCAL,
    )
    # MetricCalc creates artifact_edges: D1→M1, D2→M2
    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        step_runner=Runner.LOCAL,
    )

    pipeline.run(
        AssociatedMetricConsumer,
        inputs={"primary": step0.output("datasets")},
        step_runner=Runner.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    assert count_artifacts_by_step(delta_root, 2) == 2

    # Verify association count is encoded in output filenames
    output_ids = get_execution_outputs(delta_root, 2, "result")
    assert len(output_ids) == 2


# =============================================================================
# CROSS_PRODUCT grouping via per-step override
# =============================================================================


class DualInputCrossProduct(OperationDefinition):
    """Dual-input op exercising per-step CROSS_PRODUCT override.

    Output bytes concatenate primary CSV + secondary metric JSON so each
    pair produces a distinct ``artifact_id`` (avoids the content-addressed
    collision that bites CROSS_PRODUCT when output bytes only depend on
    one input). Class-level ``group_by`` is ``None`` — the per-step
    override drives pairing.
    """

    name = "dual_input_cross_product"
    description = "Cross-product pair primary CSV with secondary metric"

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
    # No class-level group_by — the per-step override drives pairing.

    runner_resources: RunnerResources = RunnerResources(time_limit="00:10:00")
    batch_strategy: BatchStrategy = BatchStrategy(job_name="dual_input_cross_product")

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        out_dir = inputs.execute_dir
        os.makedirs(out_dir, exist_ok=True)

        primary_paths = inputs.inputs.get("primary", [])
        secondary_paths = inputs.inputs.get("secondary", [])
        if isinstance(primary_paths, (str, Path)):
            primary_paths = [primary_paths]
        if isinstance(secondary_paths, (str, Path)):
            secondary_paths = [secondary_paths]

        # Default ``artifacts_per_unit=1`` → one (primary, secondary) pair
        # per execute call. Concatenate both inputs into the output bytes
        # so distinct pairs produce distinct artifact_ids.
        primary = Path(primary_paths[0])
        secondary = Path(secondary_paths[0])
        out_path = Path(out_dir) / f"{primary.stem}__{secondary.stem}.bin"
        out_path.write_bytes(primary.read_bytes() + b"\n---\n" + secondary.read_bytes())
        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts = []
        for f in inputs.file_outputs:
            if f.endswith(".bin"):
                drafts.append(
                    DataArtifact.draft(
                        content=Path(f).read_bytes(),
                        original_name=os.path.basename(f),
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"result": drafts})


class DualInputCrossProductClassDefault(DualInputCrossProduct):
    """Same operation but with CROSS_PRODUCT declared at the class level.

    Confirms that the field-default path (post ClassVar → field migration)
    matches the per-step override path.
    """

    name = "dual_input_cross_product_class_default"
    group_by: GroupByStrategy | None = GroupByStrategy.CROSS_PRODUCT


def _build_one_by_three_pipeline(pipeline_env: dict[str, str], name: str):
    """Build a pipeline with steps 0-2 producing 1 primary and 3 secondaries.

    Steps:
        0. DataGenerator(count=1, seed=42)  → 1 primary dataset
        1. DataGenerator(count=3, seed=100) → 3 secondary datasets
        2. MetricCalculator(step1.datasets) → 3 metrics (one per dataset)

    Returns the PipelineManager with step references attached as
    ``.step_primary``, ``.step_metrics`` for the downstream dispatch.
    """
    delta_root = pipeline_env["delta_root"]
    pipeline = PipelineManager.create(
        name=name,
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )
    step_primary = pipeline.run(
        DataGenerator,
        params={"count": 1, "seed": 42},
        step_runner=Runner.LOCAL,
    )
    step_secondary = pipeline.run(
        DataGenerator,
        params={"count": 3, "seed": 100},
        step_runner=Runner.LOCAL,
    )
    step_metrics = pipeline.run(
        MetricCalculator,
        inputs={"dataset": step_secondary.output("datasets")},
        step_runner=Runner.LOCAL,
    )
    pipeline.step_primary = step_primary  # type: ignore[attr-defined]
    pipeline.step_metrics = step_metrics  # type: ignore[attr-defined]
    return pipeline


def _assert_cross_product_provenance(delta_root: str, op_step_number: int) -> None:
    """Shared correctness floor for a 1x3 CROSS_PRODUCT step.

    Asserts:
      - 3 output artifacts, 3 executions for the operation step.
      - The 3 output artifact_ids are distinct (CROSS_PRODUCT output
        collision floor — bytes-depend-on-both-inputs guard).
      - Lineage builds 2 incoming edges per output (one primary, one
        secondary), 6 total.
      - Per-pair edges share a group_id; the 3 pair group_ids are
        distinct.
    """
    assert count_artifacts_by_step(delta_root, op_step_number) == 3
    assert count_executions_by_step(delta_root, op_step_number) == 3

    output_ids = get_execution_outputs(delta_root, op_step_number, "result")
    assert len(set(output_ids)) == 3, (
        "CROSS_PRODUCT outputs collided on artifact_id — execute() bytes "
        "must depend on both inputs."
    )

    primary_ids = set(get_execution_outputs(delta_root, 0, "datasets"))
    secondary_ids = set(get_execution_outputs(delta_root, 2, "metrics"))
    assert len(primary_ids) == 1
    assert len(secondary_ids) == 3

    edges = load_artifact_edges(delta_root, output_ids)
    assert edges.height == 6, (
        f"Expected 6 incoming edges (3 outputs x 2 sources), got {edges.height}"
    )

    by_output: dict[str, list[dict]] = {}
    for row in edges.iter_rows(named=True):
        by_output.setdefault(row["target_artifact_id"], []).append(row)
    assert len(by_output) == 3

    pair_group_ids: set[str] = set()
    for tid, in_edges in by_output.items():
        sources = {e["source_artifact_id"] for e in in_edges}
        assert sources & primary_ids, f"output {tid} missing primary edge"
        assert sources & secondary_ids, f"output {tid} missing secondary edge"

        primary_edge = next(
            e for e in in_edges if e["source_artifact_id"] in primary_ids
        )
        secondary_edge = next(
            e for e in in_edges if e["source_artifact_id"] in secondary_ids
        )
        # Both edges in a pair must share a group_id.
        assert primary_edge["group_id"] is not None
        assert primary_edge["group_id"] == secondary_edge["group_id"]
        pair_group_ids.add(primary_edge["group_id"])

    assert len(pair_group_ids) == 3, (
        "CROSS_PRODUCT pairs should have distinct group_ids"
    )


def test_cross_product_grouping(pipeline_env: dict[str, str]):
    """1x3 CROSS_PRODUCT via per-step ``group_by`` override.

    Asserts artifact count, distinct artifact_ids, correct co-input
    edges with shared group_ids, and distinct group_ids across pairs.
    """
    delta_root = pipeline_env["delta_root"]
    pipeline = _build_one_by_three_pipeline(pipeline_env, "test_cross_product")

    pipeline.run(
        DualInputCrossProduct,
        inputs={
            "primary": pipeline.step_primary.output("datasets"),
            "secondary": pipeline.step_metrics.output("metrics"),
        },
        group_by=GroupByStrategy.CROSS_PRODUCT,
        step_runner=Runner.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]
    _assert_cross_product_provenance(delta_root, op_step_number=3)


def test_cross_product_default_preserves_classvar_behavior(
    pipeline_env: dict[str, str],
):
    """An op declaring ``group_by=CROSS_PRODUCT`` at class level produces
    the same provenance structure as the per-step-override path.

    Locks in the backward-compatible-defaults criterion across the
    ClassVar → field migration.
    """
    delta_root = pipeline_env["delta_root"]
    pipeline = _build_one_by_three_pipeline(
        pipeline_env, "test_cross_product_class_default"
    )

    pipeline.run(
        DualInputCrossProductClassDefault,
        inputs={
            "primary": pipeline.step_primary.output("datasets"),
            "secondary": pipeline.step_metrics.output("metrics"),
        },
        step_runner=Runner.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]
    _assert_cross_product_provenance(delta_root, op_step_number=3)
