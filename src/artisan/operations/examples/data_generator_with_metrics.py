"""Generative operation that produces CSV datasets and co-produced statistics."""

from __future__ import annotations

import csv
import os
import random
import statistics
from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.compute import Compute, ModalComputeConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput
from artisan.schemas.specs.output_spec import OutputSpec


class DataGeneratorWithMetrics(OperationDefinition):
    """Generate CSV datasets and compute statistics with output-to-output lineage.

    Produces N datasets (like DataGenerator) and also computes statistics for
    each one (like MetricCalculator). The metric's lineage points to the
    co-produced dataset, not to any input.

    Demonstrates ``infer_lineage_from={"outputs": ["datasets"]}``.

    Output Roles:
        datasets (data) -- Generated CSV dataset files
        metrics (metric) -- Statistics derived from co-produced datasets
    """

    # ---------- Metadata ----------
    name = "data_generator_with_metrics"
    description = "Generate datasets with co-produced statistics"

    # ---------- Inputs ----------
    inputs: ClassVar[dict] = {}

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        datasets = auto()
        metrics = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.datasets: OutputSpec(
            artifact_type="data",
            description="Generated CSV dataset files",
            infer_lineage_from={"inputs": []},
        ),
        OutputRole.metrics: OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            description="Statistics derived from co-produced datasets",
            infer_lineage_from={"outputs": ["datasets"]},
        ),
    }

    # ---------- Parameters ----------
    class Params(BaseModel):
        """Algorithm parameters for DataGeneratorWithMetrics."""

        count: int = Field(
            default=1,
            ge=1,
            description="Number of CSV files to generate",
        )
        rows_per_file: int = Field(
            default=10,
            ge=1,
            description="Number of data rows per CSV file",
        )
        seed: int | None = Field(
            default=None,
            description="Random seed for reproducibility",
        )

    params: Params = Params()

    # ---------- Resources ----------
    resources: ResourceConfig = ResourceConfig(time_limit="00:30:00")

    # ---------- Execution ----------
    execution: ExecutionConfig = ExecutionConfig(
        artifacts_per_unit=100,
        units_per_worker=1,
        job_name="data_generator_with_metrics",
    )

    # ---------- Compute ----------
    compute: Compute = Compute(
        modal=ModalComputeConfig(image="python:3.12-slim"),
    )

    # ---------- Lifecycle ----------
    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Write CSV datasets and compute per-file summary statistics."""
        output_dir = inputs.execute_dir
        os.makedirs(output_dir, exist_ok=True)

        rng = random.Random(self.params.seed)
        created_files = []
        calculated_metrics = []

        for i in range(self.params.count):
            filename = f"dataset_{i:05d}.csv"
            filepath = os.path.join(output_dir, filename)

            xs, ys, zs, scores = [], [], [], []
            with open(filepath, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["id", "x", "y", "z", "score"])
                for row_idx in range(self.params.rows_per_file):
                    x = round(rng.uniform(0.0, 10.0), 4)
                    y = round(rng.uniform(0.0, 10.0), 4)
                    z = round(rng.uniform(0.0, 10.0), 4)
                    score = round(rng.uniform(0.0, 1.0), 4)
                    writer.writerow([row_idx, x, y, z, score])
                    xs.append(x)
                    ys.append(y)
                    zs.append(z)
                    scores.append(score)

            created_files.append(filepath)

            n = len(xs)
            calculated_metrics.append({
                "metric_key": f"dataset_{i:05d}_metrics.json",
                "mean_x": statistics.mean(xs),
                "mean_y": statistics.mean(ys),
                "mean_z": statistics.mean(zs),
                "mean_score": statistics.mean(scores),
                "std_score": statistics.stdev(scores) if n >= 2 else 0.0,
                "row_count": n,
            })

        return {
            "created_files": created_files,
            "calculated_metrics": calculated_metrics,
        }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build DataArtifact and MetricArtifact drafts from execution output."""
        raw = inputs.memory_outputs

        dataset_drafts: list[DataArtifact] = []
        for file_path in inputs.file_outputs:
            if file_path.endswith(".csv"):
                with open(file_path, "rb") as f:
                    content = f.read()
                dataset_drafts.append(
                    DataArtifact.draft(
                        content=content,
                        original_name=os.path.basename(file_path),
                        step_number=inputs.step_number,
                    )
                )

        metric_drafts: list[MetricArtifact] = []
        for metric_data in raw.get("calculated_metrics", []):
            metric_drafts.append(
                MetricArtifact.draft(
                    content={
                        "mean_x": metric_data["mean_x"],
                        "mean_y": metric_data["mean_y"],
                        "mean_z": metric_data["mean_z"],
                        "mean_score": metric_data["mean_score"],
                        "std_score": metric_data["std_score"],
                        "row_count": metric_data["row_count"],
                    },
                    original_name=metric_data["metric_key"],
                    step_number=inputs.step_number,
                )
            )

        return ArtifactResult(
            success=True,
            artifacts={
                "datasets": dataset_drafts,
                "metrics": metric_drafts,
            },
            metadata={
                "operation": "data_generator_with_metrics",
                "count": self.params.count,
                "seed": self.params.seed,
            },
        )
