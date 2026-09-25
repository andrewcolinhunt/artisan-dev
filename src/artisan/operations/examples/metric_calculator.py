"""Compute distribution statistics from CSV datasets."""

from __future__ import annotations

import csv
import os
import statistics
from enum import StrEnum, auto
from typing import Any, ClassVar

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.compute import ComputeProvider, ModalComputeConfig
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class MetricCalculator(OperationDefinition):
    """Calculate distribution statistics from CSV dataset inputs.

    Computes nested metrics for each input CSV:
      - distribution: {min, max, median, range}
      - summary: {cv, row_count}

    Complements DataGeneratorWithMetrics which produces central-tendency
    stats (mean, std).

    Input Roles:
        dataset (data) -- Input CSV dataset for metric calculation

    Output Roles:
        metrics (metric) -- Computed metric values (nested dict)
    """

    name = "metric_calculator"
    description = "Calculate distribution statistics from CSV datasets"

    class InputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(
            artifact_type="data",
            required=True,
            description="Input CSV dataset for metric calculation",
        ),
    }

    class OutputRole(StrEnum):
        metrics = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.metrics: OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            description="Computed metric values",
            derives_from={"inputs": ["dataset"]},
        ),
    }

    runner_resources: RunnerResources = RunnerResources(time_limit="00:30:00")  # type: ignore[call-arg]  # pydantic defaults

    batch_strategy: BatchStrategy = BatchStrategy(
        job_name="metric_calculator", artifacts_per_unit=10000
    )  # type: ignore[call-arg]  # pydantic defaults

    compute_provider: ComputeProvider = ComputeProvider(
        modal=ModalComputeConfig(),
    )

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Carry each dataset's path, identity, and human name together."""
        return {
            "dataset": PerArtifact(
                [
                    {
                        "path": artifact.materialized_path,
                        "artifact_id": artifact.artifact_id,
                        "original_name": artifact.original_name,
                    }
                    for artifact in inputs.input_artifacts["dataset"]
                ]
            )
        }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Compute score distribution statistics for each input CSV."""
        dataset_input = inputs.inputs.get("dataset")
        if dataset_input is None:
            msg = "No dataset input provided"
            raise ValueError(msg)

        calculated_metrics = []

        for dataset in dataset_input:
            input_path = dataset["path"]
            if not os.path.exists(input_path):
                msg = f"Input file not found: {input_path}"
                raise FileNotFoundError(msg)

            metrics = _compute_csv_statistics(input_path)
            metric_key = f"{dataset['original_name']}_metrics.json"

            calculated_metrics.append(
                {
                    "input": input_path,
                    "source_artifact_id": dataset["artifact_id"],
                    "metric_key": metric_key,
                    **metrics,
                }
            )

        return {"calculated_metrics": calculated_metrics}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build MetricArtifact drafts from computed statistics."""
        raw = inputs.memory_outputs
        calculated_metrics = raw.get("calculated_metrics", [])

        result = ArtifactResult(
            artifacts={"metrics": []},
            lineage={"metrics": []},
            metadata={
                "operation": "metric_calculator",
                "n_datasets": len(calculated_metrics),
                "calculated_metrics": calculated_metrics,
            },
        )

        for metric_data in calculated_metrics:
            result.add_artifact(
                "metrics",
                MetricArtifact.draft(
                    content={
                        "distribution": metric_data["distribution"],
                        "summary": metric_data["summary"],
                    },
                    original_name=metric_data["metric_key"],
                    step_number=inputs.step_number,
                ),
                sources={"dataset": [metric_data["source_artifact_id"]]},
            )

        return result


def _compute_csv_statistics(path: str) -> dict[str, dict[str, float]]:
    """Compute distribution statistics from a CSV file.

    Focuses on score distribution (min, max, median, range, CV) to complement
    DataGeneratorWithMetrics which computes central-tendency stats (mean, std).

    Args:
        path: Path to CSV file with x, y, z, score columns.

    Returns:
        Nested dict with ``distribution`` (min, max, median, range) and
        ``summary`` (cv, row_count) groups.
    """
    with open(path) as f:
        reader = csv.DictReader(f)
        scores = [float(row["score"]) for row in reader]

    n = len(scores)
    if n == 0:
        return {
            "distribution": {"min": 0.0, "max": 0.0, "median": 0.0, "range": 0.0},
            "summary": {"cv": 0.0, "row_count": 0},
        }

    mean = statistics.mean(scores)
    stdev = statistics.stdev(scores) if n >= 2 else 0.0

    return {
        "distribution": {
            "min": min(scores),
            "max": max(scores),
            "median": statistics.median(scores),
            "range": max(scores) - min(scores),
        },
        "summary": {
            "cv": round(stdev / mean, 6) if mean != 0 else 0.0,
            "row_count": n,
        },
    }
