"""Generative operation that produces CSV datasets with random numeric data."""

from __future__ import annotations

import csv
import os
import random
from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas import ArtifactResult
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput
from artisan.schemas.specs.output_spec import OutputSpec


class DataGenerator(OperationDefinition):
    """Generate CSV datasets with random numeric data.

    Demonstrates the generative pattern: no inputs, config params control
    file generation. Each CSV has columns id, x, y, z, score.

    Output Roles:
        datasets (data) -- Generated CSV dataset files
    """

    # ---------- Metadata ----------
    name = "data_generator"
    description = "Generate CSV datasets with random numeric data"

    # ---------- Inputs ----------
    inputs: ClassVar[dict] = {}

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        datasets = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.datasets: OutputSpec(
            artifact_type="data",
            description="Generated CSV dataset files",
            infer_lineage_from={"inputs": []},
        ),
    }

    # ---------- Parameters ----------
    class Params(BaseModel):
        """Algorithm parameters for DataGenerator."""

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
            description="Random seed for reproducibility. If None, uses random seed.",
        )

    params: Params = Params()

    # ---------- Resources ----------
    resources: ResourceConfig = ResourceConfig(time_limit="00:30:00")

    # ---------- Execution ----------
    execution: ExecutionConfig = ExecutionConfig(
        units_per_worker=100, job_name="data_generator"
    )

    # ---------- Lifecycle ----------
    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Write CSV files with columns id, x, y, z, score to execute_dir."""
        output_dir = inputs.execute_dir
        os.makedirs(output_dir, exist_ok=True)

        rng = random.Random(self.params.seed)
        created_files = []

        for i in range(self.params.count):
            filename = f"dataset_{i:05d}.csv"
            filepath = os.path.join(output_dir, filename)

            with open(filepath, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["id", "x", "y", "z", "score"])
                for row_idx in range(self.params.rows_per_file):
                    writer.writerow([
                        row_idx,
                        round(rng.uniform(0.0, 10.0), 4),
                        round(rng.uniform(0.0, 10.0), 4),
                        round(rng.uniform(0.0, 10.0), 4),
                        round(rng.uniform(0.0, 1.0), 4),
                    ])

            created_files.append(filepath)

        return {"created_files": created_files}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build DataArtifact drafts from generated CSV files."""
        raw = inputs.memory_outputs
        created_files = raw.get("created_files", [])

        drafts: list[DataArtifact] = []
        for file_path in inputs.file_outputs:
            if file_path.endswith(".csv"):
                with open(file_path, "rb") as f:
                    content = f.read()
                drafts.append(
                    DataArtifact.draft(
                        content=content,
                        original_name=os.path.basename(file_path),
                        step_number=inputs.step_number,
                    )
                )

        return ArtifactResult(
            success=True,
            artifacts={"datasets": drafts},
            metadata={
                "operation": "data_generator",
                "count": self.params.count,
                "rows_per_file": self.params.rows_per_file,
                "seed": self.params.seed,
                "created_files": created_files,
            },
        )
