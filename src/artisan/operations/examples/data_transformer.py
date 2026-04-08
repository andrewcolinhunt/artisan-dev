"""Transform CSV datasets by scaling and adding noise to numeric columns."""

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
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class DataTransformer(OperationDefinition):
    """Transform CSV datasets by scaling and adding noise.

    Reads input CSVs, applies scale_factor to numeric columns (x, y, z, score),
    optionally adds uniform noise, and writes output CSVs. Produces `variants`
    output files per input.

    Input Roles:
        dataset (data) -- Input CSV dataset to transform

    Output Roles:
        dataset (data) -- Transformed CSV dataset file(s)
    """

    # ---------- Metadata ----------
    name = "data_transformer"
    description = "Transform CSV datasets by scaling and adding noise"

    # ---------- Inputs ----------
    class InputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(
            artifact_type="data",
            required=True,
            description="Input CSV dataset to transform",
        ),
    }

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="data",
            description="Transformed CSV dataset file(s)",
            infer_lineage_from={"inputs": ["dataset"]},
        ),
    }

    # ---------- Parameters ----------
    class Params(BaseModel):
        """Algorithm parameters for DataTransformer."""

        scale_factor: float = Field(
            default=1.5,
            ge=0.0,
            description="Multiplicative scale factor for numeric columns",
        )
        noise_amplitude: float = Field(
            default=0.1,
            ge=0.0,
            description="Maximum amplitude of uniform noise added to numeric columns",
        )
        variants: int = Field(
            default=1,
            ge=1,
            description="Number of transformed variants per input dataset",
        )
        seed: int | None = Field(
            default=None,
            description="Random seed for reproducibility",
        )
        output_prefix: str = Field(
            default="",
            description="Optional prefix prepended to output filenames",
        )

    params: Params = Params()

    # ---------- Resources ----------
    resources: ResourceConfig = ResourceConfig(time_limit="00:30:00")

    # ---------- Execution ----------
    execution: ExecutionConfig = ExecutionConfig(job_name="data_transformer")

    # ---------- Lifecycle ----------
    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Extract materialized paths from input artifacts."""
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Apply scale factor and noise to numeric columns of input CSVs."""
        output_dir = inputs.execute_dir
        os.makedirs(output_dir, exist_ok=True)

        dataset_input = inputs.inputs.get("dataset")
        if dataset_input is None:
            raise ValueError("No dataset input provided")

        if isinstance(dataset_input, str):
            input_files = [dataset_input]
        else:
            input_files = list(dataset_input)

        rng = random.Random(self.params.seed)
        created_files = []
        numeric_cols = {"x", "y", "z", "score"}

        for input_path in input_files:
            if not os.path.exists(input_path):
                raise FileNotFoundError(f"Input file not found: {input_path}")

            with open(input_path) as f:
                reader = csv.DictReader(f)
                headers = list(reader.fieldnames or [])
                rows = list(reader)

            stem = os.path.splitext(os.path.basename(input_path))[0]

            for variant_idx in range(self.params.variants):
                suffix = f"_{self.params.output_prefix}" if self.params.output_prefix else ""
                output_path = os.path.join(output_dir, f"{stem}_{variant_idx}{suffix}.csv")
                with open(output_path, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    for row in rows:
                        new_row = dict(row)
                        for col in headers:
                            if col in numeric_cols:
                                val = float(row[col])
                                val *= self.params.scale_factor
                                if self.params.noise_amplitude > 0:
                                    val += rng.uniform(
                                        -self.params.noise_amplitude,
                                        self.params.noise_amplitude,
                                    )
                                new_row[col] = round(val, 4)
                        writer.writerow(new_row)

                created_files.append(output_path)

        return {"created_files": created_files}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build DataArtifact drafts from transformed CSV files."""
        raw = inputs.memory_outputs

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
            artifacts={"dataset": drafts},
            metadata={
                "operation": "data_transformer",
                "scale_factor": self.params.scale_factor,
                "noise_amplitude": self.params.noise_amplitude,
                "variants": self.params.variants,
                "seed": self.params.seed,
                "created_files": raw.get("created_files", []),
            },
        )
