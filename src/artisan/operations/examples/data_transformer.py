"""Transform CSV datasets by scaling and adding noise to numeric columns."""

from __future__ import annotations

import csv
import os
import random
from enum import StrEnum
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
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

    name = "data_transformer"
    description = "Transform CSV datasets by scaling and adding noise"

    class InputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(
            artifact_type="data",
            required=True,
            description="Input CSV dataset to transform",
        ),
    }

    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="data",
            description="Transformed CSV dataset file(s)",
            derives_from={"inputs": ["dataset"]},
        ),
    }

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
            description="Optional suffix appended to output filenames",
        )

    params: Params = Params()

    runner_resources: RunnerResources = RunnerResources(time_limit="00:30:00")  # type: ignore[call-arg]  # pydantic defaults

    batch_strategy: BatchStrategy = BatchStrategy(job_name="data_transformer")  # type: ignore[call-arg]  # pydantic defaults

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
        """Apply scale factor and noise to numeric columns of input CSVs."""
        output_dir = inputs.execute_dir
        os.makedirs(output_dir, exist_ok=True)

        dataset_input = inputs.inputs.get("dataset")
        if dataset_input is None:
            msg = "No dataset input provided"
            raise ValueError(msg)

        rng = random.Random(self.params.seed)
        created_files = []
        outputs = []
        numeric_cols = {"x", "y", "z", "score"}

        for dataset in dataset_input:
            input_path = dataset["path"]
            if not os.path.exists(input_path):
                msg = f"Input file not found: {input_path}"
                raise FileNotFoundError(msg)

            with open(input_path) as f:
                reader = csv.DictReader(f)
                headers = list(reader.fieldnames or [])
                rows = list(reader)

            stem = os.path.splitext(os.path.basename(input_path))[0]

            for variant_idx in range(self.params.variants):
                suffix = (
                    f"_{self.params.output_prefix}" if self.params.output_prefix else ""
                )
                output_path = os.path.join(
                    output_dir, f"{stem}_{variant_idx}{suffix}.csv"
                )
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
                outputs.append(
                    {
                        "path": output_path,
                        "original_name": f"{dataset['original_name']}_{variant_idx}{suffix}.csv",
                        "source_artifact_id": dataset["artifact_id"],
                    }
                )

        return {"created_files": created_files, "outputs": outputs}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build DataArtifact drafts from transformed CSV files."""
        raw = inputs.memory_outputs

        result = ArtifactResult(
            artifacts={"dataset": []},
            lineage={"dataset": []},
            metadata={
                "operation": "data_transformer",
                "scale_factor": self.params.scale_factor,
                "noise_amplitude": self.params.noise_amplitude,
                "variants": self.params.variants,
                "seed": self.params.seed,
                "created_files": raw.get("created_files", []),
            },
        )
        for output in raw["outputs"]:
            with open(output["path"], "rb") as f:
                draft = DataArtifact.draft(
                    content=f.read(),
                    original_name=output["original_name"],
                    step_number=inputs.step_number,
                )
            result.add_artifact(
                "dataset", draft, sources={"dataset": [output["source_artifact_id"]]}
            )
        return result


class SequentialDataTransformer(DataTransformer):
    """DataTransformer with per-artifact dispatch disabled.

    All artifacts in a unit are sent to a single execute_function() call.
    Use this when the operation wraps an external tool that loads
    model weights per invocation — batching amortizes the cost.
    """

    name: ClassVar[str] = "sequential_data_transformer"
    per_artifact_dispatch: ClassVar[bool] = False
