"""Transform that sleeps per artifact to demonstrate local dispatch timing."""

from __future__ import annotations

import os
import time
from enum import StrEnum
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.operation_config.compute import ComputeProvider, ModalComputeConfig
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class SlowTransformer(OperationDefinition):
    """Sleep per artifact, then write a timing marker.

    Simulates a fixed-duration task using local Python execution. Per-artifact
    dispatch makes one call per artifact; SequentialSlowTransformer handles
    the batch in one call. Use the command operation WaitTool to demonstrate
    remote Modal execution.

    Input Roles:
        dataset (data) -- Input dataset (content is ignored)

    Output Roles:
        dataset (data) -- Timing marker CSV per artifact
    """

    name: ClassVar[str] = "slow_transformer"
    description: ClassVar[str] = "Sleep per artifact to demonstrate dispatch timing"

    class InputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(
            artifact_type="data",
            required=True,
            description="Input dataset (content is ignored — only the count matters)",
        ),
    }

    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="data",
            description="Timing marker CSV recording sleep duration",
            derives_from={"inputs": ["dataset"]},
        ),
    }

    class Params(BaseModel):
        """Parameters for SlowTransformer."""

        duration: float = Field(
            default=10.0,
            ge=0.0,
            description="Seconds to sleep per artifact",
        )

    params: Params = Params()

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
        """Sleep for the configured duration per artifact, write timing markers."""
        output_dir = inputs.execute_dir
        os.makedirs(output_dir, exist_ok=True)

        dataset_input = inputs.inputs.get("dataset")
        if dataset_input is None:
            msg = "No dataset input provided"
            raise ValueError(msg)

        created_files = []
        outputs = []
        for dataset in dataset_input:
            input_path = dataset["path"]
            stem = os.path.splitext(os.path.basename(input_path))[0]

            start = time.perf_counter()
            time.sleep(self.params.duration)
            elapsed = time.perf_counter() - start

            marker = os.path.join(output_dir, f"{stem}_slow.csv")
            with open(marker, "w") as f:
                f.write(
                    f"input,requested,actual\n{stem},{self.params.duration},{elapsed:.4f}\n"
                )
            created_files.append(marker)
            outputs.append(
                {
                    "path": marker,
                    "original_name": f"{dataset['original_name']}_slow.csv",
                    "source_artifact_id": dataset["artifact_id"],
                }
            )

        return {"created_files": created_files, "outputs": outputs}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build DataArtifact drafts from timing marker CSVs."""
        result = ArtifactResult(artifacts={"dataset": []}, lineage={"dataset": []})
        for output in inputs.memory_outputs["outputs"]:
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


class SequentialSlowTransformer(SlowTransformer):
    """SlowTransformer with per-artifact dispatch disabled.

    All artifacts process sequentially in a single execute_function() call.
    """

    name: ClassVar[str] = "sequential_slow_transformer"
    per_artifact_dispatch: ClassVar[bool] = False
