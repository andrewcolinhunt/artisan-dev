"""Transform that sleeps per artifact to demonstrate parallel dispatch timing."""

from __future__ import annotations

import os
import time
from enum import StrEnum
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.operation_config.compute import Compute, ModalComputeConfig
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class SlowTransformer(OperationDefinition):
    """Sleep per artifact, then write a timing marker.

    Simulates a compute-heavy operation where each artifact takes a
    fixed duration. With per-artifact dispatch on Modal, N artifacts
    execute in parallel (~duration total). Without it, they execute
    sequentially (~N * duration total).

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
            infer_lineage_from={"inputs": ["dataset"]},
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

    compute: Compute = Compute(
        modal=ModalComputeConfig(),
    )

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Extract materialized paths from input artifacts."""
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Sleep for the configured duration per artifact, write timing markers."""
        output_dir = inputs.execute_dir
        os.makedirs(output_dir, exist_ok=True)

        dataset_input = inputs.inputs.get("dataset")
        if dataset_input is None:
            msg = "No dataset input provided"
            raise ValueError(msg)

        input_files = (
            [dataset_input] if isinstance(dataset_input, str) else list(dataset_input)
        )

        created_files = []
        for input_path in input_files:
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

        return {"created_files": created_files}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build DataArtifact drafts from timing marker CSVs."""
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
        )


class SequentialSlowTransformer(SlowTransformer):
    """SlowTransformer with per-artifact dispatch disabled.

    All artifacts process sequentially in a single execute() call.
    """

    name: ClassVar[str] = "sequential_slow_transformer"
    per_artifact_dispatch: ClassVar[bool] = False
