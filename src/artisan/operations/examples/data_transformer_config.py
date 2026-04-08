"""Generate execution config artifacts for a data transformation sweep."""

from __future__ import annotations

from enum import StrEnum, auto
import os
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class DataTransformerConfig(OperationDefinition):
    """Generate execution configs with artifact references.

    Creates a Cartesian product of scale_factors x noise_amplitudes.
    Each config embeds {"$artifact": dataset_id} for the input path,
    which is resolved at materialization time.

    Input Roles:
        dataset (data) -- Input dataset to reference in configs

    Output Roles:
        config (config) -- Generated execution configs
    """

    # ---------- Metadata ----------
    name: ClassVar[str] = "data_transformer_config"
    description: ClassVar[str] = "Generate execution configs for data transformation script"

    # ---------- Inputs ----------
    class InputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(
            artifact_type="data",
            required=True,
            materialize=False,
            description="Input dataset to reference in configs",
        ),
    }

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        config = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.config: OutputSpec(
            artifact_type=ArtifactTypes.CONFIG,
            infer_lineage_from={"inputs": ["dataset"]},
            description="Generated execution configs",
        ),
    }

    # ---------- Parameters ----------
    class Params(BaseModel):
        """Algorithm parameters for DataTransformerConfig."""

        scale_factors: list[float] = Field(
            default=[1.0],
            description="List of scale factor values to sweep (Cartesian product)",
        )
        noise_amplitudes: list[float] = Field(
            default=[0.0],
            description="List of noise amplitude values to sweep (Cartesian product)",
        )
        seed: int | None = Field(
            default=None,
            description="Random seed included in all configs",
        )

    params: Params = Params()

    # ---------- Resources ----------
    resources: ResourceConfig = ResourceConfig(time_limit="00:10:00")

    # ---------- Execution ----------
    execution: ExecutionConfig = ExecutionConfig(job_name="data_transformer_config")

    # ---------- Lifecycle ----------
    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Extract dataset info for config generation."""
        dataset = inputs.input_artifacts["dataset"][0]
        return {
            "dataset_artifact_id": dataset.artifact_id,
            "dataset_stem": os.path.splitext(dataset.original_name)[0],
        }

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Build config dicts for each scale_factor x noise_amplitude combo."""
        dataset_artifact_id = inputs.inputs["dataset_artifact_id"]
        dataset_stem = inputs.inputs["dataset_stem"]

        configs = []
        index = 0
        for scale_factor in self.params.scale_factors:
            for noise_amplitude in self.params.noise_amplitudes:
                configs.append({
                    "index": index,
                    "original_name": f"{dataset_stem}_config_{index}.json",
                    "content": {
                        "input": {"$artifact": dataset_artifact_id},
                        "scale_factor": scale_factor,
                        "noise_amplitude": noise_amplitude,
                        "seed": self.params.seed,
                    },
                })
                index += 1

        return {"configs": configs}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build ExecutionConfigArtifact drafts from generated configs."""
        configs = inputs.memory_outputs["configs"]

        drafts = [
            ExecutionConfigArtifact.draft(
                content=cfg["content"],
                original_name=cfg["original_name"],
                step_number=inputs.step_number,
            )
            for cfg in configs
        ]

        return ArtifactResult(success=True, artifacts={"config": drafts})
