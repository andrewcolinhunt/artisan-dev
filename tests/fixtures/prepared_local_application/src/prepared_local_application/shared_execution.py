"""Deterministic creators for comparing local and retained computation."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.execution import run_operation
from artisan.operations.base import OperationDefinition
from artisan.schemas import ArtifactResult, InputSpec, MetricArtifact, OutputSpec
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)


class SeedValue(OperationDefinition):
    name = "shared_execution_seed"

    class OutputRole(StrEnum):
        metrics = "metrics"

    outputs: ClassVar = {
        "metrics": OutputSpec(artifact_type="metric", derives_from={"inputs": []})
    }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, list[int]]:
        return {"values": [3]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        return ArtifactResult(
            artifacts={"metrics": [MetricArtifact.draft({"value": 3}, "seed.json")]},
            lineage={"metrics": []},
        )


class ScaleValue(OperationDefinition):
    name = "shared_execution_scale"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = {"source": InputSpec(artifact_type="metric", materialize=False)}
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["source"]}
        )
    }

    class Params(BaseModel):
        factor: int = Field(
            default=2, description="Multiplier applied to the source value."
        )

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {"source": inputs.input_artifacts["source"][0]}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, list[MetricArtifact]]:
        source = inputs.inputs["source"]
        return {
            "metrics": [
                MetricArtifact.draft(
                    {"value": source.values["value"] * self.params.factor},
                    "scaled.json",
                )
            ]
        }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        result.add_artifact(
            "metrics",
            inputs.memory_outputs["metrics"][0],
            sources={"source": [inputs.ref("source", index=0)]},
        )
        return result


class SelectedScale(ScaleValue):
    name = "shared_execution_selected_scale"

    def execute_function(self, inputs: ExecuteInput) -> dict[str, list[MetricArtifact]]:
        result = run_operation(
            ScaleValue,
            selection_key="scale",
            inputs={"source": [inputs.inputs["source"]]},
        )
        return {"metrics": result.artifacts["metrics"]}
