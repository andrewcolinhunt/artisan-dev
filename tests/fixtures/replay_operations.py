"""Registered native definitions for retained diagnostic replay tests."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field

from artisan.composites import CompositeContext
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import ArtifactResult, InputSpec, OutputSpec
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)


class Increment(OperationDefinition):
    name = "retained_replay_increment"

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
        model_config = ConfigDict(strict=True)
        increments: tuple[int, ...] = Field(
            default=(1,), description="Fixed increments."
        )

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            "values": PerArtifact(
                [
                    artifact.values["value"]
                    for artifact in inputs.input_artifacts["source"]
                ]
            )
        }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {
            "values": [
                value + sum(self.params.increments) for value in inputs.inputs["values"]
            ]
        }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for index, value in enumerate(inputs.memory_outputs["values"]):
            result.add_artifact(
                "metrics",
                MetricArtifact.draft({"value": value}, "metric.json"),
                sources={"source": [inputs.ref("source", index=index)]},
            )
        return result


class TwoIncrements(CompositeDefinition):
    name = "retained_replay_two_increments"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = Increment.inputs
    outputs: ClassVar = {"metrics": OutputSpec(artifact_type="metric")}

    def compose(self, ctx: CompositeContext) -> None:
        first = ctx.call(
            Increment, inputs={"source": ctx.input("source")}, name="first"
        )
        second = ctx.call(
            Increment, inputs={"source": first.output("metrics")}, name="second"
        )
        ctx.output("metrics", second.output("metrics"))
