"""Small native scientific examples shared by invocation and controller tests."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.composites import CompositeContext
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import ArtifactResult, InputSpec, OutputSpec
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)


def structure(value: int) -> CSVArtifact:
    return CSVArtifact.draft(
        f"coordinate\n{value}\n".encode(), "structure.csv"
    ).finalize()


def coordinate(artifact: Artifact) -> int:
    assert artifact.content is not None
    return int(artifact.content.decode().splitlines()[1])


class Transform(OperationDefinition):
    name = "composition_transform"

    class InputRole(StrEnum):
        structure = "structure"

    class OutputRole(StrEnum):
        structure = "structure"

    inputs: ClassVar = {"structure": InputSpec(artifact_type="csv", materialize=False)}
    outputs: ClassVar = {
        "structure": OutputSpec(
            artifact_type="csv", derives_from={"inputs": ["structure"]}
        )
    }
    phase_sizes: ClassVar[list[tuple[str, int]]] = []

    class Params(BaseModel):
        add: int = Field(default=1, description="Coordinate increment.")
        multiply: int = Field(default=1, description="Coordinate multiplier.")

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        values = [
            coordinate(artifact) for artifact in inputs.input_artifacts["structure"]
        ]
        self.phase_sizes.append(("preprocess", len(values)))
        return {"values": PerArtifact(values)}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        self.phase_sizes.append(("execute", len(inputs.inputs["values"])))
        return {
            "values": [
                value * self.params.multiply + self.params.add
                for value in inputs.inputs["values"]
            ]
        }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        self.phase_sizes.append(("postprocess", len(inputs.memory_outputs["values"])))
        result = ArtifactResult()
        for index, value in enumerate(inputs.memory_outputs["values"]):
            result.add_artifact(
                "structure",
                CSVArtifact.draft(
                    f"coordinate\n{value}\n".encode(),
                    "structure.csv",
                    inputs.step_number,
                ),
                sources={"structure": [inputs.ref("structure", index=index)]},
            )
        return result


class Measure(OperationDefinition):
    name = "composition_measure"

    class InputRole(StrEnum):
        structure = "structure"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = {"structure": InputSpec(artifact_type="csv", materialize=False)}
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["structure"]}
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            "values": PerArtifact(
                [
                    coordinate(artifact)
                    for artifact in inputs.input_artifacts["structure"]
                ]
            )
        }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {"values": inputs.inputs["values"]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for index, value in enumerate(inputs.memory_outputs["values"]):
            result.add_artifact(
                "metrics",
                MetricArtifact.draft(
                    {"score": value}, "metric.json", inputs.step_number
                ),
                sources={"structure": [inputs.ref("structure", index=index)]},
            )
        return result


class SummarizeMetrics(OperationDefinition):
    name = "composition_summary"
    independent_input_streams = True
    per_artifact_dispatch = False

    class InputRole(StrEnum):
        final = "final"
        history = "history"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = {
        "final": InputSpec(artifact_type="csv", materialize=False),
        "history": InputSpec(artifact_type="metric", materialize=False),
    }
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["final", "history"]}
        )
    }
    observed: ClassVar[list[tuple[int, int]]] = []

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        self.observed.append(
            (
                len(inputs.input_artifacts["final"]),
                len(inputs.input_artifacts["history"]),
            )
        )
        return {
            "final": coordinate(inputs.input_artifacts["final"][0]),
            "history": [
                artifact.values["score"]
                for artifact in inputs.input_artifacts["history"]
            ],
        }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {
            "summary": [
                {
                    "selected": inputs.inputs["final"],
                    "history": inputs.inputs["history"],
                }
            ]
        }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        result.add_artifact(
            "metrics",
            MetricArtifact.draft(
                inputs.memory_outputs["summary"][0], "summary.json", inputs.step_number
            ),
            sources={
                role: [inputs.ref(role, index=index) for index in range(len(values))]
                for role, values in inputs.input_artifacts.items()
                if values
            },
        )
        return result


class PredictMeasure(CompositeDefinition):
    name = "composition_predict_measure"

    class InputRole(StrEnum):
        structure = "structure"

    class OutputRole(StrEnum):
        structure = "structure"
        metrics = "metrics"

    inputs: ClassVar = {"structure": InputSpec(artifact_type="csv")}
    outputs: ClassVar = {
        "structure": OutputSpec(artifact_type="csv"),
        "metrics": OutputSpec(artifact_type="metric"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        prediction = ctx.call(
            Transform, name="predict", inputs={"structure": ctx.input("structure")}
        )
        measurement = ctx.call(
            Measure,
            name="measure",
            inputs={"structure": prediction.output("structure")},
        )
        ctx.output("structure", prediction.output("structure"))
        ctx.output("metrics", measurement.output("metrics"))


class Iterate(PredictMeasure):
    name = "composition_iterate"

    class Params(BaseModel):
        cycles: int = Field(default=3, ge=1)

    params: Params = Field(default_factory=Params)

    def compose(self, ctx: CompositeContext) -> None:
        current = ctx.input("structure")
        history = []
        for index in range(self.params.cycles):
            current = ctx.call(
                Transform,
                name=f"a_{index}",
                inputs={"structure": current},
                params={"add": 1},
            ).output("structure")
            current = ctx.call(
                Transform,
                name=f"b_{index}",
                inputs={"structure": current},
                params={"multiply": 2, "add": 0},
            ).output("structure")
            history.append(
                ctx.call(
                    Measure, name=f"measure_{index}", inputs={"structure": current}
                ).output("metrics")
            )
        summary = ctx.call(
            SummarizeMetrics,
            name="summary",
            inputs={"final": current, "history": history},
        )
        ctx.output("structure", current)
        ctx.output("metrics", summary.output("metrics"))
