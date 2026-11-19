"""Ordinary metric inputs exercise optional parents and singleton sharing."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, ClassVar

from pydantic import BaseModel, model_validator

from artisan.composites import CompositeContext
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import (
    AnchoredNameGrouping,
    ArtifactResult,
    InputSpec,
    MetricArtifact,
    OutputSpec,
)
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)


def metric(value: int) -> MetricArtifact:
    return MetricArtifact.draft({"value": value}, f"value-{value}.json").finalize()


class SharedOffset(OperationDefinition):
    name = "contract_shared_offset"
    shared_input_roles = ("offset",)
    per_artifact_dispatch = False
    batch_strategy: BatchStrategy = BatchStrategy(artifacts_per_unit=2)

    class InputRole(StrEnum):
        source = "source"
        offset = "offset"
        context = "context"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = {
        "source": InputSpec(artifact_type="metric", materialize=False),
        "offset": InputSpec(artifact_type="metric", materialize=False),
        "context": InputSpec(artifact_type="metric", materialize=False, required=False),
    }
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric",
            derives_from={"inputs": ["source", "offset", "context"]},
            optional_parent_roles=("context",),
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        offset = inputs.input_artifacts["offset"][0].values["value"]
        return {
            "values": PerArtifact(
                [
                    artifact.values["value"] + offset
                    for artifact in inputs.input_artifacts["source"]
                ]
            )
        }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {"values": inputs.inputs["values"]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for index, value in enumerate(inputs.memory_outputs["values"]):
            parents = {
                "source": [inputs.ref("source", index=index)],
                "offset": [inputs.ref("offset", index=0)],
            }
            if "context" in inputs.input_artifacts and index == 0:
                parents["context"] = [inputs.ref("context", index=index)]
            result.add_artifact(
                "metrics",
                MetricArtifact.draft({"value": value}, "result.json"),
                sources=parents,
            )
        return result


class SplitSharedOffset(SharedOffset):
    name = "contract_split_shared_offset"
    per_artifact_dispatch = True


class OffsetGenerator(OperationDefinition):
    name = "contract_offset_generator"

    class OutputRole(StrEnum):
        offset = "offset"

    outputs: ClassVar = {
        "offset": OutputSpec(artifact_type="metric", derives_from={"inputs": []})
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        result.add_artifact(
            "offset", MetricArtifact.draft({"value": 10}, "offset.json"), sources={}
        )
        return result


class NestedSharedOffset(CompositeDefinition):
    name = "contract_nested_shared_offset"
    batch_strategy: BatchStrategy = BatchStrategy(artifacts_per_unit=3)

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = {"source": InputSpec(artifact_type="metric", materialize=False)}
    outputs: ClassVar = {"metrics": OutputSpec(artifact_type="metric")}

    def compose(self, ctx: CompositeContext) -> None:
        offset = ctx.call(OffsetGenerator)
        result = ctx.call(
            SharedOffset,
            inputs={"source": ctx.input("source"), "offset": offset.output("offset")},
        )
        ctx.output("metrics", result.output("metrics"))


def named_metric(name: str, value: int) -> MetricArtifact:
    return MetricArtifact.draft({"value": value}, name).finalize()


class AnchoredOffset(SharedOffset):
    name = "contract_anchored_offset"
    group_by: AnchoredNameGrouping = AnchoredNameGrouping(
        primary_role="source", matched_roles=("context",)
    )
    batch_strategy: BatchStrategy = BatchStrategy(
        artifacts_per_unit=5, max_artifacts_per_unit=5
    )

    class Params(BaseModel):
        """Select whether the complete selection must include context.

        Attributes:
            require_context: Require at least one context occurrence globally.
        """

        require_context: bool = True

    params: Params = Params()

    @model_validator(mode="after")
    def _context_requirement(self) -> AnchoredOffset:
        self.required_nonempty_inputs = (
            ("context",) if self.params.require_context else ()
        )
        return self

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        contexts = {
            artifact.original_name: index
            for index, artifact in enumerate(inputs.input_artifacts.get("context", ()))
        }
        for index, value in enumerate(inputs.memory_outputs["values"]):
            source = inputs.input_artifacts["source"][index]
            parents = {
                "source": [inputs.ref("source", index=index)],
                "offset": [inputs.ref("offset", index=0)],
            }
            if source.original_name in contexts:
                parents["context"] = [
                    inputs.ref("context", index=contexts[source.original_name])
                ]
            result.add_artifact(
                "metrics",
                MetricArtifact.draft({"value": value}, source.original_name),
                sources=parents,
            )
        return result


class NestedAnchoredOffset(NestedSharedOffset):
    name = "contract_nested_anchored_offset"
    batch_strategy: BatchStrategy = BatchStrategy(artifacts_per_unit=20)

    def compose(self, ctx: CompositeContext) -> None:
        offset = ctx.call(OffsetGenerator)
        result = ctx.call(
            AnchoredOffset,
            params={"require_context": False},
            inputs={"source": ctx.input("source"), "offset": offset.output("offset")},
        )
        ctx.output("metrics", result.output("metrics"))


class InvalidAnchoredOffset(AnchoredOffset):
    name = "contract_invalid_anchored_offset"

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = super().postprocess(inputs)
        result.add_artifact(
            "metrics", MetricArtifact.draft({"value": -1}, "invalid"), sources={}
        )
        return result
