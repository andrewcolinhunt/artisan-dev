"""Installed creator compositions for local and pipeline tutorials."""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar

from pydantic import BaseModel, Field

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
from artisan.schemas import InputSpec, OutputSpec

__all__ = ["GenerateAndScore", "TransformAndScore"]


class TransformAndScore(CompositeDefinition):
    """Transform each dataset and export its data and directly associated metrics."""

    name = "transform_and_score"
    description = "Transform and score datasets within one scientific step"

    class InputRole(StrEnum):
        dataset = "dataset"

    class OutputRole(StrEnum):
        dataset = "dataset"
        metrics = "metrics"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.dataset: InputSpec(artifact_type="csv"),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.dataset: OutputSpec(artifact_type="csv"),
        OutputRole.metrics: OutputSpec(artifact_type="metric"),
    }

    class Params(BaseModel):
        scale_factor: float = Field(
            default=2.0, description="CSV coordinate multiplier"
        )
        seed: int = Field(default=100, description="Deterministic transformation seed")

    params: Params = Field(default_factory=Params)

    def compose(self, ctx: CompositeContext) -> None:
        """Keep the transformed data and its metrics under the same boundary."""
        transformed = ctx.call(
            DataTransformer,
            name="transform",
            inputs={"dataset": ctx.input("dataset")},
            params={
                "scale_factor": self.params.scale_factor,
                "variants": 1,
                "seed": self.params.seed,
            },
        )
        scored = ctx.call(
            MetricCalculator,
            name="score",
            inputs={"dataset": transformed.output("dataset")},
        )
        ctx.output("dataset", transformed.output("dataset"))
        ctx.output("metrics", scored.output("metrics"))


class GenerateAndScore(CompositeDefinition):
    """Generate datasets and invoke a nested transform-and-score composition."""

    name = "generate_and_score"
    description = "Generate, transform and score within one scientific step"

    class OutputRole(StrEnum):
        dataset = "dataset"
        metrics = "metrics"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.dataset: OutputSpec(artifact_type="csv"),
        OutputRole.metrics: OutputSpec(artifact_type="metric"),
    }

    class Params(BaseModel):
        count: int = Field(default=2, ge=1, description="Number of datasets")
        scale_factor: float = Field(
            default=2.0, description="CSV coordinate multiplier"
        )
        seed: int = Field(default=42, description="Deterministic generation seed")

    params: Params = Field(default_factory=Params)

    def compose(self, ctx: CompositeContext) -> None:
        """Retain final data and scores while leaving intermediate data internal."""
        generated = ctx.call(
            DataGenerator,
            name="generate",
            params={"count": self.params.count, "seed": self.params.seed},
        )
        scored = ctx.call(
            TransformAndScore,
            name="transform_and_score",
            inputs={"dataset": generated.output("datasets")},
            params={"scale_factor": self.params.scale_factor},
        )
        ctx.output("dataset", scored.output("dataset"))
        ctx.output("metrics", scored.output("metrics"))
