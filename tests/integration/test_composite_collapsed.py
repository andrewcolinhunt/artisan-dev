"""Integration tests for collapsed composites.

Exercises pipeline.run(CompositeDefinition) with creators and intermediates modes.
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import pytest

pytestmark = pytest.mark.slow

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

# ---------------------------------------------------------------------------
# Test composites
# ---------------------------------------------------------------------------


class GenerateAndTransform(CompositeDefinition):
    """Generate data then transform it."""

    name = "test_gen_transform"
    description = "Generate and transform data"

    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "dataset": OutputSpec(artifact_type="data"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        gen = ctx.run(DataGenerator, params={"count": 2, "seed": 42})
        transformed = ctx.run(
            DataTransformer,
            inputs={"dataset": gen.output("datasets")},
            params={
                "scale_factor": 1.5,
                "noise_amplitude": 0.1,
                "variants": 1,
                "seed": 100,
            },
        )
        ctx.output("dataset", transformed.output("dataset"))


class TransformWithInput(CompositeDefinition):
    """Takes external input and transforms it."""

    name = "test_transform_input"
    description = "Transform external data"

    class InputRole(StrEnum):
        DATA = "data"

    class OutputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        "data": InputSpec(artifact_type="data", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "dataset": OutputSpec(artifact_type="data"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        transformed = ctx.run(
            DataTransformer,
            inputs={"dataset": ctx.input("data")},
            params={
                "scale_factor": 2.0,
                "noise_amplitude": 0.0,
                "variants": 1,
                "seed": 200,
            },
        )
        ctx.output("dataset", transformed.output("dataset"))


class NestedComposite(CompositeDefinition):
    """Composite that contains another composite."""

    name = "test_nested"
    description = "Nested composite"

    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "dataset": OutputSpec(artifact_type="data"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        # Run GenerateAndTransform as a nested composite
        result = ctx.run(GenerateAndTransform)
        ctx.output("dataset", result.output("dataset"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_composite_creators_only(pipeline_env: dict[str, Path]):
    """Basic collapsed composite with two creator operations."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_composite_basic",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step = pipeline.run(
        GenerateAndTransform,
        backend=Backend.LOCAL,
    )
    pipeline.finalize()

    assert step.success is True
    ref = step.output("dataset")
    assert ref.source_step == 0
    assert ref.role == "dataset"


def test_composite_with_upstream(pipeline_env: dict[str, Path]):
    """Composite receiving input from an upstream pipeline step."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_composite_upstream",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    gen = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )

    step = pipeline.run(
        TransformWithInput,
        inputs={"data": gen.output("datasets")},
        backend=Backend.LOCAL,
    )
    pipeline.finalize()

    assert step.success is True
    ref = step.output("dataset")
    assert ref.source_step == 1


def test_composite_to_downstream(pipeline_env: dict[str, Path]):
    """Composite output wired to a downstream operation."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_composite_downstream",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    composite_step = pipeline.run(
        GenerateAndTransform,
        backend=Backend.LOCAL,
    )

    # Wire composite output to MetricCalculator
    metrics_step = pipeline.run(
        MetricCalculator,
        inputs={"dataset": composite_step.output("dataset")},
        backend=Backend.LOCAL,
    )
    pipeline.finalize()

    assert composite_step.success is True
    assert metrics_step.success is True


def test_composite_nested(pipeline_env: dict[str, Path]):
    """Nested composite: composite inside a composite."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_composite_nested",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step = pipeline.run(
        NestedComposite,
        backend=Backend.LOCAL,
    )
    pipeline.finalize()

    assert step.success is True
    ref = step.output("dataset")
    assert ref.source_step == 0
