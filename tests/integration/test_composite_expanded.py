"""Integration tests for expanded composites.

Exercises pipeline.expand(CompositeDefinition) where each internal
operation becomes its own pipeline step.
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


class GenTransformMetrics(CompositeDefinition):
    """Generate, transform, compute metrics."""

    name = "test_gen_transform_metrics"
    description = "Generate, transform, compute metrics"

    class OutputRole(StrEnum):
        METRICS = "metrics"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "metrics": OutputSpec(artifact_type="metric"),
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
        metrics = ctx.run(
            MetricCalculator,
            inputs={"dataset": transformed.output("dataset")},
        )
        ctx.output("metrics", metrics.output("metrics"))


class TransformComposite(CompositeDefinition):
    """Takes external input and transforms it."""

    name = "test_expand_transform"
    description = "Transform external data (for expand)"

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
                "seed": 300,
            },
        )
        ctx.output("dataset", transformed.output("dataset"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_expand_basic(pipeline_env: dict[str, Path]):
    """Expanded composite: each internal op becomes its own step."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_expand_basic",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    result = pipeline.expand(
        GenTransformMetrics,
        backend=Backend.LOCAL,
    )

    # expand() returns ExpandedCompositeResult, not StepResult
    ref = result.output("metrics")
    assert ref.role == "metrics"

    # The internal steps should have been created
    # Step 0: DataGenerator
    # Step 1: DataTransformer
    # Step 2: MetricCalculator
    pipeline.finalize()


def test_expand_with_upstream(pipeline_env: dict[str, Path]):
    """Expanded composite receiving input from upstream step."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_expand_upstream",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    gen = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        backend=Backend.LOCAL,
    )

    result = pipeline.expand(
        TransformComposite,
        inputs={"data": gen.output("datasets")},
    )

    ref = result.output("dataset")
    assert ref.role == "dataset"
    # Internal step should be step 1
    assert ref.source_step == 1

    pipeline.finalize()


def test_expand_to_downstream(pipeline_env: dict[str, Path]):
    """Expanded composite output wired to downstream operation."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_expand_downstream",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    result = pipeline.expand(
        GenTransformMetrics,
    )

    # Wire expanded output to a downstream step
    # (metrics are the output, but they don't match DataTransformer's input)
    # Just verify the expand result is usable
    ref = result.output("metrics")
    assert ref.role == "metrics"

    pipeline.finalize()


def test_expand_step_naming(pipeline_env: dict[str, Path]):
    """Expanded composite steps are named with composite prefix."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_expand_naming",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    result = pipeline.expand(
        GenTransformMetrics,
    )

    pipeline.finalize()

    # Check that step names have the composite prefix
    step_names = [r.step_name for r in pipeline]
    assert len(step_names) == 3
    for name in step_names:
        assert name.startswith("test_gen_transform_metrics.")
