"""Integration tests for composites.

Exercises pipeline.submit_composite(MyComposite) where each internal
operation becomes its own pipeline step, composite-level overrides forward
to child steps, and nested composites expand recursively.
"""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar

import pytest

pytestmark = pytest.mark.integration

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners import Runner
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

    name = "test_transform_composite"
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
                "seed": 300,
            },
        )
        ctx.output("dataset", transformed.output("dataset"))


class GenThenNested(CompositeDefinition):
    """Generates data, then runs a nested composite on it."""

    name = "test_gen_then_nested"
    description = "Generate then run a nested composite"

    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "dataset": OutputSpec(artifact_type="data"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        gen = ctx.run(DataGenerator, params={"count": 2, "seed": 42})
        nested = ctx.run(
            TransformComposite,
            inputs={"data": gen.output("datasets")},
        )
        ctx.output("dataset", nested.output("dataset"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_composite_basic(pipeline_env: dict[str, str]):
    """Each internal op becomes its own step; output is wired through."""
    pipeline = PipelineManager.create(
        name="test_composite_basic",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    result = pipeline.submit_composite(
        GenTransformMetrics,
        step_runner=Runner.LOCAL,
    )

    ref = result.output("metrics")
    assert ref.role == "metrics"

    pipeline.finalize()


def test_composite_with_upstream(pipeline_env: dict[str, str]):
    """Composite receiving input from an upstream step."""
    pipeline = PipelineManager.create(
        name="test_composite_upstream",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    gen = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        step_runner=Runner.LOCAL,
    )

    result = pipeline.submit_composite(
        TransformComposite,
        inputs={"data": gen.output("datasets")},
    )

    ref = result.output("dataset")
    assert ref.role == "dataset"
    # Internal step should be step 1 (step 0 is the upstream generator).
    assert ref.source_step == 1

    pipeline.finalize()


def test_composite_step_naming(pipeline_env: dict[str, str]):
    """Child steps are named with the composite prefix."""
    pipeline = PipelineManager.create(
        name="test_composite_naming",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    pipeline.submit_composite(GenTransformMetrics)
    pipeline.finalize()

    step_names = [r.step_name for r in pipeline]
    assert len(step_names) == 3
    for name in step_names:
        assert name.startswith("test_gen_transform_metrics.")


def test_composite_level_step_runner_reaches_child(pipeline_env: dict[str, str]):
    """A composite-level step_runner default drives child steps.

    Every child inherits the composite-level runner (no per-op override), and
    the pipeline completes — the forwarded runner had to reach each child's
    submit() for that to happen.
    """
    pipeline = PipelineManager.create(
        name="test_composite_forward_runner",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    result = pipeline.run_composite(
        GenTransformMetrics,
        step_runner=Runner.LOCAL,
    )
    assert result.output("metrics").role == "metrics"

    pipeline.finalize()
    results = list(pipeline)
    assert len(results) == 3
    assert all(r.success for r in results)


def test_composite_level_environment_reaches_child(pipeline_env: dict[str, str]):
    """A composite-level environment default reaches each child's submit().

    An environment string that no child operation configures must be rejected
    when the first child step is submitted inside compose() — demonstrating
    that the composite-level default is forwarded rather than silently dropped.
    """
    pipeline = PipelineManager.create(
        name="test_composite_forward_env",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    with pytest.raises(ValueError, match="Environment"):
        pipeline.submit_composite(
            GenTransformMetrics,
            environment="definitely_not_configured",
        )

    pipeline.finalize()


def test_nested_composite(pipeline_env: dict[str, str]):
    """A composite that runs a nested composite expands recursively."""
    pipeline = PipelineManager.create(
        name="test_nested_composite",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    result = pipeline.run_composite(GenThenNested, step_runner=Runner.LOCAL)
    assert result.output("dataset").role == "dataset"

    pipeline.finalize()
    step_names = [r.step_name for r in pipeline]
    # DataGenerator step + the nested composite's DataTransformer step.
    assert any(n == "test_gen_then_nested.data_generator" for n in step_names)
    assert any(
        n.startswith("test_gen_then_nested.test_transform_composite.")
        for n in step_names
    )
