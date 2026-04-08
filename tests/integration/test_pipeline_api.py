"""Integration tests for PipelineManager public API surface.

Tests dunder methods, output() name-based lookup, StepFuture properties,
finalize() ordering, and pipeline state inspection with real operations.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.slow

from artisan.operations.examples import DataGenerator, MetricCalculator
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend
from artisan.schemas.orchestration.output_reference import OutputReference

# =============================================================================
# Dunder methods
# =============================================================================


class TestPipelineDunderMethods:
    """Integration tests for __len__, __iter__, __getitem__, __bool__,
    __contains__, __str__, __repr__ with real pipeline execution."""

    def test_len_tracks_completed_steps(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_len",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        assert len(pipeline) == 0

        pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
        )
        assert len(pipeline) == 1

        pipeline.finalize()

    def test_iter_yields_step_results(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_iter",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        step0 = pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
        )
        step1 = pipeline.run(
            MetricCalculator,
            inputs={"dataset": step0.output("datasets")},
            backend=Backend.LOCAL,
        )

        results = list(pipeline)
        assert len(results) == 2
        assert results[0].step_name == "data_generator"
        assert results[1].step_name == "metric_calculator"

        pipeline.finalize()

    def test_getitem_by_index(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_getitem",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        step0 = pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
        )

        assert pipeline[0].step_name == "data_generator"
        assert pipeline[-1] is pipeline[0]

        with pytest.raises(IndexError):
            pipeline[5]

        pipeline.finalize()

    def test_getitem_slice(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_slice",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        step0 = pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
        )
        pipeline.run(
            MetricCalculator,
            inputs={"dataset": step0.output("datasets")},
            backend=Backend.LOCAL,
        )

        sliced = pipeline[0:1]
        assert len(sliced) == 1
        assert sliced[0].step_name == "data_generator"

        pipeline.finalize()

    def test_bool_true_when_all_succeed(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_bool_true",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
        )

        assert pipeline
        pipeline.finalize()

    def test_bool_false_when_empty(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_bool_false",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        assert not pipeline
        pipeline.finalize()

    def test_contains_by_step_name(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_contains",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
            name="my_generator",
        )

        assert "my_generator" in pipeline
        assert "nonexistent_step" not in pipeline

        pipeline.finalize()

    def test_str_and_repr(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_str_repr",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        assert "no steps executed" in str(pipeline)
        assert "PipelineManager(" in repr(pipeline)

        pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
        )

        assert "1 steps" in str(pipeline)
        assert "all succeeded" in str(pipeline)

        pipeline.finalize()


# =============================================================================
# output() name-based lookup
# =============================================================================


class TestOutputNameLookup:
    """Integration tests for pipeline.output(name, role)."""

    def test_output_with_custom_name(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_output_name",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
            name="gen_step",
        )

        ref = pipeline.output("gen_step", "datasets")
        assert isinstance(ref, OutputReference)
        assert ref.source_step == 0
        assert ref.role == "datasets"

        pipeline.finalize()

    def test_output_with_default_name(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_output_default",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
        )

        ref = pipeline.output("data_generator", "datasets")
        assert ref.source_step == 0

        pipeline.finalize()

    def test_output_unknown_name_raises(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_output_unknown",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        with pytest.raises(ValueError, match="No step named"):
            pipeline.output("nonexistent", "role")

        pipeline.finalize()

    def test_output_unknown_role_raises(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_output_bad_role",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
        )

        with pytest.raises(ValueError, match="Output role .* not available"):
            pipeline.output("data_generator", "bad_role")

        pipeline.finalize()

    def test_output_step_number_disambiguation(self, pipeline_env: dict[str, str]):
        """Two steps with the same name: step_number selects the right one."""
        pipeline = PipelineManager.create(
            name="test_output_disambig",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
            name="gen",
        )
        pipeline.run(
            DataGenerator,
            params={"count": 3, "seed": 99},
            backend=Backend.LOCAL,
            name="gen",
        )

        # Default: last wins
        ref_last = pipeline.output("gen", "datasets")
        assert ref_last.source_step == 1

        # Explicit step_number=0
        ref_first = pipeline.output("gen", "datasets", step_number=0)
        assert ref_first.source_step == 0

        pipeline.finalize()

    def test_output_wires_to_downstream(self, pipeline_env: dict[str, str]):
        """pipeline.output() reference can be used to wire downstream steps."""
        pipeline = PipelineManager.create(
            name="test_output_wiring",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
            name="gen",
        )

        ref = pipeline.output("gen", "datasets")

        step1 = pipeline.run(
            MetricCalculator,
            inputs={"dataset": ref},
            backend=Backend.LOCAL,
        )

        assert step1.success is True
        assert step1.succeeded_count == 2

        pipeline.finalize()


# =============================================================================
# StepFuture properties
# =============================================================================


class TestStepFutureProperties:
    """Integration tests for StepFuture.output, .status, .done."""

    def test_step_future_output_before_completion(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_future_output",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        future = pipeline.submit(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
        )

        ref = future.output("datasets")
        assert isinstance(ref, OutputReference)
        assert ref.source_step == 0
        assert ref.role == "datasets"

        future.result(timeout=30)
        pipeline.finalize()

    def test_step_future_done_and_status(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_future_status",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        future = pipeline.submit(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
        )

        result = future.result(timeout=30)
        assert future.done is True
        assert future.status == "completed"
        assert result.success is True

        pipeline.finalize()

    def test_step_future_chain(self, pipeline_env: dict[str, str]):
        """submit() output can wire to subsequent submit() via StepFuture.output()."""
        pipeline = PipelineManager.create(
            name="test_future_chain",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        future0 = pipeline.submit(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
        )

        future1 = pipeline.submit(
            MetricCalculator,
            inputs={"dataset": future0.output("datasets")},
            backend=Backend.LOCAL,
        )

        result0 = future0.result(timeout=30)
        result1 = future1.result(timeout=30)

        assert result0.success is True
        assert result1.success is True

        pipeline.finalize()


# =============================================================================
# Finalize ordering
# =============================================================================


class TestFinalizeOrdering:
    """Tests that finalize() returns steps sorted by step_number."""

    def test_finalize_steps_sorted(self, pipeline_env: dict[str, str]):
        """finalize() summary has steps in step_number order."""
        pipeline = PipelineManager.create(
            name="test_finalize_sort",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        step0 = pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
        )
        step1 = pipeline.run(
            MetricCalculator,
            inputs={"dataset": step0.output("datasets")},
            backend=Backend.LOCAL,
        )

        summary = pipeline.finalize()

        step_numbers = [s["step_number"] for s in summary["steps"]]
        assert step_numbers == sorted(step_numbers)
        assert step_numbers == [0, 1]


# =============================================================================
# Config and current_step properties
# =============================================================================


class TestPipelineProperties:
    """Integration tests for config and current_step properties."""

    def test_config_property(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_config_prop",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        assert pipeline.config.name == "test_config_prop"
        assert pipeline.config.delta_root == pipeline_env["delta_root"]

        pipeline.finalize()

    def test_current_step_increments(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_step_counter",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        assert pipeline.current_step == 0

        pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
        )
        assert pipeline.current_step == 1

        pipeline.run(
            DataGenerator,
            params={"count": 3, "seed": 99},
            backend=Backend.LOCAL,
        )
        assert pipeline.current_step == 2

        pipeline.finalize()


# =============================================================================
# Custom step name
# =============================================================================


class TestCustomStepName:
    """Integration tests for the name= parameter on run()."""

    def test_custom_name_propagates(self, pipeline_env: dict[str, str]):
        pipeline = PipelineManager.create(
            name="test_custom_name",
            delta_root=pipeline_env["delta_root"],
            staging_root=pipeline_env["staging_root"],
            working_root=pipeline_env["working_root"],
        )

        result = pipeline.run(
            DataGenerator,
            params={"count": 2, "seed": 42},
            backend=Backend.LOCAL,
            name="my_generation_step",
        )

        assert result.step_name == "my_generation_step"

        summary = pipeline.finalize()
        assert summary["steps"][0]["name"] == "my_generation_step"
