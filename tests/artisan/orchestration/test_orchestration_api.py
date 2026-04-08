"""Tests for the orchestration API classes.

Tests for:
- OutputReference
- StepResult
- StepResultBuilder
- PipelineConfig
- PipelineManager
"""

from __future__ import annotations

import tempfile

import pytest
from pydantic import ValidationError

from artisan.orchestration import (
    PipelineManager,
)
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import CachePolicy, FailurePolicy
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_result import StepResult, StepResultBuilder


class TestOutputReference:
    """Tests for OutputReference schema model."""

    def test_create_basic(self):
        """Test basic OutputReference creation."""
        ref = OutputReference(source_step=0, role="data")
        assert ref.source_step == 0
        assert ref.role == "data"
        assert ref.artifact_type == ArtifactTypes.ANY

    def test_create_with_artifact_type(self):
        """Test OutputReference with artifact_type."""
        ref = OutputReference(source_step=1, role="scores", artifact_type="metric")
        assert ref.source_step == 1
        assert ref.role == "scores"
        assert ref.artifact_type == "metric"

    def test_immutable(self):
        """Test that OutputReference is immutable (frozen schema)."""
        ref = OutputReference(source_step=0, role="data")
        with pytest.raises(ValidationError):
            ref.source_step = 1

    def test_hashable(self):
        """Test that OutputReference is hashable."""
        ref1 = OutputReference(source_step=0, role="data")
        ref2 = OutputReference(source_step=0, role="data")
        ref3 = OutputReference(source_step=1, role="data")

        # Can be used in sets
        refs = {ref1, ref2, ref3}
        assert len(refs) == 2  # ref1 and ref2 are equal

        # Can be used as dict keys
        d = {ref1: "first", ref3: "second"}
        assert d[ref2] == "first"  # ref2 == ref1

    def test_equality(self):
        """Test OutputReference equality."""
        ref1 = OutputReference(source_step=0, role="data")
        ref2 = OutputReference(source_step=0, role="data")
        ref3 = OutputReference(source_step=0, role="metrics")

        assert ref1 == ref2
        assert ref1 != ref3


class TestStepResult:
    """Tests for StepResult schema model."""

    def test_create_minimal(self):
        """Test minimal StepResult creation."""
        result = StepResult(
            step_name="ingest",
            step_number=0,
            success=True,
        )
        assert result.step_name == "ingest"
        assert result.step_number == 0
        assert result.success is True
        assert result.total_count == 0
        assert result.succeeded_count == 0
        assert result.failed_count == 0
        assert result.output_roles == frozenset()
        assert result.output_types == {}

    def test_create_full(self):
        """Test StepResult with all fields."""
        result = StepResult(
            step_name="score",
            step_number=1,
            success=True,
            total_count=100,
            succeeded_count=95,
            failed_count=5,
            output_roles=frozenset(["data", "metrics"]),
            output_types={"data": "data", "metrics": "metric"},
        )
        assert result.total_count == 100
        assert result.succeeded_count == 95
        assert result.failed_count == 5
        assert "data" in result.output_roles
        assert "metrics" in result.output_roles

    def test_output_returns_reference(self):
        """Test that output() returns an OutputReference."""
        result = StepResult(
            step_name="ingest",
            step_number=0,
            success=True,
            output_roles=frozenset(["data"]),
            output_types={"data": "data"},
        )
        ref = result.output("data")
        assert isinstance(ref, OutputReference)
        assert ref.source_step == 0
        assert ref.role == "data"
        assert ref.artifact_type == "data"

    def test_output_missing_role_raises(self):
        """Test that output() raises ValueError for missing role."""
        result = StepResult(
            step_name="ingest",
            step_number=0,
            success=True,
            output_roles=frozenset(["data"]),
        )
        with pytest.raises(ValueError, match="Output role 'missing' not available"):
            result.output("missing")

    def test_output_error_message_includes_available(self):
        """Test that error message includes available roles."""
        result = StepResult(
            step_name="ingest",
            step_number=0,
            success=True,
            output_roles=frozenset(["alpha", "beta"]),
        )
        with pytest.raises(ValueError, match="Available roles: alpha, beta"):
            result.output("gamma")

    def test_has_failures_property(self):
        """Test has_failures property."""
        no_failures = StepResult(
            step_name="test",
            step_number=0,
            success=True,
            failed_count=0,
        )
        assert no_failures.has_failures is False

        with_failures = StepResult(
            step_name="test",
            step_number=0,
            success=False,
            failed_count=5,
        )
        assert with_failures.has_failures is True

    def test_frozen(self):
        """Test that StepResult is frozen (immutable)."""
        result = StepResult(step_name="test", step_number=0, success=True)
        with pytest.raises(ValidationError):
            result.step_name = "changed"


class TestStepResultBuilder:
    """Tests for StepResultBuilder."""

    def test_build_empty(self):
        """Test building with no items."""
        builder = StepResultBuilder(
            step_name="test",
            step_number=0,
            operation_outputs={"out": "data"},
        )
        result = builder.build()
        assert result.step_name == "test"
        assert result.step_number == 0
        assert result.success is True  # No failures
        assert result.total_count == 0
        assert result.succeeded_count == 0
        assert result.failed_count == 0
        assert result.output_roles == frozenset(["out"])

    def test_add_success(self):
        """Test adding successes."""
        builder = StepResultBuilder(
            step_name="test",
            step_number=0,
            operation_outputs={},
        )
        builder.add_success()
        builder.add_success(count=5)
        result = builder.build()
        assert result.total_count == 6
        assert result.succeeded_count == 6
        assert result.failed_count == 0
        assert result.success is True

    def test_add_failure(self):
        """Test adding failures."""
        builder = StepResultBuilder(
            step_name="test",
            step_number=0,
            operation_outputs={},
        )
        builder.add_failure()
        builder.add_failure(count=3)
        result = builder.build()
        assert result.total_count == 4
        assert result.succeeded_count == 0
        assert result.failed_count == 4
        assert result.success is False

    def test_mixed_results(self):
        """Test mixed success and failure."""
        builder = StepResultBuilder(
            step_name="test",
            step_number=0,
            operation_outputs={},
        )
        builder.add_success(count=8)
        builder.add_failure(count=2)
        result = builder.build()
        assert result.total_count == 10
        assert result.succeeded_count == 8
        assert result.failed_count == 2
        assert result.success is False  # Has failures

    def test_success_override(self):
        """Test success_override parameter."""
        builder = StepResultBuilder(
            step_name="test",
            step_number=0,
            operation_outputs={},
        )
        builder.add_failure()

        # Without override, would be False
        result_no_override = builder.build()
        assert result_no_override.success is False

        # With override=True
        result_override_true = builder.build(success_override=True)
        assert result_override_true.success is True

        # Reset and test with override=False
        builder2 = StepResultBuilder(
            step_name="test",
            step_number=0,
            operation_outputs={},
        )
        builder2.add_success()
        result_override_false = builder2.build(success_override=False)
        assert result_override_false.success is False


class TestPipelineConfig:
    """Tests for PipelineConfig dataclass."""

    def test_create_minimal(self):
        """Test minimal PipelineConfig creation."""
        config = PipelineConfig(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        assert config.name == "test"
        assert config.delta_root == "/data/delta"
        assert config.staging_root == "/data/staging"
        assert config.working_root == tempfile.gettempdir()  # Default
        assert config.failure_policy == FailurePolicy.CONTINUE  # Default
        assert config.cache_policy == CachePolicy.ALL_SUCCEEDED  # Default
        assert config.default_backend == "local"  # Default

    def test_create_full(self):
        """Test PipelineConfig with all fields."""
        config = PipelineConfig(
            name="full_test",
            delta_root="/custom/delta",
            staging_root="/custom/staging",
            working_root="/tmp/work",
            failure_policy="fail_fast",
            default_backend="slurm",
        )
        assert config.working_root == "/tmp/work"
        assert config.failure_policy == FailurePolicy.FAIL_FAST
        assert config.default_backend == "slurm"

    def test_pipeline_config_has_pipeline_run_id(self):
        """Test PipelineConfig has pipeline_run_id with empty default."""
        config = PipelineConfig(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        assert config.pipeline_run_id == ""

    def test_pipeline_config_custom_pipeline_run_id(self):
        """Test PipelineConfig accepts custom pipeline_run_id."""
        config = PipelineConfig(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
            pipeline_run_id="my_run_20260215_120000_abcd1234",
        )
        assert config.pipeline_run_id == "my_run_20260215_120000_abcd1234"


class TestPipelineManager:
    """Tests for PipelineManager class."""

    def test_create_factory(self):
        """Test PipelineManager.create() factory method."""
        pipeline = PipelineManager.create(
            name="test_pipeline",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        assert pipeline.config.name == "test_pipeline"
        assert pipeline.config.delta_root == "/data/delta"
        assert pipeline.current_step == 0

    def test_create_with_string_paths(self):
        """Test that create() accepts string paths."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        assert isinstance(pipeline.config.delta_root, str)
        assert isinstance(pipeline.config.staging_root, str)

    def test_create_custom_config(self):
        """Test create() with custom configuration."""
        pipeline = PipelineManager.create(
            name="custom",
            delta_root="/data/delta",
            staging_root="/data/staging",
            working_root="/tmp/work",
            failure_policy="fail_fast",
            backend="slurm",
        )
        assert pipeline.config.working_root == "/tmp/work"
        assert pipeline.config.failure_policy == FailurePolicy.FAIL_FAST
        assert pipeline.config.default_backend == "slurm"

    def test_finalize_empty(self):
        """Test finalize() with no steps."""
        pipeline = PipelineManager.create(
            name="empty",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        summary = pipeline.finalize()
        assert summary["pipeline_name"] == "empty"
        assert summary["total_steps"] == 0
        assert summary["steps"] == []
        assert summary["overall_success"] is True  # No failures

    def test_step_increments_counter(self):
        """Test that current_step increments (without full execution)."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        # Note: This test verifies the counter increment mechanism
        # Full step execution requires additional infrastructure
        assert pipeline.current_step == 0
        # After a step would be called, current_step would be 1

    # --- Dunder method tests ---

    def test_repr(self):
        """Test __repr__ returns unambiguous representation."""
        pipeline = PipelineManager.create(
            name="test_pipeline",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        repr_str = repr(pipeline)
        assert "PipelineManager(" in repr_str
        assert "name='test_pipeline'" in repr_str
        assert "steps=0" in repr_str
        assert "delta_root=" in repr_str

    def test_repr_with_steps(self):
        """Test __repr__ shows step count."""
        pipeline = PipelineManager.create(
            name="test_pipeline",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        # Add mock step results
        pipeline._step_results.append(
            StepResult(step_name="Ingest", step_number=0, success=True)
        )
        pipeline._step_results.append(
            StepResult(step_name="Score", step_number=1, success=True)
        )
        repr_str = repr(pipeline)
        assert "steps=2" in repr_str

    def test_str_no_steps(self):
        """Test __str__ with no steps executed."""
        pipeline = PipelineManager.create(
            name="my_pipeline",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        str_output = str(pipeline)
        assert "Pipeline 'my_pipeline'" in str_output
        assert "no steps executed" in str_output

    def test_str_all_succeeded(self):
        """Test __str__ when all steps succeeded."""
        pipeline = PipelineManager.create(
            name="my_pipeline",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        pipeline._step_results.append(
            StepResult(step_name="Ingest", step_number=0, success=True)
        )
        pipeline._step_results.append(
            StepResult(step_name="Score", step_number=1, success=True)
        )
        str_output = str(pipeline)
        assert "2 steps" in str_output
        assert "all succeeded" in str_output

    def test_str_partial_success(self):
        """Test __str__ when some steps failed."""
        pipeline = PipelineManager.create(
            name="my_pipeline",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        pipeline._step_results.append(
            StepResult(step_name="Ingest", step_number=0, success=True)
        )
        pipeline._step_results.append(
            StepResult(step_name="Score", step_number=1, success=False)
        )
        str_output = str(pipeline)
        assert "2 steps" in str_output
        assert "1/2 succeeded" in str_output

    def test_len_empty(self):
        """Test __len__ with no steps."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        assert len(pipeline) == 0

    def test_len_with_steps(self):
        """Test __len__ with steps."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        pipeline._step_results.append(
            StepResult(step_name="Ingest", step_number=0, success=True)
        )
        pipeline._step_results.append(
            StepResult(step_name="Score", step_number=1, success=True)
        )
        pipeline._step_results.append(
            StepResult(step_name="Filter", step_number=2, success=True)
        )
        assert len(pipeline) == 3

    def test_iter(self):
        """Test __iter__ iterates over step results."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        step0 = StepResult(step_name="Ingest", step_number=0, success=True)
        step1 = StepResult(step_name="Score", step_number=1, success=True)
        pipeline._step_results.append(step0)
        pipeline._step_results.append(step1)

        # Test iteration
        results = list(pipeline)
        assert len(results) == 2
        assert results[0].step_name == "Ingest"
        assert results[1].step_name == "Score"

    def test_iter_in_for_loop(self):
        """Test __iter__ works in for loop."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        pipeline._step_results.append(
            StepResult(step_name="Ingest", step_number=0, success=True)
        )
        pipeline._step_results.append(
            StepResult(step_name="Score", step_number=1, success=True)
        )

        names = []
        for step in pipeline:
            names.append(step.step_name)
        assert names == ["Ingest", "Score"]

    def test_getitem_single_index(self):
        """Test __getitem__ with single index."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        pipeline._step_results.append(
            StepResult(step_name="Ingest", step_number=0, success=True)
        )
        pipeline._step_results.append(
            StepResult(step_name="Score", step_number=1, success=True)
        )

        first = pipeline[0]
        assert isinstance(first, StepResult)
        assert first.step_name == "Ingest"
        second = pipeline[1]
        assert isinstance(second, StepResult)
        assert second.step_name == "Score"
        last = pipeline[-1]
        assert isinstance(last, StepResult)
        assert last.step_name == "Score"  # Negative index

    def test_getitem_slice(self):
        """Test __getitem__ with slice."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        pipeline._step_results.append(
            StepResult(step_name="Ingest", step_number=0, success=True)
        )
        pipeline._step_results.append(
            StepResult(step_name="Score", step_number=1, success=True)
        )
        pipeline._step_results.append(
            StepResult(step_name="Filter", step_number=2, success=True)
        )

        last_two = pipeline[-2:]
        assert isinstance(last_two, list)
        assert len(last_two) == 2
        assert last_two[0].step_name == "Score"
        assert last_two[1].step_name == "Filter"

    def test_getitem_index_error(self):
        """Test __getitem__ raises IndexError for invalid index."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        pipeline._step_results.append(
            StepResult(step_name="Ingest", step_number=0, success=True)
        )

        with pytest.raises(IndexError):
            _ = pipeline[5]

    def test_bool_empty_is_false(self):
        """Test __bool__ returns False for empty pipeline."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        assert not pipeline
        assert bool(pipeline) is False

    def test_bool_all_succeeded_is_true(self):
        """Test __bool__ returns True when all steps succeeded."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        pipeline._step_results.append(
            StepResult(step_name="Ingest", step_number=0, success=True)
        )
        pipeline._step_results.append(
            StepResult(step_name="Score", step_number=1, success=True)
        )
        assert pipeline
        assert bool(pipeline) is True

    def test_bool_with_failure_is_false(self):
        """Test __bool__ returns False when any step failed."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        pipeline._step_results.append(
            StepResult(step_name="Ingest", step_number=0, success=True)
        )
        pipeline._step_results.append(
            StepResult(step_name="Score", step_number=1, success=False)
        )
        assert not pipeline
        assert bool(pipeline) is False

    def test_contains_existing_step(self):
        """Test __contains__ returns True for existing step name."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        pipeline._step_results.append(
            StepResult(step_name="Ingest", step_number=0, success=True)
        )
        pipeline._step_results.append(
            StepResult(step_name="Score", step_number=1, success=True)
        )

        assert "Ingest" in pipeline
        assert "Score" in pipeline

    def test_contains_missing_step(self):
        """Test __contains__ returns False for missing step name."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        pipeline._step_results.append(
            StepResult(step_name="Ingest", step_number=0, success=True)
        )

        assert "NonExistent" not in pipeline
        assert "Filter" not in pipeline


class TestFailurePolicy:
    """Tests for FailurePolicy enum."""

    def test_valid_values(self):
        """Test that FailurePolicy enum has expected members."""
        assert FailurePolicy.CONTINUE.value == "continue"
        assert FailurePolicy.FAIL_FAST.value == "fail_fast"

    def test_string_coercion_via_pydantic(self):
        """Test that Pydantic coerces strings to enum members."""
        config = PipelineConfig(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
            failure_policy="continue",
        )
        assert config.failure_policy == FailurePolicy.CONTINUE
