"""Tests for StepState and StepStartRecord schema models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.orchestration.step_state import StepState


class TestStepState:
    """Tests for StepState Pydantic model."""

    def test_to_step_result(self):
        """Round-trip produces valid StepResult with correct fields."""
        state = StepState(
            pipeline_run_id="test_20260215_120000_abcd1234",
            step_number=1,
            step_name="ToolC",
            step_spec_id="a" * 32,
            status="completed",
            operation_class="artisan.operations.examples.data_generator.DataGenerator",
            params_json='{"model": "v2"}',
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json="{}",
            total_count=10,
            succeeded_count=8,
            failed_count=2,
            duration_seconds=45.3,
            output_roles=frozenset(["data", "metrics"]),
            output_types={"data": "data", "metrics": "metric"},
        )
        result = state.to_step_result()

        assert result.step_name == "ToolC"
        assert result.step_number == 1
        assert result.success is False  # has failed_count > 0
        assert result.total_count == 10
        assert result.succeeded_count == 8
        assert result.failed_count == 2
        assert result.duration_seconds == 45.3
        assert result.output_roles == frozenset(["data", "metrics"])
        assert result.output_types == {
            "data": "data",
            "metrics": "metric",
        }

    def test_to_step_result_success_when_no_failures(self):
        """StepResult.success is True when failed_count is 0."""
        state = StepState(
            pipeline_run_id="test_run",
            step_number=0,
            step_name="Ingest",
            step_spec_id="b" * 32,
            status="completed",
            operation_class="artisan.operations.curator.filter.Filter",
            params_json="{}",
            input_refs_json="null",
            compute_backend="local",
            compute_options_json="{}",
            total_count=5,
            succeeded_count=5,
            failed_count=0,
            duration_seconds=1.2,
            output_roles=frozenset(["file"]),
            output_types={"file": "file_ref"},
        )
        result = state.to_step_result()
        assert result.success is True

    def test_frozen(self):
        """StepState is immutable."""
        state = StepState(
            pipeline_run_id="test_run",
            step_number=0,
            step_name="Ingest",
            step_spec_id="c" * 32,
            status="completed",
            operation_class="test.Op",
            params_json="{}",
            input_refs_json="null",
            compute_backend="local",
            compute_options_json="{}",
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            duration_seconds=None,
            output_roles=frozenset(),
            output_types={},
        )
        with pytest.raises(ValidationError):
            state.step_number = 5


class TestStepStartRecord:
    """Tests for StepStartRecord Pydantic model."""

    def test_creation(self):
        """StepStartRecord can be created with all required fields."""
        record = StepStartRecord(
            step_run_id="run123",
            step_spec_id="spec456",
            step_number=0,
            step_name="Ingest",
            operation_class="artisan.operations.curator.filter.Filter",
            params_json="{}",
            input_refs_json="null",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json='["file"]',
            output_types_json='{"file": "file_ref"}',
        )
        assert record.step_run_id == "run123"
        assert record.step_spec_id == "spec456"
        assert record.step_number == 0
        assert record.step_name == "Ingest"

    def test_frozen(self):
        """StepStartRecord is immutable."""
        record = StepStartRecord(
            step_run_id="run123",
            step_spec_id="spec456",
            step_number=0,
            step_name="Ingest",
            operation_class="test.Op",
            params_json="{}",
            input_refs_json="null",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json="[]",
            output_types_json="{}",
        )
        with pytest.raises(ValidationError):
            record.step_number = 5
