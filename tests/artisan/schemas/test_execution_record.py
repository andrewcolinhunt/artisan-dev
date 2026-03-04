"""Tests for execution_record.py"""

from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from artisan.schemas.execution.execution_record import ExecutionRecord


class TestExecutionRecord:
    """Tests for ExecutionRecord model."""

    def test_create_valid_execution_record(self):
        """Create a valid execution record."""
        record = ExecutionRecord(
            execution_run_id="r" * 32,
            execution_spec_id="s" * 32,
            origin_step_number=1,
            operation_name="relax",
            timestamp_start=datetime.now(),
        )
        assert record.operation_name == "relax"
        assert record.origin_step_number == 1

    def test_dual_identity(self):
        """Verify dual ID fields are both present."""
        record = ExecutionRecord(
            execution_run_id="run123" + "x" * 26,
            execution_spec_id="spec456" + "y" * 25,
            origin_step_number=0,
            operation_name="test",
            timestamp_start=datetime.now(),
        )
        assert record.execution_run_id.startswith("run123")
        assert record.execution_spec_id.startswith("spec456")

    def test_id_length_validation(self):
        """IDs must be exactly 32 characters."""
        with pytest.raises(ValidationError) as exc_info:
            ExecutionRecord(
                execution_run_id="short",
                execution_spec_id="s" * 32,
                origin_step_number=0,
                operation_name="test",
                timestamp_start=datetime.now(),
            )
        assert "string_too_short" in str(exc_info.value)

    def test_compute_backend_default(self):
        """Default compute backend is LOCAL."""
        record = ExecutionRecord(
            execution_run_id="r" * 32,
            execution_spec_id="s" * 32,
            origin_step_number=0,
            operation_name="test",
            timestamp_start=datetime.now(),
        )
        assert record.compute_backend == "local"

    def test_slurm_backend(self):
        """Test SLURM compute backend."""
        record = ExecutionRecord(
            execution_run_id="r" * 32,
            execution_spec_id="s" * 32,
            origin_step_number=0,
            operation_name="test",
            timestamp_start=datetime.now(),
            compute_backend="slurm",
            source_worker=42,
        )
        assert record.compute_backend == "slurm"
        assert record.source_worker == 42

    def test_failure_with_error(self):
        """Test execution record for failed execution."""
        record = ExecutionRecord(
            execution_run_id="r" * 32,
            execution_spec_id="s" * 32,
            origin_step_number=1,
            operation_name="test",
            timestamp_start=datetime.now(),
            success=False,
            error="Process crashed with exit code 1",
        )
        assert record.success is False
        assert "exit code" in record.error
