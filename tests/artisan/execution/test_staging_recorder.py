"""Tests for execution/staging/recorder.py — failure recording."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from artisan.execution.staging.parquet_writer import StagingResult
from artisan.execution.staging.recorder import record_execution_failure


def _make_execution_context(tmp_path: Path) -> MagicMock:
    """Create a mock ExecutionContext with required attributes."""
    from fsspec.implementations.local import LocalFileSystem

    staging_dir = tmp_path / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    ctx = MagicMock()
    ctx.staging_root = str(staging_dir)
    ctx.fs = LocalFileSystem()
    ctx.execution_run_id = "a" * 32
    ctx.execution_spec_id = "b" * 32
    ctx.operation_name = "test_op"
    ctx.step_number = 0
    ctx.timestamp_start = datetime.now(UTC)
    ctx.worker_id = 0
    ctx.compute_backend = "local"
    return ctx


class TestRecordExecutionFailure:
    """Tests for record_execution_failure()."""

    def test_returns_success_false(self, tmp_path):
        """Failure recording must set success=False on the StagingResult."""
        ctx = _make_execution_context(tmp_path)
        result = record_execution_failure(
            execution_context=ctx,
            error="something broke",
            inputs={},
            timestamp_end=datetime.now(UTC),
        )
        assert isinstance(result, StagingResult)
        assert result.success is False

    def test_populates_error(self, tmp_path):
        """Failure recording must propagate the error string."""
        ctx = _make_execution_context(tmp_path)
        result = record_execution_failure(
            execution_context=ctx,
            error="ValueError: bad input",
            inputs={},
            timestamp_end=datetime.now(UTC),
        )
        assert result.error == "ValueError: bad input"

    def test_populates_execution_run_id(self, tmp_path):
        """Failure recording must set the execution_run_id."""
        ctx = _make_execution_context(tmp_path)
        result = record_execution_failure(
            execution_context=ctx,
            error="err",
            inputs={},
            timestamp_end=datetime.now(UTC),
        )
        assert result.execution_run_id == "a" * 32

    def test_double_fault_returns_combined_error(self, tmp_path):
        """If staging itself fails, return a combined error (no raise)."""
        ctx = _make_execution_context(tmp_path)

        with patch(
            "artisan.execution.staging.parquet_writer._stage_execution",
            side_effect=OSError("disk full"),
        ):
            result = record_execution_failure(
                execution_context=ctx,
                error="original error",
                inputs={},
                timestamp_end=datetime.now(UTC),
            )

        assert result.success is False
        assert "original error" in result.error
        assert "disk full" in result.error
        assert "staging the failure record failed" in result.error

    def test_double_fault_still_has_execution_run_id(self, tmp_path):
        """Double-fault result still carries the execution_run_id."""
        ctx = _make_execution_context(tmp_path)

        with patch(
            "artisan.execution.staging.parquet_writer._stage_execution",
            side_effect=RuntimeError("boom"),
        ):
            result = record_execution_failure(
                execution_context=ctx,
                error="original",
                inputs={},
                timestamp_end=datetime.now(UTC),
            )

        assert result.execution_run_id == "a" * 32
