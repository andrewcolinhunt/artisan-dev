"""Tests for orchestration/engine/inputs.py."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from artisan.orchestration.engine.inputs import resolve_output_reference
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.storage.core.table_schemas import EXECUTIONS_SCHEMA


def _create_executions_df(**overrides) -> pl.DataFrame:
    """Create a test executions DataFrame with default values."""
    defaults = {
        "execution_run_id": ["e" * 32],
        "execution_spec_id": ["s" * 32],
        "step_run_id": [None],
        "origin_step_number": [0],
        "operation_name": ["TestOp"],
        "timestamp_start": [datetime.now(UTC)],
        "timestamp_end": [datetime.now(UTC)],
        "source_worker": [0],
        "success": [True],
        "error": [None],
        "params": ["{}"],
        "user_overrides": ["{}"],
        "compute_backend": ["local"],
        "tool_output": [None],
        "worker_log": [None],
        "metadata": ["{}"],
    }
    defaults.update(overrides)
    return pl.DataFrame(defaults, schema=EXECUTIONS_SCHEMA)


class TestResolveOutputReferenceEmptyUpstream:
    """Tests for F20: empty upstream returns empty list instead of raising."""

    def test_missing_executions_table_returns_empty(self, tmp_path):
        """When executions table doesn't exist, return [] (not raise)."""
        ref = OutputReference(source_step=0, role="data")
        result = resolve_output_reference(ref, tmp_path)
        assert result == []

    def test_missing_execution_edges_table_returns_empty(self, tmp_path):
        """When execution_edges table is missing, return []."""
        # Create executions table with a successful execution
        executions_path = tmp_path / TablePath.EXECUTIONS
        df = _create_executions_df()
        df.write_delta(str(executions_path))

        # No execution_edges table
        ref = OutputReference(source_step=0, role="data")
        result = resolve_output_reference(ref, tmp_path)
        assert result == []

    def test_no_successful_executions_returns_empty(self, tmp_path):
        """When all executions failed, return [] (not raise)."""
        executions_path = tmp_path / TablePath.EXECUTIONS
        df = _create_executions_df(success=[False], error=["some error"])
        df.write_delta(str(executions_path))

        ref = OutputReference(source_step=0, role="data")
        result = resolve_output_reference(ref, tmp_path)
        assert result == []
