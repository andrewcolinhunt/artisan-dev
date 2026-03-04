"""Tests for reference resolver with execution_edges table.

This module tests resolve_output_reference with the normalized schema that uses
the `execution_edges` table for input/output edges instead of storing
arrays in executions.

Reference: design_utility-operation-lifecycle-refactor-v2.md
"""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from artisan.orchestration.engine.inputs import (
    resolve_output_reference,
)
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.storage.core.table_schemas import (
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
)


def _create_executions_df(**overrides) -> pl.DataFrame:
    """Create a test executions DataFrame with default values.

    The new schema does not have inputs/outputs columns - those are in
    execution_edges table instead.
    """
    defaults = {
        "execution_run_id": ["e" * 32],
        "execution_spec_id": ["s" * 32],
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


def _create_execution_edges_df(rows: list[dict]) -> pl.DataFrame:
    """Create a test execution_edges DataFrame from rows.

    Each row should have: execution_run_id, direction, role, artifact_id.
    """
    if not rows:
        return pl.DataFrame(
            {
                "execution_run_id": [],
                "direction": [],
                "role": [],
                "artifact_id": [],
            },
            schema=EXECUTION_EDGES_SCHEMA,
        )

    return pl.DataFrame(rows, schema=EXECUTION_EDGES_SCHEMA)


def _write_tables(
    tmp_path,
    records_df: pl.DataFrame,
    execution_edges: list[dict],
):
    """Write both executions and execution_edges tables."""
    records_path = tmp_path / "orchestration/executions"
    provenance_path = tmp_path / "provenance/execution_edges"

    records_df.write_delta(str(records_path))
    _create_execution_edges_df(execution_edges).write_delta(str(provenance_path))


class TestResolveOutputReferenceNewSchema:
    """Tests for resolve_output_reference with execution_edges table."""

    def test_extracts_from_outputs_list(self, tmp_path):
        """Test that outputs are extracted from execution_edges table."""
        exec_run_id = "e" * 32

        records_df = _create_executions_df(execution_run_id=[exec_run_id])
        execution_edges = [
            {
                "execution_run_id": exec_run_id,
                "direction": "input",
                "role": "input",
                "artifact_id": "i" * 32,
            },
            {
                "execution_run_id": exec_run_id,
                "direction": "output",
                "role": "data",
                "artifact_id": "a" * 32,
            },
            {
                "execution_run_id": exec_run_id,
                "direction": "output",
                "role": "data",
                "artifact_id": "b" * 32,
            },
        ]
        _write_tables(tmp_path, records_df, execution_edges)

        ref = OutputReference(source_step=0, role="data")
        result = resolve_output_reference(ref, tmp_path)

        assert len(result) == 2
        assert "a" * 32 in result
        assert "b" * 32 in result

    def test_filters_by_role(self, tmp_path):
        """Test that only matching role is returned."""
        exec_run_id = "e" * 32

        records_df = _create_executions_df(execution_run_id=[exec_run_id])
        execution_edges = [
            {
                "execution_run_id": exec_run_id,
                "direction": "output",
                "role": "data",
                "artifact_id": "a" * 32,
            },
            {
                "execution_run_id": exec_run_id,
                "direction": "output",
                "role": "metrics",
                "artifact_id": "m" * 32,
            },
        ]
        _write_tables(tmp_path, records_df, execution_edges)

        ref = OutputReference(source_step=0, role="metrics")
        result = resolve_output_reference(ref, tmp_path)

        assert len(result) == 1
        assert result[0] == "m" * 32

    def test_deduplicates_results(self, tmp_path):
        """Test that duplicate artifact IDs are deduplicated."""
        exec_run_id_1 = "e1" + "x" * 30
        exec_run_id_2 = "e2" + "y" * 30

        records_df = _create_executions_df(
            execution_run_id=[exec_run_id_1, exec_run_id_2],
            execution_spec_id=["s1" + "x" * 30, "s2" + "y" * 30],
            origin_step_number=[0, 0],
            operation_name=["TestOp", "TestOp"],
            timestamp_start=[datetime.now(UTC)] * 2,
            timestamp_end=[datetime.now(UTC)] * 2,
            source_worker=[0, 0],
            success=[True, True],
            error=[None, None],
            params=["{}"] * 2,
            user_overrides=["{}"] * 2,
            compute_backend=["local"] * 2,
            tool_output=[None, None],
            worker_log=[None, None],
            metadata=["{}"] * 2,
        )
        # Both executions produce the same artifact
        execution_edges = [
            {
                "execution_run_id": exec_run_id_1,
                "direction": "output",
                "role": "data",
                "artifact_id": "a" * 32,
            },
            {
                "execution_run_id": exec_run_id_2,
                "direction": "output",
                "role": "data",
                "artifact_id": "a" * 32,
            },
        ]
        _write_tables(tmp_path, records_df, execution_edges)

        ref = OutputReference(source_step=0, role="data")
        result = resolve_output_reference(ref, tmp_path)

        # Should be deduplicated
        assert len(result) == 1
        assert result[0] == "a" * 32

    def test_filters_by_step_number(self, tmp_path):
        """Test that only executions from the correct step are considered."""
        exec_run_id_0 = "e1" + "x" * 30
        exec_run_id_1 = "e2" + "y" * 30

        records_df = _create_executions_df(
            execution_run_id=[exec_run_id_0, exec_run_id_1],
            execution_spec_id=["s1" + "x" * 30, "s2" + "y" * 30],
            origin_step_number=[0, 1],  # Different steps
            operation_name=["TestOp", "TestOp"],
            timestamp_start=[datetime.now(UTC)] * 2,
            timestamp_end=[datetime.now(UTC)] * 2,
            source_worker=[0, 0],
            success=[True, True],
            error=[None, None],
            params=["{}"] * 2,
            user_overrides=["{}"] * 2,
            compute_backend=["local"] * 2,
            tool_output=[None, None],
            worker_log=[None, None],
            metadata=["{}"] * 2,
        )
        execution_edges = [
            {
                "execution_run_id": exec_run_id_0,
                "direction": "output",
                "role": "data",
                "artifact_id": "a" * 32,
            },
            {
                "execution_run_id": exec_run_id_1,
                "direction": "output",
                "role": "data",
                "artifact_id": "b" * 32,
            },
        ]
        _write_tables(tmp_path, records_df, execution_edges)

        # Get step 0 output
        ref0 = OutputReference(source_step=0, role="data")
        result0 = resolve_output_reference(ref0, tmp_path)
        assert len(result0) == 1
        assert result0[0] == "a" * 32

        # Get step 1 output
        ref1 = OutputReference(source_step=1, role="data")
        result1 = resolve_output_reference(ref1, tmp_path)
        assert len(result1) == 1
        assert result1[0] == "b" * 32

    def test_filters_by_success(self, tmp_path):
        """Test that only successful executions are considered."""
        exec_run_id_success = "e1" + "x" * 30
        exec_run_id_failed = "e2" + "y" * 30

        records_df = _create_executions_df(
            execution_run_id=[exec_run_id_success, exec_run_id_failed],
            execution_spec_id=["s1" + "x" * 30, "s2" + "y" * 30],
            origin_step_number=[0, 0],
            operation_name=["TestOp", "TestOp"],
            timestamp_start=[datetime.now(UTC)] * 2,
            timestamp_end=[datetime.now(UTC)] * 2,
            source_worker=[0, 0],
            success=[True, False],  # One success, one failure
            error=[None, "Some error"],
            params=["{}"] * 2,
            user_overrides=["{}"] * 2,
            compute_backend=["local"] * 2,
            tool_output=[None, None],
            worker_log=[None, None],
            metadata=["{}"] * 2,
        )
        execution_edges = [
            {
                "execution_run_id": exec_run_id_success,
                "direction": "output",
                "role": "data",
                "artifact_id": "a" * 32,
            },
            {
                "execution_run_id": exec_run_id_failed,
                "direction": "output",
                "role": "data",
                "artifact_id": "b" * 32,
            },
        ]
        _write_tables(tmp_path, records_df, execution_edges)

        ref = OutputReference(source_step=0, role="data")
        result = resolve_output_reference(ref, tmp_path)

        # Should only get the successful execution's output
        assert len(result) == 1
        assert result[0] == "a" * 32

    def test_results_sorted(self, tmp_path):
        """Test that results are sorted alphabetically."""
        exec_run_id = "e" * 32

        records_df = _create_executions_df(execution_run_id=[exec_run_id])
        execution_edges = [
            {
                "execution_run_id": exec_run_id,
                "direction": "output",
                "role": "data",
                "artifact_id": "z" * 32,
            },
            {
                "execution_run_id": exec_run_id,
                "direction": "output",
                "role": "data",
                "artifact_id": "a" * 32,
            },
            {
                "execution_run_id": exec_run_id,
                "direction": "output",
                "role": "data",
                "artifact_id": "m" * 32,
            },
        ]
        _write_tables(tmp_path, records_df, execution_edges)

        ref = OutputReference(source_step=0, role="data")
        result = resolve_output_reference(ref, tmp_path)

        assert result == sorted(result)
        assert result[0] == "a" * 32
        assert result[-1] == "z" * 32

    def test_no_executions_returns_empty(self, tmp_path):
        """Test that missing executions table returns empty list."""
        ref = OutputReference(source_step=0, role="data")

        result = resolve_output_reference(ref, tmp_path)
        assert result == []

    def test_no_successful_executions_raises(self, tmp_path):
        """Test that no successful executions raises ValueError."""
        exec_run_id = "e" * 32

        records_df = _create_executions_df(
            execution_run_id=[exec_run_id],
            success=[False],  # All failed
            error=["Some error"],
        )
        execution_edges = [
            {
                "execution_run_id": exec_run_id,
                "direction": "output",
                "role": "data",
                "artifact_id": "a" * 32,
            },
        ]
        _write_tables(tmp_path, records_df, execution_edges)

        ref = OutputReference(source_step=0, role="data")

        result = resolve_output_reference(ref, tmp_path)
        assert result == []

    def test_no_outputs_for_role_returns_empty(self, tmp_path):
        """Test that missing role in outputs returns empty list."""
        exec_run_id = "e" * 32

        records_df = _create_executions_df(execution_run_id=[exec_run_id])
        execution_edges = [
            {
                "execution_run_id": exec_run_id,
                "direction": "output",
                "role": "data",
                "artifact_id": "a" * 32,
            },
        ]
        _write_tables(tmp_path, records_df, execution_edges)

        ref = OutputReference(source_step=0, role="metrics")  # Requesting metrics
        result = resolve_output_reference(ref, tmp_path)
        assert result == []

    def test_successful_execution_zero_output_edges_returns_empty(self, tmp_path):
        """Test that a successful execution with no output edges returns empty list.

        This is the actual filter scenario: the filter step runs successfully
        but produces no output edges because all artifacts were filtered out.
        """
        exec_run_id = "e" * 32

        records_df = _create_executions_df(execution_run_id=[exec_run_id])
        # Only input edges, no output edges at all
        execution_edges = [
            {
                "execution_run_id": exec_run_id,
                "direction": "input",
                "role": "data",
                "artifact_id": "a" * 32,
            },
        ]
        _write_tables(tmp_path, records_df, execution_edges)

        ref = OutputReference(source_step=0, role="data")
        result = resolve_output_reference(ref, tmp_path)
        assert result == []

    def test_multiple_executions_aggregated(self, tmp_path):
        """Test that outputs from multiple executions are aggregated."""
        exec_run_ids = [f"e{i}" + "x" * 30 for i in range(3)]

        records_df = _create_executions_df(
            execution_run_id=exec_run_ids,
            execution_spec_id=[f"s{i}" + "x" * 30 for i in range(3)],
            origin_step_number=[0, 0, 0],
            operation_name=["TestOp"] * 3,
            timestamp_start=[datetime.now(UTC)] * 3,
            timestamp_end=[datetime.now(UTC)] * 3,
            source_worker=[0, 1, 2],
            success=[True, True, True],
            error=[None, None, None],
            params=["{}"] * 3,
            user_overrides=["{}"] * 3,
            compute_backend=["local"] * 3,
            tool_output=[None, None, None],
            worker_log=[None, None, None],
            metadata=["{}"] * 3,
        )
        execution_edges = [
            {
                "execution_run_id": exec_run_ids[0],
                "direction": "output",
                "role": "data",
                "artifact_id": "a" * 32,
            },
            {
                "execution_run_id": exec_run_ids[1],
                "direction": "output",
                "role": "data",
                "artifact_id": "b" * 32,
            },
            {
                "execution_run_id": exec_run_ids[2],
                "direction": "output",
                "role": "data",
                "artifact_id": "c" * 32,
            },
        ]
        _write_tables(tmp_path, records_df, execution_edges)

        ref = OutputReference(source_step=0, role="data")
        result = resolve_output_reference(ref, tmp_path)

        # Should get all three outputs
        assert len(result) == 3
        assert "a" * 32 in result
        assert "b" * 32 in result
        assert "c" * 32 in result

    def test_no_provenance_table_returns_empty(self, tmp_path):
        """Test that missing execution_edges table returns empty list."""
        records_df = _create_executions_df()
        records_df.write_delta(str(tmp_path / "orchestration/executions"))
        # Don't create provenance table

        ref = OutputReference(source_step=0, role="data")

        result = resolve_output_reference(ref, tmp_path)
        assert result == []

    def test_ignores_input_direction(self, tmp_path):
        """Test that input direction entries are not returned as outputs."""
        exec_run_id = "e" * 32

        records_df = _create_executions_df(execution_run_id=[exec_run_id])
        execution_edges = [
            # Input entry should be ignored
            {
                "execution_run_id": exec_run_id,
                "direction": "input",
                "role": "data",
                "artifact_id": "i" * 32,
            },
            # Only output should be returned
            {
                "execution_run_id": exec_run_id,
                "direction": "output",
                "role": "data",
                "artifact_id": "o" * 32,
            },
        ]
        _write_tables(tmp_path, records_df, execution_edges)

        ref = OutputReference(source_step=0, role="data")
        result = resolve_output_reference(ref, tmp_path)

        assert len(result) == 1
        assert result[0] == "o" * 32
        assert "i" * 32 not in result
