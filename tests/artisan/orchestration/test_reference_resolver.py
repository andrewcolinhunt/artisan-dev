"""Tests for reference resolver with execution_edges table.

This module tests resolve_output_reference with the normalized schema that uses
the `execution_edges` table for input/output edges instead of storing
arrays in executions.

Reference: design_utility-operation-lifecycle-refactor-v2.md
"""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest
from fixtures.execution_records import executions_df
from fixtures.store_format import commit_test_tables, publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.orchestration.engine.inputs import (
    resolve_output_reference,
)
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.table_schemas import (
    ARTIFACT_INDEX_SCHEMA,
    CACHE_REUSE_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
)
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import build_commit_plan
from artisan.storage.io.staging import StagingManager


@pytest.fixture(autouse=True)
def _supported_store(tmp_path):
    publish_test_store(str(tmp_path), LocalFileSystem())


def _create_executions_df(**overrides) -> pl.DataFrame:
    """Create a test executions DataFrame with default values.

    The new schema does not have inputs/outputs columns - those are in
    execution_edges table instead.
    """
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
    # Infer row count from overrides so nullable defaults auto-scale.
    if overrides:
        n = len(next(iter(overrides.values())))
        for key, val in defaults.items():
            if key not in overrides and len(val) != n:
                defaults[key] = val * n
    defaults.update(overrides)
    return executions_df(**defaults)


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
    """Commit executions and execution edges as visible format-2 rows."""
    edges = _create_execution_edges_df(execution_edges)
    for (step_number,), records in records_df.group_by(
        "origin_step_number", maintain_order=True
    ):
        step_run_id = f"seed-step-{step_number}"
        execution_ids = records["execution_run_id"].to_list()
        committed_records = records.with_columns(
            pl.lit(step_run_id).alias("step_run_id")
        )
        committed_edges = edges.filter(pl.col("execution_run_id").is_in(execution_ids))
        commit_test_tables(
            str(tmp_path),
            str(tmp_path / "staging"),
            LocalFileSystem(),
            {
                TablePath.EXECUTIONS.value: committed_records,
                TablePath.EXECUTION_EDGES.value: committed_edges,
            },
            step_run_id=step_run_id,
            step_number=step_number,
        )


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
        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())

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
        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())

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
        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())

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
        result0 = resolve_output_reference(ref0, str(tmp_path), fs=LocalFileSystem())
        assert len(result0) == 1
        assert result0[0] == "a" * 32

        # Get step 1 output
        ref1 = OutputReference(source_step=1, role="data")
        result1 = resolve_output_reference(ref1, str(tmp_path), fs=LocalFileSystem())
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
        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())

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
        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())

        assert result == sorted(result)
        assert result[0] == "a" * 32
        assert result[-1] == "z" * 32

    def test_scoped_resolution_unions_direct_and_reused_outputs(self, tmp_path):
        """Current membership, not artifact origin, defines scoped outputs."""
        fs = LocalFileSystem()
        DeltaCommitter(
            str(tmp_path),
            StagingManager(str(tmp_path / "staging"), fs),
            fs=fs,
        ).initialize_tables()
        current = "a" * 32
        direct = "b" * 32
        cached = "c" * 32
        failed = "d" * 32
        now = datetime.now(UTC)
        tracker = StepTracker(str(tmp_path), "current-run")
        tracker.create_attempt(
            StepStartRecord(
                step_run_id=current,
                step_spec_id="e" * 32,
                step_number=7,
                step_name="current",
                operation_class="example.Operation",
                params_json="{}",
                input_refs_json="{}",
                compute_backend="local",
                compute_options_json="{}",
                output_roles_json='["data"]',
                output_types_json='{"data":"data"}',
            )
        )
        tracker.transition(
            current,
            StepStatus.PENDING,
            StepStatus.RUNNING,
            step_spec_id="e" * 32,
        )
        result = StepResult(
            step_run_id=current,
            step_name="current",
            step_number=7,
            status=StepStatus.PARTIAL,
            disposition=StepDisposition.EXECUTED,
            total_count=3,
            succeeded_count=2,
            failed_count=1,
            output_roles=frozenset({"data"}),
            output_types={"data": "data"},
            duration_seconds=1.0,
        )
        candidate = tracker.prepare_terminal_candidate(
            current,
            StepStatus.RUNNING,
            StepStatus.PARTIAL,
            step_spec_id="e" * 32,
            result=result,
        )
        records = _create_executions_df(
            execution_run_id=[direct, cached, failed],
            execution_spec_id=["1" * 32, "2" * 32, "3" * 32],
            step_run_id=[current, "4" * 32, "5" * 32],
            origin_step_number=[7, 1, 1],
            operation_name=["current", "source", "source"],
            timestamp_start=[now] * 3,
            timestamp_end=[now] * 3,
            source_worker=[0] * 3,
            success=[True, True, False],
            error=[None, None, "failed"],
            params=["{}"] * 3,
            user_overrides=["{}"] * 3,
            compute_backend=["local"] * 3,
            tool_output=[None] * 3,
            worker_log=[None] * 3,
            metadata=["{}"] * 3,
        )
        reuse = pl.DataFrame(
            [
                {"current_step_run_id": current, "cached_execution_run_id": cached},
                {"current_step_run_id": current, "cached_execution_run_id": failed},
            ],
            schema=CACHE_REUSE_SCHEMA,
        )
        artifact_ids = ["6" * 32, "7" * 32, "8" * 32, "9" * 32]
        artifacts = pl.DataFrame(
            [
                {
                    "artifact_id": artifact_id,
                    "artifact_type": "data",
                    "origin_step_number": 99,
                    "metadata": "{}",
                }
                for artifact_id in artifact_ids
            ],
            schema=ARTIFACT_INDEX_SCHEMA,
        )
        edges = [
            {
                "execution_run_id": direct,
                "direction": "output",
                "role": "data",
                "artifact_id": artifact_ids[0],
            },
            {
                "execution_run_id": cached,
                "direction": "output",
                "role": "data",
                "artifact_id": artifact_ids[0],
            },
            {
                "execution_run_id": cached,
                "direction": "output",
                "role": "data",
                "artifact_id": artifact_ids[1],
            },
            {
                "execution_run_id": cached,
                "direction": "output",
                "role": "other",
                "artifact_id": artifact_ids[2],
            },
            {
                "execution_run_id": failed,
                "direction": "output",
                "role": "data",
                "artifact_id": artifact_ids[3],
            },
        ]
        edge_frame = pl.DataFrame(edges, schema=EXECUTION_EDGES_SCHEMA)

        _write_tables(
            tmp_path,
            records.slice(1),
            edges[1:],
        )
        commit_test_tables(
            str(tmp_path),
            str(tmp_path / "staging"),
            fs,
            {TablePath.ARTIFACT_INDEX.value: artifacts},
            step_run_id="artifact-seed",
            step_number=99,
        )
        staging = StagingManager(str(tmp_path / "staging"), fs)
        for table, frame in {
            TablePath.EXECUTIONS.value: records.slice(0, 1),
            TablePath.EXECUTION_EDGES.value: edge_frame.slice(0, 1),
            TablePath.CACHE_REUSE.value: reuse,
            TablePath.STEPS.value: candidate,
        }.items():
            staging.stage_orchestrator_dataframe(
                frame,
                table,
                commit_kind="step_result",
                step_run_id=current,
                step_number=7,
                operation_name="current",
            )
        plan = build_commit_plan(
            delta_root=str(tmp_path),
            staging_root=str(tmp_path / "staging"),
            fs=fs,
            commit_kind="step_result",
            step_run_id=current,
            step_number=7,
            operation_name="current",
        )
        DeltaCommitter(
            str(tmp_path),
            staging,
            fs=fs,
        ).commit_logical(plan)

        result = resolve_output_reference(
            OutputReference(source_step=7, role="data"),
            str(tmp_path),
            fs,
            step_run_id=current,
        )
        no_origin_fallback = resolve_output_reference(
            OutputReference(source_step=99, role="data"),
            str(tmp_path),
            fs,
            step_run_id=current,
        )

        assert result == artifact_ids[:2]
        assert no_origin_fallback == []

    def test_no_executions_returns_empty(self, tmp_path):
        """Test that missing executions table returns empty list."""
        ref = OutputReference(source_step=0, role="data")

        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())
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

        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())
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
        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())
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
        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())
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
        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())

        # Should get all three outputs
        assert len(result) == 3
        assert "a" * 32 in result
        assert "b" * 32 in result
        assert "c" * 32 in result

    def test_no_provenance_rows_returns_empty(self, tmp_path):
        """A committed execution with no edges resolves to an empty list."""
        records_df = _create_executions_df()
        _write_tables(tmp_path, records_df, [])

        ref = OutputReference(source_step=0, role="data")

        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())
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
        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())

        assert len(result) == 1
        assert result[0] == "o" * 32
        assert "i" * 32 not in result
