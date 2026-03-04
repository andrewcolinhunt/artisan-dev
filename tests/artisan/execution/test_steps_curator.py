"""Tests for curator-specific execution flow.

These tests verify:
- _handle_passthrough_result populates artifact_ids correctly
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from artisan.execution.executors.curator import _handle_passthrough_result
from artisan.schemas.execution.curator_result import PassthroughResult

# =============================================================================
# Constants
# =============================================================================

_MOCK_MODULE = "artisan.execution.executors.curator"
_PARQUET_MODULE = "artisan.execution.staging.parquet_writer"

# =============================================================================
# Tests for _handle_passthrough_result artifact_ids
# =============================================================================


_PARQUET_MODULE = "artisan.execution.staging.parquet_writer"


class TestPassthroughResultArtifactIds:
    """Verify _handle_passthrough_result populates artifact_ids from passthrough."""

    @patch(f"{_PARQUET_MODULE}._stage_execution")
    @patch(f"{_PARQUET_MODULE}._create_staging_path", return_value="/tmp/staging")
    @patch(f"{_MOCK_MODULE}.build_execution_edges", return_value=[])
    @patch(f"{_MOCK_MODULE}.validate_passthrough_result")
    def test_artifact_ids_populated_from_passthrough(
        self,
        _mock_validate,
        _mock_edges,
        _mock_staging_path,
        _mock_stage,
    ):
        """StagingResult.artifact_ids should contain all passthrough IDs."""
        result = PassthroughResult(
            passthrough={"passthrough": ["id_a", "id_b", "id_c"]},
        )
        operation = MagicMock()
        operation.outputs = {}
        ctx = MagicMock()
        ctx.execution_run_id = "run1"
        ctx.staging_root = "/tmp/staging"
        ctx.step_number = 1
        ctx.operation_name = "filter"

        staging_result = _handle_passthrough_result(
            result=result,
            operation=operation,
            execution_context=ctx,
            inputs={"passthrough": ["id_a", "id_b", "id_c"]},
            timestamp_end=MagicMock(),
        )

        assert staging_result.success is True
        assert staging_result.artifact_ids == ["id_a", "id_b", "id_c"]

    @patch(f"{_PARQUET_MODULE}._stage_execution")
    @patch(f"{_PARQUET_MODULE}._create_staging_path", return_value="/tmp/staging")
    @patch(f"{_MOCK_MODULE}.build_execution_edges", return_value=[])
    @patch(f"{_MOCK_MODULE}.validate_passthrough_result")
    def test_artifact_ids_empty_when_nothing_passes(
        self,
        _mock_validate,
        _mock_edges,
        _mock_staging_path,
        _mock_stage,
    ):
        """StagingResult.artifact_ids should be empty when no artifacts pass."""
        result = PassthroughResult(
            passthrough={"passthrough": []},
        )
        operation = MagicMock()
        operation.outputs = {}
        ctx = MagicMock()
        ctx.execution_run_id = "run1"
        ctx.staging_root = "/tmp/staging"
        ctx.step_number = 1
        ctx.operation_name = "filter"

        staging_result = _handle_passthrough_result(
            result=result,
            operation=operation,
            execution_context=ctx,
            inputs={"passthrough": ["id_a", "id_b"]},
            timestamp_end=MagicMock(),
        )

        assert staging_result.success is True
        assert staging_result.artifact_ids == []

    @patch(f"{_PARQUET_MODULE}._stage_execution")
    @patch(f"{_PARQUET_MODULE}._create_staging_path", return_value="/tmp/staging")
    @patch(f"{_MOCK_MODULE}.build_execution_edges", return_value=[])
    @patch(f"{_MOCK_MODULE}.validate_passthrough_result")
    def test_artifact_ids_flattened_across_roles(
        self,
        _mock_validate,
        _mock_edges,
        _mock_staging_path,
        _mock_stage,
    ):
        """StagingResult.artifact_ids should flatten all passthrough roles."""
        result = PassthroughResult(
            passthrough={"role_a": ["id_1", "id_2"], "role_b": ["id_3"]},
        )
        operation = MagicMock()
        operation.outputs = {}
        ctx = MagicMock()
        ctx.execution_run_id = "run1"
        ctx.staging_root = "/tmp/staging"
        ctx.step_number = 1
        ctx.operation_name = "merge"

        staging_result = _handle_passthrough_result(
            result=result,
            operation=operation,
            execution_context=ctx,
            inputs={"role_a": ["id_1", "id_2"], "role_b": ["id_3"]},
            timestamp_end=MagicMock(),
        )

        assert staging_result.success is True
        assert set(staging_result.artifact_ids) == {"id_1", "id_2", "id_3"}
        assert len(staging_result.artifact_ids) == 3
