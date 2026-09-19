"""Tests for curator-specific execution flow.

These tests verify:
- _handle_passthrough_result populates artifact_ids correctly
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from artisan.execution.executors.curator import (
    _handle_passthrough_result,
    run_curator_flow,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.curator.merge import Merge
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.curator_result import PassthroughResult
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment

_MOCK_MODULE = "artisan.execution.executors.curator"
_PARQUET_MODULE = "artisan.execution.recording.parquet_writer"


@pytest.mark.parametrize("failure", [None, "result", "exception"])
def test_curator_records_runtime_worker_identity(
    tmp_path: Path, failure: str | None
) -> None:
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        worker_id=-7,
    )
    unit = ExecutionUnit(
        operation=Merge(), inputs={"source": ["a" * 32]}, step_number=1
    )
    if failure is None:
        result = run_curator_flow(unit, runtime)
    else:
        with patch.object(
            Merge,
            "execute_curator",
            side_effect=ValueError("curator failed")
            if failure == "exception"
            else None,
            return_value=PassthroughResult(success=False, error="curator failed"),
        ):
            result = run_curator_flow(unit, runtime)
    assert result.success is (failure is None)
    row = pl.read_parquet(Path(result.staging_path) / "executions.parquet").row(
        0, named=True
    )
    assert row["source_worker"] == -7
    assert row["execution_run_id"] == result.execution_run_id
    assert row["success"] is (failure is None)


class TestPassthroughResultArtifactIds:
    """Verify _handle_passthrough_result populates artifact_ids from passthrough."""

    @patch(f"{_PARQUET_MODULE}._stage_execution")
    @patch(f"{_PARQUET_MODULE}._create_staging_path", return_value="/tmp/staging")
    @patch(f"{_MOCK_MODULE}.validate_passthrough_result")
    def test_artifact_ids_populated_from_passthrough(
        self,
        _mock_validate,
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
            command_recording=CommandRecording.empty(),
            replay_snapshot=ReplaySnapshot.unavailable("direct_recorder_fixture"),
            replay_of_execution_run_id=None,
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
    @patch(f"{_MOCK_MODULE}.validate_passthrough_result")
    def test_artifact_ids_empty_when_nothing_passes(
        self,
        _mock_validate,
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
            command_recording=CommandRecording.empty(),
            replay_snapshot=ReplaySnapshot.unavailable("direct_recorder_fixture"),
            replay_of_execution_run_id=None,
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
    @patch(f"{_MOCK_MODULE}.validate_passthrough_result")
    def test_artifact_ids_flattened_across_roles(
        self,
        _mock_validate,
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
            command_recording=CommandRecording.empty(),
            replay_snapshot=ReplaySnapshot.unavailable("direct_recorder_fixture"),
            replay_of_execution_run_id=None,
            result=result,
            operation=operation,
            execution_context=ctx,
            inputs={"role_a": ["id_1", "id_2"], "role_b": ["id_3"]},
            timestamp_end=MagicMock(),
        )

        assert staging_result.success is True
        assert set(staging_result.artifact_ids) == {"id_1", "id_2", "id_3"}
        assert len(staging_result.artifact_ids) == 3


@pytest.mark.parametrize("failure", [None, "result", "exception"])
def test_curator_command_evidence_survives_every_result(tmp_path, monkeypatch, failure):
    import sys

    from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
    from artisan.utils.external_tools import run_command

    def execute(self, **kwargs):
        run_command(LocalEnvironmentSpec(), [sys.executable, "-c", "pass"])
        if failure == "exception":
            msg = "after command"
            raise RuntimeError(msg)
        return PassthroughResult(
            success=failure is None,
            error="returned failure" if failure else None,
            passthrough={"merged": ["a" * 32]},
            metadata={"custom": "preserved", "command_recording": "untrusted metadata"},
        )

    monkeypatch.setattr(Merge, "execute_curator", execute)
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"), staging_root=str(tmp_path / "staging")
    )
    result = run_curator_flow(
        ExecutionUnit(operation=Merge(), inputs={"source": ["a" * 32]}, step_number=1),
        runtime,
    )
    assert result.success is (failure is None)
    row = pl.read_parquet(Path(result.staging_path) / "executions.parquet").row(
        0, named=True
    )
    recording = CommandRecording.model_validate_json(row["command_recording"])
    assert recording.status == "complete"
    assert len(recording.commands) == 1
    assert recording.commands[0].outcome == "succeeded"
