"""Tests for PipelineManager.resume() and list_runs()."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import ClassVar
from unittest.mock import patch

import pytest

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class MockOp(OperationDefinition):
    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        data = auto()

    name: ClassVar[str] = "MockOp"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.DATA),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.data: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    def preprocess(self, inputs):
        return {}

    def execute(self, inputs, output_dir):
        pass


class IngestMockOp(OperationDefinition):
    class OutputRole(StrEnum):
        file = auto()

    name: ClassVar[str] = "Ingest"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.file: OutputSpec(artifact_type=ArtifactTypes.DATA),
    }

    def execute_curator(self, execute_input):
        from artisan.schemas.execution.curator_result import ArtifactResult

        return ArtifactResult(success=True)


def _mock_execute_step(**kwargs):
    from artisan.orchestration.engine.step_executor import build_step_result

    return build_step_result(
        operation=kwargs["operation_class"],
        step_number=kwargs["step_number"],
        succeeded_count=5,
        failed_count=0,
        failure_policy=kwargs["failure_policy"],
    )


class TestResume:
    """Tests for PipelineManager.resume()."""

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_resume_reconstructs_state(self, mock_exec, tmp_path):
        """step_results, current_step, step_spec_ids are restored."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"

        # Run 2 steps
        p1 = PipelineManager.create(
            name="test", delta_root=str(delta), staging_root=str(staging)
        )
        run_id = p1.config.pipeline_run_id
        p1.run(IngestMockOp, inputs=None)
        p1.run(MockOp, inputs={"data": p1[0].output("file")})

        # Resume
        p2 = PipelineManager.resume(
            delta_root=str(delta),
            staging_root=str(staging),
            pipeline_run_id=run_id,
        )
        assert p2.current_step == 2
        assert len(p2._step_results) == 2
        assert 0 in p2._step_spec_ids
        assert 1 in p2._step_spec_ids

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_resume_output_chaining(self, mock_exec, tmp_path):
        """pipeline[N].output(role) works after resume."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"

        p1 = PipelineManager.create(
            name="test", delta_root=str(delta), staging_root=str(staging)
        )
        run_id = p1.config.pipeline_run_id
        p1.run(IngestMockOp, inputs=None)

        p2 = PipelineManager.resume(
            delta_root=str(delta),
            staging_root=str(staging),
            pipeline_run_id=run_id,
        )
        ref = p2[0].output("file")
        assert ref.source_step == 0
        assert ref.role == "file"

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_resume_most_recent_run(self, mock_exec, tmp_path):
        """resume() with no run_id picks latest completed run."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"

        # Run 1 — Ingest only
        p1 = PipelineManager.create(
            name="test", delta_root=str(delta), staging_root=str(staging)
        )
        p1.run(IngestMockOp, inputs=None)
        run1_id = p1.config.pipeline_run_id

        # Run 2 — Ingest + MockOp (different steps, so not all cache hits)
        p2 = PipelineManager.create(
            name="test", delta_root=str(delta), staging_root=str(staging)
        )
        p2.run(IngestMockOp, inputs=None)  # cache hit from run 1
        p2.run(MockOp, inputs={"data": p2[0].output("file")})
        run2_id = p2.config.pipeline_run_id

        # Resume without specifying run_id — should pick run 2 (most recent completed)
        p3 = PipelineManager.resume(delta_root=str(delta), staging_root=str(staging))
        assert p3.config.pipeline_run_id == run2_id
        assert p3.current_step == 2

    def test_resume_no_runs_raises(self, tmp_path):
        """ValueError on empty table."""
        with pytest.raises(ValueError, match="No completed steps found"):
            PipelineManager.resume(
                delta_root=str(tmp_path / "delta"),
                staging_root=str(tmp_path / "staging"),
            )


class TestListRuns:
    """Tests for PipelineManager.list_runs()."""

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_list_runs(self, mock_exec, tmp_path):
        """Returns correct DataFrame."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"

        p1 = PipelineManager.create(
            name="test", delta_root=str(delta), staging_root=str(staging)
        )
        p1.run(IngestMockOp, inputs=None)

        runs = PipelineManager.list_runs(str(delta))
        assert len(runs) == 1
        assert "pipeline_run_id" in runs.columns

    def test_list_runs_empty(self, tmp_path):
        """Empty table returns empty DataFrame."""
        runs = PipelineManager.list_runs(str(tmp_path / "delta"))
        assert len(runs) == 0
