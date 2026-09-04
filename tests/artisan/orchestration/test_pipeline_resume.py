"""Tests for PipelineManager.resume()."""

from __future__ import annotations

import json
from enum import StrEnum, auto
from pathlib import Path
from typing import ClassVar
from unittest.mock import patch

import polars as pl
import pytest

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class ExternalRunner(LocalRunner):
    """Concrete stand-in for a runner supplied by an external provider."""

    name = "external_test"


class LegacySlurmRunner(LocalRunner):
    """Stand-in for the external provider required by historical SLURM rows."""

    name = "slurm"


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

    def execute_function(self, inputs, output_dir):
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
    from artisan.schemas.enums import FailurePolicy

    return build_step_result(
        operation=kwargs["operation_class"],
        step_number=kwargs["step_number"],
        succeeded_count=5,
        failed_count=0,
        failure_policy=kwargs["ov"].failure_policy or FailurePolicy.CONTINUE,
    )


def _write_legacy_completed_step(
    delta_root: Path,
    *,
    compute_backend: str = "local",
    step_number: int = 0,
) -> str:
    """Write a pre-runner-metadata step row for compatibility coverage."""
    pipeline_run_id = "legacy_20260904_120000_abcdef12"
    tracker = StepTracker(str(delta_root), pipeline_run_id)
    record = StepStartRecord(
        step_run_id=f"legacy_step_run_{step_number}",
        step_spec_id=f"legacy_step_spec_{step_number}",
        step_number=step_number,
        step_name="Ingest",
        operation_class=f"{IngestMockOp.__module__}.{IngestMockOp.__qualname__}",
        params_json="{}",
        input_refs_json="null",
        compute_backend=compute_backend,
        compute_options_json="{}",
        output_roles_json='["file"]',
        output_types_json='{"file": "data"}',
    )
    result = StepResult(
        step_name="Ingest",
        step_number=step_number,
        success=True,
        total_count=1,
        succeeded_count=1,
        failed_count=0,
        output_roles=frozenset({"file"}),
        output_types={"file": ArtifactTypes.DATA},
    )
    tracker.record_step_start(record)
    tracker.record_step_completed(record, result)
    return pipeline_run_id


def _run_external_default_pipeline(
    delta_root: Path,
    staging_root: Path,
    *,
    local_override: bool = False,
) -> tuple[PipelineManager, ExternalRunner]:
    """Run steps whose effective runners can differ from an external default."""
    runner = ExternalRunner()
    pipeline = PipelineManager.create(
        name="test",
        delta_root=str(delta_root),
        staging_root=str(staging_root),
        default_step_runner=runner,
    )
    pipeline.run(IngestMockOp, inputs=None)
    if local_override:
        pipeline.run(
            MockOp,
            inputs={"data": pipeline[0].output("file")},
            step_runner="local",
        )
    return pipeline, runner


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

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_resume_accepts_matching_persisted_external_runner_instance(
        self, mock_exec, tmp_path
    ):
        """An imported provider instance restores its persisted default."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        p1, runner = _run_external_default_pipeline(delta, staging)

        resumed = PipelineManager.resume(
            delta_root=str(delta),
            staging_root=str(staging),
            pipeline_run_id=p1.config.pipeline_run_id,
            default_step_runner=runner,
        )

        assert resumed.config.default_step_runner == "external_test"
        assert resumed._default_step_runner is runner

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_resume_does_not_infer_default_from_curator_or_override(
        self, mock_exec, tmp_path
    ):
        """Effective local step runners cannot hide a persisted external default."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        p1, _ = _run_external_default_pipeline(
            delta,
            staging,
            local_override=True,
        )

        rows = pl.read_delta(delta / "orchestration" / "steps").filter(
            pl.col("status") == "completed"
        )
        assert set(rows["compute_backend"]) == {"local"}
        decoded_options = [
            json.loads(options) for options in rows["compute_options_json"]
        ]
        assert {
            options["pipeline_default_step_runner"] for options in decoded_options
        } == {"external_test"}
        assert all(
            "pipeline_default_local_runner" not in options
            for options in decoded_options
        )

        with pytest.raises(ValueError, match="initialized provider runner"):
            PipelineManager.resume(
                delta_root=str(delta),
                staging_root=str(staging),
                pipeline_run_id=p1.config.pipeline_run_id,
            )

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_resume_rejects_runner_mismatch_with_persisted_default(
        self, mock_exec, tmp_path
    ):
        """An explicit runner cannot replace the persisted pipeline default."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        p1, _ = _run_external_default_pipeline(delta, staging)

        with pytest.raises(ValueError, match="does not match persisted"):
            PipelineManager.resume(
                delta_root=str(delta),
                staging_root=str(staging),
                pipeline_run_id=p1.config.pipeline_run_id,
                default_step_runner=LocalRunner(),
            )

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_resume_reconstructs_configured_local_runner(self, mock_exec, tmp_path):
        """A built-in local pool size survives an omitted resume argument."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        pipeline = PipelineManager.create(
            name="test",
            delta_root=str(delta),
            staging_root=str(staging),
            default_step_runner=LocalRunner(default_max_workers=9),
        )
        pipeline.run(IngestMockOp, inputs=None)

        completed = pl.read_delta(delta / "orchestration" / "steps").filter(
            pl.col("status") == "completed"
        )
        options = json.loads(completed.item(0, "compute_options_json"))
        assert options["pipeline_default_step_runner"] == "local"
        assert options["pipeline_default_local_runner"] == {"default_max_workers": 9}
        assert completed.item(0, "compute_backend") == "local"

        resumed = PipelineManager.resume(
            delta_root=str(delta),
            staging_root=str(staging),
            pipeline_run_id=pipeline.config.pipeline_run_id,
        )

        assert type(resumed._default_step_runner) is LocalRunner
        assert resumed._default_step_runner.default_max_workers == 9

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_resume_retains_matching_configured_local_runner(self, mock_exec, tmp_path):
        """An explicit compatible LocalRunner remains the runtime instance."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        pipeline = PipelineManager.create(
            name="test",
            delta_root=str(delta),
            staging_root=str(staging),
            default_step_runner=LocalRunner(default_max_workers=9),
        )
        pipeline.run(IngestMockOp, inputs=None)
        supplied = LocalRunner(default_max_workers=9)

        resumed = PipelineManager.resume(
            delta_root=str(delta),
            staging_root=str(staging),
            pipeline_run_id=pipeline.config.pipeline_run_id,
            default_step_runner=supplied,
        )

        assert resumed._default_step_runner is supplied

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_resume_rejects_incompatible_configured_local_runner(
        self, mock_exec, tmp_path
    ):
        """An explicit LocalRunner must match the persisted pool size."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        pipeline = PipelineManager.create(
            name="test",
            delta_root=str(delta),
            staging_root=str(staging),
            default_step_runner=LocalRunner(default_max_workers=9),
        )
        pipeline.run(IngestMockOp, inputs=None)

        with pytest.raises(ValueError, match="does not match persisted"):
            PipelineManager.resume(
                delta_root=str(delta),
                staging_root=str(staging),
                pipeline_run_id=pipeline.config.pipeline_run_id,
                default_step_runner=LocalRunner(default_max_workers=4),
            )

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_mock_execute_step,
    )
    def test_resume_rejects_external_runner_name_without_instance(
        self, mock_exec, tmp_path
    ):
        """A historical provider name cannot be reconstructed by core alone."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        p1, _ = _run_external_default_pipeline(delta, staging)

        with pytest.raises(ValueError, match="initialized provider runner"):
            PipelineManager.resume(
                delta_root=str(delta),
                staging_root=str(staging),
                pipeline_run_id=p1.config.pipeline_run_id,
                default_step_runner="external_test",
            )

    def test_resume_legacy_record_requires_explicit_default(self, tmp_path):
        """All-local effective rows do not prove the historical default was local."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        run_id = _write_legacy_completed_step(delta)

        with pytest.raises(ValueError, match="historical pipeline default"):
            PipelineManager.resume(
                delta_root=str(delta),
                staging_root=str(staging),
                pipeline_run_id=run_id,
            )

    def test_resume_legacy_record_accepts_explicit_local(self, tmp_path):
        """The caller may explicitly restore local as a legacy default."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        run_id = _write_legacy_completed_step(delta)

        resumed = PipelineManager.resume(
            delta_root=str(delta),
            staging_root=str(staging),
            pipeline_run_id=run_id,
            default_step_runner="local",
        )

        assert resumed.config.default_step_runner == "local"
        assert isinstance(resumed._default_step_runner, LocalRunner)

    def test_resume_legacy_record_accepts_explicit_external_runner(self, tmp_path):
        """Legacy rows allow callers to restore an external default explicitly."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        run_id = _write_legacy_completed_step(delta)
        runner = ExternalRunner()

        resumed = PipelineManager.resume(
            delta_root=str(delta),
            staging_root=str(staging),
            pipeline_run_id=run_id,
            default_step_runner=runner,
        )

        assert resumed.config.default_step_runner == "external_test"
        assert resumed._default_step_runner is runner

    @pytest.mark.parametrize("backend", ["slurm", "slurm_intra"])
    def test_resume_legacy_slurm_record_requires_provider_instance(
        self,
        tmp_path,
        backend,
    ):
        """Historical SLURM work must never silently resume on LocalRunner."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        run_id = _write_legacy_completed_step(delta, compute_backend=backend)

        with pytest.raises(ValueError, match="historical pipeline default"):
            PipelineManager.resume(
                delta_root=str(delta),
                staging_root=str(staging),
                pipeline_run_id=run_id,
            )

    def test_resume_legacy_slurm_record_accepts_matching_provider(self, tmp_path):
        """A matching provider instance safely restores a historical SLURM run."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        run_id = _write_legacy_completed_step(delta, compute_backend="slurm")
        runner = LegacySlurmRunner()

        resumed = PipelineManager.resume(
            delta_root=str(delta),
            staging_root=str(staging),
            pipeline_run_id=run_id,
            default_step_runner=runner,
        )

        assert resumed.config.default_step_runner == "slurm"
        assert resumed._default_step_runner is runner

    def test_resume_legacy_rows_accept_explicit_unobserved_provider(self, tmp_path):
        """Effective legacy runners do not constrain an explicitly stated default."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        run_id = _write_legacy_completed_step(delta, compute_backend="slurm")
        runner = ExternalRunner()

        resumed = PipelineManager.resume(
            delta_root=str(delta),
            staging_root=str(staging),
            pipeline_run_id=run_id,
            default_step_runner=runner,
        )

        assert resumed.config.default_step_runner == "external_test"
        assert resumed._default_step_runner is runner

    def test_resume_mixed_legacy_runners_requires_explicit_default(self, tmp_path):
        """Mixed effective runners do not reveal the historical default."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        run_id = _write_legacy_completed_step(delta)
        _write_legacy_completed_step(
            delta,
            compute_backend="slurm",
            step_number=1,
        )

        with pytest.raises(ValueError, match="historical pipeline default"):
            PipelineManager.resume(
                delta_root=str(delta),
                staging_root=str(staging),
                pipeline_run_id=run_id,
            )

    def test_resume_mixed_legacy_runners_accepts_explicit_local(self, tmp_path):
        """The caller may identify local as the ambiguous historical default."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        run_id = _write_legacy_completed_step(delta)
        _write_legacy_completed_step(
            delta,
            compute_backend="slurm",
            step_number=1,
        )

        resumed = PipelineManager.resume(
            delta_root=str(delta),
            staging_root=str(staging),
            pipeline_run_id=run_id,
            default_step_runner="local",
        )

        assert resumed.config.default_step_runner == "local"
        assert isinstance(resumed._default_step_runner, LocalRunner)

    def test_resume_mixed_legacy_runners_accepts_explicit_provider(self, tmp_path):
        """The caller may identify SLURM as the ambiguous historical default."""
        delta = tmp_path / "delta"
        staging = tmp_path / "staging"
        run_id = _write_legacy_completed_step(delta)
        _write_legacy_completed_step(
            delta,
            compute_backend="slurm",
            step_number=1,
        )
        runner = LegacySlurmRunner()

        resumed = PipelineManager.resume(
            delta_root=str(delta),
            staging_root=str(staging),
            pipeline_run_id=run_id,
            default_step_runner=runner,
        )

        assert resumed.config.default_step_runner == "slurm"
        assert resumed._default_step_runner is runner


# NOTE: TestListRuns moved to test_run_history.py (PR 4 — list_runs is now
# a module-level function, not a classmethod).
