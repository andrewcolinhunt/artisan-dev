"""Public manager behavior over real admissions and current controller ownership."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.composition_operations import PredictMeasure, Transform, structure
from fixtures.retained_pipeline import NativePipelineRunner

from artisan.operations.curator.ingest_csv import IngestCSV
from artisan.orchestration import PipelineManager
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.specs.input_models import PreprocessInput


class _FailingTransform(Transform):
    name = "public_failing_transform"

    def preprocess(self, inputs: PreprocessInput) -> dict[str, object]:
        message = "authored preprocessing failure"
        raise ValueError(message)


def _pipeline(tmp_path: Path) -> tuple[PipelineManager, NativePipelineRunner]:
    pipeline = PipelineManager.create(
        "public-manager",
        str(tmp_path / "manager-store"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    runner = NativePipelineRunner(pipeline.committer)
    pipeline.runner = runner
    return pipeline, runner


def test_explicit_default_instance_and_composite_share_public_submission(
    tmp_path: Path,
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    with pipeline:
        first = pipeline.run(PredictMeasure, inputs={"structure": [structure(0)]})
        second = pipeline.submit(
            operation=Transform, inputs={"structure": first.output("structure")}
        ).result()
        assert first.status == second.status == StepStatus.SUCCEEDED
        assert pipeline.runner is runner
        assert runner.resolutions == 2
        assert pipeline.current_step == 2
        assert [item.step_run_id for item in pipeline] == [
            first.step_run_id,
            second.step_run_id,
        ]
        assert len(pipeline.executions) == 2
    assert pipeline.ownership_state() == OwnershipState.CLOSED


def test_failed_science_is_recorded_and_independent_work_continues(
    tmp_path: Path,
) -> None:
    pipeline, _ = _pipeline(tmp_path)
    with pipeline:
        failed = pipeline.run(_FailingTransform, inputs={"structure": [structure(0)]})
        independent = pipeline.run(Transform, inputs={"structure": [structure(1)]})
        assert failed.status == StepStatus.FAILED
        assert failed.failed_count == failed.total_count == 1
        assert "authored preprocessing failure" in failed.error
        assert read_current_step_outputs(pipeline.committer, failed.step_run_id) == ()
        assert independent.status == StepStatus.SUCCEEDED
        state = pipeline.tracker.current_state(failed.step_run_id)
        assert state.status == StepStatus.FAILED
        assert state.step_name == _FailingTransform.name
        assert pipeline[0] == failed
        assert pipeline[1] == independent
        assert not pipeline
    summary = pipeline.finalize()
    assert summary is pipeline.finalize()
    assert summary["total_steps"] == 2
    assert not summary["overall_success"]


def test_failed_predecessor_resolves_without_reusing_earlier_outputs(
    tmp_path: Path,
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    with pipeline:
        failed = pipeline.submit(
            _FailingTransform, inputs={"structure": [structure(0)]}
        )
        dependent = pipeline.submit(
            Transform, inputs={"structure": failed.output("structure")}
        )
        assert failed.result().status == StepStatus.FAILED
        skipped = dependent.result()
        assert skipped.status == StepStatus.SKIPPED
        assert runner.resolutions == 1
        assert (
            pipeline.tracker.current_state(skipped.step_run_id).status
            == StepStatus.SKIPPED
        )
        assert read_current_step_outputs(pipeline.committer, skipped.step_run_id) == ()


@pytest.mark.parametrize("bad_input", ["missing", "directory", "mixed"])
def test_invalid_raw_input_set_publishes_no_partial_registration(
    tmp_path: Path, bad_input: str
) -> None:
    valid = tmp_path / "valid.csv"
    valid.write_text("value\n1\n")
    directory = tmp_path / "directory"
    directory.mkdir()
    missing = tmp_path / "missing.csv"
    paths = {
        "missing": [str(missing)],
        "directory": [str(directory)],
        "mixed": [str(valid), str(missing)],
    }[bad_input]
    pipeline, runner = _pipeline(tmp_path)
    with pipeline:
        failed = pipeline.run(IngestCSV, inputs=paths)
        assert failed.status == StepStatus.FAILED
        assert runner.curator_resolutions == 0
        assert runner.curator_launches == 0
        snapshot = pipeline.committer._snapshot(TablePath.ARTIFACT_INDEX.value)
        assert all(frame.is_empty() for frame in snapshot.iter_frames())
        assert read_current_step_outputs(pipeline.committer, failed.step_run_id) == ()


def test_valid_raw_inputs_are_registered_and_curator_cache_keeps_source(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\n7\n")
    pipeline, runner = _pipeline(tmp_path)
    with pipeline:
        first = pipeline.run(IngestCSV, inputs=[str(source)])
        cached = pipeline.run(IngestCSV, inputs=[str(source)])
        assert first.status == cached.status == StepStatus.SUCCEEDED
        assert cached.disposition == StepDisposition.CACHE_HIT
        assert runner.curator_launches == 1
        assert read_current_step_outputs(
            pipeline.committer, cached.step_run_id
        ) == read_current_step_outputs(pipeline.committer, first.step_run_id)


def test_invalid_creator_role_rejects_before_raw_input_registration(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    source.write_text("value\n7\n")
    pipeline, runner = _pipeline(tmp_path)
    with pipeline:
        with pytest.raises(ValueError, match="missing required or undeclared roles"):
            pipeline.run(Transform, inputs=[str(source)])
        assert pipeline.current_step == 0
        assert not pipeline.tracker.load_current_states(pipeline.config.pipeline_run_id)
        assert runner.resolutions == 0
        snapshot = pipeline.committer._snapshot(TablePath.ARTIFACT_INDEX.value)
        assert all(frame.is_empty() for frame in snapshot.iter_frames())


@pytest.mark.parametrize("finalize", [False, True])
def test_closed_manager_rejects_new_work_without_new_pending_attempt(
    tmp_path: Path, finalize: bool
) -> None:
    pipeline, runner = _pipeline(tmp_path)
    if finalize:
        summary = pipeline.finalize()
        assert not summary["overall_success"]
        assert summary["total_steps"] == 0
    else:
        pipeline.close()
    with pytest.raises(RuntimeError, match="closed"):
        pipeline.submit(Transform, inputs={"structure": [structure(0)]})
    assert pipeline.current_step == 0
    assert not pipeline.executions
    assert not pipeline.tracker.load_current_states(pipeline.config.pipeline_run_id)
    assert runner.resolutions == 0
