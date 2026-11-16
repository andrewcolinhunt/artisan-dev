"""Resume selects verified attempts and requires explicit configured runners."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from fixtures.composition_operations import Transform, structure
from fixtures.curator_cache import DynamicCurator
from fixtures.retained_pipeline import NativePipelineRunner

from artisan.orchestration import pipeline_manager as module
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.engine.retained_step_results import RetainedStepResults
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.enums import CachePolicy
from artisan.schemas.orchestration.step_lifecycle import StepStatus


class ExternalRunner(LocalRunner):
    """Provider identity; tests override actual work with a native fixture."""

    name = "external_test"


def _create(tmp_path: Path, **overrides: Any) -> PipelineManager:
    return PipelineManager.create(
        name="resume_test",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
        **overrides,
    )


def _resume(pipeline: PipelineManager, **overrides: Any) -> PipelineManager:
    return PipelineManager.resume(
        **{
            "delta_root": pipeline.config.delta_root,
            "staging_root": pipeline.config.staging_root,
            "files_root": pipeline.config.files_root,
            "working_root": pipeline.config.working_root,
            "pipeline_run_id": pipeline.config.pipeline_run_id,
            **overrides,
        }
    )


def _steps(pipeline: PipelineManager) -> pl.DataFrame:
    return pl.read_delta(Path(pipeline.config.delta_root) / "orchestration/steps")


@pytest.mark.parametrize("default", [None, *CachePolicy])
def test_resume_policy_default_applies_only_to_new_submissions(
    tmp_path: Path, default: CachePolicy | None
) -> None:
    with _create(tmp_path) as original:
        runner = NativePipelineRunner(original.committer)
        first = original.run(
            Transform,
            inputs={"structure": [structure(0)]},
            step_runner=runner,
            cache_policy=CachePolicy.STEP_COMPLETED,
        )
        second = original.run(
            Transform,
            inputs={"structure": first.output("structure")},
            step_runner=runner,
            cache_policy=CachePolicy.ALL_SUCCEEDED,
        )
        assert first.status == second.status == StepStatus.SUCCEEDED
    before = _steps(original)
    options = {} if default is None else {"cache_policy": default}
    with _resume(original, **options) as resumed:
        assert [step.step_run_id for step in resumed] == [
            first.step_run_id,
            second.step_run_id,
        ]
        inherited = resumed.submit(Transform, inputs={"structure": []}).result()
        explicit = resumed.run(
            Transform, inputs={"structure": []}, cache_policy=CachePolicy.STEP_COMPLETED
        )
    after = _steps(resumed)
    assert after.filter(
        pl.col("step_run_id").is_in([first.step_run_id, second.step_run_id])
    ).equals(before)
    for result, expected in (
        (inherited, default or CachePolicy.ALL_SUCCEEDED),
        (explicit, CachePolicy.STEP_COMPLETED),
    ):
        rows = after.filter(pl.col("step_run_id") == result.step_run_id)
        assert {
            json.loads(value)["cache_policy"] for value in rows["compute_options_json"]
        } == {expected.value}


def test_resume_reconstructs_accepted_and_skipped_state(tmp_path: Path) -> None:
    with _create(tmp_path) as original:
        accepted = original.run(
            Transform,
            inputs={"structure": [structure(0)]},
            step_runner=NativePipelineRunner(original.committer),
        )
        skipped = original.run(Transform, inputs={"structure": []})
    with _resume(original) as resumed:
        assert resumed.current_step == 2
        assert [step.step_run_id for step in resumed] == [
            accepted.step_run_id,
            skipped.step_run_id,
        ]
        states = resumed.tracker.load_current_states(resumed.config.pipeline_run_id)
        assert states[0].step_spec_id
        assert states[1].step_spec_id is None
        assert resumed[1].status == StepStatus.SKIPPED


def test_resume_output_chaining_preserves_exact_accepted_sources(
    tmp_path: Path,
) -> None:
    with _create(tmp_path) as original:
        source = original.run(
            Transform,
            inputs={"structure": [structure(0)]},
            step_runner=NativePipelineRunner(original.committer),
        )
        outputs = read_current_step_outputs(original.committer, source.step_run_id)
    with _resume(original) as resumed:
        ref = resumed[0].output("structure")
        assert ref.source.step_run_id == source.step_run_id
        assert ref.source.store == resumed.committer.store_identity
        assert ref.role == "structure"
        assert (
            read_current_step_outputs(resumed.committer, source.step_run_id) == outputs
        )
        child = resumed.run(
            Transform,
            inputs={"structure": ref},
            step_runner=NativePipelineRunner(resumed.committer),
        )
        assert child.status == StepStatus.SUCCEEDED
        assert child.step_number == 1


def test_resume_most_recent_run(tmp_path: Path) -> None:
    with _create(tmp_path) as first:
        first.run(Transform, inputs={"structure": []})
    with _create(tmp_path) as latest:
        latest.run(Transform, inputs={"structure": []})
        latest.run(Transform, inputs={"structure": []})
    with _resume(first, pipeline_run_id=None) as resumed:
        assert resumed.config.pipeline_run_id == latest.config.pipeline_run_id
        assert resumed.current_step == 2


def test_resume_no_runs_raises_without_initializing(tmp_path: Path) -> None:
    with _create(tmp_path) as original:
        pass
    before = _steps(original)
    with pytest.raises(ValueError, match="No step attempts found"):
        _resume(original)
    assert _steps(original).equals(before)


def _external_pipeline(
    tmp_path: Path, *, local_override: bool = False
) -> tuple[PipelineManager, ExternalRunner]:
    runner = ExternalRunner()
    with _create(tmp_path, default_step_runner=runner) as pipeline:
        if local_override:
            selected = pipeline.run(
                DynamicCurator, step_runner=NativePipelineRunner(pipeline.committer)
            )
            assert selected.status == StepStatus.SUCCEEDED
        skipped = pipeline.run(
            Transform, inputs={"structure": []}, step_runner=LocalRunner()
        )
        assert skipped.status == StepStatus.SKIPPED
    return pipeline, runner


def test_resume_accepts_matching_persisted_external_runner_instance(
    tmp_path: Path,
) -> None:
    pipeline, runner = _external_pipeline(tmp_path)
    with _resume(pipeline, default_step_runner=runner) as resumed:
        assert resumed.config.default_step_runner == "external_test"
        assert resumed.runner is runner


def test_resume_does_not_infer_default_from_curator_or_override(tmp_path: Path) -> None:
    pipeline, _ = _external_pipeline(tmp_path, local_override=True)
    rows = _steps(pipeline)
    assert set(rows["compute_backend"]) == {"native-test", "local"}
    options = [json.loads(value) for value in rows["compute_options_json"]]
    assert {item["pipeline_default_step_runner"] for item in options} == {
        "external_test"
    }
    assert all(item["pipeline_default_runner_requires_instance"] for item in options)
    with pytest.raises(ValueError, match="explicit matching"):
        _resume(pipeline)


def test_resume_rejects_runner_name_mismatch_with_persisted_default(
    tmp_path: Path,
) -> None:
    pipeline, _ = _external_pipeline(tmp_path)
    with pytest.raises(ValueError, match="runner name differs"):
        _resume(pipeline, default_step_runner=LocalRunner())


def _configured_pipeline(tmp_path: Path) -> tuple[PipelineManager, LocalRunner]:
    runner = replace(LocalRunner(), cache_root=tmp_path / "configured-cache")
    with _create(tmp_path, default_step_runner=runner) as pipeline:
        pipeline.run(Transform, inputs={"structure": []})
    return pipeline, runner


def test_resume_requires_explicit_configured_local_runner(tmp_path: Path) -> None:
    pipeline, runner = _configured_pipeline(tmp_path)
    options = json.loads(pipeline.executions[0].record.compute_options_json)
    assert options["pipeline_default_step_runner"] == "local"
    assert options["pipeline_default_runner_requires_instance"] is True
    assert str(runner.cache_root) not in json.dumps(options)
    assert "pipeline_default_local_runner" not in options
    with pytest.raises(ValueError, match="explicit matching"):
        _resume(pipeline)


def test_resume_retains_supplied_configured_local_runner(tmp_path: Path) -> None:
    pipeline, runner = _configured_pipeline(tmp_path)
    with _resume(pipeline, default_step_runner=runner) as resumed:
        assert resumed.runner is runner


def test_resume_uses_explicit_runtime_settings_without_reconstructing_them(
    tmp_path: Path,
) -> None:
    pipeline, _ = _configured_pipeline(tmp_path)
    supplied = replace(LocalRunner(), cache_root=tmp_path / "new-explicit-cache")
    with _resume(pipeline, default_step_runner=supplied) as resumed:
        assert resumed.runner is supplied
        result = resumed.run(Transform, inputs={"structure": []})
        options = json.loads(
            resumed.tracker.current_state(result.step_run_id).compute_options_json
        )
        assert options["pipeline_default_runner_requires_instance"] is True
    with pytest.raises(ValueError, match="explicit matching"):
        _resume(resumed)


def test_resume_rejects_external_runner_name_without_instance(tmp_path: Path) -> None:
    pipeline, _ = _external_pipeline(tmp_path)
    with pytest.raises(ValueError, match="explicit matching"):
        _resume(pipeline, default_step_runner="external_test")


def test_resume_recovers_selected_terminal_before_exposing_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ControllerInterrupted(BaseException):
        pass

    def interrupt(_self: RetainedStepResults, checkpoint: str) -> None:
        if checkpoint == "plan":
            raise ControllerInterrupted()

    pipeline = _create(tmp_path)
    runner = NativePipelineRunner(pipeline.committer)
    with monkeypatch.context() as patch:
        patch.setattr(RetainedStepResults, "_checkpoint", interrupt)
        with pytest.raises(ControllerInterrupted):
            pipeline.run(
                Transform, inputs={"structure": [structure(0)]}, step_runner=runner
            )
    pipeline.close()
    source = pipeline.executions[0].record.step_run_id
    assert pipeline.tracker.current_state(source).status == StepStatus.RUNNING
    recovery = module.recover_pipeline_results
    events: list[str] = []

    def recover(*args: Any, **kwargs: Any) -> Any:
        events.append("recover")
        return recovery(*args, **kwargs)

    monkeypatch.setattr(module, "recover_pipeline_results", recover)
    with _resume(pipeline) as resumed:
        assert events == ["recover"]
        assert resumed[0].status == StepStatus.SUCCEEDED
        assert resumed[0].step_run_id == source
        assert read_current_step_outputs(resumed.committer, source)
    assert runner.resolutions == 1


def test_resume_appends_after_failure_and_preserves_run_failure(tmp_path: Path) -> None:
    with _create(tmp_path) as pipeline:
        failed = pipeline.run(
            DynamicCurator,
            params={"fail": True},
            step_runner=NativePipelineRunner(pipeline.committer),
        )
        assert failed.status == StepStatus.FAILED
    original = _steps(pipeline)
    with _resume(pipeline) as resumed:
        assert resumed.current_step == 1
        succeeded = resumed.run(
            Transform,
            inputs={"structure": [structure(0)]},
            step_runner=NativePipelineRunner(resumed.committer),
        )
        assert succeeded.step_number == 1
        assert succeeded.status == StepStatus.SUCCEEDED
        assert resumed[0] == failed
    assert resumed.finalize()["overall_success"] is False
    assert (
        _steps(resumed)
        .filter(pl.col("step_run_id") == failed.step_run_id)
        .equals(original)
    )
