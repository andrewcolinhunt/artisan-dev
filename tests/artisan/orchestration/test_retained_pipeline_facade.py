"""Pipeline convenience methods preserve durable selection and worker ownership."""

from __future__ import annotations

import gc
import logging
import weakref
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fixtures.composition_operations import Transform, coordinate, structure
from fixtures.curator_cache import DynamicCurator
from fixtures.retained_pipeline import NativePipelineRunner

from artisan.errors import PersistenceIntegrityError
from artisan.orchestration import pipeline_manager as module
from artisan.orchestration.engine.retained_executor import RetainedStepExecution
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.specs.input_models import PreprocessInput
from artisan.utils.command_supervisor import CommandOwnershipError


def _pipeline(tmp_path: Path, name: str = "example") -> PipelineManager:
    pipeline = PipelineManager.create(
        name,
        str(tmp_path / "facade-store"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    pipeline.runner = NativePipelineRunner(pipeline.committer)
    return pipeline


def test_sequence_named_outputs_and_resume_use_logical_step_positions(
    tmp_path: Path,
) -> None:
    pipeline = _pipeline(tmp_path)
    with pipeline:
        first = pipeline.run(
            Transform, inputs={"structure": [structure(0)]}, name="repeat"
        )
        pipeline._next_step = 5
        second = pipeline.run(
            Transform, inputs={"structure": [structure(0)]}, name="repeat"
        )
        dynamic = pipeline.run(DynamicCurator, name="selected")
        assert second.disposition == StepDisposition.CACHE_HIT
        assert pipeline.current_step == 7
        assert pipeline[0] == first
        assert pipeline[1] == second
        assert pipeline[-1] == dynamic
        assert pipeline[1:] == [second, dynamic]
        assert [item.step_number for item in pipeline] == [0, 5, 6]
        assert "repeat" in pipeline
        assert "missing" not in pipeline
        assert bool(pipeline)
        assert "3 steps, all succeeded" in str(pipeline)
        assert "example" in repr(pipeline)
        assert (
            pipeline.output("repeat", "structure").source.step_run_id
            == second.step_run_id
        )
        assert (
            pipeline.output("repeat", "structure", step_number=0).source.step_run_id
            == first.step_run_id
        )
        assert pipeline.output("selected", "found") == dynamic.output("found")
        for name, role, number in (
            ("missing", "structure", None),
            ("repeat", "missing", None),
            ("repeat", "structure", 6),
        ):
            with pytest.raises(ValueError):
                pipeline.output(name, role, step_number=number)
    summary = pipeline.finalize()
    assert summary is pipeline.finalize()
    assert summary["total_steps"] == 3
    assert summary["overall_success"]
    assert set(summary) == {"pipeline_name", "total_steps", "steps", "overall_success"}
    assert set(summary["steps"][0]) == {
        "step_number",
        "name",
        "status",
        "total",
        "succeeded",
        "failed",
        "duration_seconds",
    }
    with PipelineManager.resume(
        pipeline.config.delta_root,
        pipeline.config.staging_root,
        working_root=pipeline.config.working_root,
    ) as resumed:
        assert resumed.current_step == 7
        assert resumed[1].step_number == 5
        assert (
            resumed.output("repeat", "structure").source.step_run_id
            == second.step_run_id
        )
        assert (
            resumed.output("selected", "found").source.step_run_id
            == dynamic.step_run_id
        )


def test_pending_static_reference_and_dynamic_completion_are_distinct(
    tmp_path: Path,
) -> None:
    pipeline = _pipeline(tmp_path)
    runner = pipeline.runner
    assert isinstance(runner, NativePipelineRunner)
    runner.release.clear()
    try:
        static = pipeline.submit(
            Transform, inputs={"structure": [structure(0)]}, name="waiting"
        )
        assert runner.entered.wait(5)
        dynamic = pipeline.submit(DynamicCurator, name="dynamic")
        assert not static.done
        assert not dynamic.done
        assert pipeline.output("waiting", "structure") == static.output("structure")
        assert len(pipeline) == 0
        assert "waiting" not in pipeline
        with pytest.raises(ValueError, match="Output role"):
            pipeline.output("dynamic", "found")
    finally:
        runner.release.set()
    static.result(10)
    dynamic.result(10)
    assert pipeline.output("dynamic", "found") == dynamic.result().output("found")
    pipeline.finalize()


@pytest.mark.parametrize("curator", [False, True])
def test_display_name_override_preserves_scientific_and_software_cache_identity(
    tmp_path: Path, curator: bool
) -> None:
    with _pipeline(tmp_path) as pipeline:
        definition = DynamicCurator if curator else Transform
        inputs = None if curator else {"structure": [structure(0)]}
        original_name = definition.name
        first = pipeline.run(definition, inputs=inputs, name="original-display")
        second = pipeline.run(definition, inputs=inputs, name="changed-display")
        assert first.disposition == StepDisposition.EXECUTED
        assert second.disposition == StepDisposition.CACHE_HIT
        assert second.step_name == "changed-display"
        assert definition.name == original_name


def test_explicit_falsey_step_runner_is_not_replaced_by_pipeline_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FalseyRunner(NativePipelineRunner):
        name = "falsey-configured-runner"

        def __bool__(self) -> bool:
            return False

    with _pipeline(tmp_path) as pipeline:
        selected = FalseyRunner(pipeline.committer)

        def forbidden(*args: object, **kwargs: object) -> None:
            pytest.fail("Explicit falsey runner was replaced by the default")

        monkeypatch.setattr(pipeline.runner, "resolve", forbidden)
        result = pipeline.run(
            Transform, inputs={"structure": [structure(0)]}, step_runner=selected
        )
        assert result.status == StepStatus.SUCCEEDED
        assert selected.resolutions == 1
        state = pipeline.tracker.current_state(result.step_run_id)
        assert state.compute_backend == selected.name


@pytest.mark.parametrize("outcome", ["failed", "skipped", "cancelled"])
def test_static_declarations_survive_nonoutput_terminal_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    pipeline = _pipeline(tmp_path)
    if outcome == "failed":

        def fail(*args: object, **kwargs: object) -> None:
            msg = "synthetic provider failure"
            raise RuntimeError(msg)

        monkeypatch.setattr(pipeline.runner, "resolve", fail)
    elif outcome == "cancelled":
        pipeline.cancel()
    result = pipeline.run(
        Transform,
        inputs={"structure": [] if outcome == "skipped" else [structure(0)]},
        name="static",
    )
    assert result.status.value == outcome
    assert result.output_roles == frozenset()
    assert (
        pipeline.output("static", "structure").source.step_run_id == result.step_run_id
    )
    assert bool(pipeline) is (outcome == "skipped")
    assert pipeline.finalize()["overall_success"] is (outcome == "skipped")


def test_latest_pending_and_failed_attempt_masks_older_success_and_declarations(
    tmp_path: Path,
) -> None:
    pipeline = _pipeline(tmp_path)
    pipeline.run(Transform, inputs={"structure": [structure(0)]}, name="repeated")
    record = pipeline.executions[0].record.model_copy(
        update={
            "step_run_id": uuid4().hex,
            "output_roles_json": '["replacement"]',
            "output_types_json": '{"replacement":"csv"}',
        }
    )
    pipeline.tracker.create_attempt(record)
    assert len(pipeline) == 0
    assert not pipeline
    assert (
        pipeline.output("repeated", "replacement").source.step_run_id
        == record.step_run_id
    )
    with pytest.raises(ValueError, match="Output role"):
        pipeline.output("repeated", "structure")
    with pytest.raises(PersistenceIntegrityError, match="unfinished current"):
        pipeline.finalize()
    assert pipeline._summary is None
    pipeline.tracker.transition(
        record.step_run_id, StepStatus.PENDING, StepStatus.RUNNING
    )
    result = StepResult(
        step_run_id=record.step_run_id,
        step_number=record.step_number,
        step_name=record.step_name,
        status=StepStatus.FAILED,
        error="closed fixture failure",
    )
    pipeline.tracker.transition(
        record.step_run_id, StepStatus.RUNNING, StepStatus.FAILED, result=result
    )
    assert [item.model_dump() for item in pipeline[:]] == [result.model_dump()]
    assert not pipeline.finalize()["overall_success"]


def test_finalize_surfaces_closed_controller_failure_instead_of_prior_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = _pipeline(tmp_path)
    pipeline.run(Transform, inputs={"structure": [structure(0)]})

    def fail(self: RetainedStepExecution) -> StepResult:
        msg = "controller interrupted before terminal selection"
        raise RuntimeError(msg)

    monkeypatch.setattr(RetainedStepExecution, "_run", fail)
    future = pipeline.submit(Transform, inputs={"structure": [structure(2)]})
    with pytest.raises(RuntimeError, match="controller interrupted"):
        future.result(5)
    assert pipeline.ownership_state() == OwnershipState.CLOSED
    assert len(pipeline) == 1
    for _ in range(2):
        with pytest.raises(RuntimeError, match="controller interrupted"):
            pipeline.finalize()
        assert pipeline._summary is None
    assert len(pipeline.executions) == 2


def test_finalize_unknown_preserves_owners_and_closes_only_log_sink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = _pipeline(tmp_path)
    pipeline.run(DynamicCurator)
    execution = pipeline.executions[0]
    monkeypatch.setattr(execution, "ownership_state", lambda: OwnershipState.UNKNOWN)
    for _ in range(2):
        with pytest.raises(CommandOwnershipError, match="ownership remains retained"):
            pipeline.finalize()
    assert pipeline._summary is None
    assert pipeline.executions == (execution,)
    assert pipeline._log_session is not None
    assert pipeline._log_session._handler.stream is None


def test_empty_and_failed_curator_summaries(tmp_path: Path) -> None:
    empty = _pipeline(tmp_path / "empty")
    assert not empty
    assert len(empty) == 0
    assert "no steps" in str(empty)
    assert empty.finalize() == {
        "pipeline_name": "example",
        "total_steps": 0,
        "steps": [],
        "overall_success": False,
    }
    failed = _pipeline(tmp_path / "failed")
    failed.run(DynamicCurator, params={"fail": True})
    assert not failed
    assert "0/1 succeeded" in str(failed)
    assert not failed.finalize()["overall_success"]


class _PartialTransform(Transform):
    name = "facade_partial_transform"

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        if coordinate(inputs.input_artifacts["structure"][0]) == 1:
            msg = "synthetic partial failure"
            raise ValueError(msg)
        return super().preprocess(inputs)


def test_partial_result_counts_and_usable_output_survive_finalization(
    tmp_path: Path,
) -> None:
    pipeline = _pipeline(tmp_path)
    result = pipeline.run(
        _PartialTransform,
        inputs={"structure": [structure(0), structure(1)]},
        batch_strategy={"units_per_worker": 2, "max_workers": 1},
        name="partly-usable",
    )
    assert result.status == StepStatus.PARTIAL
    assert pipeline.output("partly-usable", "structure") == result.output("structure")
    assert not pipeline
    summary = pipeline.finalize()
    assert not summary["overall_success"]
    assert summary["steps"][0]["total"] == 2
    assert summary["steps"][0]["succeeded"] == 1
    assert summary["steps"][0]["failed"] == 1


def test_each_facade_read_uses_one_finite_lifecycle_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = _pipeline(tmp_path)
    pipeline.run(Transform, inputs={"structure": [structure(0)]})
    pipeline.run(DynamicCurator)
    original = pipeline.tracker._read_lifecycle
    calls = []

    def read(*args: object, **kwargs: object) -> object:
        calls.append(None)
        return original(*args, **kwargs)

    monkeypatch.setattr(pipeline.tracker, "_read_lifecycle", read)
    assert len(tuple(iter(pipeline))) == 2
    assert len(calls) == 1
    pipeline.output(DynamicCurator.name, "found")
    assert len(calls) == 2
    pipeline.finalize()
    assert len(calls) == 3


@pytest.fixture
def log_capture() -> Iterator[logging.Logger]:
    logger = logging.getLogger("artisan")
    previous = logger.handlers[:], logger.level, logger.propagate
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    yield logger
    for handler in logger.handlers[:]:
        handler.close()
    logger.handlers[:] = previous[0]
    logger.setLevel(previous[1])
    logger.propagate = previous[2]


def test_logging_reaches_controller_context_and_isolates_sessions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    log_capture: logging.Logger,
) -> None:
    first = _pipeline(tmp_path / "one", "one")
    second = _pipeline(tmp_path / "two", "two")
    original = first.runner.resolve

    def resolve(*args: object, **kwargs: object) -> object:
        logging.getLogger("artisan.facade-test").warning("first controller marker")
        return original(*args, **kwargs)

    monkeypatch.setattr(first.runner, "resolve", resolve)
    first.run(Transform, inputs={"structure": [structure(0)]})
    first.cancel()
    log_capture.warning("unbound marker")
    first.finalize()
    second.finalize()
    assert first.log_path is not None
    assert second.log_path is not None
    one, two = Path(first.log_path).read_text(), Path(second.log_path).read_text()
    assert "first controller marker" in one
    assert "first controller marker" not in two
    assert "Pipeline 'one': cancellation requested" in one
    assert "Pipeline 'one' complete" in one
    assert "Pipeline 'two' complete" in two
    assert "unbound marker" not in one + two
    assert first._log_session is not None
    assert first._log_session._handler not in log_capture.handlers
    assert first.finalize()["total_steps"] == 1
    assert Path(first.log_path).read_text() == one


@pytest.mark.parametrize("remote", [False, True])
def test_disabled_or_remote_logging_has_no_local_sink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    remote: bool,
) -> None:
    original = _pipeline(tmp_path)
    original.close()

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Unexpected local log sink")

    monkeypatch.setattr(module, "_RunLogSession", forbidden)
    dispatch = (
        replace(original.dispatch, storage=StorageConfig(protocol="memory"))
        if remote
        else original.dispatch
    )
    with PipelineManager(
        original.committer,
        dispatch,
        pipeline_run_id=original.config.pipeline_run_id,
        configure_logging=remote,
    ) as pipeline:
        assert pipeline.log_path is None


def test_log_setup_and_cleanup_errors_do_not_replace_execution_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        msg = "synthetic sink failure"
        raise OSError(msg)

    monkeypatch.setattr(module, "_RunLogSession", fail)
    monkeypatch.setattr(module.logger, "warning", fail)
    with _pipeline(tmp_path) as pipeline:
        assert pipeline.log_path is None
    monkeypatch.undo()
    pipeline = _pipeline(tmp_path / "cleanup")
    assert pipeline._log_session is not None
    close = pipeline._log_session.close

    def close_then_fail() -> None:
        close()
        msg = "synthetic cleanup failure"
        raise OSError(msg)

    monkeypatch.setattr(pipeline._log_session, "close", close_then_fail)
    with pytest.raises(ValueError, match="body failure"), pipeline:
        msg = "body failure"
        raise ValueError(msg)


def test_forgotten_pipeline_releases_sink_without_claiming_worker_cleanup(
    tmp_path: Path,
    log_capture: logging.Logger,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = _pipeline(tmp_path)
    assert pipeline._log_session is not None
    handler = pipeline._log_session._handler

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Garbage collection must not abandon or close worker ownership")

    monkeypatch.setattr(pipeline._pool, "shutdown", forbidden)
    reference = weakref.ref(pipeline)
    del pipeline
    gc.collect()
    assert reference() is None
    assert handler not in log_capture.handlers
    assert handler.stream is None
