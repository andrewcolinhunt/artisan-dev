"""Public asynchronous submission preserves pending work, outputs and failures."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fixtures.composition_operations import Transform, coordinate, structure
from fixtures.retained_pipeline import NativePipelineRunner

from artisan.orchestration import PipelineManager, StepFuture
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.specs.input_models import PreprocessInput


@pytest.fixture
def pipeline(
    tmp_path: Path,
) -> Iterator[tuple[PipelineManager, NativePipelineRunner]]:
    with PipelineManager.create(
        "async-submission",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    ) as manager:
        runner = NativePipelineRunner(manager.committer)
        try:
            yield manager, runner
        finally:
            runner.release.set()


def test_pending_future_exposes_output_and_timeout_before_completion(
    pipeline: tuple[PipelineManager, NativePipelineRunner],
) -> None:
    manager, runner = pipeline
    runner.release.clear()
    future = manager.submit(
        Transform, inputs={"structure": [structure(0)]}, step_runner=runner
    )
    assert isinstance(future, StepFuture)
    assert runner.entered.wait(5)
    assert not future.done
    assert future.status == StepStatus.PENDING
    reference = future.output("structure")
    assert reference.source_step == 0
    assert reference.role == "structure"
    with pytest.raises(TimeoutError):
        future.result(timeout=0.01)
    assert not future.done
    runner.release.set()
    result = future.result()
    assert future.done
    assert future.status == result.status == StepStatus.SUCCEEDED
    assert future.result() == result


def test_downstream_submit_waits_without_blocking_the_caller(
    pipeline: tuple[PipelineManager, NativePipelineRunner],
) -> None:
    manager, runner = pipeline
    runner.release.clear()
    first = manager.submit(
        Transform, inputs={"structure": [structure(0)]}, step_runner=runner
    )
    assert runner.entered.wait(5)
    second = manager.submit(
        Transform, inputs={"structure": first.output("structure")}, step_runner=runner
    )
    assert not first.done
    assert not second.done
    assert len(manager.executions) == 2
    runner.release.set()
    result = second.result()
    assert first.status == result.status == StepStatus.SUCCEEDED, result.error
    outputs = read_current_step_outputs(manager.committer, result.step_run_id)
    assert coordinate(outputs[0].occurrences[0].artifact) == 2


@pytest.mark.parametrize("asynchronous", [False, True])
def test_run_and_submit_return_terminal_results_and_chain_outputs(
    pipeline: tuple[PipelineManager, NativePipelineRunner], asynchronous: bool
) -> None:
    manager, runner = pipeline
    inputs = {"structure": [structure(0)]}
    first = (
        manager.submit(Transform, inputs=inputs, step_runner=runner).result()
        if asynchronous
        else manager.run(Transform, inputs=inputs, step_runner=runner)
    )
    assert isinstance(first, StepResult)
    assert first.status == StepStatus.SUCCEEDED, first.error
    result = manager.run(
        Transform, inputs={"structure": first.output("structure")}, step_runner=runner
    )
    assert result.status == StepStatus.SUCCEEDED
    assert [item.step_number for item in manager] == [0, 1]


class _FailingTransform(Transform):
    name = "submission_failure"

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        msg = "synthetic submission failure"
        raise RuntimeError(msg)


def test_submit_scientific_failure_returns_failed_result(
    pipeline: tuple[PipelineManager, NativePipelineRunner],
) -> None:
    manager, runner = pipeline
    result = manager.submit(
        _FailingTransform, inputs={"structure": [structure(0)]}, step_runner=runner
    ).result()
    assert result.status == StepStatus.FAILED
    assert result.error is not None
    assert "synthetic submission failure" in result.error
    assert not read_current_step_outputs(manager.committer, result.step_run_id)


def test_submitted_cache_hit_preserves_original_outputs_without_launch(
    pipeline: tuple[PipelineManager, NativePipelineRunner],
) -> None:
    manager, runner = pipeline
    inputs = {"structure": [structure(0)]}
    original = manager.run(Transform, inputs=inputs, step_runner=runner)
    launches = len(runner.packs)
    future = manager.submit(Transform, inputs=inputs, step_runner=runner)
    reused = future.result()
    assert reused.status == StepStatus.SUCCEEDED
    assert reused.disposition == StepDisposition.CACHE_HIT
    assert reused.step_run_id != original.step_run_id
    assert future.done
    assert len(runner.packs) == launches
    assert read_current_step_outputs(
        manager.committer, reused.step_run_id
    ) == read_current_step_outputs(manager.committer, original.step_run_id)
