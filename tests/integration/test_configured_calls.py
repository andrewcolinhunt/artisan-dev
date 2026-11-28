"""Public submissions capture selected calls and retain one parent boundary."""

from __future__ import annotations

import pytest

from artisan.execution import CalledOperation
from artisan.operations.examples.configured_generator import ConfiguredGenerator
from artisan.operations.examples.data_generator import DataGenerator
from artisan.orchestration import PipelineManager, StepDisposition
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.runners import LocalRunner
from artisan.schemas.orchestration.step_lifecycle import StepStatus

pytestmark = pytest.mark.integration


def test_configured_calls_publish_only_parent_step(
    pipeline_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selections = {"generator": CalledOperation(DataGenerator, {"count": 1})}
    runner = LocalRunner()
    runner.prepare(ConfiguredGenerator, called_operations=selections)
    monkeypatch.setattr(
        "artisan.orchestration.runners.local.prepare_native_environment",
        lambda *args, **kwargs: pytest.fail("Submission provisioned software"),
    )
    with PipelineManager.create(
        "configured-calls", default_step_runner=runner, **pipeline_env
    ) as pipeline:
        first = pipeline.run(ConfiguredGenerator, called_operations=selections)
        assert first.status == StepStatus.SUCCEEDED, first.error
        selections = {"generator": CalledOperation(DataGenerator, {"count": 2})}
        second = pipeline.run(ConfiguredGenerator, called_operations=selections)
        assert second.status == StepStatus.SUCCEEDED, second.error
        assert second.disposition == StepDisposition.EXECUTED
        repeated = pipeline.run(ConfiguredGenerator, called_operations=selections)
        assert repeated.status == StepStatus.SUCCEEDED, repeated.error
        assert repeated.disposition == StepDisposition.EXECUTED
        assert len(pipeline) == 3
        for result, count in ((first, 2), (second, 4)):
            outputs = read_current_step_outputs(pipeline.committer, result.step_run_id)
            assert len(outputs) == 1
            assert len(outputs[0].occurrences) == count
