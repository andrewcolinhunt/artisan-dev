"""Unified public submission signatures and frozen controller override flow."""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fixtures.retained_pipeline import NativePipelineRunner

from artisan.composites.examples import GenerateAndScore
from artisan.execution import CalledOperation
from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.schemas.enums import CachePolicy, FailurePolicy, GroupByStrategy
from artisan.schemas.orchestration.step_lifecycle import StepStatus

_KWARGS = {
    "called_operations",
    "inputs",
    "params",
    "step_runner",
    "runner_resources",
    "batch_strategy",
    "group_by",
    "tool",
    "skip_cache",
    "cache_policy",
    "failure_policy",
    "name",
}


def test_run_and_submit_share_the_exact_supported_surface() -> None:
    run = inspect.signature(PipelineManager.run)
    submit = inspect.signature(PipelineManager.submit)
    assert run.parameters == submit.parameters
    assert set(run.parameters) == {"self", "definition", *_KWARGS}
    assert run.parameters["definition"].kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert all(
        run.parameters[name].kind == inspect.Parameter.KEYWORD_ONLY for name in _KWARGS
    )
    assert not hasattr(PipelineManager, "run_composite")
    assert not hasattr(PipelineManager, "submit_composite")


@pytest.mark.parametrize("operation", [DataGenerator, GenerateAndScore])
def test_run_forwards_each_option_to_unified_submit(operation: type) -> None:
    receiver = MagicMock(spec=PipelineManager)
    options = {
        "inputs": {"input": []},
        "called_operations": {"generator": CalledOperation(DataGenerator)},
        "params": {"count": 2},
        "step_runner": object(),
        "runner_resources": {"cpus": 3},
        "batch_strategy": {"units_per_worker": 2},
        "group_by": GroupByStrategy.CROSS_PRODUCT,
        "tool": {"executable": "probe"},
        "skip_cache": True,
        "cache_policy": CachePolicy.STEP_COMPLETED,
        "failure_policy": FailurePolicy.CONTINUE,
        "name": "display override",
    }
    result = PipelineManager.run(receiver, operation, **options)
    receiver.submit.assert_called_once_with(operation, **options)
    receiver.submit.return_value.result.assert_called_once_with()
    assert result is receiver.submit.return_value.result.return_value


def test_submission_freezes_overrides_for_actual_admission(tmp_path: Path) -> None:
    with PipelineManager.create(
        "override-admission",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "working"),
    ) as pipeline:
        runner = NativePipelineRunner(pipeline.committer)
        params, resources, batching = {"count": 1}, {"cpus": 3}, {"units_per_worker": 2}
        future = pipeline.submit(
            DataGenerator,
            params=params,
            step_runner=runner,
            runner_resources=resources,
            batch_strategy=batching,
            cache_policy=CachePolicy.STEP_COMPLETED,
            skip_cache=True,
            failure_policy=FailurePolicy.CONTINUE,
            name="display override",
        )
        params["count"], resources["cpus"], batching["units_per_worker"] = 99, 99, 99
        result = future.result()
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.step_name == "display override"
        execution = pipeline.executions[0]
        assert execution.runner is runner
        assert execution.runner_resources.cpus == 3
        assert execution.batch_strategy.units_per_worker == 2
        assert execution.cache_policy is CachePolicy.STEP_COMPLETED
        assert execution.skip_cache is True
        admission = pipeline.tracker.read_admission(result.step_run_id)
        assert json.loads(admission.recipe.params_json)["count"] == 1
        assert admission.recipe.definition.name == DataGenerator.name
