"""Real controller cache selection with explicitly synthetic test software.

The fixture executes the ordinary kernel, admission and acceptance in this process.
Installed worker qualification is exercised by the public pipeline cache driver.
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import pytest
from fixtures.retained_pipeline import NativePipelineRunner

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.schemas import InputSpec, OutputSpec
from artisan.schemas.enums import CachePolicy
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.io.retained_store import create_retained_store

from .conftest import FailingTransformer

pytestmark = pytest.mark.integration


def _pipeline(
    roots: dict[str, str],
    *,
    policy: CachePolicy = CachePolicy.ALL_SUCCEEDED,
    skip_cache: bool = False,
    resume: str | None = None,
) -> PipelineManager:
    files = str(Path(roots["delta_root"]).parent / "files")
    committer = create_retained_store(
        roots["delta_root"], files, fs=StorageConfig().filesystem()
    )
    runner = NativePipelineRunner(committer)
    common = {
        **roots,
        "files_root": files,
        "default_step_runner": runner,
        "cache_policy": policy,
        "skip_cache": skip_cache,
    }
    if resume is not None:
        return PipelineManager.resume(**common, pipeline_run_id=resume)
    return PipelineManager.create("cache-policy", **common)


def _generate(pipeline: PipelineManager) -> StepResult:
    return pipeline.run(
        DataGenerator, params={"count": 3, "seed": 42}, skip_cache=False
    )


def _transform(pipeline: PipelineManager, source: StepResult, **options) -> StepResult:
    return pipeline.run(
        FailingTransformer,
        inputs={"dataset": source.output("datasets")},
        params={"fail_on_index": 1},
        batch_strategy={"artifacts_per_unit": 1, "max_workers": 1},
        **options,
    )


def _seed(roots: dict[str, str]) -> tuple[PipelineManager, StepResult]:
    with _pipeline(roots) as pipeline:
        result = _transform(pipeline, _generate(pipeline))
        assert result.status == StepStatus.PARTIAL, result.error
        assert (result.succeeded_count, result.failed_count) == (2, 1)
    return pipeline, result


@pytest.mark.parametrize("policy", list(CachePolicy))
@pytest.mark.parametrize("method", ["run", "submit"])
def test_step_override_controls_whole_step_and_unit_reuse(
    pipeline_env: dict[str, str],
    policy: CachePolicy,
    method: str,
) -> None:
    original, source = _seed(pipeline_env)
    before = original.tracker.current_state(source.step_run_id)
    selected = read_current_step_outputs(original.committer, source.step_run_id)
    opposite = next(value for value in CachePolicy if value != policy)
    with _pipeline(pipeline_env, policy=opposite) as current:
        generated = _generate(current)
        result = getattr(current, method)(
            FailingTransformer,
            inputs={"dataset": generated.output("datasets")},
            params={"fail_on_index": 1},
            batch_strategy={"artifacts_per_unit": 1, "max_workers": 1},
            cache_policy=policy,
        )
        if method == "submit":
            result = result.result()
        assert result.status == StepStatus.PARTIAL, result.error
        assert (result.succeeded_count, result.failed_count) == (2, 1)
        assert (
            read_current_step_outputs(current.committer, result.step_run_id) == selected
        )
        execution = current.executions[-1]
        if policy is CachePolicy.STEP_COMPLETED:
            assert result.disposition == StepDisposition.CACHE_HIT
            assert execution.handles == ()
        else:
            assert result.disposition == StepDisposition.EXECUTED
            assert len(execution.handles) == 1
            assert execution.handles[0].attempt.assignment.unit_index == 1
        downstream = current.run(
            DataTransformer,
            inputs={"dataset": result.output("dataset")},
            batch_strategy={"max_workers": 1},
        )
        assert downstream.status == StepStatus.SUCCEEDED, downstream.error
        assert (
            len(
                read_current_step_outputs(current.committer, downstream.step_run_id)[
                    0
                ].occurrences
            )
            == 2
        )
    assert original.tracker.current_state(source.step_run_id) == before


@pytest.mark.parametrize("policy", list(CachePolicy))
@pytest.mark.parametrize(
    ("pipeline_skip", "step_skip", "expected"),
    [
        (False, None, False),
        (True, None, True),
        (False, True, True),
        (True, False, False),
    ],
)
def test_cache_bypass_precedence_preserves_recipe(
    pipeline_env: dict[str, str],
    policy: CachePolicy,
    pipeline_skip: bool,
    step_skip: bool | None,
    expected: bool,
) -> None:
    original, _source = _seed(pipeline_env)
    source_recipe = original.tracker.read_admission(_source.step_run_id).recipe
    with _pipeline(pipeline_env, policy=policy, skip_cache=pipeline_skip) as current:
        generated = _generate(current)
        result = _transform(current, generated, skip_cache=step_skip)
        assert result.status == StepStatus.PARTIAL
        execution = current.executions[-1]
        assert (
            current.tracker.read_admission(result.step_run_id).recipe == source_recipe
        )
        assert len(execution.handles) == (
            3 if expected else 0 if policy == CachePolicy.STEP_COMPLETED else 1
        )


class _PolicyLeaf(CompositeDefinition):
    name = "policy_leaf"

    class InputRole(StrEnum):
        dataset = "dataset"

    class OutputRole(StrEnum):
        dataset = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {"dataset": InputSpec(artifact_type="csv")}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "dataset": OutputSpec(artifact_type="csv")
    }

    def compose(self, ctx: CompositeContext) -> None:
        result = ctx.call(
            FailingTransformer,
            inputs={"dataset": ctx.input("dataset")},
            params={"fail_on_index": 1},
        )
        ctx.output("dataset", result.output("dataset"))


class _PolicyNested(_PolicyLeaf):
    name = "policy_nested"

    def compose(self, ctx: CompositeContext) -> None:
        result = ctx.call(_PolicyLeaf, inputs={"dataset": ctx.input("dataset")})
        ctx.output("dataset", result.output("dataset"))


@pytest.mark.parametrize("policy", list(CachePolicy))
def test_nested_composite_cache_selection_is_at_outer_boundary(
    pipeline_env: dict[str, str],
    policy: CachePolicy,
) -> None:
    for index in range(2):
        with _pipeline(pipeline_env, policy=policy) as pipeline:
            source = _generate(pipeline)
            result = pipeline.run(
                _PolicyNested,
                inputs={"dataset": source.output("datasets")},
                batch_strategy={"artifacts_per_unit": 1, "max_workers": 1},
            )
            assert result.status == StepStatus.PARTIAL, result.error
            assert len(pipeline) == 2
            assert len(pipeline.executions[-1].handles) == (
                3 if index == 0 else 0 if policy == CachePolicy.STEP_COMPLETED else 1
            )


def test_resume_keeps_partial_hit_and_applies_policy_only_to_new_work(
    pipeline_env: dict[str, str],
) -> None:
    with _pipeline(pipeline_env) as history:
        generated = _generate(history)
        for _ in range(3):
            assert _transform(history, generated).status == StepStatus.PARTIAL
    with _pipeline(pipeline_env, policy=CachePolicy.STEP_COMPLETED) as consumer:
        generated = _generate(consumer)
        reused = _transform(consumer, generated)
        assert reused.disposition == StepDisposition.CACHE_HIT
    before = consumer.tracker.current_state(reused.step_run_id)
    with _pipeline(pipeline_env, resume=consumer.config.pipeline_run_id) as resumed:
        assert resumed[1] == reused
        inherited = _transform(resumed, resumed[0])
        assert inherited.disposition == StepDisposition.EXECUTED
        assert len(resumed.executions[-1].handles) == 1
        overridden = _transform(
            resumed, resumed[0], cache_policy=CachePolicy.STEP_COMPLETED
        )
        assert overridden.disposition == StepDisposition.CACHE_HIT
        assert resumed.executions[-1].handles == ()
    assert consumer.tracker.current_state(reused.step_run_id) == before


def test_cached_creator_chain_keeps_original_output_references(
    pipeline_env: dict[str, str],
) -> None:
    selections = []
    for index in range(2):
        with _pipeline(pipeline_env) as pipeline:
            generated = _generate(pipeline)
            transformed = pipeline.run(
                DataTransformer,
                inputs={"dataset": generated.output("datasets")},
                params={"seed": 17, "noise_amplitude": 0.0},
                batch_strategy={"max_workers": 1},
            )
            assert transformed.status == StepStatus.SUCCEEDED
            selections.append(
                read_current_step_outputs(pipeline.committer, transformed.step_run_id)
            )
            if index:
                assert (
                    generated.disposition
                    == transformed.disposition
                    == StepDisposition.CACHE_HIT
                )
                assert all(not execution.handles for execution in pipeline.executions)
    assert selections[1] == selections[0]
