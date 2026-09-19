"""Integration tests for cache policies.

Tests CachePolicy.ALL_SUCCEEDED and CachePolicy.STEP_COMPLETED with
partial failures to verify cache hit/miss behavior.
"""

from __future__ import annotations

import json
from enum import StrEnum
from typing import Any, ClassVar

import polars as pl
import pytest
from pydantic import BaseModel

pytestmark = pytest.mark.integration

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners import Runner
from artisan.schemas import InputSpec, OutputSpec
from artisan.schemas.enums import CachePolicy, FailurePolicy, TablePath
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.core.committed_scan import read_committed
from artisan.storage.core.run_scope import (
    load_accepted_outputs,
    load_execution_membership,
)

from .conftest import (
    FailingTransformer,
    count_executions_by_step,
    get_execution_outputs,
)


def test_cache_hit_identical_artifact_ids(pipeline_env: dict[str, str]):
    """Two identical runs produce same artifact IDs with no new executions."""
    delta_root = pipeline_env["delta_root"]
    staging = pipeline_env["staging_root"]
    working = pipeline_env["working_root"]

    # Run 1
    p1 = PipelineManager.create(
        name="test_cache_id",
        delta_root=delta_root,
        staging_root=staging,
        working_root=working,
    )
    step0a = p1.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        step_runner=Runner.LOCAL,
    )
    p1.run(
        DataTransformer,
        inputs={"dataset": step0a.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
        step_runner=Runner.LOCAL,
    )
    p1.finalize()

    ids_run1 = set(get_execution_outputs(delta_root, 1, "dataset"))
    exec_count1 = count_executions_by_step(delta_root, 1)

    # Run 2 — identical
    p2 = PipelineManager.create(
        name="test_cache_id",
        delta_root=delta_root,
        staging_root=staging,
        working_root=working,
    )
    step0b = p2.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        step_runner=Runner.LOCAL,
    )
    p2.run(
        DataTransformer,
        inputs={"dataset": step0b.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
        step_runner=Runner.LOCAL,
    )
    p2.finalize()

    ids_run2 = set(get_execution_outputs(delta_root, 1, "dataset"))

    # Same artifact IDs
    assert ids_run1 == ids_run2

    # No new executions (cache hit)
    exec_count2 = count_executions_by_step(delta_root, 1)
    assert exec_count2 == exec_count1, "Cache hit should not create new executions"


def test_all_succeeded_partial_failure_not_cached(pipeline_env: dict[str, str]):
    """ALL_SUCCEEDED: partial failure causes re-execution on second run."""
    delta_root = pipeline_env["delta_root"]
    staging = pipeline_env["staging_root"]
    working = pipeline_env["working_root"]

    # Run 1: partial failure with CONTINUE
    p1 = PipelineManager.create(
        name="test_all_succ_cache",
        delta_root=delta_root,
        staging_root=staging,
        working_root=working,
        failure_policy=FailurePolicy.CONTINUE,
        cache_policy=CachePolicy.ALL_SUCCEEDED,
    )
    step0a = p1.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        step_runner=Runner.LOCAL,
    )
    step1a = p1.run(
        FailingTransformer,
        inputs={"dataset": step0a.output("datasets")},
        params={"fail_on_index": 1},
        step_runner=Runner.LOCAL,
    )
    p1.finalize()

    assert step1a.failed_count == 1
    exec_count1 = count_executions_by_step(delta_root, 1)

    # Run 2: same pipeline — step 1 should NOT be cached (had failures)
    p2 = PipelineManager.create(
        name="test_all_succ_cache",
        delta_root=delta_root,
        staging_root=staging,
        working_root=working,
        failure_policy=FailurePolicy.CONTINUE,
        cache_policy=CachePolicy.ALL_SUCCEEDED,
    )
    step0b = p2.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        step_runner=Runner.LOCAL,
    )
    p2.run(
        FailingTransformer,
        inputs={"dataset": step0b.output("datasets")},
        params={"fail_on_index": 1},
        step_runner=Runner.LOCAL,
    )
    p2.finalize()

    exec_count2 = count_executions_by_step(delta_root, 1)
    # Step 0 is cached (no failures), step 1 re-executes
    assert exec_count2 > exec_count1, (
        "ALL_SUCCEEDED: partial failure should not be cached"
    )


def test_step_completed_partial_failure_cached(pipeline_env: dict[str, str]):
    """STEP_COMPLETED reuses a policy-accepted partial result."""
    delta_root = pipeline_env["delta_root"]
    staging = pipeline_env["staging_root"]
    working = pipeline_env["working_root"]

    # Run 1
    p1 = PipelineManager.create(
        name="test_step_comp_cache",
        delta_root=delta_root,
        staging_root=staging,
        working_root=working,
        failure_policy=FailurePolicy.CONTINUE,
        cache_policy=CachePolicy.STEP_COMPLETED,
    )
    step0a = p1.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        step_runner=Runner.LOCAL,
    )
    step1a = p1.run(
        FailingTransformer,
        inputs={"dataset": step0a.output("datasets")},
        params={"fail_on_index": 1},
        step_runner=Runner.LOCAL,
    )
    p1.finalize()

    assert step1a.failed_count == 1
    exec_count1 = count_executions_by_step(delta_root, 1)

    # Run 2: step 1 IS cached despite partial failure
    p2 = PipelineManager.create(
        name="test_step_comp_cache",
        delta_root=delta_root,
        staging_root=staging,
        working_root=working,
        failure_policy=FailurePolicy.CONTINUE,
        cache_policy=CachePolicy.STEP_COMPLETED,
    )
    step0b = p2.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        step_runner=Runner.LOCAL,
    )
    step1b = p2.run(
        FailingTransformer,
        inputs={"dataset": step0b.output("datasets")},
        params={"fail_on_index": 1},
        step_runner=Runner.LOCAL,
    )
    p2.finalize()

    exec_count2 = count_executions_by_step(delta_root, 1)
    assert exec_count2 == exec_count1, (
        "STEP_COMPLETED: partial failure should be cached"
    )
    assert step1b.status is StepStatus.PARTIAL
    assert step1b.disposition is StepDisposition.CACHE_HIT


def _policy_pipeline(
    env: dict[str, str], policy: CachePolicy, *, skip_cache: bool = False
) -> PipelineManager:
    return PipelineManager.create(
        name="step_policy", **env, cache_policy=policy, skip_cache=skip_cache
    )


def _generate(pipeline: PipelineManager) -> StepResult:
    return pipeline.run(DataGenerator, params={"count": 3, "seed": 42}, compact=False)


def _transform(
    pipeline: PipelineManager, source: StepResult, **kwargs: Any
) -> StepResult:
    return pipeline.run(
        FailingTransformer,
        inputs={"dataset": source.output("datasets")},
        params={"fail_on_index": 1},
        batch_strategy={"artifacts_per_unit": 1},
        compact=False,
        **kwargs,
    )


def _seed_partial(env: dict[str, str]) -> tuple[PipelineManager, StepResult]:
    pipeline = _policy_pipeline(env, CachePolicy.ALL_SUCCEEDED)
    result = _transform(pipeline, _generate(pipeline))
    pipeline.finalize()
    assert (result.succeeded_count, result.failed_count) == (2, 1)
    return pipeline, result


def _rows(
    pipeline: PipelineManager, table: TablePath, result: StepResult
) -> pl.DataFrame:
    return read_committed(
        pipeline.config.delta_root, table, fs=pipeline.config.storage.filesystem()
    ).filter(pl.col("step_run_id") == result.step_run_id)


def _membership(pipeline: PipelineManager, result: StepResult) -> pl.DataFrame:
    return load_execution_membership(
        pipeline.config.delta_root,
        fs=pipeline.config.storage.filesystem(),
        pipeline_run_id=pipeline.config.pipeline_run_id,
        step_run_id=result.step_run_id,
    )


def _step_spec_id(pipeline: PipelineManager, result: StepResult) -> str:
    assert result.step_run_id is not None
    state = pipeline._step_tracker.current_state(result.step_run_id)
    assert state.status is result.status
    assert isinstance(state.step_spec_id, str)
    assert state.step_spec_id
    return state.step_spec_id


def _assert_policy(
    pipeline: PipelineManager, result: StepResult, policy: CachePolicy
) -> None:
    snapshots = _rows(pipeline, TablePath.STEPS, result)
    assert {
        json.loads(value)["cache_policy"] for value in snapshots["compute_options_json"]
    } == {policy.value}


@pytest.mark.parametrize("policy", list(CachePolicy))
@pytest.mark.parametrize("method", ["run", "submit"])
def test_step_override_controls_whole_step_and_unit_reuse(
    pipeline_env: dict[str, str], policy: CachePolicy, method: str
) -> None:
    source_pipeline, source = _seed_partial(pipeline_env)
    source_rows = _rows(source_pipeline, TablePath.STEPS, source)
    source_executions = _rows(source_pipeline, TablePath.EXECUTIONS, source)
    opposite = next(value for value in CachePolicy if value != policy)
    consumer = _policy_pipeline(pipeline_env, opposite)
    generated = _generate(consumer)
    result = getattr(consumer, method)(
        FailingTransformer,
        inputs={"dataset": generated.output("datasets")},
        params={"fail_on_index": 1},
        batch_strategy={"artifacts_per_unit": 1},
        cache_policy=policy,
        compact=False,
    )
    if method == "submit":
        result = result.result()
    assert result.status is StepStatus.PARTIAL
    assert (result.succeeded_count, result.failed_count) == (2, 1)
    assert result.step_run_id != source.step_run_id
    assert _step_spec_id(consumer, result) == _step_spec_id(source_pipeline, source)
    membership = _membership(consumer, result)
    assert membership.height == 3
    assert set(membership["pipeline_run_id"]) == {consumer.config.pipeline_run_id}
    direct = _rows(consumer, TablePath.EXECUTIONS, result)
    if policy is CachePolicy.STEP_COMPLETED:
        assert result.disposition is StepDisposition.CACHE_HIT
        assert result.error == source.error
        assert direct.is_empty()
        assert membership["cache_hit"].all()
        assert set(membership["execution_run_id"]) == set(
            source_executions["execution_run_id"]
        )
    else:
        assert result.disposition is StepDisposition.EXECUTED
        assert direct.height == 1
        assert direct["success"].to_list() == [False]
        assert membership.filter(pl.col("cache_hit"))["success"].to_list() == [
            True,
            True,
        ]
        failed_spec = source_executions.filter(~pl.col("success"))[
            "execution_spec_id"
        ].item()
        assert direct["execution_spec_id"].item() == failed_spec
    _assert_policy(consumer, result, policy)
    downstream = consumer.run(
        DataTransformer, inputs={"dataset": result.output("dataset")}, compact=False
    )
    assert downstream.succeeded_count == 2
    consumer.finalize()
    assert _rows(source_pipeline, TablePath.STEPS, source).equals(source_rows)
    assert _rows(source_pipeline, TablePath.EXECUTIONS, source).equals(
        source_executions
    )


class _PolicyLeaf(CompositeDefinition):
    name = "cache_policy_leaf"

    class InputRole(StrEnum):
        DATASET = "dataset"

    class OutputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        "dataset": InputSpec(artifact_type="data")
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "dataset": OutputSpec(artifact_type="data")
    }

    class Params(BaseModel):
        leaf_policy: CachePolicy | None = None
        leaf_skip_cache: bool | None = None

    params: Params = Params()

    def compose(self, ctx: CompositeContext) -> None:
        result = ctx.run(
            FailingTransformer,
            inputs={"dataset": ctx.input("dataset")},
            params={"fail_on_index": 1},
            batch_strategy={"artifacts_per_unit": 1},
            cache_policy=self.params.leaf_policy,
            skip_cache=self.params.leaf_skip_cache,
        )
        ctx.output("dataset", result.output("dataset"))


class _PolicyNested(_PolicyLeaf):
    name = "cache_policy_nested"

    class Params(_PolicyLeaf.Params):
        inner_policy: CachePolicy | None = None

    params: Params = Params()

    def compose(self, ctx: CompositeContext) -> None:
        inner = ctx.run(
            _PolicyLeaf,
            inputs={"dataset": ctx.input("dataset")},
            params={
                "leaf_policy": self.params.leaf_policy,
                "leaf_skip_cache": self.params.leaf_skip_cache,
            },
            cache_policy=self.params.inner_policy,
        )
        ctx.output("dataset", inner.output("dataset"))


@pytest.mark.parametrize(
    ("inner", "leaf", "expected"),
    [
        (None, None, CachePolicy.STEP_COMPLETED),
        (CachePolicy.ALL_SUCCEEDED, None, CachePolicy.ALL_SUCCEEDED),
        (
            CachePolicy.ALL_SUCCEEDED,
            CachePolicy.STEP_COMPLETED,
            CachePolicy.STEP_COMPLETED,
        ),
        (
            CachePolicy.STEP_COMPLETED,
            CachePolicy.ALL_SUCCEEDED,
            CachePolicy.ALL_SUCCEEDED,
        ),
    ],
)
def test_nested_composite_cache_policy_selects_real_reuse(
    pipeline_env: dict[str, str],
    inner: CachePolicy | None,
    leaf: CachePolicy | None,
    expected: CachePolicy,
) -> None:
    _seed_partial(pipeline_env)
    consumer = _policy_pipeline(pipeline_env, CachePolicy.ALL_SUCCEEDED)
    generated = _generate(consumer)
    composite = consumer.run_composite(
        _PolicyNested,
        inputs={"dataset": generated.output("datasets")},
        params={"inner_policy": inner, "leaf_policy": leaf},
        cache_policy=CachePolicy.STEP_COMPLETED,
        compact=False,
    )
    consumer.finalize()
    result = consumer[1]
    assert result.status is StepStatus.PARTIAL
    assert _rows(consumer, TablePath.EXECUTIONS, result).height == (
        0 if expected is CachePolicy.STEP_COMPLETED else 1
    )
    _assert_policy(consumer, result, expected)
    outputs = load_accepted_outputs(
        consumer.config.delta_root,
        fs=consumer.config.storage.filesystem(),
        pipeline_run_id=consumer.config.pipeline_run_id,
        step_run_id=result.step_run_id,
        role=composite.output("dataset").role,
    )
    assert outputs.height == 2


@pytest.mark.parametrize("policy", list(CachePolicy))
@pytest.mark.parametrize(
    "bypass", ["step", "pipeline", "composite", "child_false", "pipeline_child_false"]
)
def test_cache_bypass_precedence_and_spec_identity(
    pipeline_env: dict[str, str], policy: CachePolicy, bypass: str
) -> None:
    source_pipeline, source = _seed_partial(pipeline_env)
    consumer = _policy_pipeline(
        pipeline_env, policy, skip_cache=bypass in {"pipeline", "pipeline_child_false"}
    )
    generated = _generate(consumer)
    if bypass in {"step", "pipeline"}:
        result = _transform(
            consumer, generated, cache_policy=policy, skip_cache=bypass == "step"
        )
    else:
        consumer.run_composite(
            _PolicyNested,
            inputs={"dataset": generated.output("datasets")},
            params={"leaf_skip_cache": None if bypass == "composite" else False},
            cache_policy=policy,
            skip_cache=True,
            compact=False,
        )
        consumer.finalize()
        result = consumer[1]
    consumer.finalize()
    assert result.status is StepStatus.PARTIAL
    membership = _membership(consumer, result)
    direct = _rows(consumer, TablePath.EXECUTIONS, result)
    if bypass == "child_false":
        assert direct.height == (0 if policy is CachePolicy.STEP_COMPLETED else 1)
        assert membership["cache_hit"].sum() >= 2
    else:
        assert direct.height == 3
        assert not membership["cache_hit"].any()
        source_specs = _rows(source_pipeline, TablePath.EXECUTIONS, source)[
            "execution_spec_id"
        ]
        assert sorted(direct["execution_spec_id"]) == sorted(source_specs)
    assert _step_spec_id(source_pipeline, source) == _step_spec_id(consumer, result)
    _assert_policy(consumer, result, policy)


def test_resume_preserves_partial_hit_and_applies_policy_only_to_new_steps(
    pipeline_env: dict[str, str],
) -> None:
    source = _policy_pipeline(pipeline_env, CachePolicy.ALL_SUCCEEDED)
    generated = _generate(source)
    for _ in range(3):
        _transform(source, generated)
    source.finalize()
    consumer = _policy_pipeline(pipeline_env, CachePolicy.ALL_SUCCEEDED)
    generated = _generate(consumer)
    accepted = _transform(consumer, generated, cache_policy=CachePolicy.STEP_COMPLETED)
    consumer.finalize()
    original_rows = _rows(consumer, TablePath.STEPS, accepted)
    original_membership = _membership(consumer, accepted)
    execution_count = read_committed(
        consumer.config.delta_root,
        TablePath.EXECUTIONS,
        fs=consumer.config.storage.filesystem(),
    ).height
    resumed = PipelineManager.resume(
        **pipeline_env,
        pipeline_run_id=consumer.config.pipeline_run_id,
        cache_policy=CachePolicy.ALL_SUCCEEDED,
    )
    restored = resumed[1]
    assert restored.step_run_id == accepted.step_run_id
    assert restored.status is StepStatus.PARTIAL
    assert (restored.succeeded_count, restored.failed_count) == (2, 1)
    assert restored.disposition is StepDisposition.CACHE_HIT
    assert restored.error == accepted.error
    assert (
        read_committed(
            resumed.config.delta_root,
            TablePath.EXECUTIONS,
            fs=resumed.config.storage.filesystem(),
        ).height
        == execution_count
    )
    inherited = _transform(resumed, resumed[0])
    overridden = _transform(
        resumed, resumed[0], cache_policy=CachePolicy.STEP_COMPLETED
    )
    assert _rows(resumed, TablePath.EXECUTIONS, inherited).height == 1
    assert _rows(resumed, TablePath.EXECUTIONS, overridden).is_empty()
    _assert_policy(resumed, inherited, CachePolicy.ALL_SUCCEEDED)
    _assert_policy(resumed, overridden, CachePolicy.STEP_COMPLETED)
    downstream = resumed.run(
        DataTransformer, inputs={"dataset": restored.output("dataset")}, compact=False
    )
    assert downstream.succeeded_count == 2
    resumed.finalize()
    assert _rows(resumed, TablePath.STEPS, restored).equals(original_rows)
    assert (
        _membership(resumed, restored)
        .sort("execution_run_id")
        .equals(original_membership.sort("execution_run_id"))
    )
