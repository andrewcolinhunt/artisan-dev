"""Integration tests for cache policies.

Tests CachePolicy.ALL_SUCCEEDED and CachePolicy.STEP_COMPLETED with
partial failures to verify cache hit/miss behavior.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.slow

from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend
from artisan.schemas.enums import CachePolicy, FailurePolicy

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
        backend=Backend.LOCAL,
    )
    step1a = p1.run(
        DataTransformer,
        inputs={"dataset": step0a.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
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
        backend=Backend.LOCAL,
    )
    step1b = p2.run(
        DataTransformer,
        inputs={"dataset": step0b.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
        backend=Backend.LOCAL,
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
        backend=Backend.LOCAL,
    )
    step1a = p1.run(
        FailingTransformer,
        inputs={"dataset": step0a.output("datasets")},
        params={"fail_on_index": 1},
        backend=Backend.LOCAL,
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
        backend=Backend.LOCAL,
    )
    step1b = p2.run(
        FailingTransformer,
        inputs={"dataset": step0b.output("datasets")},
        params={"fail_on_index": 1},
        backend=Backend.LOCAL,
    )
    p2.finalize()

    exec_count2 = count_executions_by_step(delta_root, 1)
    # Step 0 is cached (no failures), step 1 re-executes
    assert (
        exec_count2 > exec_count1
    ), "ALL_SUCCEEDED: partial failure should not be cached"


def test_step_completed_partial_failure_cached(pipeline_env: dict[str, str]):
    """STEP_COMPLETED: partial failure IS cached (step completed regardless)."""
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
        backend=Backend.LOCAL,
    )
    step1a = p1.run(
        FailingTransformer,
        inputs={"dataset": step0a.output("datasets")},
        params={"fail_on_index": 1},
        backend=Backend.LOCAL,
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
        backend=Backend.LOCAL,
    )
    step1b = p2.run(
        FailingTransformer,
        inputs={"dataset": step0b.output("datasets")},
        params={"fail_on_index": 1},
        backend=Backend.LOCAL,
    )
    p2.finalize()

    exec_count2 = count_executions_by_step(delta_root, 1)
    assert (
        exec_count2 == exec_count1
    ), "STEP_COMPLETED: partial failure should be cached"
    assert step1b.success is False, "Cached result preserves failure status"
