"""Recover actual finished workers without accepting their cancelled source step."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
from artisan.orchestration import PipelineManager
from artisan.orchestration.engine import step_executor
from artisan.orchestration.engine.dispatch import (
    execute_unit_batch,
    failure_results_for_units,
)
from artisan.orchestration.engine.lifecycle_router import LifecycleRouter
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.enums import CachePolicy, TablePath
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.storage.core.committed_scan import read_committed
from artisan.storage.core.run_scope import (
    load_accepted_outputs,
    load_execution_membership,
)

pytestmark = pytest.mark.integration


class _InterruptedRunner(LocalRunner):
    """Exercise real worker recording while controlling the dispatch boundary."""

    name = "staging_recovery_test"

    def __init__(self, finished: int | None = None) -> None:
        super().__init__()
        self.finished = finished
        self.invocations: Counter[str] = Counter()

    def create_lifecycle_router(self, *args: Any, **kwargs: Any) -> LifecycleRouter:
        owner = self

        class Router(LifecycleRouter):
            def _dispatch(
                self, units: list[ExecutionUnit], runtime_env: RuntimeEnvironment
            ) -> None:
                def execute() -> list[UnitResult]:
                    selected = (
                        units if owner.finished is None else units[: owner.finished]
                    )
                    results = []
                    for unit in selected:
                        owner.invocations[unit.operation.name] += 1
                        results.extend(execute_unit_batch([unit], runtime_env))
                    return results + failure_results_for_units(
                        units[len(selected) :], "Provider stopped before dispatch"
                    )

                self._start_background(execute)

            def cancel(self) -> CancellationAcknowledgement:
                return CancellationAcknowledgement(
                    CancellationStatus.CONFIRMED,
                    "Test provider has no remaining workers",
                )

        return Router()


def _cancel_finished_work(
    roots: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    *,
    preserve: bool,
    count: int = 4,
    finished: int = 3,
) -> tuple[PipelineManager, str, dict[Path, bytes]]:
    runner = _InterruptedRunner()
    pipeline = PipelineManager.create(
        name="interrupted",
        **roots,
        default_step_runner=runner,
        preserve_staging=preserve,
    )
    generated = pipeline.run(DataGenerator, params={"count": count, "seed": 91})
    assert generated.status is StepStatus.SUCCEEDED
    runner.finished = finished
    original_logs = step_executor.persist_worker_logs

    def cancel_before_plan(*args: Any, **kwargs: Any) -> None:
        original_logs(*args, **kwargs)
        pipeline.cancel()

    with monkeypatch.context() as patcher:
        patcher.setattr(step_executor, "persist_worker_logs", cancel_before_plan)
        result = pipeline.run(
            DataTransformer,
            name="interrupted-transform",
            inputs={"dataset": generated.output("datasets")},
            params={"seed": 52},
        )
    pipeline.finalize()
    assert result.status is StepStatus.CANCELLED
    assert runner.invocations[DataTransformer.name] == finished
    assert result.step_run_id is not None
    evidence = {}
    for seal in Path(roots["staging_root"]).rglob("executions.parquet"):
        row = pl.read_parquet(seal).row(0, named=True)
        if row["step_run_id"] == result.step_run_id and row["success"]:
            evidence.update(
                {
                    path: path.read_bytes()
                    for path in seal.parent.iterdir()
                    if path.is_file()
                }
            )
    assert (
        len([path for path in evidence if path.name == "executions.parquet"])
        == finished
    )
    return pipeline, result.step_run_id, evidence


@pytest.mark.parametrize("recover", [False, True])
@pytest.mark.parametrize("preserve", [False, True])
@pytest.mark.parametrize("policy", list(CachePolicy))
def test_recovery_reuses_finished_units_and_preserves_source_history(
    pipeline_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    recover: bool,
    preserve: bool,
    policy: CachePolicy,
) -> None:
    original, source_step, evidence = _cancel_finished_work(
        pipeline_env, monkeypatch, preserve=preserve
    )
    fs = LocalFileSystem()
    delta = pipeline_env["delta_root"]
    assert (
        read_committed(delta, TablePath.EXECUTIONS, fs=fs)
        .filter(pl.col("step_run_id") == source_step)
        .is_empty()
    )
    runner = _InterruptedRunner()
    restarted = PipelineManager.create(
        name="restarted",
        **pipeline_env,
        default_step_runner=runner,
        recover_staging=recover,
        preserve_staging=preserve,
        cache_policy=policy,
    )
    recovered = read_committed(delta, TablePath.EXECUTIONS, fs=fs).filter(
        pl.col("step_run_id") == source_step
    )
    assert recovered.height == (3 if recover else 0)
    if not recover or preserve:
        assert all(path.read_bytes() == data for path, data in evidence.items())
    else:
        assert all(not path.exists() for path in evidence)
    generated = restarted.run(DataGenerator, params={"count": 4, "seed": 91})
    result = restarted.run(
        DataTransformer,
        inputs={"dataset": generated.output("datasets")},
        params={"seed": 52},
    )
    if recover and not preserve and policy is CachePolicy.ALL_SUCCEEDED:
        downstream = restarted.run(
            MetricCalculator, inputs={"dataset": result.output("dataset")}
        )
        assert downstream.status is StepStatus.SUCCEEDED
        assert downstream.succeeded_count == 4
    restarted.finalize()
    assert result.status is StepStatus.SUCCEEDED
    assert runner.invocations[DataTransformer.name] == (1 if recover else 4)
    members = load_execution_membership(delta, fs=fs, step_run_id=result.step_run_id)
    assert members.height == 4
    assert members.filter(pl.col("cache_hit")).height == (3 if recover else 0)
    assert set(members.filter(pl.col("cache_hit"))["execution_run_id"]) == set(
        recovered["execution_run_id"]
    )
    outputs = load_accepted_outputs(delta, fs=fs, step_run_id=result.step_run_id)
    assert outputs["artifact_id"].n_unique() == 4
    assert load_accepted_outputs(delta, fs=fs, step_run_id=source_step).is_empty()
    assert (
        original._step_tracker.current_state(source_step).status is StepStatus.CANCELLED
    )
    before = read_committed(delta, TablePath.EXECUTIONS, fs=fs)
    with PipelineManager.create(
        name="idempotent",
        **pipeline_env,
        recover_staging=recover,
        preserve_staging=preserve,
    ):
        assert read_committed(delta, TablePath.EXECUTIONS, fs=fs).equals(before)


@pytest.mark.parametrize("bypass", ["skip_cache", "noncacheable"])
def test_recovery_does_not_override_cache_bypass(
    pipeline_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    bypass: str,
) -> None:
    _, source_step, _ = _cancel_finished_work(pipeline_env, monkeypatch, preserve=False)
    if bypass == "noncacheable":
        monkeypatch.setattr(DataTransformer, "cacheable", False)
    runner = _InterruptedRunner()
    with PipelineManager.create(
        name="bypass",
        **pipeline_env,
        default_step_runner=runner,
        skip_cache=bypass == "skip_cache",
    ) as pipeline:
        generated = pipeline.run(DataGenerator, params={"count": 4, "seed": 91})
        result = pipeline.run(
            DataTransformer,
            inputs={"dataset": generated.output("datasets")},
            params={"seed": 52},
        )
    assert result.status is StepStatus.SUCCEEDED
    assert runner.invocations[DataTransformer.name] == 4
    fs = LocalFileSystem()
    assert (
        read_committed(pipeline_env["delta_root"], TablePath.EXECUTIONS, fs=fs)
        .filter(pl.col("step_run_id") == source_step)
        .height
        == 3
    )
    assert not load_execution_membership(
        pipeline_env["delta_root"], fs=fs, step_run_id=result.step_run_id
    )["cache_hit"].any()


@pytest.mark.parametrize(
    "boundary",
    [
        "before_seal",
        "sealed",
        "published",
        "planned",
        "table:artifacts/data",
        "table:artifacts/index",
        "table:orchestration/executions",
        "table:provenance/execution_edges",
        "table:orchestration/steps",
        "complete",
        "cleanup",
    ],
)
def test_process_death_at_persistence_boundary_recovers_exactly(
    pipeline_env: dict[str, str],
    boundary: str,
) -> None:
    import json
    import subprocess
    import sys
    import textwrap

    script = textwrap.dedent("""
        import os
        import sys
        sys.path.insert(0, sys.argv[1])
        import json
        from integration.test_staging_recovery import _InterruptedRunner
        from artisan.operations.examples import DataGenerator
        from artisan.orchestration import PipelineManager
        from artisan.orchestration.engine import step_executor
        from artisan.execution.recording import parquet_writer
        from artisan.storage.io import commit_plan
        from artisan.storage.io.commit import DeltaCommitter
        from fsspec.implementations.local import LocalFileSystem

        roots, boundary = json.loads(sys.argv[2]), sys.argv[3]
        def die(*args, **kwargs):
            os._exit(77)
        if boundary == 'before_seal':
            parquet_writer._write_execution_record = die
        elif boundary == 'sealed':
            step_executor.persist_worker_logs = die
        elif boundary == 'published':
            original = commit_plan.publish_commit_plan
            def publish(*args, **kwargs):
                original(*args, **kwargs)
                die()
            commit_plan.publish_commit_plan = publish
        elif boundary == 'cleanup':
            original_rm = LocalFileSystem.rm
            def remove(fs, path, *args, **kwargs):
                original_rm(fs, path, *args, **kwargs)
                if str(path).startswith(roots['staging_root']) and str(path).endswith('.parquet'):
                    die()
            LocalFileSystem.rm = remove
        else:
            def checkpoint(self, phase, plan, table):
                found = phase if table is None else phase + ':' + table
                if found == boundary:
                    die()
            DeltaCommitter._checkpoint = checkpoint
        with PipelineManager.create(name='killed', **roots, default_step_runner=_InterruptedRunner()) as pipeline:
            pipeline.run(DataGenerator, params={'count': 1, 'seed': 721})
        raise AssertionError('Expected persistence boundary was not reached')
    """)
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(Path(__file__).parents[1]),
            json.dumps(pipeline_env),
            boundary,
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert child.returncode == 77, child.stdout + child.stderr
    evidence = {
        path: path.read_bytes()
        for path in Path(pipeline_env["staging_root"]).rglob("*.parquet")
    }
    runner = _InterruptedRunner()
    fs = LocalFileSystem()
    with PipelineManager.create(
        name="after_death", **pipeline_env, default_step_runner=runner
    ) as pipeline:
        result = pipeline.run(DataGenerator, params={"count": 1, "seed": 721})
    assert result.status is StepStatus.SUCCEEDED
    assert runner.invocations[DataGenerator.name] == (
        1 if boundary == "before_seal" else 0
    )
    if boundary == "before_seal":
        assert evidence
        assert all(path.read_bytes() == data for path, data in evidence.items())
    else:
        assert all(not path.exists() for path in evidence)
    assert (
        read_committed(pipeline_env["delta_root"], TablePath.EXECUTIONS, fs=fs).height
        == 1
    )
    assert (
        load_accepted_outputs(
            pipeline_env["delta_root"], fs=fs, step_run_id=result.step_run_id
        ).height
        == 1
    )
    before = read_committed(pipeline_env["delta_root"], TablePath.EXECUTIONS, fs=fs)
    with PipelineManager.create(name="repeat", **pipeline_env):
        assert read_committed(
            pipeline_env["delta_root"], TablePath.EXECUTIONS, fs=fs
        ).equals(before)


def test_ninety_finished_units_leave_exactly_ten_to_execute(
    pipeline_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original, source_step, _ = _cancel_finished_work(
        pipeline_env,
        monkeypatch,
        preserve=False,
        count=100,
        finished=90,
    )
    runner = _InterruptedRunner()
    with PipelineManager.create(
        name="hundred", **pipeline_env, default_step_runner=runner
    ) as pipeline:
        generated = pipeline.run(DataGenerator, params={"count": 100, "seed": 91})
        result = pipeline.run(
            DataTransformer,
            inputs={"dataset": generated.output("datasets")},
            params={"seed": 52},
        )
    fs = LocalFileSystem()
    members = load_execution_membership(
        pipeline_env["delta_root"], fs=fs, step_run_id=result.step_run_id
    )
    assert result.status is StepStatus.SUCCEEDED
    assert runner.invocations[DataTransformer.name] == 10
    assert members.height == 100
    assert members.filter(pl.col("cache_hit")).height == 90
    assert (
        load_accepted_outputs(
            pipeline_env["delta_root"], fs=fs, step_run_id=result.step_run_id
        )["artifact_id"].n_unique()
        == 100
    )
    assert load_accepted_outputs(
        pipeline_env["delta_root"], fs=fs, step_run_id=source_step
    ).is_empty()
    assert (
        original._step_tracker.current_state(source_step).status is StepStatus.CANCELLED
    )


@pytest.mark.parametrize("preserve", [False, True])
def test_s3_recovery_retention_and_cache_reuse(s3_pipeline_env, monkeypatch, preserve):
    from artisan.schemas.orchestration.pipeline_config import PipelineConfig

    env = s3_pipeline_env
    roots = {
        key: env[key]
        for key in (
            "delta_root",
            "staging_root",
            "working_root",
            "files_root",
            "storage",
        )
    }
    first_runner = _InterruptedRunner()
    config = PipelineConfig(
        name="s3_source",
        pipeline_run_id="s3_source",
        default_step_runner=first_runner.name,
        preserve_staging=preserve,
        **roots,
    )
    source = PipelineManager(config, default_step_runner=first_runner)
    original_logs = step_executor.persist_worker_logs

    def cancel_before_plan(*args, **kwargs):
        original_logs(*args, **kwargs)
        source.cancel()

    with monkeypatch.context() as patcher:
        patcher.setattr(step_executor, "persist_worker_logs", cancel_before_plan)
        cancelled = source.run(DataGenerator, params={"count": 1, "seed": 839})
    source.finalize()
    assert cancelled.status is StepStatus.CANCELLED
    fs = env["fs"]
    evidence = {
        path: fs.cat_file(path)
        for path in fs.find(env["staging_root"])
        if path.endswith(".parquet")
    }
    assert evidence
    runner = _InterruptedRunner()
    restarted = PipelineManager(
        config.model_copy(
            update={"name": "s3_restart", "pipeline_run_id": "s3_restart"}
        ),
        default_step_runner=runner,
    )
    with restarted:
        result = restarted.run(DataGenerator, params={"count": 1, "seed": 839})
    assert result.status is StepStatus.SUCCEEDED
    assert runner.invocations[DataGenerator.name] == 0
    if preserve:
        assert all(fs.cat_file(path) == data for path, data in evidence.items())
    else:
        assert all(not fs.exists(path) for path in evidence)
    options = env["storage"].delta_storage_options()
    members = load_execution_membership(
        env["delta_root"],
        fs=fs,
        storage_options=options,
        step_run_id=result.step_run_id,
    )
    assert members.height == 1
    assert members["cache_hit"].all()
    assert load_accepted_outputs(
        env["delta_root"],
        fs=fs,
        storage_options=options,
        step_run_id=cancelled.step_run_id,
    ).is_empty()
