"""Real-store replay preserves captured inputs while the source store evolves."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from artisan.operations.curator import Merge
from artisan.operations.examples import (
    DataGenerator,
    DataGeneratorWithMetrics,
    LargeFileGenerator,
    MetricCalculator,
)
from artisan.orchestration import (
    PipelineConfig,
    PipelineManager,
    StepResult,
    StepStatus,
    replay_execution,
)
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.storage.core.committed_scan import read_committed

from .test_multi_input import AssociatedMetricConsumer, DualInputLineage

pytestmark = pytest.mark.integration


def _manager(root: Path) -> PipelineManager:
    return PipelineManager.create(
        "replay-inputs",
        delta_root=str(root / "delta"),
        staging_root=str(root / "staging"),
        working_root=str(root / "work"),
    )


def _runtime(root: Path) -> RuntimeEnvironment:
    return RuntimeEnvironment(
        delta_root=str(root / "delta"),
        staging_root=str(root / "debug" / "staging"),
        working_root=str(root / "debug" / "work"),
        files_root=str(root / "debug" / "files"),
        failure_logs_root=str(root / "debug" / "logs"),
    )


def _execution(runtime: RuntimeEnvironment, step: StepResult) -> dict[str, Any]:
    assert step.status == StepStatus.SUCCEEDED
    rows = read_committed(
        runtime.delta_root,
        TablePath.EXECUTIONS,
        fs=runtime.storage.filesystem(),
        storage_options=runtime.storage.delta_storage_options(),
    ).filter(pl.col("step_run_id") == step.step_run_id)
    assert rows.height == 1
    return rows.row(0, named=True)


def _outputs(runtime: RuntimeEnvironment, execution_id: str) -> list[str]:
    edges = read_committed(
        runtime.delta_root,
        TablePath.EXECUTION_EDGES,
        fs=runtime.storage.filesystem(),
        storage_options=runtime.storage.delta_storage_options(),
    )
    return sorted(
        edges.filter(
            (pl.col("execution_run_id") == execution_id)
            & (pl.col("direction") == "output")
        )["artifact_id"].to_list()
    )


def test_replay_freezes_associations_before_new_descendants(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    with _manager(tmp_path) as manager:
        generated = manager.run(
            DataGeneratorWithMetrics, params={"count": 1, "seed": 37}, compact=False
        )
        consumed = manager.run(
            AssociatedMetricConsumer,
            inputs={"primary": generated.output("datasets")},
            compact=False,
        )
        source = _execution(runtime, consumed)
        original = ReplaySnapshot.model_validate_json(source["replay_snapshot"])
        assert original.associated_complete
        assert len(original.associated) == 1
        assert len(original.associated[0].artifact_ids) == 1

        manager.run(
            MetricCalculator,
            inputs={"dataset": generated.output("datasets")},
            compact=False,
        )
        current = manager.run(
            AssociatedMetricConsumer,
            inputs={"primary": generated.output("datasets")},
            skip_cache=True,
            compact=False,
        )
        refreshed = _execution(runtime, current)
        new_snapshot = ReplaySnapshot.model_validate_json(refreshed["replay_snapshot"])
        assert len(new_snapshot.associated[0].artifact_ids) == 2

    result = replay_execution(source["execution_run_id"], runtime=runtime)
    replayed = _execution(runtime, result.step_result)
    frozen = ReplaySnapshot.model_validate_json(replayed["replay_snapshot"])
    assert frozen.associated == original.associated
    assert frozen.inputs == original.inputs
    assert _outputs(runtime, replayed["execution_run_id"]) == _outputs(
        runtime, source["execution_run_id"]
    )
    assert _outputs(runtime, refreshed["execution_run_id"]) != _outputs(
        runtime, source["execution_run_id"]
    )


def test_replay_keeps_unequal_independent_streams_and_occurrences(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    with _manager(tmp_path) as manager:
        generated = manager.run(
            DataGenerator, params={"count": 3, "seed": 38}, compact=False
        )
        generated_row = _execution(runtime, generated)
        ids = _outputs(runtime, generated_row["execution_run_id"])
        inputs = {"left": [ids[2], ids[0], ids[2]], "right": [ids[1]]}
        merged = manager.run(Merge, inputs=inputs, compact=False)
        source = _execution(runtime, merged)
    original = ReplaySnapshot.model_validate_json(source["replay_snapshot"])
    assert original.group_ids is None
    assert {
        role: [entry.artifact_id for entry in occurrences]
        for role, occurrences in original.inputs.items()
    } == inputs

    result = replay_execution(source["execution_run_id"], runtime=runtime)
    replayed = _execution(runtime, result.step_result)
    snapshot = ReplaySnapshot.model_validate_json(replayed["replay_snapshot"])
    assert snapshot.inputs == original.inputs
    assert snapshot.group_ids is None
    assert replayed["execution_spec_id"] == source["execution_spec_id"]
    assert _outputs(runtime, replayed["execution_run_id"]) == ids


def test_replay_retains_paired_groups_in_one_unit(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    with _manager(tmp_path) as manager:
        generated = manager.run(
            DataGenerator, params={"count": 2, "seed": 39}, compact=False
        )
        metrics = manager.run(
            MetricCalculator,
            inputs={"dataset": generated.output("datasets")},
            compact=False,
        )
        paired = manager.run(
            DualInputLineage,
            inputs={
                "primary": generated.output("datasets"),
                "secondary": metrics.output("metrics"),
            },
            batch_strategy={"artifacts_per_unit": 2},
            compact=False,
        )
        source = _execution(runtime, paired)
    original = ReplaySnapshot.model_validate_json(source["replay_snapshot"])
    assert original.group_ids is not None
    assert len(set(original.group_ids)) == 2
    assert len(original.inputs["primary"]) == len(original.inputs["secondary"]) == 2

    result = replay_execution(source["execution_run_id"], runtime=runtime)
    replayed = _execution(runtime, result.step_result)
    snapshot = ReplaySnapshot.model_validate_json(replayed["replay_snapshot"])
    assert snapshot.inputs == original.inputs
    assert snapshot.group_ids == original.group_ids
    assert replayed["execution_spec_id"] == source["execution_spec_id"]
    assert _outputs(runtime, replayed["execution_run_id"]) == _outputs(
        runtime, source["execution_run_id"]
    )


def test_replay_preserves_cloud_staging_and_external_files(
    tmp_path: Path, s3_pipeline_env: dict[str, Any]
) -> None:
    env = s3_pipeline_env
    prefix = env["uri_prefix"] + "/debug"
    runtime = RuntimeEnvironment(
        delta_root=env["delta_root"],
        staging_root=prefix + "/staging",
        files_root=prefix + "/files",
        working_root=str(tmp_path / "debug" / "work"),
        failure_logs_root=str(tmp_path / "debug" / "logs"),
        storage=env["storage"],
    )
    with PipelineManager(
        PipelineConfig(
            name="replay-cloud",
            pipeline_run_id="replay-cloud-source",
            delta_root=env["delta_root"],
            staging_root=env["staging_root"],
            files_root=env["files_root"],
            working_root=env["working_root"],
            storage=env["storage"],
        )
    ) as manager:
        generated = manager.run(
            LargeFileGenerator,
            params={"count": 1, "file_size_bytes": 128, "seed": 40},
            compact=False,
        )
        source = _execution(runtime, generated)
    source_files = {
        path: env["fs"].cat_file(path)
        for path in env["fs"].find(env["files_root"])
        if env["fs"].isfile(path)
    }
    assert source_files

    result = replay_execution(source["execution_run_id"], runtime=runtime)
    replayed = _execution(runtime, result.step_result)
    assert result.diagnostic_status == "complete"
    assert _outputs(runtime, replayed["execution_run_id"]) == _outputs(
        runtime, source["execution_run_id"]
    )
    assert all(env["fs"].cat_file(path) == data for path, data in source_files.items())
    staging = result.diagnostic_roots["staging_root"]
    files = result.diagnostic_roots["files_root"]
    assert staging is not None
    assert staging.startswith(prefix + "/staging/")
    assert files is not None
    assert files.startswith(prefix + "/files/")
    assert any(path.endswith("executions.parquet") for path in env["fs"].find(staging))
    preserved = [
        env["fs"].cat_file(path)
        for path in env["fs"].find(files)
        if env["fs"].isfile(path)
    ]
    assert list(source_files.values()) == preserved
