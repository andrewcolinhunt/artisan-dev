"""Replay restores ordered streams, grouping, and managed external input bytes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from artisan.operations.curator import Merge
from artisan.operations.examples import CsvPair, DataGenerator, FileGenerator
from artisan.orchestration import PipelineManager, StepStatus, replay_execution
from artisan.orchestration.engine.retained_execution_query import (
    read_retained_execution_membership,
)
from artisan.schemas import GroupByStrategy
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    ScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.core.retained_reader import RetainedReader

from .test_execution_replay import _input_ids, _manager, _scientific_source

pytestmark = pytest.mark.integration


def test_replay_keeps_unequal_independent_curator_streams(tmp_path: Path) -> None:
    with _manager(tmp_path) as manager:
        generated = manager.run(DataGenerator, params={"count": 3, "seed": 38})
        assert generated.status is StepStatus.SUCCEEDED, generated.error
        reader = RetainedReader(manager.committer)
        generated_source = _scientific_source(manager, generated.step_run_id)
        ids = [
            item.artifact.artifact_id
            for item in reader.read(generated_source.plan.receipt).output("datasets")
        ]
        inputs = {"left": [ids[2], ids[0], ids[2]], "right": [ids[1]]}
        step = manager.run(Merge, inputs=inputs)
        assert step.status is StepStatus.SUCCEEDED, step.error
        sources = [
            item.source
            for item in read_retained_execution_membership(
                manager.committer, pipeline_run_id=manager.config.pipeline_run_id
            )
            if item.state.step_run_id == step.step_run_id
        ]
        assert len(sources) == 1
        source = sources[0]
        assert isinstance(source, CuratorExecutionSource)
    result = replay_execution(
        source.execution_record["execution_run_id"],
        delta_root=manager.committer.delta_root,
        operation=Merge,
        working_root=tmp_path / "debug",
    )
    assert result.step_result.status is StepStatus.SUCCEEDED, result.step_result.error
    diagnostic = read_execution_source(manager.committer, result.execution_run_id)
    assert isinstance(diagnostic, CuratorExecutionSource)
    assert diagnostic.admission.inputs == source.admission.inputs
    assert {
        role.role: [item.artifact_id for item in role.members]
        for role in diagnostic.admission.inputs
    } == inputs


def test_replay_keeps_paired_inputs_in_one_scientific_unit(tmp_path: Path) -> None:
    with _manager(tmp_path) as manager:
        generated = manager.run(DataGenerator, params={"count": 2, "seed": 39})
        assert generated.status is StepStatus.SUCCEEDED, generated.error
        step = manager.run(
            CsvPair,
            inputs={
                "left": generated.output("datasets"),
                "right": generated.output("datasets"),
            },
            group_by=GroupByStrategy.ZIP,
            batch_strategy={"artifacts_per_unit": 2},
        )
        assert step.status is StepStatus.SUCCEEDED, step.error
        source = _scientific_source(manager, step.step_run_id)
    result = replay_execution(
        source.plan.receipt.delivery.delivery_id,
        delta_root=manager.committer.delta_root,
        operation=CsvPair,
        working_root=tmp_path / "debug",
    )
    assert result.step_result.status is StepStatus.SUCCEEDED, result.step_result.error
    diagnostic = read_execution_source(manager.committer, result.execution_run_id)
    assert isinstance(diagnostic, ScientificExecutionSource)
    assert diagnostic.admission.recipe == source.admission.recipe
    for role in ("left", "right"):
        assert _input_ids(diagnostic.admission.boundary, role) == _input_ids(
            source.admission.boundary, role
        )
    reader = RetainedReader(manager.committer)
    assert [
        item.artifact.artifact_id
        for item in reader.read(diagnostic.plan.receipt).output("pair")
    ] == [
        item.artifact.artifact_id
        for item in reader.read(source.plan.receipt).output("pair")
    ]


@pytest.mark.s3
def test_replay_preserves_cloud_files_binding_and_separate_staging(
    tmp_path: Path, s3_pipeline_env: dict[str, Any]
) -> None:
    env = s3_pipeline_env
    with PipelineManager.create(
        "replay-cloud",
        delta_root=env["delta_root"],
        staging_root=env["staging_root"],
        files_root=env["files_root"],
        working_root=env["working_root"],
        storage=env["storage"],
    ) as manager:
        generated = manager.run(
            FileGenerator, params={"count": 1, "file_size_bytes": 128, "seed": 40}
        )
        assert generated.status is StepStatus.SUCCEEDED, generated.error
        source = _scientific_source(manager, generated.step_run_id)
    original = {
        path: env["fs"].cat_file(path)
        for path in env["fs"].find(env["files_root"])
        if env["fs"].isfile(path)
    }
    assert original
    diagnostic_staging = env["uri_prefix"] + "/debug/staging"
    result = replay_execution(
        source.plan.receipt.delivery.delivery_id,
        delta_root=env["delta_root"],
        files_root=env["files_root"],
        storage=env["storage"],
        operation=FileGenerator,
        working_root=tmp_path / "debug",
        staging_root=diagnostic_staging,
    )
    assert result.step_result.status is StepStatus.SUCCEEDED, result.step_result.error
    assert result.diagnostic_status == "complete", result.diagnostic_errors
    assert result.diagnostic_roots["files_root"] == env["files_root"]
    assert result.diagnostic_roots["staging_root"].startswith(diagnostic_staging + "/")
    assert all(
        env["fs"].cat_file(path) == content for path, content in original.items()
    )
    diagnostic = read_execution_source(manager.committer, result.execution_run_id)
    assert isinstance(diagnostic, ScientificExecutionSource)
    assert env["fs"].isfile(diagnostic.plan.receipt.seal_uri)
    reader = RetainedReader(manager.committer)
    assert [
        item.artifact.artifact_id
        for item in reader.read(diagnostic.plan.receipt).output("files")
    ] == [
        item.artifact.artifact_id
        for item in reader.read(source.plan.receipt).output("files")
    ]
