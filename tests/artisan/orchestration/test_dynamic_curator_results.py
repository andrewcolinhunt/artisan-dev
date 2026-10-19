"""Dynamic curator results survive real commitment, cache reuse and reopening."""

from __future__ import annotations

import json
from enum import StrEnum, auto
from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest
from fixtures.run_outputs import commit_outputs
from pydantic import BaseModel, Field

from artisan.errors import PersistenceIntegrityError
from artisan.execution.executors.curator import run_curator_flow
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator.ingest_pipeline_step import IngestPipelineStep
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.committed_scan import read_committed
from artisan.storage.core.run_scope import load_accepted_outputs
from artisan.storage.io.commit import DeltaCommitter


class DynamicCurator(OperationDefinition):
    name: ClassVar[str] = "dynamic_results"

    class Params(BaseModel):
        empty: bool = Field(default=False, description="Emit no artifacts.")
        heterogeneous: bool = Field(
            default=False, description="Emit multiple artifact types."
        )
        reserved: str | None = Field(
            default=None, description="Inject a metadata collision."
        )

    params: Params = Params()

    def execute_curator(self, inputs, step_number, artifact_store):
        artifacts = (
            []
            if self.params.empty
            else [
                DataArtifact.draft(b"value\n1\n", "result.csv", step_number=step_number)
            ]
        )
        if self.params.heterogeneous:
            artifacts.append(
                MetricArtifact.draft(
                    {"score": 2}, "metric.json", step_number=step_number
                )
            )
        return ArtifactResult(
            artifacts={"found": artifacts, "empty": []},
            lineage={"found": [], "empty": []},
            metadata={
                "authored": {"selection": ["exact-source"]},
                **({self.params.reserved: "collision"} if self.params.reserved else {}),
            },
        )


class StaticCurator(DynamicCurator):
    name: ClassVar[str] = "static_results"

    class OutputRole(StrEnum):
        found = auto()
        empty = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        role: OutputSpec(
            artifact_type="data", required=False, derives_from={"inputs": []}
        )
        for role in ("found", "empty")
    }


@pytest.fixture(autouse=True)
def inline_curator(monkeypatch):
    # Keep the actual worker, seals, and commit path; avoid spawning for these tests.
    monkeypatch.setattr(
        "artisan.orchestration.engine.step_executor._run_curator_in_subprocess",
        lambda unit, runtime, cancel_event, **kwargs: run_curator_flow(unit, runtime),
    )


def _config(root: Path, run: str = "dynamic-run") -> PipelineConfig:
    return PipelineConfig(
        name="dynamic",
        pipeline_run_id=run,
        delta_root=str(root / "delta"),
        staging_root=str(root / "staging"),
        working_root=str(root / "working"),
        recover_staging=False,
    )


@pytest.mark.parametrize(
    ("empty", "heterogeneous", "expected"),
    [
        (False, False, {"found": "data"}),
        (False, True, {"found": ArtifactTypes.ANY}),
        (True, False, {}),
    ],
)
def test_dynamic_result_commit_reopen_resume(tmp_path, empty, heterogeneous, expected):
    config = _config(tmp_path)
    with PipelineManager(config) as pipeline:
        result = pipeline.run(
            DynamicCurator,
            params={"empty": empty, "heterogeneous": heterogeneous},
            compact=False,
        )
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.output_types == expected
        assert result.output_roles == frozenset(expected)
        assert result.metadata["authored"] == {"selection": ["exact-source"]}
        state = pipeline._step_tracker.current_state(result.step_run_id)
        assert state.to_step_result() == result
        accepted = load_accepted_outputs(
            config.delta_root,
            fs=config.storage.filesystem(),
            step_run_id=result.step_run_id,
        )
        assert set(accepted["role"]) == set(expected)
    rows = pl.read_delta(str(tmp_path / "delta/orchestration/steps")).sort(
        "state_sequence"
    )
    assert all(
        json.loads(value)["dynamic_output_roles"] is True
        for value in rows["compute_options_json"]
    )
    assert rows["output_roles_json"].to_list()[:2] == ["[]", "[]"]
    # Reopening relies on persisted evidence, even when the stored class is not importable.
    rows.with_columns(
        pl.lit("missing.module.Operation").alias("operation_class")
    ).write_delta(str(tmp_path / "delta/orchestration/steps"), mode="overwrite")
    with PipelineManager.resume(
        delta_root=config.delta_root,
        staging_root=config.staging_root,
        recover_staging=False,
    ) as resumed:
        assert resumed._step_results[0] == result


@pytest.mark.parametrize("whole_step", [True, False])
def test_dynamic_cache_preserves_metadata_and_exact_source(
    tmp_path, monkeypatch, whole_step
):
    config = _config(tmp_path)
    with PipelineManager(config) as pipeline:
        original = pipeline.run(DynamicCurator, compact=False)
    with PipelineManager(_config(tmp_path, "reuse-run")) as pipeline:
        if not whole_step:
            monkeypatch.setattr(pipeline._step_tracker, "check_cache", lambda *a: None)
        monkeypatch.setattr(
            "artisan.orchestration.engine.step_executor._run_curator_in_subprocess",
            lambda *args, **kwargs: pytest.fail("cached curator must not run"),
        )
        reused = pipeline.run(DynamicCurator, compact=False)
        assert reused.status == StepStatus.SUCCEEDED, reused.error
        assert reused.disposition == StepDisposition.CACHE_HIT
        assert reused.output_types == original.output_types
        assert reused.metadata.get("authored") == original.metadata["authored"]
        assert reused.step_run_id != original.step_run_id
        evidence = read_committed(
            config.delta_root, TablePath.EXECUTIONS, fs=config.storage.filesystem()
        )
        assert evidence.height == 1
        relations = read_committed(
            config.delta_root, TablePath.CACHE_REUSE, fs=config.storage.filesystem()
        )
        assert (
            relations["cached_execution_run_id"].to_list()
            == evidence["execution_run_id"].to_list()
        )
        assert (
            pipeline._step_tracker.current_state(reused.step_run_id).to_step_result()
            == reused
        )


def test_static_curator_retains_empty_roles_and_metadata(tmp_path):
    with PipelineManager(_config(tmp_path)) as pipeline:
        result = pipeline.run(StaticCurator, params={"empty": True}, compact=False)
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.output_types == {"found": "data", "empty": "data"}
        assert result.metadata["authored"] == {"selection": ["exact-source"]}
        state = pipeline._step_tracker.current_state(result.step_run_id)
        assert json.loads(state.compute_options_json)["dynamic_output_roles"] is False


@pytest.mark.parametrize(
    "key", ["timings", "skip_reason", "status", "cancelled", "skipped"]
)
def test_metadata_collisions_fail_before_worker_timings(tmp_path, key):
    with PipelineManager(_config(tmp_path)) as pipeline:
        result = pipeline.run(DynamicCurator, params={"reserved": key}, compact=False)
        assert result.status == StepStatus.FAILED
        assert "reserved framework keys" in result.error
        assert result.output_types == {}
        execution = read_committed(
            pipeline.config.delta_root,
            TablePath.EXECUTIONS,
            fs=pipeline.config.storage.filesystem(),
        ).row(0, named=True)
        assert execution["success"] is False
        assert "collision" not in (execution["metadata"] or "")


def test_ingest_public_result_preserves_exact_source_metadata(tmp_path):
    source = tmp_path / "source"
    source_step, _ = commit_outputs(
        str(source),
        artifacts=[
            DataArtifact.draft(b"source", "source.csv", step_number=0).finalize()
        ],
    )
    with PipelineManager(_config(tmp_path)) as pipeline:
        result = pipeline.run(
            IngestPipelineStep,
            params={
                "source_delta_root": str(source),
                "source_run_id": "source-run",
                "source_step": 0,
            },
            compact=False,
        )
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.output_types == {"data": "data"}
        assert result.metadata["ingest_source"] == {
            "pipeline_run_id": "source-run",
            "source_step": 0,
            "include_prior_steps": False,
            "step_run_ids": [source_step],
        }
        assert (
            pipeline._step_tracker.current_state(result.step_run_id).to_step_result()
            == result
        )
        assert IngestPipelineStep.cacheable is False


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("output_roles_json", '["invented"]'),
        ("output_roles_json", '["found", "found"]'),
        ("output_roles_json", "[1]"),
        ("output_roles_json", "{}"),
        ("output_types_json", '{"found": "metric"}'),
        ("metadata", '{"authored": "changed"}'),
    ],
)
def test_reopen_rejects_dynamic_payload_not_matching_committed_evidence(
    tmp_path, field, value
):
    config = _config(tmp_path)
    with PipelineManager(config) as pipeline:
        result = pipeline.run(DynamicCurator, compact=False)
    path = str(tmp_path / "delta/orchestration/steps")
    rows = pl.read_delta(path).with_columns(
        pl.when(pl.col("status") == "succeeded")
        .then(pl.lit(value))
        .otherwise(pl.col(field))
        .alias(field)
    )
    rows.write_delta(path, mode="overwrite")
    with pytest.raises(PersistenceIntegrityError, match="invalid dynamic result"):
        StepTracker(config.delta_root).current_state(result.step_run_id)


def test_commit_failure_never_exposes_dynamic_outputs(tmp_path, monkeypatch):
    config = _config(tmp_path)

    def fail(boundary, plan, table_path):
        if boundary == "table" and table_path == TablePath.STEPS.value:
            msg = "incomplete logical commit"
            raise RuntimeError(msg)

    monkeypatch.setattr(DeltaCommitter, "_checkpoint", staticmethod(fail))
    with PipelineManager(config) as pipeline:
        result = pipeline.run(DynamicCurator, compact=False)
        assert result.status == StepStatus.FAILED
        assert not result.output_roles
        accepted = load_accepted_outputs(
            config.delta_root,
            fs=config.storage.filesystem(),
            step_run_id=result.step_run_id,
        )
        assert accepted.is_empty()


@pytest.mark.parametrize("change", ["role", "type", "metadata"])
def test_dynamic_summary_must_match_plan_before_publication(
    tmp_path, monkeypatch, change
):
    from artisan.orchestration.engine import step_executor

    original = step_executor._curator_summary

    def forged(*args, **kwargs):
        types, metadata = original(*args, **kwargs)
        if change == "role":
            types = {"invented": "data"}
        elif change == "type":
            types = {"found": "metric"}
        else:
            metadata = {"authored": "forged"}
        return types, metadata

    monkeypatch.setattr(step_executor, "_curator_summary", forged)
    config = _config(tmp_path)
    with PipelineManager(config) as pipeline:
        result = pipeline.run(DynamicCurator, compact=False)
        assert result.status == StepStatus.FAILED
        assert "invalid dynamic result" in result.error
        assert not result.output_roles
        assert read_committed(
            config.delta_root, TablePath.EXECUTIONS, fs=config.storage.filesystem()
        ).is_empty()
        assert not (
            tmp_path
            / "delta/_artisan/commit_plans"
            / result.step_run_id
            / "step_result.json"
        ).exists()


def test_completed_dynamic_output_feeds_downstream_before_and_after_resume(tmp_path):
    config = _config(tmp_path)
    with PipelineManager(config) as pipeline:
        result = pipeline.run(DynamicCurator, compact=False)
        merged = pipeline.run(
            Merge, inputs={"source": result.output("found")}, compact=False
        )
        assert merged.status == StepStatus.SUCCEEDED, merged.error
        assert merged.succeeded_count == 1
    with PipelineManager.resume(
        delta_root=config.delta_root,
        staging_root=config.staging_root,
        recover_staging=False,
    ) as pipeline:
        restored = pipeline._step_results[0]
        assert restored == result
        merged = pipeline.run(
            Merge, inputs={"source": restored.output("found")}, compact=False
        )
        assert merged.status == StepStatus.SUCCEEDED, merged.error
        assert merged.succeeded_count == 1


@pytest.mark.parametrize("operation", [DynamicCurator, StaticCurator])
def test_candidate_cannot_change_dynamic_marker_before_publication(
    tmp_path, monkeypatch, operation
):
    original = StepTracker.prepare_terminal_candidate

    def forge_marker(self, *args, **kwargs):
        candidate = original(self, *args, **kwargs)
        options = json.loads(candidate["compute_options_json"][0])
        options["dynamic_output_roles"] = not options["dynamic_output_roles"]
        return candidate.with_columns(
            pl.lit(json.dumps(options)).alias("compute_options_json")
        )

    monkeypatch.setattr(StepTracker, "prepare_terminal_candidate", forge_marker)
    config = _config(tmp_path)
    with PipelineManager(config) as pipeline:
        result = pipeline.run(operation, compact=False)
        assert result.status == StepStatus.FAILED
        assert "changed owner metadata" in result.error
        assert not result.output_roles
        assert read_committed(
            config.delta_root, TablePath.EXECUTIONS, fs=config.storage.filesystem()
        ).is_empty()
        assert not (
            tmp_path
            / "delta/_artisan/commit_plans"
            / result.step_run_id
            / "step_result.json"
        ).exists()
