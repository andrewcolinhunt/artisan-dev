"""Dynamic curator results survive accepted publication, reuse and reopening."""

from __future__ import annotations

import json
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
from fixtures.retained_pipeline import NativePipelineRunner
from fixtures.run_outputs import commit_outputs
from pydantic import BaseModel, Field

from artisan.errors import PersistenceIntegrityError, StoreIntegrityError
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator.ingest_pipeline_step import IngestPipelineStep
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.io.curator_plan import CuratorCommitPlan, read_curator_plan
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan


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

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        artifacts: list[Artifact] = (
            []
            if self.params.empty
            else [
                CSVArtifact.draft(b"value\n1\n", "result.csv", step_number=step_number)
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
            artifact_type="csv", required=False, derives_from={"inputs": []}
        )
        for role in ("found", "empty")
    }


def _create(root: Path) -> PipelineManager:
    return PipelineManager.create(
        name="dynamic",
        delta_root=str(root / "delta"),
        staging_root=str(root / "staging"),
        working_root=str(root / "working"),
        recover_staging=False,
    )


def _run(
    pipeline: PipelineManager,
    operation: type[OperationDefinition] = DynamicCurator,
    **kwargs: Any,
) -> StepResult:
    return pipeline.run(
        operation, step_runner=NativePipelineRunner(pipeline.committer), **kwargs
    )


def _resume(pipeline: PipelineManager) -> PipelineManager:
    return PipelineManager.resume(
        delta_root=pipeline.config.delta_root,
        staging_root=pipeline.config.staging_root,
        working_root=pipeline.config.working_root,
        pipeline_run_id=pipeline.config.pipeline_run_id,
        recover_staging=False,
    )


@pytest.mark.parametrize(
    ("empty", "heterogeneous", "expected"),
    [
        (False, False, {"found": "csv"}),
        (False, True, {"found": ArtifactTypes.ANY}),
        (True, False, {}),
    ],
)
def test_dynamic_result_commit_reopen_resume(
    tmp_path: Path, empty: bool, heterogeneous: bool, expected: dict[str, str]
) -> None:
    with _create(tmp_path) as pipeline:
        result = _run(pipeline, params={"empty": empty, "heterogeneous": heterogeneous})
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.output_types == expected
        assert result.output_roles == frozenset(expected)
        assert result.metadata["authored"] == {"selection": ["exact-source"]}
        persisted = pipeline.tracker.current_state(result.step_run_id).to_step_result()
        assert persisted.model_dump() == result.model_dump()
        accepted = read_current_step_outputs(pipeline.committer, result.step_run_id)
        assert {group.role for group in accepted} == set(expected)
    rows = pl.read_delta(tmp_path / "delta/orchestration/steps").sort("state_sequence")
    assert all(
        json.loads(value)["dynamic_output_roles"]
        for value in rows["compute_options_json"]
    )
    assert rows["output_roles_json"].to_list()[:2] == ["[]", "[]"]
    with _resume(pipeline) as resumed:
        assert resumed[0] == result
        assert (
            read_current_step_outputs(resumed.committer, result.step_run_id) == accepted
        )


@pytest.mark.parametrize("empty", [False, True])
def test_dynamic_cache_preserves_metadata_and_exact_source(
    tmp_path: Path, empty: bool
) -> None:
    with _create(tmp_path) as pipeline:
        original = _run(pipeline, params={"empty": empty})
        source = read_curator_plan(
            pipeline.committer.delta_root, original.step_run_id, pipeline.committer.fs
        )
        assert isinstance(source, CuratorCommitPlan)
    with _create(tmp_path) as pipeline:
        runner = NativePipelineRunner(pipeline.committer)
        reused = pipeline.run(
            DynamicCurator, params={"empty": empty}, step_runner=runner
        )
        assert reused.status == StepStatus.SUCCEEDED, reused.error
        assert reused.disposition == StepDisposition.CACHE_HIT
        assert runner.curator_launches == 0
        assert reused.output_types == original.output_types
        assert reused.metadata.get("authored") == original.metadata["authored"]
        assert reused.step_run_id != original.step_run_id
        plan = read_curator_plan(
            pipeline.committer.delta_root, reused.step_run_id, pipeline.committer.fs
        )
        assert isinstance(plan, CuratorReusePlan)
        assert plan.source.step_run_id == original.step_run_id
        assert plan.source.plan_digest == source.plan_digest
        execution = source.table(TablePath.EXECUTIONS.value)
        assert execution is not None
        assert plan.source.execution_run_id == execution.row_keys[0][0]
        assert read_current_step_outputs(
            pipeline.committer, reused.step_run_id
        ) == read_current_step_outputs(pipeline.committer, original.step_run_id)
        persisted = pipeline.tracker.current_state(reused.step_run_id).to_step_result()
        assert persisted.model_dump() == reused.model_dump()


def test_static_curator_retains_empty_roles_and_metadata(tmp_path: Path) -> None:
    with _create(tmp_path) as pipeline:
        result = _run(pipeline, StaticCurator, params={"empty": True})
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.output_types == {"found": "csv", "empty": "csv"}
        assert result.metadata["authored"] == {"selection": ["exact-source"]}
        state = pipeline.tracker.current_state(result.step_run_id)
        assert json.loads(state.compute_options_json)["dynamic_output_roles"] is False


@pytest.mark.parametrize(
    "key", ["timings", "skip_reason", "status", "cancelled", "skipped"]
)
def test_metadata_collisions_fail_before_worker_timings(
    tmp_path: Path, key: str
) -> None:
    with _create(tmp_path) as pipeline:
        result = _run(pipeline, params={"reserved": key})
        assert result.status == StepStatus.FAILED
        assert result.error is not None
        assert "reserved framework keys" in result.error
        assert result.output_types == {}
        assert read_current_step_outputs(pipeline.committer, result.step_run_id) == ()
        plan = read_curator_plan(
            pipeline.committer.delta_root, result.step_run_id, pipeline.committer.fs
        )
        assert isinstance(plan, CuratorCommitPlan)
        with pipeline.committer._read_snapshot() as read:
            read.verify_effect(plan, TablePath.EXECUTIONS.value)
            execution = read.effect_row(plan, TablePath.EXECUTIONS.value)
        assert execution["success"] is False
        assert "collision" not in (execution["metadata"] or "")


def test_ingest_public_result_preserves_exact_source_metadata(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source_step, _ = commit_outputs(
        str(source),
        artifacts=[
            CSVArtifact.draft(b"source", "source.csv", step_number=0).finalize()
        ],
    )
    with _create(tmp_path) as pipeline:
        result = _run(
            pipeline,
            IngestPipelineStep,
            params={
                "source_delta_root": str(source),
                "source_files_root": str(tmp_path / "files"),
                "source_run_id": "source-run",
                "source_step": 0,
            },
        )
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.output_types == {"csv": "csv"}
        assert result.metadata["ingest_source"] == {
            "pipeline_run_id": "source-run",
            "source_step": 0,
            "include_prior_steps": False,
            "step_run_ids": [source_step],
        }
        persisted = pipeline.tracker.current_state(result.step_run_id).to_step_result()
        assert persisted.model_dump() == result.model_dump()
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
    tmp_path: Path, field: str, value: str
) -> None:
    with _create(tmp_path) as pipeline:
        result = _run(pipeline)
    path = tmp_path / "delta/orchestration/steps"
    rows = pl.read_delta(path).with_columns(
        pl.when(pl.col("status") == "succeeded")
        .then(pl.lit(value))
        .otherwise(pl.col(field))
        .alias(field)
    )
    rows.write_delta(path, mode="overwrite")
    with pytest.raises((PersistenceIntegrityError, StoreIntegrityError)):
        pipeline.tracker.current_state(result.step_run_id)
    with pytest.raises((PersistenceIntegrityError, StoreIntegrityError)):
        _resume(pipeline)


@pytest.mark.parametrize("change", ["role", "type", "metadata"])
def test_dynamic_summary_must_match_seal_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    original = CuratorResults.finish

    def forged(
        self: CuratorResults,
        reference: CuratorAdmissionReference,
        execution_run_id: str,
        result: StepResult,
        **kwargs: Any,
    ) -> StepResult:
        updates = {
            "role": {
                "output_roles": frozenset({"invented"}),
                "output_types": {"invented": "csv"},
            },
            "type": {"output_types": {"found": "metric"}},
            "metadata": {"metadata": {"authored": "forged"}},
        }[change]
        return original(
            self,
            reference,
            execution_run_id,
            result.model_copy(update=updates),
            **kwargs,
        )

    monkeypatch.setattr(CuratorResults, "finish", forged)
    with _create(tmp_path) as pipeline:
        result = _run(pipeline)
        assert result.status == StepStatus.FAILED
        assert not result.output_roles
        assert read_current_step_outputs(pipeline.committer, result.step_run_id) == ()


def test_completed_dynamic_output_feeds_downstream_before_and_after_resume(
    tmp_path: Path,
) -> None:
    with _create(tmp_path) as pipeline:
        result = _run(pipeline)
        merged = _run(pipeline, Merge, inputs={"source": result.output("found")})
        assert merged.status == StepStatus.SUCCEEDED, merged.error
        assert merged.succeeded_count == 1
    with _resume(pipeline) as resumed:
        restored = resumed[0]
        assert restored == result
        merged = _run(resumed, Merge, inputs={"source": restored.output("found")})
        assert merged.status == StepStatus.SUCCEEDED, merged.error
        assert merged.succeeded_count == 1


@pytest.mark.parametrize("operation", [DynamicCurator, StaticCurator])
def test_candidate_cannot_change_dynamic_marker_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: type[OperationDefinition],
) -> None:
    original = RetainedStepTracker._terminal_candidate

    def forge_marker(
        self: RetainedStepTracker, *args: Any, **kwargs: Any
    ) -> pl.DataFrame:
        candidate = original(self, *args, **kwargs)
        options = json.loads(candidate["compute_options_json"][0])
        options["dynamic_output_roles"] = not options["dynamic_output_roles"]
        return candidate.with_columns(
            pl.lit(json.dumps(options)).alias("compute_options_json")
        )

    monkeypatch.setattr(RetainedStepTracker, "_terminal_candidate", forge_marker)
    pipeline = _create(tmp_path)
    try:
        with pytest.raises(PersistenceIntegrityError, match="changed owner metadata"):
            _run(pipeline, operation)
        step_run_id = pipeline.executions[-1].record.step_run_id
        assert (
            read_curator_plan(
                pipeline.committer.delta_root, step_run_id, pipeline.committer.fs
            )
            is None
        )
        with pytest.raises(StoreIntegrityError, match="not terminal"):
            read_current_step_outputs(pipeline.committer, step_run_id)
    finally:
        pipeline.close()
