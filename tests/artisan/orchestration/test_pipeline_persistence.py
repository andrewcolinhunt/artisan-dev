"""Public pipelines persist exact lifecycle history and content-based reuse."""

from __future__ import annotations

import json
from enum import StrEnum, auto
from pathlib import Path
from typing import ClassVar

import polars as pl
from fixtures.retained_pipeline import NativePipelineRunner
from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore


class RepeatableSource(OperationDefinition):
    """A changed parameter deliberately produces the same scientific content."""

    name: ClassVar[str] = "repeatable_source"

    class OutputRole(StrEnum):
        data = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "data": OutputSpec(artifact_type="csv", derives_from={"inputs": []})
    }

    class Params(BaseModel):
        seed: int = Field(
            default=42, description="Parameter deliberately ignored by this fixture."
        )

    params: Params = Params()

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        return ArtifactResult(
            artifacts={
                "data": [
                    CSVArtifact.draft(
                        b"value\n1\n", "source.csv", step_number=step_number
                    )
                ]
            },
            lineage={"data": []},
        )


def _create(root: Path) -> PipelineManager:
    return PipelineManager.create(
        name="persistent-pipeline",
        delta_root=str(root / "delta"),
        staging_root=str(root / "staging"),
        working_root=str(root / "working"),
    )


def test_run_writes_exact_pending_running_terminal_history(tmp_path: Path) -> None:
    with _create(tmp_path) as pipeline:
        result = pipeline.run(
            RepeatableSource, step_runner=NativePipelineRunner(pipeline.committer)
        )
        assert result.status == StepStatus.SUCCEEDED, result.error
        persisted = pipeline.tracker.current_state(result.step_run_id).to_step_result()
        assert persisted.model_dump() == result.model_dump()
        reference = result.output("data")
        assert reference.source.store == pipeline.committer.store_identity
        assert reference.source.step_run_id == result.step_run_id
        assert (
            len(
                read_current_step_outputs(pipeline.committer, result.step_run_id)[
                    0
                ].occurrences
            )
            == 1
        )
    rows = pl.read_delta(tmp_path / "delta/orchestration/steps").sort("state_sequence")
    assert rows["status"].to_list() == ["pending", "running", "succeeded"]
    assert rows["state_sequence"].to_list() == [0, 1, 2]


def test_run_cache_hit_has_new_identity_and_original_accepted_outputs(
    tmp_path: Path,
) -> None:
    with _create(tmp_path) as original:
        source = original.run(
            RepeatableSource, step_runner=NativePipelineRunner(original.committer)
        )
        accepted = read_current_step_outputs(original.committer, source.step_run_id)
    with _create(tmp_path) as current:
        runner = NativePipelineRunner(current.committer)
        result = current.run(RepeatableSource, step_runner=runner)
        assert result.status == StepStatus.SUCCEEDED, result.error
        assert result.disposition == StepDisposition.CACHE_HIT
        assert runner.curator_launches == 0
        assert result.step_run_id != source.step_run_id
        assert (
            read_current_step_outputs(current.committer, result.step_run_id) == accepted
        )
        state = current.tracker.current_state(result.step_run_id)
        assert state.to_step_result().model_dump() == result.model_dump()
        assert result.output("data").source.step_run_id == result.step_run_id
        options = json.loads(state.compute_options_json)
        assert options["pipeline_default_step_runner"] == "local"
        assert state.compute_backend == runner.name


def test_same_content_from_changed_producer_does_not_reuse_curator_membership(
    tmp_path: Path,
) -> None:
    with _create(tmp_path) as first:
        runner = NativePipelineRunner(first.committer)
        source = first.run(RepeatableSource, step_runner=runner)
        selected = first.run(
            Merge, inputs={"source": source.output("data")}, step_runner=runner
        )
        assert selected.status == StepStatus.SUCCEEDED, selected.error
        accepted = read_current_step_outputs(first.committer, selected.step_run_id)
    with _create(tmp_path) as second:
        runner = NativePipelineRunner(second.committer)
        changed = second.run(RepeatableSource, params={"seed": 99}, step_runner=runner)
        assert changed.disposition == StepDisposition.EXECUTED
        assert runner.curator_launches == 1
        reused = second.run(
            Merge, inputs={"source": changed.output("data")}, step_runner=runner
        )
        assert reused.status == StepStatus.SUCCEEDED, reused.error
        assert reused.disposition == StepDisposition.EXECUTED
        assert runner.curator_launches == 2
        current = read_current_step_outputs(second.committer, reused.step_run_id)
        assert [item.artifact.artifact_id for item in current[0].occurrences] == [
            item.artifact.artifact_id for item in accepted[0].occurrences
        ]
        assert (
            current[0].occurrences[0].reference != accepted[0].occurrences[0].reference
        )
        exact_retry = second.run(
            Merge, inputs={"source": changed.output("data")}, step_runner=runner
        )
        assert exact_retry.disposition == StepDisposition.CACHE_HIT, exact_retry.error
        assert runner.curator_launches == 2
        assert (
            read_current_step_outputs(second.committer, exact_retry.step_run_id)
            == current
        )


def test_pipeline_run_ids_are_generated_and_distinct(tmp_path: Path) -> None:
    with _create(tmp_path) as first, _create(tmp_path) as second:
        assert first.config.pipeline_run_id.startswith("persistent-pipeline_")
        assert second.config.pipeline_run_id != first.config.pipeline_run_id


def test_finalize_collects_pending_future_and_closes_ownership(tmp_path: Path) -> None:
    with _create(tmp_path) as pipeline:
        future = pipeline.submit(
            RepeatableSource, step_runner=NativePipelineRunner(pipeline.committer)
        )
        summary = pipeline.finalize()
        assert future.result().status == StepStatus.SUCCEEDED
        assert summary["total_steps"] == 1
        assert summary["overall_success"] is True
        assert pipeline.ownership_state() == OwnershipState.CLOSED
        assert pipeline.finalize() == summary
