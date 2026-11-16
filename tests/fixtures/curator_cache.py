"""Observed curator execution shared by cache lookup and reuse lifecycle tests."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import ClassVar

import polars as pl
from pydantic import BaseModel, ConfigDict

from artisan.execution.executors.curator import run_curator_flow
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.schemas import InputSpec, MetricArtifact, OutputSpec
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.storage.cache.curator_dependencies import CuratorReadRecorder
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_view import CuratorView

STAMPS = {"software_digest": "e" * 64}


class DynamicCurator(OperationDefinition):
    """An ordinary query-free curator with dynamic populated or empty outputs."""

    name: ClassVar[str] = "observed_dynamic"

    class Params(BaseModel):
        """Fixture output controls.

        Attributes:
            empty: Emit no populated output role.
            fail: Return a scientific failure.
        """

        model_config = ConfigDict(strict=True)
        empty: bool = False
        fail: bool = False

    params: Params = Params()

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        if self.params.fail:
            return ArtifactResult(success=False, error="synthetic science failure")
        return ArtifactResult(
            artifacts={
                "found": []
                if self.params.empty
                else [CSVArtifact.draft(b"value\n1\n", "result.csv", step_number)],
                "empty": [],
            },
            lineage={"found": [], "empty": []},
            metadata={"authored": {"selected": "original-source"}},
        )


def stage_observed(
    coordinator: CuratorResults,
    admission: CuratorAdmission,
    operation: DynamicCurator,
    *,
    storage: StorageConfig | None = None,
) -> tuple[StagingResult, StepResult, CuratorReadRecorder]:
    committer = coordinator.committer
    view = CuratorView(committer, admission.view)
    recorder = CuratorReadRecorder(view, admission)
    staged = run_curator_flow(
        ExecutionUnit(
            operation=operation,
            inputs={},
            step_number=admission.record.step_number,
            step_run_id=admission.record.step_run_id,
            execution_spec_id=admission.digest,
            replay_snapshot=ReplaySnapshot.unavailable("synthetic-curator-replay"),
        ),
        RuntimeEnvironment(
            delta_root=committer.delta_root,
            staging_root=coordinator.staging.staging_dir,
            files_root=committer.files_root,
            storage=storage or StorageConfig(),
        ),
        curator_view=view,
        read_recorder=recorder,
    )
    populated = staged.success and bool(staged.artifact_ids)
    terminal = StepResult(
        step_run_id=admission.record.step_run_id,
        step_number=admission.record.step_number,
        step_name=operation.name,
        status=StepStatus.SUCCEEDED if staged.success else StepStatus.FAILED,
        disposition=StepDisposition.EXECUTED if staged.success else None,
        total_count=1,
        succeeded_count=int(staged.success),
        failed_count=int(not staged.success),
        output_roles=frozenset({"found"}) if populated else frozenset(),
        output_types={"found": "csv"} if populated else {},
        metadata={"authored": {"selected": "original-source"}}
        if staged.success
        else {},
        error=staged.error,
    )
    return staged, terminal, recorder


class MeasureCSV(OperationDefinition):
    """Native metric whose exact parent is the selected curator CSV occurrence."""

    name = "curator_cache_measure_csv"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = {"source": InputSpec(artifact_type="csv", materialize=False)}
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["source"]}
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, object]:
        return {}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, object]:
        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        result.add_artifact(
            "metrics",
            MetricArtifact.draft({"score": 1}, "score.json"),
            sources={"source": [inputs.ref("source", index=0)]},
        )
        return result
