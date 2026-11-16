"""Stream ingestion publishes all outputs or a closed failure with no prefix."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from fsspec import AbstractFileSystem
from pydantic import BaseModel, Field

from artisan.operations.curator import IngestCSV
from artisan.orchestration import PipelineManager
from artisan.schemas import CSVArtifact, FileRefArtifact
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.storage import ArtifactStore
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.io.curator_plan import read_curator_plan

pytestmark = pytest.mark.integration


class FailingCSV(IngestCSV):
    name = "failing_csv_integration"

    class Params(BaseModel):
        """Select the last file to exercise late conversion failure."""

        fail_on: str = Field(description="Source basename to reject.")

    params: Params

    def convert_file(
        self,
        file_ref: FileRefArtifact,
        step_number: int,
        *,
        fs: AbstractFileSystem | None = None,
    ) -> CSVArtifact:
        if file_ref.original_name == self.params.fail_on:
            message = "late file conversion failed"
            raise RuntimeError(message)
        return super().convert_file(file_ref, step_number, fs=fs)


def test_hundred_csvs_publish_one_complete_curator_execution(
    tmp_path: Path, native_pipeline: Callable[[str], PipelineManager]
) -> None:
    files = []
    for index in range(100):
        path = tmp_path / f"source-{index}.csv"
        path.write_text(f"value\n{index}\n")
        files.append(str(path))
    with native_pipeline("ingest") as pipeline:
        result = pipeline.run(IngestCSV, inputs=files)
        assert result.status == StepStatus.SUCCEEDED, result.error
        plan = read_curator_plan(
            pipeline.committer.delta_root, result.step_run_id, pipeline.committer.fs
        )
        assert plan is not None
        accepted = CuratorReader(pipeline.committer).read_result(
            result.step_run_id, plan.plan_digest
        )
        assert len(accepted.output("csv")) == 100
        assert len(accepted.artifacts) == 200  # Input FileRefs remain retained support.
        assert accepted.result.total_count == accepted.result.succeeded_count == 1


def test_late_conversion_failure_has_no_accepted_prefix(
    tmp_path: Path, native_pipeline: Callable[[str], PipelineManager]
) -> None:
    paths = []
    for index in range(3):
        path = tmp_path / f"source-{index}.csv"
        path.write_text(f"value\n{index}\n")
        paths.append(str(path))
    with native_pipeline("failure") as pipeline:
        result = pipeline.run(FailingCSV, inputs=paths, params={"fail_on": "source-2"})
        assert result.status == StepStatus.FAILED
        assert "late file conversion failed" in result.error
        store = ArtifactStore(
            pipeline.config.delta_root, files_root=pipeline.config.files_root
        )
        assert not store.provenance.load_artifact_ids_by_type("csv")
        assert len(store.provenance.load_artifact_ids_by_type("file_ref")) == 3
