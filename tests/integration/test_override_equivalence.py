"""Typed and mapping scheduling patches preserve one admitted scientific recipe."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.retained_pipeline import NativePipelineRunner

from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.storage.io.retained_store import create_retained_store

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    ("keyword", "mapping", "typed"),
    [
        ("runner_resources", {"cpus": 1}, RunnerResources(cpus=1)),
        (
            "batch_strategy",
            {"artifacts_per_unit": 2, "units_per_worker": 2, "max_workers": 1},
            BatchStrategy(artifacts_per_unit=2, units_per_worker=2, max_workers=1),
        ),
    ],
)
def test_mapping_and_typed_patches_match_admission_and_reuse(
    tmp_path: Path,
    keyword: str,
    mapping: dict[str, int],
    typed: RunnerResources | BatchStrategy,
) -> None:
    committer = create_retained_store(
        str(tmp_path / "delta"),
        str(tmp_path / "files"),
        fs=StorageConfig().filesystem(),
    )
    runner = NativePipelineRunner(committer)
    admissions = []
    configurations = []
    for index, patch in enumerate((mapping, typed)):
        with PipelineManager.create(
            f"patch-{index}",
            committer.delta_root,
            str(tmp_path / "staging"),
            files_root=committer.files_root,
            working_root=str(tmp_path / "working"),
            default_step_runner=runner,
        ) as pipeline:
            result = pipeline.run(
                DataGenerator, params={"count": 1}, **{keyword: patch}
            )
            assert result.status == StepStatus.SUCCEEDED, result.error
            admissions.append(pipeline.tracker.read_admission(result.step_run_id))
            execution = pipeline.executions[-1]
            configurations.append(getattr(execution, keyword).model_dump())
            if index:
                assert result.disposition == StepDisposition.CACHE_HIT
                assert execution.handles == ()
    assert admissions[0].recipe == admissions[1].recipe
    assert admissions[0].software_digest == admissions[1].software_digest
    assert configurations[0] == configurations[1]
    assert all(configurations[0][key] == value for key, value in mapping.items())
