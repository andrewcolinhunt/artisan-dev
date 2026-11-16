"""Actual replay CLI processes preserve outcomes and machine-readable stdout."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from artisan.operations.curator import IngestCSV
from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager, ReplayResult, StepStatus
from artisan.orchestration.engine.retained_execution_query import (
    read_retained_execution_membership,
)
from artisan.storage.core.execution_source import (
    FailedScientificExecutionSource,
    ScientificExecutionSource,
    read_execution_source,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("successful", [True, False], ids=["success", "failure"])
def test_replay_cli_json_retains_committed_outcome(
    tmp_path: Path, successful: bool
) -> None:
    delta_root = str(tmp_path / "delta")
    with PipelineManager.create(
        "cli-source",
        delta_root=delta_root,
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    ) as manager:
        if successful:
            operation = DataGenerator
            step = manager.run(DataGenerator, params={"count": 1, "seed": 41})
        else:
            operation = DataTransformer
            source_file = tmp_path / "invalid.csv"
            source_file.write_text("id,x\n0,replay-invalid-number\n")
            ingested = manager.run(IngestCSV, inputs=[str(source_file)])
            step = manager.run(
                DataTransformer, inputs={"dataset": ingested.output("csv")}
            )
    expected = StepStatus.SUCCEEDED if successful else StepStatus.FAILED
    assert step.status == expected, step.error
    before = read_retained_execution_membership(
        manager.committer, pipeline_run_id=manager.config.pipeline_run_id
    )
    source = next(
        item.source for item in before if item.state.step_run_id == step.step_run_id
    )
    assert isinstance(
        source, (ScientificExecutionSource, FailedScientificExecutionSource)
    )
    source_id = (
        source.plan.receipt.delivery.delivery_id
        if isinstance(source, ScientificExecutionSource)
        else source.unit.failed_delivery.delivery_id
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "artisan.cli",
            "execution",
            "replay",
            source_id,
            "--delta-root",
            delta_root,
            "--operation",
            f"{operation.__module__}:{operation.__qualname__}",
            "--debug-root",
            str(tmp_path / "debug"),
            # Polars prepends allocator settings again in the fresh CLI process.
            # This diagnostic test permits that observed runtime change explicitly.
            "--allow-code-change",
            "--json",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    assert completed.stdout.strip(), completed.stderr
    result = ReplayResult.model_validate(json.loads(completed.stdout))
    assert completed.returncode == (0 if successful else 1), (
        result.step_result.error,
        completed.stderr,
    )
    assert result.source_execution_run_id == source_id
    assert result.execution_run_id is not None
    assert result.execution_run_id != source_id
    assert result.step_result.status == expected, result.step_result.error
    assert result.step_result.step_number == step.step_number
    assert result.diagnostic_status == "complete", result.diagnostic_errors
    if not successful:
        assert "replay-invalid-number" in (result.step_result.error or "")
    assert read_execution_source(manager.committer, result.execution_run_id) is not None
    assert read_execution_source(manager.committer, source_id) == source
    assert (
        read_retained_execution_membership(
            manager.committer, pipeline_run_id=manager.config.pipeline_run_id
        )
        == before
    )
