"""Actual replay CLI processes keep machine output separate from worker streams."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.operations.curator import IngestData
from artisan.operations.examples import CsvHead, DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager, ReplayResult, StepStatus
from artisan.schemas.enums import TablePath
from artisan.storage.core.committed_scan import read_committed

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "successful", [True, False], ids=["verbose-success", "failure"]
)
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
            generated = manager.run(
                DataGenerator, params={"count": 1, "seed": 41}, compact=False
            )
            step = manager.run(
                CsvHead,
                inputs={"dataset": generated.output("datasets")},
                params={"rows": 2},
                compact=False,
            )
        else:
            source_file = tmp_path / "invalid.csv"
            source_file.write_text("id,x\n0,replay-invalid-number\n")
            ingested = manager.run(IngestData, inputs=[str(source_file)], compact=False)
            step = manager.run(
                DataTransformer,
                inputs={"dataset": ingested.output("data")},
                compact=False,
            )
    expected = StepStatus.SUCCEEDED if successful else StepStatus.FAILED
    assert step.status == expected
    before = read_committed(delta_root, TablePath.EXECUTIONS, fs=LocalFileSystem())
    source_id = before.filter(pl.col("step_run_id") == step.step_run_id)[
        "execution_run_id"
    ].item()

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
            "--debug-root",
            str(tmp_path / "debug"),
            "--json",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    result = ReplayResult.model_validate(json.loads(completed.stdout))
    assert completed.returncode == (0 if successful else 1), completed.stderr
    assert result.source_execution_run_id == source_id
    assert result.execution_run_id is not None
    assert result.execution_run_id != source_id
    assert result.step_result.status == expected
    assert result.step_result.step_number == step.step_number
    assert result.diagnostic_status == "complete"
    if successful:
        assert "csv_head: wrote" in completed.stderr
        assert "csv_head: wrote" not in completed.stdout
    else:
        assert "replay-invalid-number" in (result.step_result.error or "")
    after = read_committed(delta_root, TablePath.EXECUTIONS, fs=LocalFileSystem())
    assert (
        after.filter(pl.col("execution_run_id") == result.execution_run_id).height == 1
    )
    assert after.filter(
        pl.col("execution_run_id").is_in(before["execution_run_id"])
    ).equals(before)
