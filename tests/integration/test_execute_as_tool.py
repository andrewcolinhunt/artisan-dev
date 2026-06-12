"""End-to-end pipeline test for execute_as_tool ops.

CsvHead's Python body runs as a framework-generated subprocess
(``artisan op run``) — the pipeline results must be indistinguishable
from an in-process function op: outputs committed, per-artifact
dispatch fanned out, lineage tied to each source artifact.
"""

from __future__ import annotations

import polars as pl
import pytest

pytestmark = pytest.mark.integration

from artisan.operations.examples import CsvHead, DataGenerator
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners import Runner

from .conftest import (
    count_artifacts_by_step,
    count_executions_by_step,
    load_artifact_edges,
    read_table,
)


def test_flag_op_runs_as_subprocess_in_pipeline(pipeline_env: dict[str, str]):
    """Flag-flip is a non-event for results: the subprocess-executed body
    produces committed artifacts with correct content and lineage."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_execute_as_tool",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        operation=DataGenerator,
        params={"count": 2, "rows_per_file": 10, "seed": 42},
        step_runner=Runner.LOCAL,
    )
    pipeline.run(
        operation=CsvHead,
        inputs={"dataset": step0.output("datasets")},
        params={"rows": 3},
        step_runner=Runner.LOCAL,
    )
    result = pipeline.finalize()

    assert result["overall_success"], "Pipeline should complete successfully"
    assert count_artifacts_by_step(delta_root, 0) == 2
    assert count_artifacts_by_step(delta_root, 1) == 2, "one head per input"
    # per-artifact dispatch: each input ran in its own subprocess
    assert count_executions_by_step(delta_root, 1) == 2

    # the subprocess-run body truncated each CSV to header + 3 rows
    data = read_table(delta_root, "artifacts/data")
    heads = data.filter(pl.col("origin_step_number") == 1)
    assert heads.height == 2
    for name, content in zip(
        heads["original_name"].to_list(), heads["content"].to_list(), strict=True
    ):
        assert name.endswith("_head")
        lines = bytes(content).decode().strip().splitlines()
        assert len(lines) == 4, "header + 3 data rows"

    # lineage: every head traces to a distinct step-0 source artifact
    head_ids = set(heads["artifact_id"].to_list())
    source_ids = set(
        data.filter(pl.col("origin_step_number") == 0)["artifact_id"].to_list()
    )
    edges = load_artifact_edges(delta_root, head_ids)
    assert set(edges["target_artifact_id"].to_list()) == head_ids
    assert set(edges["source_artifact_id"].to_list()) == source_ids

    # both subprocess executions recorded as successes
    executions = read_table(delta_root, "orchestration/executions")
    step1 = executions.filter(pl.col("origin_step_number") == 1)
    assert step1.height == 2
    assert step1["success"].all()
