"""Integration tests for InteractiveFilter end-to-end workflow.

Tests the full load-explore-filter-commit cycle against a real pipeline
that produces CSV artifacts and metrics in a Delta store.
"""

from __future__ import annotations

import polars as pl
import pytest

pytestmark = pytest.mark.integration

from artisan.operations.curator.interactive_filter import (
    FilterSummary,
    InteractiveFilter,
)
from artisan.operations.examples import DataGenerator, MetricCalculator
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners import LocalRunner
from artisan.schemas.orchestration.step_lifecycle import StepStatus

from .conftest import (
    get_execution_inputs,
    get_execution_outputs,
    get_step_status,
    read_table,
)


def test_interactive_filter_end_to_end(pipeline_env: dict[str, str]):
    """Full workflow: pipeline -> InteractiveFilter load/filter/commit."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_interactive_filter",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 4, "seed": 42},
        step_runner=LocalRunner(),
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        step_runner=LocalRunner(),
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    filt = InteractiveFilter(delta_root)
    filt.load()

    assert filt.wide_df.height == 4
    assert "artifact_id" in filt.wide_df.columns

    metric_cols = [c for c in filt.wide_df.columns if c != "artifact_id"]
    assert "distribution.min" in metric_cols
    assert "distribution.median" in metric_cols
    assert "summary.row_count" in metric_cols

    assert filt.tidy_df.height > 0

    filt.set_criteria(
        [
            {"metric": "distribution.min", "operator": "ge", "value": 0},
            {"metric": "summary.row_count", "operator": "eq", "value": 10},
        ]
    )

    filtered = filt.filtered_ids
    assert len(filtered) == 4

    s = filt.summary()
    assert isinstance(s, FilterSummary)
    assert s.criteria.height == 2
    assert "mean" in s.criteria.columns

    step_result = filt.commit("interactive_filter")
    assert step_result.status is StepStatus.SUCCEEDED
    assert step_result.succeeded_count == 1
    assert step_result.step_number == 2

    assert get_step_status(delta_root, 2) == "succeeded"

    outputs = get_execution_outputs(delta_root, 2, "passthrough")
    assert set(outputs) == set(filtered)

    inputs = get_execution_inputs(delta_root, 2, "passthrough")
    assert len(inputs) == 4

    diag = step_result.metadata["diagnostics"]
    assert diag["version"] == 4
    assert diag["interactive"] is True
    assert diag["total_passed"] == 4


def test_interactive_filter_selective_criteria(pipeline_env: dict[str, str]):
    """Criteria that filter out some artifacts."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_interactive_selective",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 5, "seed": 99},
        step_runner=LocalRunner(),
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        step_runner=LocalRunner(),
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    filt = InteractiveFilter(delta_root)
    filt.load()

    filt.set_criteria(
        [{"metric": "distribution.median", "operator": "gt", "value": 5.5}]
    )

    filtered = filt.filtered_ids
    assert len(filtered) < 5

    if len(filtered) > 0:
        step_result = filt.commit("selective_filter")
        assert step_result.status is StepStatus.SUCCEEDED
        assert step_result.succeeded_count == 1

        outputs = get_execution_outputs(delta_root, 2, "passthrough")
        assert set(outputs) == set(filtered)


def test_interactive_filter_load_with_step_numbers(pipeline_env: dict[str, str]):
    """Loading with step_numbers restricts primary artifacts."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_interactive_step_filter",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        step_runner=LocalRunner(),
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        step_runner=LocalRunner(),
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    filt = InteractiveFilter(delta_root)
    filt.load(step_numbers=[0])

    assert filt.wide_df.height == 3

    filt.set_criteria([{"metric": "summary.row_count", "operator": "eq", "value": 10}])
    assert len(filt.filtered_ids) == 3


def test_interactive_filter_output_reference(pipeline_env: dict[str, str]):
    """Committed step result supports output() for downstream wiring."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_interactive_output_ref",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        step_runner=LocalRunner(),
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        step_runner=LocalRunner(),
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    filt = InteractiveFilter(delta_root)
    filt.load()
    filt.set_criteria([{"metric": "distribution.min", "operator": "ge", "value": 0}])

    step_result = filt.commit("interactive_filter")
    ref = step_result.output("passthrough")

    assert ref.source.step_run_id == step_result.step_run_id
    assert ref.role == "passthrough"

    exec_table = read_table(delta_root, "orchestration/executions")
    filter_execs = exec_table.filter(pl.col("origin_step_number") == 2)
    assert filter_execs.height == 1
