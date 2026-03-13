"""Integration tests for InteractiveFilter end-to-end workflow.

Tests the full load-explore-filter-commit cycle against a real pipeline
that produces data artifacts and metrics in a Delta store.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

pytestmark = pytest.mark.slow

from artisan.operations.curator.interactive_filter import (
    FilterSummary,
    InteractiveFilter,
)
from artisan.operations.examples import DataGenerator, MetricCalculator
from artisan.orchestration import PipelineManager
from artisan.orchestration.backends import Backend

from .conftest import (
    get_execution_inputs,
    get_execution_outputs,
    get_step_status,
    read_table,
)


def test_interactive_filter_end_to_end(pipeline_env: dict[str, Path]):
    """Full workflow: pipeline -> InteractiveFilter load/filter/commit."""
    delta_root = pipeline_env["delta_root"]

    # Run a pipeline: DataGenerator -> MetricCalculator
    pipeline = PipelineManager.create(
        name="test_interactive_filter",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    step0 = pipeline.run(
        DataGenerator,
        params={"count": 4, "seed": 42},
        backend=Backend.LOCAL,
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # Use InteractiveFilter against the Delta store
    filt = InteractiveFilter(delta_root)
    filt.load()

    # wide_df should have 4 rows (one per data artifact)
    assert filt.wide_df.height == 4
    assert "artifact_id" in filt.wide_df.columns

    # Raw field names (not qualified with step name)
    metric_cols = [c for c in filt.wide_df.columns if c != "artifact_id"]
    assert "distribution.min" in metric_cols
    assert "distribution.median" in metric_cols
    assert "summary.row_count" in metric_cols

    # tidy_df should have rows for exploration
    assert filt.tidy_df.height > 0

    # Set criteria that all generated data satisfies (min >= 0, row_count == 10)
    filt.set_criteria(
        [
            {"metric": "distribution.min", "operator": "ge", "value": 0},
            {"metric": "summary.row_count", "operator": "eq", "value": 10},
        ]
    )

    # All 4 should pass
    filtered = filt.filtered_ids
    assert len(filtered) == 4

    # Summary
    s = filt.summary()
    assert isinstance(s, FilterSummary)
    assert s.criteria.height == 2
    assert "mean" in s.criteria.columns

    # Commit
    step_result = filt.commit("interactive_filter")
    assert step_result.success
    assert step_result.succeeded_count == 4
    assert step_result.step_number == 2

    # Verify committed step visible in steps table
    assert get_step_status(delta_root, 2) == "completed"

    # Verify execution edges
    outputs = get_execution_outputs(delta_root, 2, "passthrough")
    assert set(outputs) == set(filtered)

    inputs = get_execution_inputs(delta_root, 2, "passthrough")
    assert len(inputs) == 4

    # Verify v4 diagnostics
    diag = step_result.metadata["diagnostics"]
    assert diag["version"] == 4
    assert diag["interactive"] is True
    assert diag["total_passed"] == 4


def test_interactive_filter_selective_criteria(pipeline_env: dict[str, Path]):
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
        backend=Backend.LOCAL,
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    filt = InteractiveFilter(delta_root)
    filt.load()

    # Use a criterion on distribution.median that may filter some out.
    # Generated data is uniform 0-10, so median is roughly 5 but varies by seed.
    # Use a high threshold so at least one is filtered.
    filt.set_criteria(
        [{"metric": "distribution.median", "operator": "gt", "value": 5.5}]
    )

    filtered = filt.filtered_ids
    # Some but not all should pass (seed 99 gives varied medians)
    assert len(filtered) < 5

    # Commit and verify only passing artifacts are in outputs
    if len(filtered) > 0:
        step_result = filt.commit("selective_filter")
        assert step_result.success
        assert step_result.succeeded_count == len(filtered)

        outputs = get_execution_outputs(delta_root, 2, "passthrough")
        assert set(outputs) == set(filtered)


def test_interactive_filter_load_with_step_numbers(pipeline_env: dict[str, Path]):
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
        backend=Backend.LOCAL,
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # Load with explicit step_numbers=[0] — only data artifacts from step 0
    filt = InteractiveFilter(delta_root)
    filt.load(step_numbers=[0])

    assert filt.wide_df.height == 3

    # Set permissive criteria and commit
    filt.set_criteria([{"metric": "summary.row_count", "operator": "eq", "value": 10}])
    assert len(filt.filtered_ids) == 3


def test_interactive_filter_output_reference(pipeline_env: dict[str, Path]):
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
        backend=Backend.LOCAL,
    )

    pipeline.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        backend=Backend.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    filt = InteractiveFilter(delta_root)
    filt.load()
    filt.set_criteria([{"metric": "distribution.min", "operator": "ge", "value": 0}])

    step_result = filt.commit("interactive_filter")
    ref = step_result.output("passthrough")

    assert ref.source_step == step_result.step_number
    assert ref.role == "passthrough"

    # Verify the execution record has correct metadata
    exec_table = read_table(delta_root, "orchestration/executions")
    filter_execs = exec_table.filter(pl.col("origin_step_number") == 2)
    assert filter_execs.height == 1
