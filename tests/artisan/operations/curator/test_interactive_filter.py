"""Unit tests for InteractiveFilter — interactive metric exploration and filtering.

Tests cover:
1. load() — builds tidy and wide DataFrames from delta tables
2. load() with step_numbers — filters primary artifacts
3. load() error handling — missing tables, no artifacts, no metrics
4. set_criteria() — validates metric names and Pydantic models
5. filtered_ids / filtered_wide_df — AND semantics, consistency
6. summary() — FilterSummary with criteria and funnel DataFrames
7. summary() _repr_html_ — returns valid HTML
8. plot() — returns matplotlib Figure with correct subplot count
9. commit() — writes steps, executions, execution_edges tables
10. commit() — result.output("passthrough") works for downstream wiring
11. commit() precondition errors
12. Wide column disambiguation — same step_name from different step_numbers
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from artisan.operations.curator.interactive_filter import (
    FilterSummary,
    InteractiveFilter,
)
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    STEPS_SCHEMA,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_delta(
    delta_root: Path, rel_path: str, rows: list[dict], schema: dict
) -> None:
    table_path = delta_root / rel_path
    table_path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(rows, schema=schema)
    df.write_delta(str(table_path))


def _pad(short_id: str) -> str:
    """Pad a short ID to 32 characters."""
    return short_id.ljust(32, "0")[:32]


def _metric_content(values: dict) -> bytes:
    return json.dumps(values, sort_keys=True).encode("utf-8")


# ---------------------------------------------------------------------------
# Fixture: minimal delta store with 4 data artifacts and 2 metric steps
# ---------------------------------------------------------------------------


@pytest.fixture
def delta_root(tmp_path: Path) -> Path:
    """Build a test delta store with known data.

    Layout:
      - 4 data artifacts at step 0 (s0..s3)
      - 2 metric steps:
        - step 1 "calc_metrics": computes confidence and accuracy for each artifact
        - step 2 "extra_metrics": computes score for each artifact
      - Provenance edges: each data artifact -> its metrics
    """
    root = tmp_path / "delta"
    root.mkdir()

    s_ids = [_pad(f"s{i}") for i in range(4)]
    m1_ids = [_pad(f"m1_{i}") for i in range(4)]
    m2_ids = [_pad(f"m2_{i}") for i in range(4)]

    # -- artifact_index --
    index_rows = []
    for i, sid in enumerate(s_ids):
        index_rows.append(
            {
                "artifact_id": sid,
                "artifact_type": "data",
                "origin_step_number": 0,
                "metadata": "{}",
            }
        )
    for i, mid in enumerate(m1_ids):
        index_rows.append(
            {
                "artifact_id": mid,
                "artifact_type": "metric",
                "origin_step_number": 1,
                "metadata": "{}",
            }
        )
    for i, mid in enumerate(m2_ids):
        index_rows.append(
            {
                "artifact_id": mid,
                "artifact_type": "metric",
                "origin_step_number": 2,
                "metadata": "{}",
            }
        )
    _write_delta(root, "artifacts/index", index_rows, ARTIFACT_INDEX_SCHEMA)

    # -- metrics table --
    confidence_vals = [90.0, 70.0, 50.0, 30.0]
    accuracy_vals = [1.0, 2.0, 3.0, 4.0]
    score_vals = [0.9, 0.7, 0.5, 0.3]

    metric_rows = []
    for i, mid in enumerate(m1_ids):
        metric_rows.append(
            {
                "artifact_id": mid,
                "origin_step_number": 1,
                "content": _metric_content(
                    {"confidence": confidence_vals[i], "accuracy": accuracy_vals[i]}
                ),
                "original_name": f"m1_{i}",
                "extension": ".json",
                "metadata": "{}",
                "external_path": None,
            }
        )
    for i, mid in enumerate(m2_ids):
        metric_rows.append(
            {
                "artifact_id": mid,
                "origin_step_number": 2,
                "content": _metric_content({"score": score_vals[i]}),
                "original_name": f"m2_{i}",
                "extension": ".json",
                "metadata": "{}",
                "external_path": None,
            }
        )
    _write_delta(root, "artifacts/metrics", metric_rows, MetricArtifact.POLARS_SCHEMA)

    # -- artifact_edges (data -> metric) --
    edge_rows = []
    for i in range(4):
        edge_rows.append(
            {
                "execution_run_id": _pad("exec1"),
                "source_artifact_id": s_ids[i],
                "target_artifact_id": m1_ids[i],
                "source_artifact_type": "data",
                "target_artifact_type": "metric",
                "source_role": "samples",
                "target_role": "metrics",
                "group_id": None,
            }
        )
        edge_rows.append(
            {
                "execution_run_id": _pad("exec2"),
                "source_artifact_id": s_ids[i],
                "target_artifact_id": m2_ids[i],
                "source_artifact_type": "data",
                "target_artifact_type": "metric",
                "source_role": "samples",
                "target_role": "metrics",
                "group_id": None,
            }
        )
    _write_delta(root, "provenance/artifact_edges", edge_rows, ARTIFACT_EDGES_SCHEMA)

    # -- steps table --
    from datetime import UTC, datetime

    now = datetime.now(UTC)
    steps_rows = []
    for step_num, step_name, op_class in [
        (0, "ingest", "artisan.operations.curator.ingest_files.IngestFiles"),
        (1, "calc_metrics", "artisan.operations.examples.MetricCalculator"),
        (2, "extra_metrics", "artisan.operations.examples.MetricCalculator"),
    ]:
        steps_rows.append(
            {
                "step_run_id": _pad(f"sr{step_num}"),
                "step_spec_id": _pad(f"ss{step_num}"),
                "pipeline_run_id": "test-run-001",
                "step_number": step_num,
                "step_name": step_name,
                "status": "completed",
                "operation_class": op_class,
                "params_json": "{}",
                "input_refs_json": "{}",
                "compute_backend": "local",
                "compute_options_json": "{}",
                "output_roles_json": '["samples"]',
                "output_types_json": '{"samples": "data"}',
                "total_count": 4,
                "succeeded_count": 4,
                "failed_count": 0,
                "timestamp": now,
                "duration_seconds": 1.0,
                "error": None,
                "dispatch_error": None,
                "commit_error": None,
                "metadata": None,
            }
        )
    _write_delta(root, "orchestration/steps", steps_rows, STEPS_SCHEMA)

    return root


# ---------------------------------------------------------------------------
# Tests: load
# ---------------------------------------------------------------------------


class TestLoad:
    def test_load_builds_tidy_and_wide_dataframes(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()

        assert filt.tidy_df.height > 0
        assert "artifact_id" in filt.tidy_df.columns
        assert "metric_name" in filt.tidy_df.columns
        assert "metric_value" in filt.tidy_df.columns

        assert filt.wide_df.height == 4
        assert "artifact_id" in filt.wide_df.columns
        # Should have columns like calc_metrics.confidence, calc_metrics.accuracy, extra_metrics.score
        metric_cols = [c for c in filt.wide_df.columns if c != "artifact_id"]
        assert len(metric_cols) == 3
        assert "calc_metrics.confidence" in metric_cols
        assert "calc_metrics.accuracy" in metric_cols
        assert "extra_metrics.score" in metric_cols

    def test_load_with_step_numbers_filters_primary_artifacts(
        self, delta_root: Path
    ) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load(step_numbers=[0])

        assert filt.wide_df.height == 4

    def test_load_raises_on_empty_delta(self, tmp_path: Path) -> None:
        filt = InteractiveFilter(tmp_path / "nonexistent")
        with pytest.raises(ValueError, match="Artifact index not found"):
            filt.load()

    def test_load_raises_on_no_matching_artifacts(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        with pytest.raises(ValueError, match="No primary artifacts found"):
            filt.load(step_numbers=[99])

    def test_load_raises_on_no_metrics(self, tmp_path: Path) -> None:
        """Delta store with data artifacts but no metrics."""
        root = tmp_path / "delta_no_metrics"
        root.mkdir()

        _write_delta(
            root,
            "artifacts/index",
            [
                {
                    "artifact_id": _pad("s0"),
                    "artifact_type": "data",
                    "origin_step_number": 0,
                    "metadata": "{}",
                }
            ],
            ARTIFACT_INDEX_SCHEMA,
        )

        filt = InteractiveFilter(root)
        with pytest.raises(ValueError, match="No metric artifacts found"):
            filt.load()


# ---------------------------------------------------------------------------
# Tests: set_criteria
# ---------------------------------------------------------------------------


class TestSetCriteria:
    def test_set_criteria_validates_metric_names(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()

        with pytest.raises(ValueError, match="not found in loaded data"):
            filt.set_criteria(
                [{"metric": "nonexistent.col", "operator": "gt", "value": 0}]
            )

    def test_set_criteria_validates_via_pydantic(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()

        with pytest.raises(Exception):  # Pydantic ValidationError
            filt.set_criteria(
                [
                    {
                        "metric": "calc_metrics.confidence",
                        "operator": "INVALID",
                        "value": 0,
                    }
                ]
            )

    def test_set_criteria_before_load(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        with pytest.raises(ValueError, match="No data loaded"):
            filt.set_criteria(
                [{"metric": "calc_metrics.confidence", "operator": "gt", "value": 50}]
            )


# ---------------------------------------------------------------------------
# Tests: filtering
# ---------------------------------------------------------------------------


class TestFiltering:
    def test_filtered_ids_applies_all_criteria(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria(
            [
                {"metric": "calc_metrics.confidence", "operator": "gt", "value": 60},
                {"metric": "extra_metrics.score", "operator": "gt", "value": 0.5},
            ]
        )

        # confidence > 60: s0(90), s1(70) pass; confidence 50 and 30 fail
        # score > 0.5: s0(0.9), s1(0.7) pass; score 0.5 and 0.3 fail
        # AND: s0, s1 pass
        assert len(filt.filtered_ids) == 2

    def test_filtered_wide_df_matches_filtered_ids(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria(
            [{"metric": "calc_metrics.confidence", "operator": "ge", "value": 70}]
        )

        ids_from_property = set(filt.filtered_ids)
        ids_from_df = set(filt.filtered_wide_df["artifact_id"].to_list())
        assert ids_from_property == ids_from_df
        assert len(ids_from_property) == 2  # s0 (90) and s1 (70)

    def test_filtered_raises_before_load(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        with pytest.raises(ValueError, match="No data loaded"):
            _ = filt.filtered_ids

    def test_filtered_raises_before_criteria(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        with pytest.raises(ValueError, match="No criteria set"):
            _ = filt.filtered_ids


# ---------------------------------------------------------------------------
# Tests: summary
# ---------------------------------------------------------------------------


class TestSummary:
    def test_summary_returns_filter_summary(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria(
            [{"metric": "calc_metrics.confidence", "operator": "gt", "value": 50}]
        )

        s = filt.summary()
        assert isinstance(s, FilterSummary)
        assert isinstance(s.criteria, pl.DataFrame)
        assert isinstance(s.funnel, pl.DataFrame)

        # criteria DF should have one row per criterion
        assert s.criteria.height == 1
        assert s.criteria["pass"][0] == 2  # confidence > 50: 90 and 70

        # funnel should have 2 rows: All + one criterion
        assert s.funnel.height == 2
        assert s.funnel["remaining"][0] == 4  # "All" row
        assert s.funnel["remaining"][1] == 2  # after confidence > 50

    def test_summary_repr_html(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria(
            [{"metric": "calc_metrics.confidence", "operator": "gt", "value": 50}]
        )

        html = filt.summary()._repr_html_()
        assert isinstance(html, str)
        assert "<h4>" in html
        assert "2 / 4 pass" in html


# ---------------------------------------------------------------------------
# Tests: plot
# ---------------------------------------------------------------------------


class TestPlot:
    def test_plot_returns_matplotlib_figure(self, delta_root: Path) -> None:
        import matplotlib.pyplot as plt

        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria(
            [
                {"metric": "calc_metrics.confidence", "operator": "gt", "value": 50},
                {"metric": "extra_metrics.score", "operator": "gt", "value": 0.5},
            ]
        )

        fig = filt.plot()
        assert isinstance(fig, plt.Figure)
        # One subplot per criterion
        assert len(fig.axes) == 2


# ---------------------------------------------------------------------------
# Tests: commit
# ---------------------------------------------------------------------------


class TestCommit:
    def test_commit_writes_three_tables(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria(
            [{"metric": "calc_metrics.confidence", "operator": "gt", "value": 50}]
        )

        result = filt.commit()

        assert result.success
        assert result.succeeded_count == 2
        assert result.total_count == 4
        assert "passthrough" in result.output_roles

        # Verify steps table was updated
        steps_df = pl.read_delta(str(delta_root / "orchestration/steps"))
        new_steps = steps_df.filter(pl.col("step_name") == "interactive_filter")
        # Should have 2 rows: running + completed
        assert new_steps.height == 2

        # Verify executions table was written
        exec_df = pl.read_delta(str(delta_root / "orchestration/executions"))
        assert exec_df.height >= 1
        last_exec = exec_df.filter(pl.col("operation_name") == "filter")
        assert last_exec.height >= 1

        # Verify execution_edges table was written
        edges_df = pl.read_delta(str(delta_root / "provenance/execution_edges"))
        assert edges_df.height > 0

    def test_commit_step_compatible_with_output_reference(
        self, delta_root: Path
    ) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria(
            [{"metric": "calc_metrics.confidence", "operator": "gt", "value": 50}]
        )

        result = filt.commit()
        ref = result.output("passthrough")

        assert ref.source_step == result.step_number
        assert ref.role == "passthrough"

    def test_commit_raises_before_load(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        with pytest.raises(ValueError, match="No data loaded"):
            filt.commit()

    def test_commit_raises_before_criteria(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        with pytest.raises(ValueError, match="No criteria set"):
            filt.commit()

    def test_commit_raises_on_empty_filtered(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        # Set criteria that nothing passes
        filt.set_criteria(
            [{"metric": "calc_metrics.confidence", "operator": "gt", "value": 999}]
        )
        with pytest.raises(ValueError, match="No artifacts pass"):
            filt.commit()


# ---------------------------------------------------------------------------
# Tests: wide column disambiguation
# ---------------------------------------------------------------------------


class TestWideColumnDisambiguation:
    def test_wide_column_disambiguation(self, tmp_path: Path) -> None:
        """Same step_name from different step_numbers gets [N] suffix."""
        root = tmp_path / "delta_ambiguous"
        root.mkdir()

        s_ids = [_pad("sa0"), _pad("sa1")]
        m1_id = _pad("ma1")
        m2_id = _pad("ma2")

        _write_delta(
            root,
            "artifacts/index",
            [
                {
                    "artifact_id": s_ids[0],
                    "artifact_type": "data",
                    "origin_step_number": 0,
                    "metadata": "{}",
                },
                {
                    "artifact_id": s_ids[1],
                    "artifact_type": "data",
                    "origin_step_number": 0,
                    "metadata": "{}",
                },
                {
                    "artifact_id": m1_id,
                    "artifact_type": "metric",
                    "origin_step_number": 1,
                    "metadata": "{}",
                },
                {
                    "artifact_id": m2_id,
                    "artifact_type": "metric",
                    "origin_step_number": 2,
                    "metadata": "{}",
                },
            ],
            ARTIFACT_INDEX_SCHEMA,
        )

        _write_delta(
            root,
            "artifacts/metrics",
            [
                {
                    "artifact_id": m1_id,
                    "origin_step_number": 1,
                    "content": _metric_content({"val": 1.0}),
                    "original_name": "ma1",
                    "extension": ".json",
                    "metadata": "{}",
                    "external_path": None,
                },
                {
                    "artifact_id": m2_id,
                    "origin_step_number": 2,
                    "content": _metric_content({"val": 2.0}),
                    "original_name": "ma2",
                    "extension": ".json",
                    "metadata": "{}",
                    "external_path": None,
                },
            ],
            MetricArtifact.POLARS_SCHEMA,
        )

        _write_delta(
            root,
            "provenance/artifact_edges",
            [
                {
                    "execution_run_id": _pad("e1"),
                    "source_artifact_id": s_ids[0],
                    "target_artifact_id": m1_id,
                    "source_artifact_type": "data",
                    "target_artifact_type": "metric",
                    "source_role": "s",
                    "target_role": "m",
                    "group_id": None,
                },
                {
                    "execution_run_id": _pad("e2"),
                    "source_artifact_id": s_ids[1],
                    "target_artifact_id": m2_id,
                    "source_artifact_type": "data",
                    "target_artifact_type": "metric",
                    "source_role": "s",
                    "target_role": "m",
                    "group_id": None,
                },
            ],
            ARTIFACT_EDGES_SCHEMA,
        )

        # Both metric steps have the same step_name "repeat"
        from datetime import UTC, datetime

        now = datetime.now(UTC)
        _write_delta(
            root,
            "orchestration/steps",
            [
                {
                    "step_run_id": _pad("r1"),
                    "step_spec_id": _pad("p1"),
                    "pipeline_run_id": "run1",
                    "step_number": 1,
                    "step_name": "repeat",
                    "status": "completed",
                    "operation_class": "SomeOp",
                    "params_json": "{}",
                    "input_refs_json": "{}",
                    "compute_backend": "local",
                    "compute_options_json": "{}",
                    "output_roles_json": "[]",
                    "output_types_json": "{}",
                    "total_count": 1,
                    "succeeded_count": 1,
                    "failed_count": 0,
                    "timestamp": now,
                    "duration_seconds": 0.1,
                    "error": None,
                    "metadata": None,
                },
                {
                    "step_run_id": _pad("r2"),
                    "step_spec_id": _pad("p2"),
                    "pipeline_run_id": "run1",
                    "step_number": 2,
                    "step_name": "repeat",
                    "status": "completed",
                    "operation_class": "SomeOp",
                    "params_json": "{}",
                    "input_refs_json": "{}",
                    "compute_backend": "local",
                    "compute_options_json": "{}",
                    "output_roles_json": "[]",
                    "output_types_json": "{}",
                    "total_count": 1,
                    "succeeded_count": 1,
                    "failed_count": 0,
                    "timestamp": now,
                    "duration_seconds": 0.1,
                    "error": None,
                    "metadata": None,
                },
            ],
            STEPS_SCHEMA,
        )

        filt = InteractiveFilter(root)
        filt.load()

        # Should disambiguate with {step_number}. prefix
        cols = filt.wide_df.columns
        metric_cols = [c for c in cols if c != "artifact_id"]
        assert (
            "1.repeat.val" in metric_cols
        ), f"Expected '1.repeat.val', got: {metric_cols}"
        assert (
            "2.repeat.val" in metric_cols
        ), f"Expected '2.repeat.val', got: {metric_cols}"
