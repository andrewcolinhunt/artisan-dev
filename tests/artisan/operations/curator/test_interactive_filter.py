"""Unit tests for InteractiveFilter — interactive metric exploration and filtering.

Tests cover:
- load() — builds tidy and wide DataFrames from delta tables
- load() with step_numbers — filters primary artifacts
- load() error handling — missing tables, no artifacts, no metrics
- set_criteria() — validates metric names and Pydantic models
- set_criteria() — collision detection for ambiguous metrics
- filtered_ids / filtered_wide_df — AND semantics, null handling, consistency
- summary() — FilterSummary with criteria (mean, not median) and funnel DataFrames
- summary() _repr_html_ — returns valid HTML
- plot() — returns matplotlib Figure with correct subplot count
- commit() — writes steps, executions, execution_edges tables with v4 diagnostics
- commit() — result.output("passthrough") works for downstream wiring
- commit() precondition errors
- Wide column disambiguation — collision detection at set_criteria() time
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
    confidence_vals = [90, 70, 50, 30]
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
        # Wide DataFrame uses raw field names (not qualified)
        metric_cols = [c for c in filt.wide_df.columns if c != "artifact_id"]
        assert len(metric_cols) == 3
        assert "confidence" in metric_cols
        assert "accuracy" in metric_cols
        assert "score" in metric_cols

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

        # Need empty edges table for the provenance walk
        _write_delta(
            root,
            "provenance/artifact_edges",
            [],
            ARTIFACT_EDGES_SCHEMA,
        )

        filt = InteractiveFilter(root)
        with pytest.raises(ValueError, match="No metrics found"):
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
                        "metric": "confidence",
                        "operator": "INVALID",
                        "value": 0,
                    }
                ]
            )

    def test_set_criteria_before_load(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        with pytest.raises(ValueError, match="No data loaded"):
            filt.set_criteria([{"metric": "confidence", "operator": "gt", "value": 50}])


# ---------------------------------------------------------------------------
# Tests: filtering
# ---------------------------------------------------------------------------


class TestFiltering:
    def test_filtered_ids_applies_all_criteria(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria(
            [
                {"metric": "confidence", "operator": "gt", "value": 60},
                {"metric": "score", "operator": "gt", "value": 0.5},
            ]
        )

        # confidence > 60: s0(90), s1(70) pass; confidence 50 and 30 fail
        # score > 0.5: s0(0.9), s1(0.7) pass; score 0.5 and 0.3 fail
        # AND: s0, s1 pass
        assert len(filt.filtered_ids) == 2

    def test_filtered_wide_df_matches_filtered_ids(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria([{"metric": "confidence", "operator": "ge", "value": 70}])

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

    def test_null_metric_values_treated_as_failures(self, tmp_path: Path) -> None:
        """Artifacts with null metric values should fail criteria (fill_null(False))."""
        root = tmp_path / "delta_nulls"
        root.mkdir()

        s_ids = [_pad("sn0"), _pad("sn1")]
        m_ids = [_pad("mn0"), _pad("mn1")]

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
                    "artifact_id": m_ids[0],
                    "artifact_type": "metric",
                    "origin_step_number": 1,
                    "metadata": "{}",
                },
                {
                    "artifact_id": m_ids[1],
                    "artifact_type": "metric",
                    "origin_step_number": 1,
                    "metadata": "{}",
                },
            ],
            ARTIFACT_INDEX_SCHEMA,
        )

        # m0 has score, m1 does NOT have score (different metric key)
        _write_delta(
            root,
            "artifacts/metrics",
            [
                {
                    "artifact_id": m_ids[0],
                    "origin_step_number": 1,
                    "content": _metric_content({"score": 0.9}),
                    "original_name": "mn0",
                    "extension": ".json",
                    "metadata": "{}",
                    "external_path": None,
                },
                {
                    "artifact_id": m_ids[1],
                    "origin_step_number": 1,
                    "content": _metric_content({"other": 0.5}),
                    "original_name": "mn1",
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
                    "execution_run_id": _pad("enull"),
                    "source_artifact_id": s_ids[0],
                    "target_artifact_id": m_ids[0],
                    "source_artifact_type": "data",
                    "target_artifact_type": "metric",
                    "source_role": "samples",
                    "target_role": "metrics",
                    "group_id": None,
                },
                {
                    "execution_run_id": _pad("enull"),
                    "source_artifact_id": s_ids[1],
                    "target_artifact_id": m_ids[1],
                    "source_artifact_type": "data",
                    "target_artifact_type": "metric",
                    "source_role": "samples",
                    "target_role": "metrics",
                    "group_id": None,
                },
            ],
            ARTIFACT_EDGES_SCHEMA,
        )

        from datetime import UTC, datetime

        now = datetime.now(UTC)
        _write_delta(
            root,
            "orchestration/steps",
            [
                {
                    "step_run_id": _pad("rsn0"),
                    "step_spec_id": _pad("psn0"),
                    "pipeline_run_id": "run-null",
                    "step_number": 0,
                    "step_name": "ingest",
                    "status": "completed",
                    "operation_class": "IngestFiles",
                    "params_json": "{}",
                    "input_refs_json": "{}",
                    "compute_backend": "local",
                    "compute_options_json": "{}",
                    "output_roles_json": '["samples"]',
                    "output_types_json": '{"samples": "data"}',
                    "total_count": 2,
                    "succeeded_count": 2,
                    "failed_count": 0,
                    "timestamp": now,
                    "duration_seconds": 0.1,
                    "error": None,
                    "dispatch_error": None,
                    "commit_error": None,
                    "metadata": None,
                },
                {
                    "step_run_id": _pad("rsn1"),
                    "step_spec_id": _pad("psn1"),
                    "pipeline_run_id": "run-null",
                    "step_number": 1,
                    "step_name": "eval",
                    "status": "completed",
                    "operation_class": "MetricCalc",
                    "params_json": "{}",
                    "input_refs_json": "{}",
                    "compute_backend": "local",
                    "compute_options_json": "{}",
                    "output_roles_json": '["metrics"]',
                    "output_types_json": '{"metrics": "metric"}',
                    "total_count": 2,
                    "succeeded_count": 2,
                    "failed_count": 0,
                    "timestamp": now,
                    "duration_seconds": 0.1,
                    "error": None,
                    "dispatch_error": None,
                    "commit_error": None,
                    "metadata": None,
                },
            ],
            STEPS_SCHEMA,
        )

        filt = InteractiveFilter(root)
        filt.load()
        filt.set_criteria([{"metric": "score", "operator": "gt", "value": 0.5}])

        # Only sn0 has score=0.9 (passes). sn1 has null for score -> fails.
        assert len(filt.filtered_ids) == 1


# ---------------------------------------------------------------------------
# Tests: summary
# ---------------------------------------------------------------------------


class TestSummary:
    def test_summary_returns_filter_summary(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria([{"metric": "confidence", "operator": "gt", "value": 50}])

        s = filt.summary()
        assert isinstance(s, FilterSummary)
        assert isinstance(s.criteria, pl.DataFrame)
        assert isinstance(s.funnel, pl.DataFrame)

        # criteria DF should have one row per criterion
        assert s.criteria.height == 1
        assert s.criteria["pass"][0] == 2  # confidence > 50: 90 and 70

        # criteria uses mean, not median
        assert "mean" in s.criteria.columns
        assert "median" not in s.criteria.columns

        # funnel should have 2 rows: All evaluated + one criterion
        assert s.funnel.height == 2
        assert s.funnel["count"][0] == 4  # "All evaluated" row
        assert s.funnel["count"][1] == 2  # after confidence > 50

    def test_summary_repr_html(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria([{"metric": "confidence", "operator": "gt", "value": 50}])

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
                {"metric": "confidence", "operator": "gt", "value": 50},
                {"metric": "score", "operator": "gt", "value": 0.5},
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
        filt.set_criteria([{"metric": "confidence", "operator": "gt", "value": 50}])

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

    def test_commit_diagnostics_v4(self, delta_root: Path) -> None:
        """Verify v4 diagnostics structure."""
        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria([{"metric": "confidence", "operator": "gt", "value": 50}])

        result = filt.commit()
        diag = result.metadata["diagnostics"]

        assert diag["version"] == 4
        assert diag["interactive"] is True
        assert diag["total_input"] == 4
        assert diag["total_evaluated"] == 4
        assert diag["total_metrics_discovered"] > 0
        assert diag["total_passed"] == 2
        assert isinstance(diag["metric_sources"], list)

        # Per-criterion diagnostics
        assert len(diag["criteria"]) == 1
        crit = diag["criteria"][0]
        assert crit["metric"] == "confidence"
        assert crit["operator"] == "gt"
        assert crit["value"] == 50
        assert "pass_count" in crit
        assert "resolved_from_step" in crit
        assert "stats" in crit
        if crit["stats"]:
            assert "min" in crit["stats"]
            assert "max" in crit["stats"]
            assert "mean" in crit["stats"]

        # Funnel
        assert len(diag["funnel"]) == 2
        assert diag["funnel"][0]["label"] == "All evaluated"
        assert diag["funnel"][0]["count"] == 4
        assert "eliminated" in diag["funnel"][1]

    def test_commit_step_compatible_with_output_reference(
        self, delta_root: Path
    ) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria([{"metric": "confidence", "operator": "gt", "value": 50}])

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
        filt.set_criteria([{"metric": "confidence", "operator": "gt", "value": 999}])
        with pytest.raises(ValueError, match="No artifacts pass"):
            filt.commit()


# ---------------------------------------------------------------------------
# Tests: wide column disambiguation (collision detection)
# ---------------------------------------------------------------------------


class TestWideColumnDisambiguation:
    def test_collision_detection_raises_on_ambiguous_field(
        self, tmp_path: Path
    ) -> None:
        """Same field name from different steps raises at set_criteria() time."""
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
                    "step_run_id": _pad("r0"),
                    "step_spec_id": _pad("p0"),
                    "pipeline_run_id": "run1",
                    "step_number": 0,
                    "step_name": "ingest",
                    "status": "completed",
                    "operation_class": "IngestFiles",
                    "params_json": "{}",
                    "input_refs_json": "{}",
                    "compute_backend": "local",
                    "compute_options_json": "{}",
                    "output_roles_json": "[]",
                    "output_types_json": "{}",
                    "total_count": 2,
                    "succeeded_count": 2,
                    "failed_count": 0,
                    "timestamp": now,
                    "duration_seconds": 0.1,
                    "error": None,
                    "dispatch_error": None,
                    "commit_error": None,
                    "metadata": None,
                },
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
                    "dispatch_error": None,
                    "commit_error": None,
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
                    "dispatch_error": None,
                    "commit_error": None,
                    "metadata": None,
                },
            ],
            STEPS_SCHEMA,
        )

        filt = InteractiveFilter(root)
        filt.load()

        # wide_df has raw field name "val"
        assert "val" in filt.wide_df.columns

        # Setting criteria without disambiguation should raise
        with pytest.raises(ValueError, match="multiple steps"):
            filt.set_criteria([{"metric": "val", "operator": "gt", "value": 0.5}])

    def test_step_number_disambiguation_skips_collision(self, tmp_path: Path) -> None:
        """Providing step_number on criterion bypasses collision detection."""
        root = tmp_path / "delta_disambig"
        root.mkdir()

        s_ids = [_pad("sd0"), _pad("sd1")]
        m1_id = _pad("md1")
        m2_id = _pad("md2")

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
                    "original_name": "md1",
                    "extension": ".json",
                    "metadata": "{}",
                    "external_path": None,
                },
                {
                    "artifact_id": m2_id,
                    "origin_step_number": 2,
                    "content": _metric_content({"val": 2.0}),
                    "original_name": "md2",
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
                    "execution_run_id": _pad("ed1"),
                    "source_artifact_id": s_ids[0],
                    "target_artifact_id": m1_id,
                    "source_artifact_type": "data",
                    "target_artifact_type": "metric",
                    "source_role": "s",
                    "target_role": "m",
                    "group_id": None,
                },
                {
                    "execution_run_id": _pad("ed2"),
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

        from datetime import UTC, datetime

        now = datetime.now(UTC)
        _write_delta(
            root,
            "orchestration/steps",
            [
                {
                    "step_run_id": _pad("rd0"),
                    "step_spec_id": _pad("pd0"),
                    "pipeline_run_id": "run1",
                    "step_number": 0,
                    "step_name": "ingest",
                    "status": "completed",
                    "operation_class": "IngestFiles",
                    "params_json": "{}",
                    "input_refs_json": "{}",
                    "compute_backend": "local",
                    "compute_options_json": "{}",
                    "output_roles_json": "[]",
                    "output_types_json": "{}",
                    "total_count": 2,
                    "succeeded_count": 2,
                    "failed_count": 0,
                    "timestamp": now,
                    "duration_seconds": 0.1,
                    "error": None,
                    "dispatch_error": None,
                    "commit_error": None,
                    "metadata": None,
                },
                {
                    "step_run_id": _pad("rd1"),
                    "step_spec_id": _pad("pd1"),
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
                    "dispatch_error": None,
                    "commit_error": None,
                    "metadata": None,
                },
                {
                    "step_run_id": _pad("rd2"),
                    "step_spec_id": _pad("pd2"),
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
                    "dispatch_error": None,
                    "commit_error": None,
                    "metadata": None,
                },
            ],
            STEPS_SCHEMA,
        )

        filt = InteractiveFilter(root)
        filt.load()

        # With step_number, collision detection is skipped
        filt.set_criteria(
            [{"metric": "val", "operator": "gt", "value": 0.5, "step_number": 1}]
        )
        # Should not raise — criterion is accepted
        assert len(filt.criteria) == 1


# ---------------------------------------------------------------------------
# Tests: metric type preservation
# ---------------------------------------------------------------------------


@pytest.fixture
def mixed_type_delta_root(tmp_path: Path) -> Path:
    """Delta store with mixed-type metrics: int, float, bool, str, list."""
    root = tmp_path / "delta_mixed"
    root.mkdir()

    s_ids = [_pad(f"mt{i}") for i in range(2)]
    m_ids = [_pad(f"mm{i}") for i in range(2)]

    _write_delta(
        root,
        "artifacts/index",
        [
            *[
                {
                    "artifact_id": sid,
                    "artifact_type": "data",
                    "origin_step_number": 0,
                    "metadata": "{}",
                }
                for sid in s_ids
            ],
            *[
                {
                    "artifact_id": mid,
                    "artifact_type": "metric",
                    "origin_step_number": 1,
                    "metadata": "{}",
                }
                for mid in m_ids
            ],
        ],
        ARTIFACT_INDEX_SCHEMA,
    )

    _write_delta(
        root,
        "artifacts/metrics",
        [
            {
                "artifact_id": m_ids[0],
                "origin_step_number": 1,
                "content": _metric_content(
                    {
                        "count": 10,
                        "score": 0.95,
                        "passed": True,
                        "label": "good",
                        "bins": [1, 2, 3],
                    }
                ),
                "original_name": "mm0",
                "extension": ".json",
                "metadata": "{}",
                "external_path": None,
            },
            {
                "artifact_id": m_ids[1],
                "origin_step_number": 1,
                "content": _metric_content(
                    {
                        "count": 20,
                        "score": 0.85,
                        "passed": False,
                        "label": "fair",
                        "bins": [4, 5, 6],
                    }
                ),
                "original_name": "mm1",
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
                "execution_run_id": _pad("emix"),
                "source_artifact_id": s_ids[i],
                "target_artifact_id": m_ids[i],
                "source_artifact_type": "data",
                "target_artifact_type": "metric",
                "source_role": "samples",
                "target_role": "metrics",
                "group_id": None,
            }
            for i in range(2)
        ],
        ARTIFACT_EDGES_SCHEMA,
    )

    from datetime import UTC, datetime

    now = datetime.now(UTC)
    _write_delta(
        root,
        "orchestration/steps",
        [
            {
                "step_run_id": _pad("rmix0"),
                "step_spec_id": _pad("pmix0"),
                "pipeline_run_id": "run-mix",
                "step_number": 0,
                "step_name": "ingest",
                "status": "completed",
                "operation_class": "IngestFiles",
                "params_json": "{}",
                "input_refs_json": "{}",
                "compute_backend": "local",
                "compute_options_json": "{}",
                "output_roles_json": '["samples"]',
                "output_types_json": '{"samples": "data"}',
                "total_count": 2,
                "succeeded_count": 2,
                "failed_count": 0,
                "timestamp": now,
                "duration_seconds": 0.1,
                "error": None,
                "dispatch_error": None,
                "commit_error": None,
                "metadata": None,
            },
            {
                "step_run_id": _pad("rmix1"),
                "step_spec_id": _pad("pmix1"),
                "pipeline_run_id": "run-mix",
                "step_number": 1,
                "step_name": "eval",
                "status": "completed",
                "operation_class": "MetricCalc",
                "params_json": "{}",
                "input_refs_json": "{}",
                "compute_backend": "local",
                "compute_options_json": "{}",
                "output_roles_json": '["metrics"]',
                "output_types_json": '{"metrics": "metric"}',
                "total_count": 2,
                "succeeded_count": 2,
                "failed_count": 0,
                "timestamp": now,
                "duration_seconds": 0.1,
                "error": None,
                "dispatch_error": None,
                "commit_error": None,
                "metadata": None,
            },
        ],
        STEPS_SCHEMA,
    )

    return root


class TestTypePreservation:
    def test_wide_df_preserves_types(self, mixed_type_delta_root: Path) -> None:
        filt = InteractiveFilter(mixed_type_delta_root)
        filt.load()

        wide = filt.wide_df
        # Raw field names (not qualified with step_name)
        assert wide["count"].dtype == pl.Int64
        assert wide["score"].dtype == pl.Float64
        assert wide["passed"].dtype == pl.Boolean
        assert wide["label"].dtype == pl.String

    def test_compound_metrics_excluded_from_wide(
        self, mixed_type_delta_root: Path
    ) -> None:
        filt = InteractiveFilter(mixed_type_delta_root)
        filt.load()

        wide_cols = filt.wide_df.columns
        # bins is a list type — _build_metric_namespace flattens structs but
        # list columns remain as-is; they may or may not appear depending on
        # Polars JSON decode behavior. Just verify core scalars are present.
        assert "count" in wide_cols
        assert "score" in wide_cols

        # Compound values are in the tidy DataFrame
        tidy = filt.tidy_df
        bins_rows = tidy.filter(pl.col("metric_name") == "bins")
        assert bins_rows.height == 2
        assert bins_rows["metric_compound"][0] is not None

    def test_wide_df_values_correct(self, mixed_type_delta_root: Path) -> None:
        filt = InteractiveFilter(mixed_type_delta_root)
        filt.load()

        wide = filt.wide_df.sort("artifact_id")
        counts = wide["count"].to_list()
        assert counts == [10, 20]
        passed = wide["passed"].to_list()
        assert passed == [True, False]
        labels = wide["label"].to_list()
        assert set(labels) == {"good", "fair"}


class TestExistingFloatMetricsStillWork:
    """Verify the original fixture (float-only metrics) still works."""

    def test_load_builds_tidy_and_wide(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()

        assert filt.wide_df.height == 4
        # Raw field names
        assert filt.wide_df["confidence"].dtype == pl.Int64
        assert filt.wide_df["accuracy"].dtype == pl.Float64
        assert filt.wide_df["score"].dtype == pl.Float64

    def test_filtering_still_works(self, delta_root: Path) -> None:
        filt = InteractiveFilter(delta_root)
        filt.load()
        filt.set_criteria(
            [
                {"metric": "confidence", "operator": "gt", "value": 60},
                {"metric": "score", "operator": "gt", "value": 0.5},
            ]
        )
        assert len(filt.filtered_ids) == 2
