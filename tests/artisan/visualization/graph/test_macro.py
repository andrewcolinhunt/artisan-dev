"""Tests for macro-level (step-level) pipeline graph."""

from __future__ import annotations

import json
from pathlib import Path

import graphviz
import polars as pl
import pytest

from artisan.storage.core.table_schemas import STEPS_SCHEMA
from artisan.visualization.graph import build_macro_graph, render_macro_graph
from artisan.visualization.graph.macro import _parse_input_refs

# =============================================================================
# Helpers
# =============================================================================


def _write_steps(delta_root: Path, rows: list[dict]) -> None:
    """Write steps rows to a Delta table under *delta_root*."""
    defaults = {
        "step_run_id": "run_0",
        "step_spec_id": "spec_0",
        "pipeline_run_id": "pipe_0",
        "status": "completed",
        "operation_class": "test.Op",
        "params_json": "{}",
        "input_refs_json": "null",
        "compute_backend": "local",
        "compute_options_json": "{}",
        "output_roles_json": "[]",
        "output_types_json": "{}",
        "total_count": 1,
        "succeeded_count": 1,
        "failed_count": 0,
        "timestamp": None,
        "duration_seconds": 0.1,
        "error": None,
        "dispatch_error": None,
        "commit_error": None,
        "metadata": "{}",
    }
    full_rows = [{**defaults, **r} for r in rows]
    df = pl.DataFrame(full_rows, schema=STEPS_SCHEMA)
    df.write_delta(str(delta_root / "orchestration/steps"), mode="overwrite")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def empty_delta_root(tmp_path: Path) -> Path:
    """Empty Delta Lake root directory (no tables)."""
    delta_root = tmp_path / "delta"
    delta_root.mkdir()
    return delta_root


@pytest.fixture
def simple_pipeline(tmp_path: Path) -> Path:
    """Three-step pipeline: Ingest → Transform → Metrics.

    Step 0 (Ingest): no inputs, outputs [file] (file_ref)
    Step 1 (Transform): input from step 0/file, outputs [data] (data)
    Step 2 (Metrics): input from step 1/data, outputs [metrics] (metric)
    """
    delta_root = tmp_path / "delta"
    delta_root.mkdir()

    _write_steps(
        delta_root,
        [
            {
                "step_run_id": "run_0",
                "step_spec_id": "spec_0",
                "step_number": 0,
                "step_name": "Ingest",
                "input_refs_json": "null",
                "output_roles_json": json.dumps(["file"]),
                "output_types_json": json.dumps({"file": "file_ref"}),
            },
            {
                "step_run_id": "run_1",
                "step_spec_id": "spec_1",
                "step_number": 1,
                "step_name": "Transform",
                "input_refs_json": json.dumps(
                    {
                        "input": {
                            "type": "output_ref",
                            "source_step": 0,
                            "role": "file",
                            "artifact_type": "file_ref",
                        }
                    }
                ),
                "output_roles_json": json.dumps(["data"]),
                "output_types_json": json.dumps({"data": "data"}),
            },
            {
                "step_run_id": "run_2",
                "step_spec_id": "spec_2",
                "step_number": 2,
                "step_name": "Metrics",
                "input_refs_json": json.dumps(
                    {
                        "input": {
                            "type": "output_ref",
                            "source_step": 1,
                            "role": "data",
                            "artifact_type": "data",
                        }
                    }
                ),
                "output_roles_json": json.dumps(["metrics"]),
                "output_types_json": json.dumps({"metrics": "metric"}),
            },
        ],
    )
    return delta_root


@pytest.fixture
def passthrough_pipeline(tmp_path: Path) -> Path:
    """Pipeline with a passthrough step (null output type)."""
    delta_root = tmp_path / "delta"
    delta_root.mkdir()

    _write_steps(
        delta_root,
        [
            {
                "step_run_id": "run_0",
                "step_spec_id": "spec_0",
                "step_number": 0,
                "step_name": "Ingest",
                "output_roles_json": json.dumps(["file"]),
                "output_types_json": json.dumps({"file": "file_ref"}),
            },
            {
                "step_run_id": "run_1",
                "step_spec_id": "spec_1",
                "step_number": 1,
                "step_name": "Filter",
                "input_refs_json": json.dumps(
                    {
                        "input": {
                            "type": "output_ref",
                            "source_step": 0,
                            "role": "file",
                            "artifact_type": "file_ref",
                        }
                    }
                ),
                "output_roles_json": json.dumps(["passthrough"]),
                "output_types_json": json.dumps({"passthrough": None}),
            },
        ],
    )
    return delta_root


@pytest.fixture
def multi_input_pipeline(tmp_path: Path) -> Path:
    """Pipeline with a step consuming two upstream outputs."""
    delta_root = tmp_path / "delta"
    delta_root.mkdir()

    _write_steps(
        delta_root,
        [
            {
                "step_run_id": "run_0",
                "step_spec_id": "spec_0",
                "step_number": 0,
                "step_name": "Ingest",
                "output_roles_json": json.dumps(["file"]),
                "output_types_json": json.dumps({"file": "file_ref"}),
            },
            {
                "step_run_id": "run_1",
                "step_spec_id": "spec_1",
                "step_number": 1,
                "step_name": "Design",
                "input_refs_json": json.dumps(
                    {
                        "input": {
                            "type": "output_ref",
                            "source_step": 0,
                            "role": "file",
                            "artifact_type": "file_ref",
                        }
                    }
                ),
                "output_roles_json": json.dumps(["data"]),
                "output_types_json": json.dumps({"data": "data"}),
            },
            {
                "step_run_id": "run_2",
                "step_spec_id": "spec_2",
                "step_number": 2,
                "step_name": "Merge",
                "input_refs_json": json.dumps(
                    [
                        {
                            "type": "output_ref",
                            "source_step": 0,
                            "role": "file",
                            "artifact_type": "file_ref",
                        },
                        {
                            "type": "output_ref",
                            "source_step": 1,
                            "role": "data",
                            "artifact_type": "data",
                        },
                    ]
                ),
                "output_roles_json": json.dumps(["merged"]),
                "output_types_json": json.dumps({"merged": "data"}),
            },
        ],
    )
    return delta_root


# =============================================================================
# Tests: _parse_input_refs
# =============================================================================


class TestParseInputRefs:
    """Tests for the _parse_input_refs helper."""

    def test_dict_format(self) -> None:
        """Dict-style input_refs produces (source_step, role) tuples."""
        refs_json = json.dumps(
            {
                "data": {
                    "type": "output_ref",
                    "source_step": 1,
                    "role": "data",
                    "artifact_type": "data",
                }
            }
        )
        result = _parse_input_refs(refs_json)
        assert result == [(1, "data")]

    def test_list_format(self) -> None:
        """List-style input_refs produces (source_step, role) tuples."""
        refs_json = json.dumps(
            [
                {
                    "type": "output_ref",
                    "source_step": 0,
                    "role": "file",
                    "artifact_type": "file_ref",
                },
                {
                    "type": "output_ref",
                    "source_step": 1,
                    "role": "data",
                    "artifact_type": "data",
                },
            ]
        )
        result = _parse_input_refs(refs_json)
        assert result == [(0, "file"), (1, "data")]

    def test_literals_skipped(self) -> None:
        """Literal entries (e.g. Ingest paths) are silently skipped."""
        refs_json = json.dumps(
            [
                {"type": "literal", "value": "/data/input.csv"},
            ]
        )
        result = _parse_input_refs(refs_json)
        assert result == []

    def test_null_json(self) -> None:
        """JSON 'null' produces empty list."""
        assert _parse_input_refs("null") == []

    def test_empty_string(self) -> None:
        """Empty string produces empty list."""
        assert _parse_input_refs("") == []

    def test_mixed_dict_with_literal(self) -> None:
        """Dict with both output_ref and literal values."""
        refs_json = json.dumps(
            {
                "data": {
                    "type": "output_ref",
                    "source_step": 0,
                    "role": "data",
                    "artifact_type": "data",
                },
                "config": {"type": "literal", "value": "some_value"},
            }
        )
        result = _parse_input_refs(refs_json)
        assert result == [(0, "data")]


# =============================================================================
# Tests: build_macro_graph
# =============================================================================


class TestBuildMacroGraph:
    """Tests for build_macro_graph function."""

    def test_returns_digraph(self, simple_pipeline: Path) -> None:
        """Returns a graphviz.Digraph object."""
        graph = build_macro_graph(simple_pipeline)
        assert isinstance(graph, graphviz.Digraph)

    def test_empty_pipeline(self, empty_delta_root: Path) -> None:
        """Empty delta root produces a graph with no real nodes."""
        graph = build_macro_graph(empty_delta_root)
        assert isinstance(graph, graphviz.Digraph)
        source = graph.source
        assert "digraph" in source

    def test_has_execution_nodes(self, simple_pipeline: Path) -> None:
        """Graph contains execution nodes with step numbers and names."""
        graph = build_macro_graph(simple_pipeline)
        source = graph.source
        assert "(0) Ingest" in source
        assert "(1) Transform" in source
        assert "(2) Metrics" in source

    def test_has_data_nodes(self, simple_pipeline: Path) -> None:
        """Graph contains data nodes labelled by output role."""
        graph = build_macro_graph(simple_pipeline)
        source = graph.source
        assert "data_0_file" in source
        assert "data_1_data" in source
        assert "data_2_metrics" in source

    def test_has_edges(self, simple_pipeline: Path) -> None:
        """Graph has directed edges."""
        graph = build_macro_graph(simple_pipeline)
        source = graph.source
        assert "->" in source

    def test_execution_nodes_grey(self, simple_pipeline: Path) -> None:
        """Execution nodes use grey fill."""
        graph = build_macro_graph(simple_pipeline)
        source = graph.source
        assert "fillcolor=lightgray" in source

    def test_data_nodes_coloured_by_type(self, simple_pipeline: Path) -> None:
        """Data nodes are coloured with blue shades from the registry palette."""
        graph = build_macro_graph(simple_pipeline)
        source = graph.source
        # All typed data nodes should get a blue shade from the palette
        assert '"#' in source  # At least one hex color present

    def test_left_to_right_layout(self, simple_pipeline: Path) -> None:
        """Graph uses LR layout."""
        graph = build_macro_graph(simple_pipeline)
        assert "rankdir=LR" in graph.source

    def test_column_ordering(self, simple_pipeline: Path) -> None:
        """Graph uses anchor nodes for strict column ordering."""
        graph = build_macro_graph(simple_pipeline)
        source = graph.source
        assert "_anchor_exec_" in source
        assert "style=invis" in source


class TestPassthroughStyling:
    """Tests for passthrough (null output_type) data nodes."""

    def test_passthrough_node_uses_passthrough_colour(
        self, passthrough_pipeline: Path
    ) -> None:
        """Data nodes with null artifact type get passthrough styling."""
        graph = build_macro_graph(passthrough_pipeline)
        source = graph.source
        assert "#E0F0FF" in source  # PASSTHROUGH_STYLE colour


class TestMultiInputOperations:
    """Tests for steps with multiple input references."""

    def test_multiple_edges_into_execution(self, multi_input_pipeline: Path) -> None:
        """Multi-input step has edges from each source data node."""
        graph = build_macro_graph(multi_input_pipeline)
        source = graph.source
        # Merge (step 2) should receive edges from data_0_file and data_1_data
        assert "data_0_file" in source
        assert "data_1_data" in source

        # Count visible edges involving exec_2 (exclude invisible anchor edges)
        lines = source.split("\n")
        edges_to_exec_2 = [
            line
            for line in lines
            if "->" in line and "exec_2" in line and "invis" not in line
        ]
        # Two input edges (data_0_file → exec_2, data_1_data → exec_2)
        # plus one output edge (exec_2 → data_2_merged)
        assert len(edges_to_exec_2) == 3


# =============================================================================
# Tests: render_macro_graph
# =============================================================================


class TestRenderMacroGraph:
    """Tests for render_macro_graph function."""

    def test_renders_svg(self, simple_pipeline: Path, tmp_path: Path) -> None:
        """Renders graph to an SVG file."""
        output_path = tmp_path / "output" / "macro"
        result = render_macro_graph(simple_pipeline, output_path, format="svg")

        assert result.exists()
        assert result.suffix == ".svg"

    def test_renders_png(self, simple_pipeline: Path, tmp_path: Path) -> None:
        """Renders graph to a PNG file."""
        output_path = tmp_path / "output" / "macro"
        result = render_macro_graph(simple_pipeline, output_path, format="png")

        assert result.exists()
        assert result.suffix == ".png"
        content = result.read_bytes()
        assert content[:8] == b"\x89PNG\r\n\x1a\n"

    def test_creates_output_directory(
        self, simple_pipeline: Path, tmp_path: Path
    ) -> None:
        """Creates parent directories if they don't exist."""
        output_path = tmp_path / "nested" / "deep" / "macro"
        result = render_macro_graph(simple_pipeline, output_path)

        assert result.exists()
        assert result.parent.exists()

    def test_returns_path(self, simple_pipeline: Path, tmp_path: Path) -> None:
        """Returns a Path object."""
        output_path = tmp_path / "macro"
        result = render_macro_graph(simple_pipeline, output_path)
        assert isinstance(result, Path)
