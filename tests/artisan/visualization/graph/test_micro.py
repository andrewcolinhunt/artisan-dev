"""Tests for micro-level (artifact-level) provenance graph."""

from __future__ import annotations

import os
from pathlib import Path

import graphviz
import polars as pl
import pytest

from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
)
from artisan.visualization.graph import (
    build_micro_graph,
    get_max_step_number,
    render_micro_graph,
    render_micro_graph_steps,
)


@pytest.fixture
def delta_root_with_data(tmp_path: Path) -> Path:
    """Create Delta Lake tables with test provenance data."""
    delta_root = tmp_path / "delta"
    delta_root.mkdir()

    # Create executions
    exec_data = {
        "execution_run_id": ["exec_1", "exec_2"],
        "execution_spec_id": ["spec_1", "spec_2"],
        "step_run_id": [None, None],
        "origin_step_number": [1, 2],
        "operation_name": ["data_parser", "metric_calc"],
        "params": ["{}", "{}"],
        "user_overrides": ["{}", "{}"],
        "timestamp_start": [None, None],
        "timestamp_end": [None, None],
        "source_worker": [0, 0],
        "compute_backend": ["local", "local"],
        "success": [True, True],
        "error": [None, None],
        "tool_output": [None, None],
        "worker_log": [None, None],
        "metadata": ["{}", "{}"],
    }
    exec_df = pl.DataFrame(exec_data, schema=EXECUTIONS_SCHEMA)
    exec_df.write_delta(str(delta_root / "orchestration/executions"), mode="overwrite")

    # Create artifact_index
    artifact_data = {
        "artifact_id": ["art_ext_1", "art_inter_1", "art_metric_1"],
        "artifact_type": ["file_ref", "metric", "metric"],
        "origin_step_number": [0, 1, 2],
        "metadata": ["{}", "{}", "{}"],
    }
    artifact_df = pl.DataFrame(artifact_data, schema=ARTIFACT_INDEX_SCHEMA)
    artifact_df.write_delta(str(delta_root / "artifacts/index"), mode="overwrite")

    # Create metrics (intermediate + final)
    metric_data = {
        "artifact_id": ["art_inter_1", "art_metric_1"],
        "origin_step_number": [1, 2],
        "content": [b'{"parsed": true}', b'{"energy": -100.5}'],
        "original_name": ["parsed_result", "energy"],
        "extension": [".json", ".json"],
        "metadata": ["{}", "{}"],
        "external_path": [None, None],
    }
    metric_df = pl.DataFrame(metric_data, schema=MetricArtifact.POLARS_SCHEMA)
    metric_df.write_delta(str(delta_root / "artifacts/metrics"), mode="overwrite")

    # Create file_refs
    ext_data = {
        "artifact_id": ["art_ext_1"],
        "origin_step_number": [0],
        "content_hash": ["hash123"],
        "path": ["/data/input/sample.csv"],
        "size_bytes": [200],
        "metadata": ["{}"],
        "original_name": ["sample"],
        "extension": [".csv"],
        "external_path": [None],
    }
    ext_df = pl.DataFrame(ext_data, schema=FileRefArtifact.POLARS_SCHEMA)
    ext_df.write_delta(str(delta_root / "artifacts/file_refs"), mode="overwrite")

    # Create execution_edges
    exec_prov_data = {
        "execution_run_id": ["exec_1", "exec_1", "exec_2", "exec_2"],
        "direction": ["input", "output", "input", "output"],
        "role": ["input_file", "parsed", "intermediate", "energy"],
        "artifact_id": ["art_ext_1", "art_inter_1", "art_inter_1", "art_metric_1"],
    }
    exec_prov_df = pl.DataFrame(exec_prov_data, schema=EXECUTION_EDGES_SCHEMA)
    exec_prov_df.write_delta(
        str(delta_root / "provenance/execution_edges"), mode="overwrite"
    )

    # Create artifact_edges
    art_prov_data = {
        "execution_run_id": ["exec_1", "exec_2"],
        "source_artifact_id": ["art_ext_1", "art_inter_1"],
        "target_artifact_id": ["art_inter_1", "art_metric_1"],
        "source_artifact_type": ["file_ref", "metric"],
        "target_artifact_type": ["metric", "metric"],
        "source_role": ["input_file", "intermediate"],
        "target_role": ["parsed", "energy"],
        "group_id": [None, None],
        "step_boundary": [True, True],
    }
    art_prov_df = pl.DataFrame(art_prov_data, schema=ARTIFACT_EDGES_SCHEMA)
    art_prov_df.write_delta(
        str(delta_root / "provenance/artifact_edges"), mode="overwrite"
    )

    return delta_root


@pytest.fixture
def empty_delta_root(tmp_path: Path) -> Path:
    """Create empty Delta Lake root directory."""
    delta_root = tmp_path / "delta_empty"
    delta_root.mkdir()
    return delta_root


class TestBuildMicroGraph:
    """Tests for build_micro_graph function."""

    def test_returns_graphviz_digraph(self, delta_root_with_data: Path) -> None:
        """Function returns a graphviz.Digraph object."""
        graph = build_micro_graph(delta_root_with_data)
        assert isinstance(graph, graphviz.Digraph)

    def test_empty_delta_root_returns_empty_graph(self, empty_delta_root: Path) -> None:
        """Empty delta root produces a graph with no nodes/edges."""
        graph = build_micro_graph(empty_delta_root)
        assert isinstance(graph, graphviz.Digraph)
        # Graph source should have minimal content (just header)
        source = graph.source
        assert "digraph" in source

    def test_graph_has_execution_nodes(self, delta_root_with_data: Path) -> None:
        """Graph contains execution nodes with step numbers and names."""
        graph = build_micro_graph(delta_root_with_data)
        source = graph.source

        # Execution nodes should have step numbers and operation names as labels
        assert "(1) data_parser" in source
        assert "(2) metric_calc" in source

    def test_graph_has_artifact_nodes(self, delta_root_with_data: Path) -> None:
        """Graph contains artifact nodes with appropriate labels."""
        graph = build_micro_graph(delta_root_with_data)
        source = graph.source

        # Intermediate metric should use original_name
        assert "parsed_result" in source
        # External file should use basename without extension
        assert "sample" in source
        # Final metric should use original_name
        assert "energy" in source

    def test_graph_has_left_to_right_layout(self, delta_root_with_data: Path) -> None:
        """Graph uses left-to-right layout direction."""
        graph = build_micro_graph(delta_root_with_data)
        source = graph.source
        assert "rankdir=LR" in source

    def test_graph_has_strict_column_ordering(self, delta_root_with_data: Path) -> None:
        """Graph uses anchor nodes with invisible edges for strict column order."""
        graph = build_micro_graph(delta_root_with_data)
        source = graph.source

        # Should have invisible anchor nodes for enforcing column order
        assert "_anchor_exec_" in source or "_anchor_art_" in source

        # Should have invisible edges between anchors
        assert "style=invis" in source

    def test_graph_has_execution_edges(self, delta_root_with_data: Path) -> None:
        """Graph contains edges between artifacts and executions."""
        graph = build_micro_graph(delta_root_with_data)
        source = graph.source

        # Should have edges (arrow notation in DOT format)
        assert "->" in source

    def test_graph_has_lineage_edges_in_orange(
        self, delta_root_with_data: Path
    ) -> None:
        """Artifact-to-artifact lineage edges are colored orange."""
        graph = build_micro_graph(delta_root_with_data)
        source = graph.source

        # Lineage edges should have orange color
        assert "color=orange" in source

    def test_all_nodes_are_boxes(self, delta_root_with_data: Path) -> None:
        """All nodes use box shape."""
        graph = build_micro_graph(delta_root_with_data)
        source = graph.source
        assert "shape=box" in source

    def test_execution_nodes_are_grey(self, delta_root_with_data: Path) -> None:
        """Execution nodes use grey fill color."""
        graph = build_micro_graph(delta_root_with_data)
        source = graph.source
        assert "fillcolor=lightgray" in source

    def test_artifact_nodes_are_blue_shades(self, delta_root_with_data: Path) -> None:
        """Artifact nodes use blue shades from the registry-driven palette."""
        graph = build_micro_graph(delta_root_with_data)
        source = graph.source
        # All artifact nodes should get a blue shade from the palette
        assert '"#' in source  # At least one hex color present

    def test_accepts_path_or_string(self, delta_root_with_data: Path) -> None:
        """Function accepts both Path and string delta_root."""
        # Test with Path
        graph1 = build_micro_graph(delta_root_with_data)
        # Test with string
        graph2 = build_micro_graph(str(delta_root_with_data))

        assert isinstance(graph1, graphviz.Digraph)
        assert isinstance(graph2, graphviz.Digraph)


class TestRenderMicroGraph:
    """Tests for render_micro_graph function."""

    def test_renders_svg_file(self, delta_root_with_data: Path, tmp_path: Path) -> None:
        """Renders graph to SVG format."""
        output_path = tmp_path / "output" / "graph"

        result = render_micro_graph(
            str(delta_root_with_data), output_path, format="svg"
        )

        assert os.path.exists(result)
        assert result.endswith(".svg")
        with open(result) as f:
            content = f.read()
        assert content.startswith("<?xml") or "<svg" in content

    def test_renders_png_file(self, delta_root_with_data: Path, tmp_path: Path) -> None:
        """Renders graph to PNG format."""
        output_path = tmp_path / "output" / "graph"

        result = render_micro_graph(
            str(delta_root_with_data), output_path, format="png"
        )

        assert os.path.exists(result)
        assert result.endswith(".png")
        # PNG files start with specific magic bytes
        with open(result, "rb") as f:
            content = f.read()
        assert content[:8] == b"\x89PNG\r\n\x1a\n"

    def test_creates_output_directory(
        self, delta_root_with_data: Path, tmp_path: Path
    ) -> None:
        """Creates parent directories if they don't exist."""
        output_path = tmp_path / "nested" / "deep" / "graph"

        result = render_micro_graph(str(delta_root_with_data), output_path)

        assert os.path.exists(result)
        assert os.path.exists(os.path.dirname(result))

    def test_returns_str(self, delta_root_with_data: Path, tmp_path: Path) -> None:
        """Returns a str path pointing to the rendered file."""
        output_path = tmp_path / "graph"

        result = render_micro_graph(str(delta_root_with_data), output_path)

        assert isinstance(result, str)

    def test_default_format_is_svg(
        self, delta_root_with_data: Path, tmp_path: Path
    ) -> None:
        """Default output format is SVG."""
        output_path = tmp_path / "graph"

        result = render_micro_graph(str(delta_root_with_data), output_path)

        assert result.endswith(".svg")


class TestNodeLabeling:
    """Tests for artifact node label generation."""

    def test_metric_uses_original_name(self, delta_root_with_data: Path) -> None:
        """Metric artifacts labeled with original_name."""
        graph = build_micro_graph(delta_root_with_data)
        source = graph.source
        assert "parsed_result" in source
        assert "energy" in source

    def test_file_ref_uses_basename_without_extension(
        self, delta_root_with_data: Path
    ) -> None:
        """File ref artifacts labeled with basename (no extension)."""
        graph = build_micro_graph(delta_root_with_data)
        source = graph.source
        # Should have basename without extension, not full path
        assert "sample" in source
        assert "sample.csv" not in source
        assert "/data/input/" not in source


class TestEdgeStyling:
    """Tests for edge styling in the graph."""

    def test_lineage_edges_are_orange(self, delta_root_with_data: Path) -> None:
        """Artifact-to-artifact lineage edges are colored orange."""
        graph = build_micro_graph(delta_root_with_data)
        source = graph.source
        assert "color=orange" in source

    def test_execution_edges_have_no_special_color(
        self, delta_root_with_data: Path
    ) -> None:
        """Execution provenance edges use default color (no color attr)."""
        graph = build_micro_graph(delta_root_with_data)
        source = graph.source
        # Count orange edges vs total edges
        # Lineage edges should be orange, execution edges should not have color
        lines = source.split("\n")
        edge_lines = [line for line in lines if "->" in line]

        # We have 2 lineage edges (orange) and 4 execution edges (no color)
        orange_edges = [line for line in edge_lines if "color=orange" in line]
        assert len(orange_edges) == 2  # Two lineage edges in test data


class TestMaxStepFiltering:
    """Tests for max_step parameter in build_micro_graph."""

    def test_max_step_none_returns_all_steps(self, delta_root_with_data: Path) -> None:
        """max_step=None (default) includes all steps."""
        graph = build_micro_graph(delta_root_with_data, max_step=None)
        source = graph.source

        # Should have all executions
        assert "(1) data_parser" in source
        assert "(2) metric_calc" in source

    def test_max_step_filters_executions(self, delta_root_with_data: Path) -> None:
        """max_step filters execution nodes to steps <= max_step."""
        graph = build_micro_graph(delta_root_with_data, max_step=1)
        source = graph.source

        # Step 1 should be included
        assert "(1) data_parser" in source
        # Step 2 should be excluded
        assert "(2) metric_calc" not in source

    def test_max_step_filters_artifacts(self, delta_root_with_data: Path) -> None:
        """max_step filters artifact nodes to steps <= max_step."""
        graph = build_micro_graph(delta_root_with_data, max_step=1)
        source = graph.source

        # Step 0 and 1 artifacts should be included
        assert "sample" in source  # External file from step 0
        assert "parsed_result" in source  # Intermediate metric from step 1

        # Step 2 execution and artifacts should be excluded
        assert "metric_calc" not in source
        assert "art_metric_1" not in source

    def test_max_step_filters_execution_edges(self, delta_root_with_data: Path) -> None:
        """max_step filters execution provenance edges appropriately."""
        graph = build_micro_graph(delta_root_with_data, max_step=1)
        source = graph.source

        # Edges for exec_2 (step 2) should not be present
        assert "exec_exec_2" not in source

    def test_max_step_filters_artifact_edges(self, delta_root_with_data: Path) -> None:
        """max_step filters artifact provenance edges where target is excluded."""
        graph = build_micro_graph(delta_root_with_data, max_step=1)
        source = graph.source

        # Lineage edge to art_metric_1 (step 2) should not exist
        # Only 1 lineage edge should remain (ext_1 -> inter_1)
        lines = source.split("\n")
        edge_lines = [line for line in lines if "->" in line]
        orange_edges = [line for line in edge_lines if "color=orange" in line]
        assert len(orange_edges) == 1  # Only ext_1 -> inter_1

    def test_max_step_zero_shows_only_step_zero(
        self, delta_root_with_data: Path
    ) -> None:
        """max_step=0 shows only step 0 nodes."""
        graph = build_micro_graph(delta_root_with_data, max_step=0)
        source = graph.source

        # Step 0 artifact (external file) should be included
        assert "sample" in source

        # Step 1+ executions should not be included
        assert "data_parser" not in source
        assert "metric_calc" not in source


class TestGetMaxStepNumber:
    """Tests for get_max_step_number function."""

    def test_returns_max_step_from_executions(self, delta_root_with_data: Path) -> None:
        """Returns the maximum step number from execution records."""
        result = get_max_step_number(delta_root_with_data)
        assert result == 2  # Test data has steps 1 and 2

    def test_returns_none_for_empty_executions(self, empty_delta_root: Path) -> None:
        """Returns None when no execution records exist."""
        result = get_max_step_number(empty_delta_root)
        assert result is None


class TestRenderMicroGraphSteps:
    """Tests for render_micro_graph_steps function."""

    def test_renders_one_image_per_step(
        self, delta_root_with_data: Path, tmp_path: Path
    ) -> None:
        """Renders one image for each step (cumulative)."""
        output_dir = tmp_path / "images"

        result = render_micro_graph_steps(str(delta_root_with_data), output_dir)

        # Test data has steps 1 and 2, so max_step is 2, meaning steps 0, 1, 2
        assert len(result) == 3  # Steps 0, 1, 2
        for p in result:
            assert os.path.exists(p)
            assert p.endswith(".svg")

    def test_filenames_are_zero_padded(
        self, delta_root_with_data: Path, tmp_path: Path
    ) -> None:
        """Output filenames use zero-padded step numbers."""
        output_dir = tmp_path / "images"

        result = render_micro_graph_steps(str(delta_root_with_data), output_dir)

        assert os.path.basename(result[0]) == "step_00.svg"
        assert os.path.basename(result[1]) == "step_01.svg"
        assert os.path.basename(result[2]) == "step_02.svg"

    def test_creates_output_directory(
        self, delta_root_with_data: Path, tmp_path: Path
    ) -> None:
        """Creates output directory if it doesn't exist."""
        output_dir = tmp_path / "nested" / "images"

        render_micro_graph_steps(str(delta_root_with_data), output_dir)

        assert output_dir.exists()

    def test_returns_empty_list_for_no_executions(
        self, empty_delta_root: Path, tmp_path: Path
    ) -> None:
        """Returns empty list when no executions exist."""
        output_dir = tmp_path / "images"

        result = render_micro_graph_steps(str(empty_delta_root), output_dir)

        assert result == []

    def test_each_step_is_cumulative(
        self, delta_root_with_data: Path, tmp_path: Path
    ) -> None:
        """Each step's image includes all prior steps."""
        output_dir = tmp_path / "images"

        result = render_micro_graph_steps(str(delta_root_with_data), output_dir)

        # Step 0: only external file
        with open(result[0]) as f:
            step0_content = f.read()
        assert "sample" in step0_content
        assert "data_parser" not in step0_content

        # Step 1: external file + step 1 execution + intermediate metric
        with open(result[1]) as f:
            step1_content = f.read()
        assert "sample" in step1_content
        assert "data_parser" in step1_content
        assert "parsed_result" in step1_content
        assert "metric_calc" not in step1_content

        # Step 2: everything
        with open(result[2]) as f:
            step2_content = f.read()
        assert "sample" in step2_content
        assert "data_parser" in step2_content
        assert "parsed_result" in step2_content
        assert "metric_calc" in step2_content

    def test_supports_png_format(
        self, delta_root_with_data: Path, tmp_path: Path
    ) -> None:
        """Can render steps in PNG format."""
        output_dir = tmp_path / "images"

        result = render_micro_graph_steps(
            str(delta_root_with_data), output_dir, format="png"
        )

        assert len(result) == 3
        for p in result:
            assert p.endswith(".png")
            with open(p, "rb") as f:
                content = f.read()
            assert content[:8] == b"\x89PNG\r\n\x1a\n"
