"""Tests for micro-level (artifact-level) provenance graph."""

from __future__ import annotations

import os
from pathlib import Path

import graphviz
import polars as pl
import pytest
from fixtures.retained_cache_isolation_store import build_cache_isolation_store
from fixtures.retained_inspection import commit_inspection_attempt, inspection_store

from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.utils.hashing import compute_content_digest
from artisan.visualization.graph import (
    build_micro_graph,
    get_max_step_number,
    render_micro_graph,
    render_micro_graph_steps,
)


@pytest.fixture(scope="module")
def delta_root_with_data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Persist two real outer deliveries with an original direct lineage chain."""
    workspace = tmp_path_factory.mktemp("micro-graph-inputs")
    root = workspace / "delta"
    payload = b"sample bytes"
    source = workspace / "sample.csv"
    source.write_bytes(payload)
    external = FileRefArtifact.draft(
        str(source),
        compute_content_digest(payload),
        len(payload),
        0,
        original_name="sample",
    ).finalize()
    parsed = MetricArtifact.draft({"parsed": True}, "parsed_result.json", 1).finalize()
    energy = MetricArtifact.draft({"energy": -100.5}, "energy.json", 2).finalize()
    commit_inspection_attempt(
        root,
        number=1,
        name="data_parser",
        inputs={"input_file": [external]},
        outputs={"parsed": [parsed]},
    )
    commit_inspection_attempt(
        root,
        number=2,
        name="metric_calc",
        inputs={"intermediate": [parsed]},
        outputs={"energy": [energy]},
    )
    return root


@pytest.fixture(scope="module")
def empty_delta_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create an initialized store with no accepted executions or artifacts."""
    root = tmp_path_factory.mktemp("micro-graph-empty") / "delta"
    inspection_store(root)
    return root


class TestBuildMicroGraph:
    """Tests for build_micro_graph function."""

    def test_default_graph_has_labeled_nodes_and_styles(
        self, delta_root_with_data: Path
    ) -> None:
        """Default graph layout and styling distinguish executions and artifacts."""
        graph = build_micro_graph(delta_root_with_data)
        assert isinstance(graph, graphviz.Digraph)
        source = graph.source
        assert "(1) data_parser" in source
        assert "(2) metric_calc" in source
        assert "parsed_result" in source
        assert "sample" in source
        assert "energy" in source
        assert "rankdir=LR" in source
        assert "_anchor_exec_" in source or "_anchor_art_" in source
        assert "style=invis" in source
        assert "->" in source
        assert "color=orange" in source
        assert "shape=box" in source
        assert "fillcolor=lightgray" in source
        assert '"#' in source

    def test_empty_delta_root_returns_empty_graph(self, empty_delta_root: Path) -> None:
        """Empty delta root produces a graph with no nodes/edges."""
        graph = build_micro_graph(empty_delta_root)
        assert isinstance(graph, graphviz.Digraph)
        source = graph.source
        assert "digraph" in source

    def test_accepts_path_or_string(self, delta_root_with_data: Path) -> None:
        """Function accepts both Path and string delta_root."""
        graph1 = build_micro_graph(delta_root_with_data)
        graph2 = build_micro_graph(str(delta_root_with_data))

        assert isinstance(graph1, graphviz.Digraph)
        assert isinstance(graph2, graphviz.Digraph)

    def test_run_scope_places_cached_participation_and_excludes_other_run(
        self, tmp_path: Path
    ) -> None:
        store = build_cache_isolation_store(tmp_path)

        source = build_micro_graph(
            store.root,
            pipeline_run_id=store.current_run,
        ).source

        assert "(0) current_data" in source
        assert "(5) source_metric" in source
        assert "other_data" not in source
        assert "other_metric" not in source
        participation = (
            f"exec_{store.current_cache_step_id}_{store.source_metric_execution}"
        )
        assert participation in source
        assert source.count(f"art_{store.metric_id}") >= 1


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

        assert "(1) data_parser" in source
        assert "(2) metric_calc" in source

    def test_max_step_filters_executions(self, delta_root_with_data: Path) -> None:
        """max_step filters execution nodes to steps <= max_step."""
        graph = build_micro_graph(delta_root_with_data, max_step=1)
        source = graph.source

        assert "(1) data_parser" in source
        assert "(2) metric_calc" not in source

    def test_max_step_filters_artifacts(self, delta_root_with_data: Path) -> None:
        """max_step filters artifact nodes to steps <= max_step."""
        graph = build_micro_graph(delta_root_with_data, max_step=1)
        source = graph.source

        # Step 0 and 1 artifacts should be included
        assert "sample" in source  # External file from step 0
        assert "parsed_result" in source  # Intermediate metric from step 1

        assert "metric_calc" not in source
        assert "energy" not in source

    def test_max_step_filters_execution_edges(self, delta_root_with_data: Path) -> None:
        """max_step filters execution provenance edges appropriately."""
        graph = build_micro_graph(delta_root_with_data, max_step=1)
        source = graph.source

        # Edges for exec_2 (step 2) should not be present
        assert "metric_calc" not in source

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

        assert "data_parser" not in source
        assert "metric_calc" not in source

    def test_run_scoped_max_step_uses_current_cached_participation(
        self, tmp_path: Path
    ) -> None:
        store = build_cache_isolation_store(
            tmp_path,
            reuse_source_metric_at_step_zero=True,
        )
        steps = pl.read_delta(str(store.root / "orchestration/steps"))
        current_step_id = (
            steps.filter(
                (pl.col("pipeline_run_id") == store.current_run)
                & (pl.col("step_number") == 0)
            )["step_run_id"]
            .unique()
            .item()
        )
        source = build_micro_graph(
            store.root,
            max_step=0,
            pipeline_run_id=store.current_run,
        ).source

        participation = f"exec_{current_step_id}_{store.source_metric_execution}"
        assert "(0) source_metric" in source
        assert participation in source
        assert f"art_{store.metric_id}" in source


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
