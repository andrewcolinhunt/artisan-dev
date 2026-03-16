"""Micro-level (artifact-level) provenance graph visualization using Graphviz.

This module provides utilities to visualize the provenance graph stored in
Delta Lake tables. The visualization overlays both the execution graph
(artifact ↔ execution relationships) and the lineage graph (artifact → artifact
derivations) into a single unified view.

Each individual artifact and execution appears as its own node — hence "micro".
For a higher-level step-based view, see ``artisan.visualization.graph.macro``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import graphviz
import polars as pl

from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.visualization.graph._styles import EXECUTION_STYLE, get_artifact_style

# =============================================================================
# Data Loading
# =============================================================================


def _load_executions(delta_root: Path) -> pl.DataFrame:
    """Return execution records from the executions Delta table."""
    table_path = delta_root / TablePath.EXECUTIONS
    if not table_path.exists():
        return pl.DataFrame(
            schema={
                "execution_run_id": pl.String,
                "operation_name": pl.String,
                "origin_step_number": pl.Int32,
            }
        )

    return (
        pl.scan_delta(str(table_path))
        .select(["execution_run_id", "operation_name", "origin_step_number"])
        .collect()
    )


def _load_artifact_index(delta_root: Path) -> pl.DataFrame:
    """Return artifact index records from the artifact_index Delta table."""
    table_path = delta_root / TablePath.ARTIFACT_INDEX
    if not table_path.exists():
        return pl.DataFrame(
            schema={
                "artifact_id": pl.String,
                "artifact_type": pl.String,
                "origin_step_number": pl.Int32,
            }
        )

    return (
        pl.scan_delta(str(table_path))
        .select(["artifact_id", "artifact_type", "origin_step_number"])
        .collect()
    )


def _load_artifact_labels(delta_root: Path) -> dict[str, str]:
    """Map artifact IDs to human-readable labels from type-specific tables.

    Iterates registered ArtifactTypeDefs, loading ``original_name`` (or
    ``path``) from each type's table when available and returning the
    stem as the display label.
    """
    labels: dict[str, str] = {}

    for _key, typedef in ArtifactTypeDef.get_all().items():
        schema = typedef.model.POLARS_SCHEMA
        table_path = delta_root / typedef.table_path

        if not table_path.exists():
            continue

        if "original_name" in schema:
            df = (
                pl.scan_delta(str(table_path))
                .select(["artifact_id", "original_name"])
                .collect()
            )
            for row in df.iter_rows(named=True):
                name = row["original_name"]
                if name:
                    labels[row["artifact_id"]] = Path(name).stem
        elif "path" in schema:
            df = (
                pl.scan_delta(str(table_path)).select(["artifact_id", "path"]).collect()
            )
            for row in df.iter_rows(named=True):
                path = row["path"]
                if path:
                    labels[row["artifact_id"]] = Path(path).stem

    return labels


def _load_execution_edges(delta_root: Path) -> pl.DataFrame:
    """Return execution-to-artifact provenance edges."""
    table_path = delta_root / TablePath.EXECUTION_EDGES
    if not table_path.exists():
        return pl.DataFrame(
            schema={
                "execution_run_id": pl.String,
                "direction": pl.String,
                "artifact_id": pl.String,
            }
        )

    return (
        pl.scan_delta(str(table_path))
        .select(["execution_run_id", "direction", "artifact_id"])
        .collect()
    )


def _load_artifact_edges(delta_root: Path) -> pl.DataFrame:
    """Return artifact-to-artifact lineage edges."""
    table_path = delta_root / TablePath.ARTIFACT_EDGES
    if not table_path.exists():
        return pl.DataFrame(
            schema={
                "source_artifact_id": pl.String,
                "target_artifact_id": pl.String,
            }
        )

    return (
        pl.scan_delta(str(table_path))
        .select(["source_artifact_id", "target_artifact_id"])
        .collect()
    )


# =============================================================================
# Label Generation
# =============================================================================


def _build_artifact_labels(
    artifact_index: pl.DataFrame,
    name_labels: dict[str, str],
) -> dict[str, str]:
    """Merge pre-loaded name labels with truncated-ID fallbacks."""
    labels: dict[str, str] = {}
    for row in artifact_index.iter_rows(named=True):
        artifact_id = row["artifact_id"]
        labels[artifact_id] = name_labels.get(artifact_id, artifact_id[:8])
    return labels


# =============================================================================
# Graph Building
# =============================================================================


def build_micro_graph(
    delta_root: Path,
    max_step: int | None = None,
) -> graphviz.Digraph:
    """Build a Graphviz Digraph from Delta Lake provenance tables.

    Creates a unified visualization showing:
    - Execution nodes (operations) as rectangles
    - Artifact nodes (data, files, metrics) with type-specific shapes
    - Execution provenance edges (artifact ↔ execution)
    - Lineage edges (artifact → artifact) in blue

    Layout uses strict left-to-right column ordering:
    - Step 0 executions → Step 0 outputs → Step 1 executions → Step 1 outputs → ...

    Args:
        delta_root: Path to Delta Lake root directory.
        max_step: If provided, only include steps 0 through max_step (inclusive).
            Useful for step-by-step visualization of pipeline execution.

    Returns:
        Graphviz Digraph object (renders inline in Jupyter).
    """
    delta_root = Path(delta_root)

    # Load all data
    executions = _load_executions(delta_root)
    artifact_index = _load_artifact_index(delta_root)
    name_labels = _load_artifact_labels(delta_root)
    exec_edges = _load_execution_edges(delta_root)
    artifact_edges = _load_artifact_edges(delta_root)

    # Filter by max_step if provided
    if max_step is not None:
        # Filter executions to steps <= max_step
        executions = executions.filter(pl.col("origin_step_number") <= max_step)

        # Filter artifacts to steps <= max_step
        artifact_index = artifact_index.filter(pl.col("origin_step_number") <= max_step)

        # Get the set of included execution and artifact IDs for edge filtering
        included_exec_ids = set(executions["execution_run_id"].to_list())
        included_artifact_ids = set(artifact_index["artifact_id"].to_list())

        # Filter execution provenance to only edges where both endpoints are included
        exec_edges = exec_edges.filter(
            pl.col("execution_run_id").is_in(included_exec_ids)
            & pl.col("artifact_id").is_in(included_artifact_ids)
        )

        # Filter artifact provenance to only edges where both endpoints are included
        artifact_edges = artifact_edges.filter(
            pl.col("source_artifact_id").is_in(included_artifact_ids)
            & pl.col("target_artifact_id").is_in(included_artifact_ids)
        )

    # Build label mappings
    artifact_labels = _build_artifact_labels(artifact_index, name_labels)

    # Create graph with strict left-to-right layout
    graph = graphviz.Digraph("provenance", format="svg")
    graph.attr(rankdir="LR")
    graph.attr("node", style="filled")
    # Keep layout compact
    graph.attr(nodesep="0.25", ranksep="0.4")
    # Draw edges behind nodes for cleaner visualization
    graph.attr(outputorder="edgesfirst")
    # Use straight line edges instead of splines that route around nodes
    graph.attr(splines="line")

    # Build execution step lookup
    exec_step_lookup: dict[str, int] = {}
    for row in executions.iter_rows(named=True):
        exec_step_lookup[row["execution_run_id"]] = row["origin_step_number"]

    # Track which steps have outputs (for creating output columns)
    steps_with_outputs: set[int] = set()
    for row in exec_edges.iter_rows(named=True):
        if row["direction"] == "output":
            exec_id = row["execution_run_id"]
            step = exec_step_lookup.get(exec_id, 0)
            steps_with_outputs.add(step)

    # Group nodes by step for ranking
    exec_by_step: dict[int, list[str]] = {}
    artifacts_by_step: dict[int, list[str]] = {}

    # Add execution nodes
    for row in executions.iter_rows(named=True):
        exec_id = row["execution_run_id"]
        op_name = row["operation_name"]
        step = row["origin_step_number"]

        shape, color = EXECUTION_STYLE
        graph.node(
            f"exec_{exec_id}",
            label=f"({step}) {op_name}",
            shape=shape,
            fillcolor=color,
        )

        if step not in exec_by_step:
            exec_by_step[step] = []
        exec_by_step[step].append(f"exec_{exec_id}")

    # Add artifact nodes (grouped by ORIGIN step - where they were created)
    # Artifacts stay in their original columns; passthrough edges point backward
    artifact_origin_step: dict[str, int] = {}
    for row in artifact_index.iter_rows(named=True):
        artifact_id = row["artifact_id"]
        artifact_type_str = row["artifact_type"]
        step = row["origin_step_number"]
        artifact_origin_step[artifact_id] = step

        shape, color = get_artifact_style(artifact_type_str)

        label = artifact_labels.get(artifact_id, artifact_id[:8])
        graph.node(
            f"art_{artifact_id}",
            label=label,
            shape=shape,
            fillcolor=color,
        )

        if step not in artifacts_by_step:
            artifacts_by_step[step] = []
        artifacts_by_step[step].append(f"art_{artifact_id}")

    # ==========================================================================
    # Strict column ordering using anchor nodes and rank constraints
    # Column order: exec_0 -> art_0 -> exec_1 -> art_1 -> exec_2 -> art_2 -> ...
    # Always create art_N column for any step N that has outputs (even passthroughs)
    # ==========================================================================
    all_steps = sorted(
        set(exec_by_step.keys()) | set(artifacts_by_step.keys()) | steps_with_outputs
    )

    # Create invisible anchor nodes for each column
    anchor_nodes: list[str] = []
    for step in all_steps:
        if step in exec_by_step:
            anchor_nodes.append(f"_anchor_exec_{step}")
        # Create art column if step has artifacts OR has outputs (passthroughs)
        if step in artifacts_by_step or step in steps_with_outputs:
            anchor_nodes.append(f"_anchor_art_{step}")

    # Add anchor nodes (invisible)
    for anchor in anchor_nodes:
        graph.node(anchor, label="", width="0", height="0", style="invis")

    # Link anchors with invisible edges to enforce column order
    for i in range(len(anchor_nodes) - 1):
        graph.edge(anchor_nodes[i], anchor_nodes[i + 1], style="invis")

    # Group nodes into columns using rank=same with anchors
    # Anchors must be in the rank=same subgraph to enforce column ordering
    for step in all_steps:
        # Executions column for this step
        step_execs = exec_by_step.get(step, [])
        if step_execs:
            with graph.subgraph() as s:
                s.attr(rank="same")
                s.node(f"_anchor_exec_{step}")
                for node_id in step_execs:
                    s.node(node_id)

        # Artifacts column for this step (outputs of this step)
        # Create column if step has artifacts OR has outputs (for passthrough steps)
        step_artifacts = artifacts_by_step.get(step, [])
        if step_artifacts or step in steps_with_outputs:
            with graph.subgraph() as s:
                s.attr(rank="same")
                s.node(f"_anchor_art_{step}")
                for node_id in step_artifacts:
                    s.node(node_id)

    # Add execution provenance edges (artifact ↔ execution)
    # Use artifact origin step for column position (artifacts stay in origin columns)
    for row in exec_edges.iter_rows(named=True):
        exec_id = row["execution_run_id"]
        direction = row["direction"]
        artifact_id = row["artifact_id"]

        exec_node = f"exec_{exec_id}"
        art_node = f"art_{artifact_id}"

        exec_step = exec_step_lookup.get(exec_id, 0)
        art_step = artifact_origin_step.get(artifact_id, 0)

        if direction == "input":
            # Artifact -> Execution (input edge)
            # Column ordering: exec_N -> art_N, so if art_step >= exec_step,
            # the artifact column comes AFTER the execution column (backward edge)
            if art_step >= exec_step:
                graph.edge(
                    art_node,
                    exec_node,
                    constraint="false",
                    style="dashed",
                    dir="both",
                    arrowtail="dot",
                )
            else:
                graph.edge(art_node, exec_node, dir="both", arrowtail="dot")
        elif art_step < exec_step:
            graph.edge(
                exec_node,
                art_node,
                constraint="false",
                style="dashed",
                dir="both",
                arrowtail="dot",
            )
        else:
            graph.edge(exec_node, art_node, dir="both", arrowtail="dot")

    # Add artifact lineage edges (artifact -> artifact) in blue
    for row in artifact_edges.iter_rows(named=True):
        source_id = row["source_artifact_id"]
        target_id = row["target_artifact_id"]

        source_step = artifact_origin_step.get(source_id, 0)
        target_step = artifact_origin_step.get(target_id, 0)

        # If source is in a later column than target, it's a backward edge
        if source_step > target_step:
            graph.edge(
                f"art_{source_id}",
                f"art_{target_id}",
                color="orange",
                constraint="false",
                style="dashed",
                dir="both",
                arrowtail="dot",
            )
        else:
            graph.edge(
                f"art_{source_id}",
                f"art_{target_id}",
                color="orange",
                dir="both",
                arrowtail="dot",
            )

    return graph


def render_micro_graph(
    delta_root: Path,
    output_path: Path,
    format: Literal["svg", "png"] = "svg",
    max_step: int | None = None,
) -> Path:
    """Build and render the micro (artifact-level) provenance graph to a file.

    Args:
        delta_root: Path to Delta Lake root directory.
        output_path: Output file path (without extension).
        format: Output format ("svg" or "png").
        max_step: If provided, only include steps 0 through max_step (inclusive).

    Returns:
        Path to the rendered file.
    """
    graph = build_micro_graph(delta_root, max_step=max_step)
    graph.format = format

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Graphviz render() returns path with extension added
    rendered_path = graph.render(
        filename=str(output_path),
        cleanup=True,  # Remove intermediate .gv file
    )

    return Path(rendered_path)


def get_max_step_number(delta_root: Path) -> int | None:
    """Return the highest step number present in the executions table.

    Args:
        delta_root: Path to Delta Lake root directory.

    Returns:
        Maximum step number, or None if no executions exist.
    """
    executions = _load_executions(delta_root)
    if executions.is_empty():
        return None
    return executions["origin_step_number"].max()


def render_micro_graph_steps(
    delta_root: Path,
    output_dir: Path,
    format: Literal["svg", "png"] = "svg",
) -> list[Path]:
    """Render provenance graphs for each step (cumulative).

    Generates one image per step, where step N shows all nodes and edges
    from steps 0 through N. Useful for step-by-step visualization.

    Args:
        delta_root: Path to Delta Lake root directory.
        output_dir: Directory to write step images (step_00.svg, step_01.svg, ...).
        format: Output format ("svg" or "png").

    Returns:
        List of paths to rendered files, in step order.
    """
    delta_root = Path(delta_root)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    max_step = get_max_step_number(delta_root)
    if max_step is None:
        return []

    rendered_paths: list[Path] = []
    for step in range(max_step + 1):
        # Zero-pad step number for correct sorting
        filename = f"step_{step:02d}"
        output_path = output_dir / filename
        rendered = render_micro_graph(
            delta_root, output_path, format=format, max_step=step
        )
        rendered_paths.append(rendered)

    return rendered_paths
