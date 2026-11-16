"""Micro-level (artifact-level) provenance graph visualization using Graphviz.

This module provides utilities to visualize the provenance graph stored in
Delta Lake tables. The visualization overlays both the execution graph
(artifact ↔ execution relationships) and the lineage graph (artifact → artifact
derivations) into a single unified view.

Each individual artifact and execution appears as its own node — hence "micro".
For a higher-level step-based view, see ``artisan.visualization.graph.macro``.
"""

from __future__ import annotations

import os
from typing import Literal, cast

import graphviz
import polars as pl
from fsspec import AbstractFileSystem

from artisan.orchestration.engine.retained_execution_query import _execution_membership
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    FailedScientificExecutionSource,
)
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.retained_store import open_configured_store
from artisan.visualization._read import read_view, registered_rows
from artisan.visualization.graph._styles import (
    EXECUTION_STYLE,
    apply_default_layout,
    get_artifact_style,
    render_graph,
)


def _execution_rows(
    snapshot: RetainedReadSnapshot, pipeline_run_id: str | None
) -> pl.DataFrame:
    tracker = RetainedStepTracker(snapshot.committer, "")
    _, attempts = tracker._read_lifecycle(snapshot)
    states = (
        tracker._select_current_states(attempts, pipeline_run_id)
        if pipeline_run_id is not None
        else [state for state in attempts if state.replay_of_execution_run_id is None]
    )
    rows = []
    for member in _execution_membership(snapshot, states):
        source = member.source
        if isinstance(source, CuratorExecutionSource):
            execution_id = str(source.execution_record["execution_run_id"])
            operation = source.admission.operation_name
            origin = source.admission.record.step_number
        else:
            if isinstance(source, FailedScientificExecutionSource):
                assert source.unit.failed_delivery is not None
                execution_id = source.unit.failed_delivery.delivery_id
            else:
                execution_id = source.plan.receipt.delivery.delivery_id
            operation = source.admission.recipe.definition.name
            origin = source.admission.step_number
        rows.append(
            {
                "actual_execution_run_id": execution_id,
                "execution_run_id": f"{member.state.step_run_id}_{execution_id}"
                if pipeline_run_id is not None
                else execution_id,
                "operation_name": operation,
                "origin_step_number": member.state.step_number
                if pipeline_run_id is not None
                else origin,
            }
        )
    return pl.DataFrame(
        rows,
        schema={
            "actual_execution_run_id": pl.String,
            "execution_run_id": pl.String,
            "operation_name": pl.String,
            "origin_step_number": pl.Int32,
        },
    ).unique(maintain_order=True)


def _load_artifact_labels(
    snapshot: RetainedReadSnapshot, index: pl.DataFrame
) -> dict[str, str]:
    """Read only selected registered name fields, never external carrier bytes."""
    labels: dict[str, str] = {}
    for kind in sorted(set(index["artifact_type"])):
        definition = ArtifactTypeDef.get(kind)
        schema = snapshot.snapshot(definition.table_path).schema
        column = (
            "original_name"
            if "original_name" in schema
            else "path"
            if "path" in schema
            else None
        )
        if column is None:
            continue
        ids = set(index.filter(pl.col("artifact_type") == kind)["artifact_id"])
        rows = registered_rows(snapshot, definition.table_path, ids, columns=[column])
        for row in rows.iter_rows(named=True):
            if row[column]:
                labels[row["artifact_id"]] = os.path.splitext(
                    os.path.basename(row[column])
                )[0]
    return labels


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


def build_micro_graph(
    delta_root: str,
    max_step: int | None = None,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
    *,
    pipeline_run_id: str | None = None,
    files_root: str | None = None,
) -> graphviz.Digraph:
    """Build a Graphviz Digraph from Delta Lake provenance tables.

    Creates a unified visualization showing:
    - Execution nodes (operations) as rectangles
    - Artifact nodes (data, files, metrics) with type-specific colors
    - Execution provenance edges (artifact ↔ execution)
    - Lineage edges (artifact → artifact) in orange

    Layout uses strict left-to-right column ordering:
    - Step 0 executions → Step 0 outputs → Step 1 executions → Step 1 outputs → ...

    Args:
        delta_root: Path to Delta Lake root directory.
        max_step: If provided, only include steps 0 through max_step (inclusive).
            Useful for step-by-step visualization of pipeline execution.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks.
        files_root: Managed carrier root; required for nonlocal/custom storage.
        pipeline_run_id: Exact run to project, or all origin steps if None.
            Reused executions get a distinct participation node at every
            current logical step in a run-scoped graph.

    Returns:
        Graphviz Digraph object (renders inline in Jupyter).
    """
    committer = open_configured_store(
        delta_root, files_root=files_root, fs=fs, storage_options=storage_options
    )
    with read_view(committer) as (snapshot, view):
        participation = _execution_rows(snapshot, pipeline_run_id)
        executions = participation.select(
            "execution_run_id", "operation_name", "origin_step_number"
        )
        selected_ids = set(
            view.scan(TablePath.ARTIFACT_INDEX)
            .select("artifact_id")
            .collect()["artifact_id"]
        )
        artifact_index = registered_rows(
            snapshot, TablePath.ARTIFACT_INDEX.value, selected_ids
        )
        exec_edges = view.scan(TablePath.EXECUTION_EDGES).collect()
        artifact_edges = view.artifact_edges(direct=False).collect()
        actual_ids = set(participation["actual_execution_run_id"])
        exec_edges = (
            exec_edges.rename({"execution_run_id": "actual_execution_run_id"})
            .join(
                participation.select("actual_execution_run_id", "execution_run_id"),
                on="actual_execution_run_id",
                how="inner",
            )
            .select("execution_run_id", "direction", "artifact_id")
        )
        artifact_edges = artifact_edges.filter(
            pl.col("execution_run_id").is_in(actual_ids)
        )
        if pipeline_run_id is not None:
            included_ids = set(exec_edges["artifact_id"])
            artifact_index = artifact_index.filter(
                pl.col("artifact_id").is_in(included_ids)
            )
            artifact_edges = artifact_edges.filter(
                pl.col("source_artifact_id").is_in(included_ids)
                & pl.col("target_artifact_id").is_in(included_ids)
            )
        name_labels = _load_artifact_labels(snapshot, artifact_index)

    if max_step is not None:
        executions = executions.filter(pl.col("origin_step_number") <= max_step)

        # A run-scoped graph uses current participation for the step boundary;
        # cached artifacts retain their global origin, which may be any number.
        included_exec_ids = set(executions["execution_run_id"].to_list())
        exec_edges = exec_edges.filter(
            pl.col("execution_run_id").is_in(included_exec_ids)
        )
        if pipeline_run_id is None:
            artifact_index = artifact_index.filter(
                pl.col("origin_step_number") <= max_step
            )
        else:
            artifact_index = artifact_index.filter(
                pl.col("artifact_id").is_in(exec_edges["artifact_id"].to_list())
            )
        included_artifact_ids = set(artifact_index["artifact_id"].to_list())

        exec_edges = exec_edges.filter(
            pl.col("artifact_id").is_in(included_artifact_ids)
        )

        artifact_edges = artifact_edges.filter(
            pl.col("source_artifact_id").is_in(included_artifact_ids)
            & pl.col("target_artifact_id").is_in(included_artifact_ids)
        )

    artifact_labels = _build_artifact_labels(artifact_index, name_labels)

    graph = graphviz.Digraph("provenance", format="svg")
    apply_default_layout(graph)

    exec_step_lookup: dict[str, int] = {}
    for row in executions.iter_rows(named=True):
        exec_step_lookup[row["execution_run_id"]] = row["origin_step_number"]

    steps_with_outputs: set[int] = set()
    for row in exec_edges.iter_rows(named=True):
        if row["direction"] == "output":
            exec_id = row["execution_run_id"]
            step = exec_step_lookup.get(exec_id, 0)
            steps_with_outputs.add(step)

    exec_by_step: dict[int, list[str]] = {}
    artifacts_by_step: dict[int, list[str]] = {}

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

    # Strict column ordering using anchor nodes and rank constraints
    # Column order: exec_0 -> art_0 -> exec_1 -> art_1 -> exec_2 -> art_2 -> ...
    # Always create art_N column for any step N that has outputs (even passthroughs)
    all_steps = sorted(
        set(exec_by_step.keys()) | set(artifacts_by_step.keys()) | steps_with_outputs
    )

    anchor_nodes: list[str] = []
    for step in all_steps:
        if step in exec_by_step:
            anchor_nodes.append(f"_anchor_exec_{step}")
        # Create art column if step has artifacts OR has outputs (passthroughs)
        if step in artifacts_by_step or step in steps_with_outputs:
            anchor_nodes.append(f"_anchor_art_{step}")

    for anchor in anchor_nodes:
        graph.node(anchor, label="", width="0", height="0", style="invis")

    # Link anchors with invisible edges to enforce column order
    for i in range(len(anchor_nodes) - 1):
        graph.edge(anchor_nodes[i], anchor_nodes[i + 1], style="invis")

    # Group nodes into columns using rank=same with anchors
    # Anchors must be in the rank=same subgraph to enforce column ordering
    for step in all_steps:
        step_execs = exec_by_step.get(step, [])
        if step_execs:
            with graph.subgraph() as s:
                s.attr(rank="same")
                s.node(f"_anchor_exec_{step}")
                for node_id in step_execs:
                    s.node(node_id)

        step_artifacts = artifacts_by_step.get(step, [])
        if step_artifacts or step in steps_with_outputs:
            with graph.subgraph() as s:
                s.attr(rank="same")
                s.node(f"_anchor_art_{step}")
                for node_id in step_artifacts:
                    s.node(node_id)

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
    delta_root: str,
    output_path: str,
    format: Literal["svg", "png"] = "svg",
    max_step: int | None = None,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
    *,
    pipeline_run_id: str | None = None,
    files_root: str | None = None,
) -> str:
    """Build and render the micro (artifact-level) provenance graph to a file.

    Args:
        delta_root: Path to Delta Lake root directory.
        output_path: Output file path (without extension).
        format: Output format ("svg" or "png").
        max_step: If provided, only include steps 0 through max_step (inclusive).
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks.
        files_root: Managed carrier root; required for nonlocal/custom storage.
        pipeline_run_id: Optional exact run to project.

    Returns:
        Path to the rendered file.
    """
    graph = build_micro_graph(
        delta_root,
        max_step=max_step,
        storage_options=storage_options,
        fs=fs,
        pipeline_run_id=pipeline_run_id,
        files_root=files_root,
    )
    return render_graph(graph, output_path, format)


def get_max_step_number(
    delta_root: str,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
    *,
    pipeline_run_id: str | None = None,
    files_root: str | None = None,
) -> int | None:
    """Return the highest origin or run-scoped logical execution step.

    Args:
        delta_root: Path to Delta Lake root directory.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks.
        files_root: Managed carrier root; required for nonlocal/custom storage.
        pipeline_run_id: Exact run whose current execution positions to
            inspect. None uses origin steps across all executions.

    Returns:
        Maximum step number, or None if the selected scope has no executions.
    """
    committer = open_configured_store(
        delta_root, files_root=files_root, fs=fs, storage_options=storage_options
    )
    with committer._read_snapshot() as snapshot:
        executions = _execution_rows(snapshot, pipeline_run_id)
    if executions.is_empty():
        return None
    return cast("int | None", executions["origin_step_number"].max())


def render_micro_graph_steps(
    delta_root: str,
    output_dir: str,
    format: Literal["svg", "png"] = "svg",
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
    *,
    files_root: str | None = None,
) -> list[str]:
    """Render provenance graphs for each step (cumulative).

    Generates one image per step, where step N shows all nodes and edges
    from steps 0 through N. Useful for step-by-step visualization.

    Args:
        delta_root: Path to Delta Lake root directory.
        output_dir: Directory to write step images (step_00.svg, step_01.svg, ...).
        format: Output format ("svg" or "png").
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks.
        files_root: Managed carrier root; required for nonlocal/custom storage.

    Returns:
        List of paths to rendered files, in step order.
    """
    output_dir = str(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    max_step = get_max_step_number(
        delta_root, storage_options=storage_options, fs=fs, files_root=files_root
    )
    if max_step is None:
        return []

    rendered_paths: list[str] = []
    for step in range(max_step + 1):
        # Zero-pad step number for correct sorting
        filename = f"step_{step:02d}"
        output_path = os.path.join(output_dir, filename)
        rendered = render_micro_graph(
            delta_root,
            output_path,
            format=format,
            max_step=step,
            storage_options=storage_options,
            fs=fs,
            files_root=files_root,
        )
        rendered_paths.append(rendered)

    return rendered_paths
