"""Macro-level (step-level) pipeline graph visualization using Graphviz.

This module builds a bipartite graph from the ``steps`` Delta table
alone. Each pipeline step becomes an **execution node** and each output role
becomes a **data node**, connected by directed edges derived from
``input_refs_json``.

For the finer-grained artifact-level view, see ``artisan.visualization.graph.micro``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Literal

import graphviz
import polars as pl

from artisan.schemas.enums import TablePath
from artisan.utils.path import uri_join
from artisan.visualization.graph._styles import (
    EXECUTION_STYLE,
    PASSTHROUGH_STYLE,
    apply_default_layout,
    get_artifact_style,
    render_graph,
)

# =============================================================================
# Data Loading
# =============================================================================


def _load_completed_steps(
    delta_root: str,
    storage_options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Return completed steps, deduplicated by step_number (keeps last)."""
    table_path = uri_join(delta_root, TablePath.STEPS)
    if not os.path.exists(table_path):
        return pl.DataFrame(
            schema={
                "step_number": pl.Int32,
                "step_name": pl.String,
                "output_roles_json": pl.String,
                "output_types_json": pl.String,
                "input_refs_json": pl.String,
            }
        )

    df = (
        pl.scan_delta(table_path, storage_options=storage_options)
        .filter(pl.col("status") == "completed")
        .select(
            [
                "step_number",
                "step_name",
                "output_roles_json",
                "output_types_json",
                "input_refs_json",
            ]
        )
        .collect()
    )

    # Deduplicate by step_number (keep last row per step)
    if not df.is_empty():
        df = df.unique(subset=["step_number"], keep="last").sort("step_number")

    return df


# =============================================================================
# Input Refs Parsing
# =============================================================================


def _parse_input_refs(input_refs_json: str) -> list[tuple[int, str]]:
    """Extract (source_step, source_role) pairs from serialised input refs.

    Handles two JSON shapes produced by ``_serialize_input_refs``:

    - **dict** (most operations):
      ``{"role": {"type": "output_ref", "source_step": 0, "role": "data"}}``
    - **list** (Merge list-style, IngestData):
      ``[{"type": "output_ref", "source_step": 0, "role": "data"}]``

    Literal entries and nulls are silently skipped.
    """
    if not input_refs_json:
        return []

    parsed = json.loads(input_refs_json)
    if parsed is None:
        return []

    refs: list[tuple[int, str]] = []

    if isinstance(parsed, dict):
        for value in parsed.values():
            if isinstance(value, dict) and value.get("type") == "output_ref":
                refs.append((value["source_step"], value["role"]))
    elif isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, dict) and item.get("type") == "output_ref":
                refs.append((item["source_step"], item["role"]))

    return refs


# =============================================================================
# Graph Building
# =============================================================================


def build_macro_graph(
    delta_root: str,
    storage_options: dict[str, str] | None = None,
) -> graphviz.Digraph:
    """Create a step-level pipeline graph from the steps table.

    Creates a bipartite graph with:

    - **Execution nodes** — one per completed step, labelled ``(N) step_name``.
    - **Data nodes** — one per (step, output_role), coloured by artifact type.
    - **Edges** — output_ref connections from ``input_refs_json``.

    Args:
        delta_root: Path to Delta Lake root directory.
        storage_options: Delta-rs storage options for cloud backends.

    Returns:
        Graphviz Digraph object (renders inline in Jupyter).
    """
    steps_df = _load_completed_steps(delta_root, storage_options=storage_options)

    graph = graphviz.Digraph("pipeline", format="svg")
    apply_default_layout(graph)

    if steps_df.is_empty():
        return graph

    # Build output_types lookup per step for data-node colouring
    step_output_types: dict[int, dict[str, str | None]] = {}
    for row in steps_df.iter_rows(named=True):
        step_num = row["step_number"]
        types_json = row["output_types_json"]
        if types_json:
            step_output_types[step_num] = json.loads(types_json)
        else:
            step_output_types[step_num] = {}

    # Collect node IDs by step for column ranking
    exec_by_step: dict[int, str] = {}
    data_by_step: dict[int, list[str]] = {}
    all_steps: list[int] = []

    for row in steps_df.iter_rows(named=True):
        step_num = row["step_number"]
        step_name = row["step_name"]
        all_steps.append(step_num)

        # --- Execution node ---
        exec_node_id = f"exec_{step_num}"
        shape, color = EXECUTION_STYLE
        graph.node(
            exec_node_id,
            label=f"({step_num}) {step_name}",
            shape=shape,
            fillcolor=color,
        )
        exec_by_step[step_num] = exec_node_id

        # --- Data nodes (one per output role) ---
        roles_json = row["output_roles_json"]
        if not roles_json:
            continue
        output_roles: list[str] = json.loads(roles_json)
        output_types = step_output_types.get(step_num, {})

        step_data_nodes: list[str] = []
        for role in output_roles:
            data_node_id = f"data_{step_num}_{role}"
            artifact_type_str = output_types.get(role)

            if artifact_type_str is None:
                shape, color = PASSTHROUGH_STYLE
            else:
                shape, color = get_artifact_style(artifact_type_str)

            graph.node(data_node_id, label=role, shape=shape, fillcolor=color)
            step_data_nodes.append(data_node_id)

            # Output edge: exec → data
            graph.edge(exec_node_id, data_node_id)

        data_by_step[step_num] = step_data_nodes

    # --- Input edges (data → exec) from input_refs_json ---
    for row in steps_df.iter_rows(named=True):
        step_num = row["step_number"]
        refs_json = row["input_refs_json"]
        if not refs_json:
            continue

        input_refs = _parse_input_refs(refs_json)
        exec_node_id = f"exec_{step_num}"

        for source_step, source_role in input_refs:
            data_node_id = f"data_{source_step}_{source_role}"
            graph.edge(data_node_id, exec_node_id)

    # ==========================================================================
    # Strict column ordering using anchor nodes (same pattern as micro)
    # Column order: exec_0 → data_0 → exec_1 → data_1 → ...
    # ==========================================================================
    anchor_nodes: list[str] = []
    for step_num in all_steps:
        anchor_nodes.append(f"_anchor_exec_{step_num}")
        if step_num in data_by_step:
            anchor_nodes.append(f"_anchor_data_{step_num}")

    for anchor in anchor_nodes:
        graph.node(anchor, label="", width="0", height="0", style="invis")

    for i in range(len(anchor_nodes) - 1):
        graph.edge(anchor_nodes[i], anchor_nodes[i + 1], style="invis")

    for step_num in all_steps:
        with graph.subgraph() as s:
            s.attr(rank="same")
            s.node(f"_anchor_exec_{step_num}")
            if step_num in exec_by_step:
                s.node(exec_by_step[step_num])

        if step_num in data_by_step:
            with graph.subgraph() as s:
                s.attr(rank="same")
                s.node(f"_anchor_data_{step_num}")
                for node_id in data_by_step[step_num]:
                    s.node(node_id)

    return graph


def render_macro_graph(
    delta_root: str,
    output_path: Path,
    format: Literal["svg", "png"] = "svg",
    storage_options: dict[str, str] | None = None,
) -> Path:
    """Build and render the macro (step-level) pipeline graph to a file.

    Args:
        delta_root: Path to Delta Lake root directory.
        output_path: Output file path (without extension).
        format: Output format ("svg" or "png").
        storage_options: Delta-rs storage options for cloud backends.

    Returns:
        Path to the rendered file.
    """
    graph = build_macro_graph(delta_root, storage_options=storage_options)
    return render_graph(graph, output_path, format)
