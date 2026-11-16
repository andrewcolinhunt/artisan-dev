"""Macro-level (step-level) pipeline graph visualization using Graphviz.

This module builds a bipartite graph from the ``steps`` Delta table
alone. Each pipeline step becomes an **execution node** and each output role
becomes a **data node**, connected by directed edges derived from
``input_refs_json``.

For the finer-grained artifact-level view, see ``artisan.visualization.graph.micro``.
"""

from __future__ import annotations

import json
from typing import Literal

import graphviz
import polars as pl
from fsspec import AbstractFileSystem

from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.storage.io.retained_store import open_configured_store
from artisan.visualization.graph._styles import (
    EXECUTION_STYLE,
    PASSTHROUGH_STYLE,
    apply_default_layout,
    get_artifact_style,
    render_graph,
)


def _load_usable_steps(
    delta_root: str,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
    *,
    pipeline_run_id: str | None = None,
    files_root: str | None = None,
) -> pl.DataFrame:
    """Return current succeeded/partial steps for one pipeline run."""
    from artisan.schemas.orchestration.step_lifecycle import StepStatus

    committer = open_configured_store(
        delta_root, files_root=files_root, fs=fs, storage_options=storage_options
    )
    with committer._read_snapshot() as snapshot:
        tracker = RetainedStepTracker(committer, "")
        _, attempts = tracker._read_lifecycle(snapshot)
        states = tracker._select_current_states(attempts, pipeline_run_id)
    rows = [
        {
            "step_run_id": state.step_run_id,
            "step_number": state.step_number,
            "step_name": state.step_name,
            "output_roles_json": json.dumps(sorted(state.output_roles)),
            "output_types_json": json.dumps(state.output_types),
            "input_refs_json": state.input_refs_json,
        }
        for state in states
        if state.status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
    ]
    return pl.DataFrame(
        rows,
        schema={
            "step_run_id": pl.String,
            "step_number": pl.Int32,
            "step_name": pl.String,
            "output_roles_json": pl.String,
            "output_types_json": pl.String,
            "input_refs_json": pl.String,
        },
    )


def _parse_input_refs(input_refs_json: str) -> list[tuple[int, str, str]]:
    """Read exact submitted output references from sanitized input descriptors."""
    parsed = json.loads(input_refs_json) if input_refs_json else None
    if not isinstance(parsed, dict):
        return []
    return [
        (member["source_step"], member["role"], member["step_run_id"])
        for group in parsed.values()
        if isinstance(group, list)
        for member in group
        if isinstance(member, dict)
        and type(member.get("source_step")) is int
        and isinstance(member.get("role"), str)
        and isinstance(member.get("step_run_id"), str)
    ]


def build_macro_graph(
    delta_root: str,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
    *,
    pipeline_run_id: str | None = None,
    files_root: str | None = None,
) -> graphviz.Digraph:
    """Create a step-level pipeline graph from the steps table.

    Creates a bipartite graph with:

    - **Execution nodes** — one per usable step, labelled ``(N) step_name``.
    - **Data nodes** — one per (step, output_role), coloured by artifact type.
    - **Edges** — output_ref connections from ``input_refs_json``.

    Args:
        delta_root: Path to Delta Lake root directory.
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks.
        files_root: Managed carrier root; required for nonlocal/custom storage.
        pipeline_run_id: Pipeline run to render. None selects the latest run.

    Returns:
        Graphviz Digraph object (renders inline in Jupyter).
    """
    steps_df = _load_usable_steps(
        delta_root,
        storage_options=storage_options,
        fs=fs,
        pipeline_run_id=pipeline_run_id,
        files_root=files_root,
    )

    return _build_macro_graph(steps_df)


def _build_macro_graph(steps_df: pl.DataFrame) -> graphviz.Digraph:
    """Render already selected steps without reading lifecycle or store state."""
    graph = graphviz.Digraph("pipeline", format="svg")
    apply_default_layout(graph)

    if steps_df.is_empty():
        return graph

    step_output_types: dict[int, dict[str, str | None]] = {}
    for row in steps_df.iter_rows(named=True):
        step_num = row["step_number"]
        types_json = row["output_types_json"]
        if types_json:
            step_output_types[step_num] = json.loads(types_json)
        else:
            step_output_types[step_num] = {}

    exec_by_step: dict[int, str] = {}
    data_by_step: dict[int, list[str]] = {}
    all_steps: list[int] = []

    for row in steps_df.iter_rows(named=True):
        step_num = row["step_number"]
        step_name = row["step_name"]
        all_steps.append(step_num)

        exec_node_id = f"exec_{step_num}"
        shape, color = EXECUTION_STYLE
        graph.node(
            exec_node_id,
            label=f"({step_num}) {step_name}",
            shape=shape,
            fillcolor=color,
        )
        exec_by_step[step_num] = exec_node_id

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

            graph.edge(exec_node_id, data_node_id)

        data_by_step[step_num] = step_data_nodes

    current_attempts = dict(steps_df.select("step_number", "step_run_id").iter_rows())
    for row in steps_df.iter_rows(named=True):
        step_num = row["step_number"]
        refs_json = row["input_refs_json"]
        if not refs_json:
            continue

        input_refs = _parse_input_refs(refs_json)
        exec_node_id = f"exec_{step_num}"

        for source_step, source_role, source_attempt in input_refs:
            data_node_id = f"data_{source_step}_{source_role}"
            if current_attempts.get(source_step) != source_attempt:
                data_node_id = f"data_{source_step}_{source_attempt}_{source_role}"
                graph.node(data_node_id, label=f"{source_role} (prior attempt)")
            graph.edge(data_node_id, exec_node_id)

    # Strict column ordering using anchor nodes (same pattern as micro)
    # Column order: exec_0 → data_0 → exec_1 → data_1 → ...
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
    output_path: str,
    format: Literal["svg", "png"] = "svg",
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
    *,
    files_root: str | None = None,
) -> str:
    """Build and render the macro (step-level) pipeline graph to a file.

    Args:
        delta_root: Path to Delta Lake root directory.
        output_path: Output file path (without extension).
        format: Output format ("svg" or "png").
        storage_options: Delta-rs storage options for cloud backends.
        fs: Filesystem for existence checks.
        files_root: Managed carrier root; required for nonlocal/custom storage.

    Returns:
        Path to the rendered file.
    """
    graph = build_macro_graph(
        delta_root, storage_options=storage_options, fs=fs, files_root=files_root
    )
    return render_graph(graph, output_path, format)
