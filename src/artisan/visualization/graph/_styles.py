"""Shared styling constants and helpers for provenance graph visualization.

Both micro (artifact-level) and macro (step-level) graphs import from here
to ensure consistent visual language.
"""

from __future__ import annotations

import os
from typing import Literal

import graphviz

from artisan.schemas.artifact.registry import ArtifactTypeDef

# Shape and fill color for execution nodes (grey boxes)
EXECUTION_STYLE = ("box", "lightgray")

# Style for passthrough data nodes (null artifact type in output_types)
PASSTHROUGH_STYLE = ("box", "#E0F0FF")  # Very light blue

# Blue palette assigned by registration order
_BLUE_PALETTE = [
    "#87CEEB",  # Sky blue
    "#B0E0E6",  # Powder blue
    "#ADD8E6",  # Light blue
    "#B0C4DE",  # Light steel blue
    "#A8D0E6",
    "#90C8E8",
]

_DEFAULT_ARTIFACT_COLOR = "#A0C8E0"


def get_artifact_style(artifact_type: str) -> tuple[str, str]:
    """Return (shape, fill_color) for an artifact type.

    Colors are assigned from a blue palette by registration order.
    Unknown types get a default blue.

    Args:
        artifact_type: Artifact type key string.

    Returns:
        Tuple of (shape, fill_color_hex).
    """
    all_types = list(ArtifactTypeDef.get_all().keys())
    try:
        idx = all_types.index(artifact_type)
        color = _BLUE_PALETTE[idx % len(_BLUE_PALETTE)]
    except ValueError:
        color = _DEFAULT_ARTIFACT_COLOR
    return ("box", color)


def apply_default_layout(graph: graphviz.Digraph) -> None:
    """Apply the standard graph attributes shared by macro and micro graphs."""
    graph.attr(rankdir="LR")
    graph.attr("node", style="filled")
    graph.attr(nodesep="0.25", ranksep="0.4")
    graph.attr(outputorder="edgesfirst")
    graph.attr(splines="line")


def render_graph(
    graph: graphviz.Digraph,
    output_path: str | Path,
    format: Literal["svg", "png"] = "svg",
) -> str:
    """Render a Graphviz digraph to a file.

    Args:
        graph: The Graphviz Digraph to render.
        output_path: Output file path (without extension).
        format: Output format.

    Returns:
        Path to the rendered file.
    """
    graph.format = format
    output_path = str(output_path)
    parent = os.path.dirname(output_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    rendered_path = graph.render(filename=output_path, cleanup=True)
    return rendered_path
