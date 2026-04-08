"""Interactive provenance graph stepper for Jupyter notebooks.

This module provides a widget to step through provenance graph visualization
one step at a time, showing the cumulative graph build-up.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from fsspec import AbstractFileSystem

from artisan.utils.path import uri_join, uri_parent
from artisan.visualization.graph.micro import (
    get_max_step_number,
    render_micro_graph_steps,
)

if TYPE_CHECKING:
    import ipywidgets


def display_provenance_stepper(
    delta_root: str,
    output_dir: str | Path | None = None,
    storage_options: dict[str, str] | None = None,
    fs: AbstractFileSystem | None = None,
) -> ipywidgets.VBox:
    """Display an interactive widget to step through provenance graph evolution.

    Renders all steps upfront, then provides a slider to navigate through them.
    Each step shows all nodes and edges from steps 0 through that step.

    Args:
        delta_root: Path to Delta Lake root directory.
        output_dir: Directory to write step images. Defaults to
            {delta_root}/../images (e.g., runs/images alongside runs/delta).

    Returns:
        ipywidgets VBox containing the stepper widget.

    Raises:
        ImportError: If ipywidgets is not installed.
    """
    try:
        import ipywidgets as widgets
        from IPython.display import SVG
    except ImportError as e:
        msg = (
            "ipywidgets and IPython are required for the stepper widget. "
            "Install with: pip install ipywidgets"
        )
        raise ImportError(msg) from e

    # Default output_dir to runs/images alongside runs/delta
    if output_dir is None:
        output_dir = uri_join(uri_parent(delta_root), "images")
    else:
        output_dir = str(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Get max step to check if there's anything to show
    max_step = get_max_step_number(delta_root, storage_options=storage_options, fs=fs)
    if max_step is None:
        # No executions, show empty message
        label = widgets.Label(value="No pipeline steps to display.")
        return widgets.VBox([label])

    # Render all steps upfront
    rendered_paths = render_micro_graph_steps(
        delta_root,
        output_dir,
        format="svg",
        storage_options=storage_options,
        fs=fs,
    )

    if not rendered_paths:
        label = widgets.Label(value="No pipeline steps to display.")
        return widgets.VBox([label])

    # Create slider
    slider = widgets.IntSlider(
        value=max_step,
        min=0,
        max=max_step,
        step=1,
        description="Step:",
        continuous_update=False,
        layout=widgets.Layout(width="400px"),
    )

    # Create label showing "N / max"
    step_label = widgets.Label(value=f"{max_step} / {max_step}")

    # Create output area for the graph
    output = widgets.Output()

    def update_display(change: dict) -> None:
        """Refresh the displayed graph when the slider value changes."""
        step = change["new"]
        step_label.value = f"{step} / {max_step}"
        output.outputs = ()
        output.append_display_data(SVG(filename=str(rendered_paths[step])))

    # Connect slider to update function
    slider.observe(update_display, names="value")

    # Initial display
    output.append_display_data(SVG(filename=str(rendered_paths[max_step])))

    # Layout: slider + label on top, graph below
    header = widgets.HBox([slider, step_label])
    return widgets.VBox([header, output])
