"""Tests for provenance graph stepper widget."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from artisan.storage.core.table_schemas import (
    ARTIFACT_INDEX_SCHEMA,
    EXECUTIONS_SCHEMA,
)

# Import conditionally to handle environments without ipywidgets
try:
    import ipywidgets

    HAS_IPYWIDGETS = True
except ImportError:
    HAS_IPYWIDGETS = False


@pytest.fixture
def delta_root_with_steps(tmp_path: Path) -> Path:
    """Create Delta Lake tables with multi-step test data."""
    delta_root = tmp_path / "delta"
    delta_root.mkdir()

    # Create executions with 3 steps
    exec_data = {
        "execution_run_id": ["exec_0", "exec_1", "exec_2"],
        "execution_spec_id": ["spec_0", "spec_1", "spec_2"],
        "origin_step_number": [0, 1, 2],
        "operation_name": ["ingest", "creator", "calculate"],
        "params": ["{}", "{}", "{}"],
        "user_overrides": ["{}", "{}", "{}"],
        "timestamp_start": [None, None, None],
        "timestamp_end": [None, None, None],
        "source_worker": [0, 0, 0],
        "compute_backend": ["local", "local", "local"],
        "success": [True, True, True],
        "error": [None, None, None],
        "tool_output": [None, None, None],
        "worker_log": [None, None, None],
        "metadata": ["{}", "{}", "{}"],
    }
    exec_df = pl.DataFrame(exec_data, schema=EXECUTIONS_SCHEMA)
    exec_df.write_delta(str(delta_root / "orchestration/executions"), mode="overwrite")

    # Create artifact_index
    artifact_data = {
        "artifact_id": ["art_0", "art_1", "art_2"],
        "artifact_type": ["file_ref", "data", "metric"],
        "origin_step_number": [0, 1, 2],
        "metadata": ["{}", "{}", "{}"],
    }
    artifact_df = pl.DataFrame(artifact_data, schema=ARTIFACT_INDEX_SCHEMA)
    artifact_df.write_delta(str(delta_root / "artifacts/index"), mode="overwrite")

    return delta_root


@pytest.fixture
def empty_delta_root(tmp_path: Path) -> Path:
    """Create empty Delta Lake root directory."""
    delta_root = tmp_path / "delta_empty"
    delta_root.mkdir()
    return delta_root


@pytest.mark.skipif(not HAS_IPYWIDGETS, reason="ipywidgets not installed")
class TestDisplayProvenanceStepper:
    """Tests for display_provenance_stepper function."""

    def test_returns_vbox_widget(self, delta_root_with_steps: Path) -> None:
        """Returns an ipywidgets VBox."""
        from artisan.visualization.graph import display_provenance_stepper

        widget = display_provenance_stepper(delta_root_with_steps)

        assert isinstance(widget, ipywidgets.VBox)

    def test_renders_images_to_output_dir(
        self, delta_root_with_steps: Path, tmp_path: Path
    ) -> None:
        """Renders step images to the specified output directory."""
        from artisan.visualization.graph import display_provenance_stepper

        output_dir = tmp_path / "images"

        display_provenance_stepper(delta_root_with_steps, output_dir=output_dir)

        # Should have created step images
        assert output_dir.exists()
        svg_files = list(output_dir.glob("step_*.svg"))
        assert len(svg_files) == 3  # Steps 0, 1, 2

    def test_default_output_dir_is_sibling_of_delta(
        self, delta_root_with_steps: Path
    ) -> None:
        """Default output_dir is {delta_root}/../images."""
        from artisan.visualization.graph import display_provenance_stepper

        display_provenance_stepper(delta_root_with_steps)

        expected_output_dir = delta_root_with_steps.parent / "images"
        assert expected_output_dir.exists()
        svg_files = list(expected_output_dir.glob("step_*.svg"))
        assert len(svg_files) == 3

    def test_empty_pipeline_shows_message(self, empty_delta_root: Path) -> None:
        """Empty pipeline shows a message instead of slider."""
        from artisan.visualization.graph import display_provenance_stepper

        widget = display_provenance_stepper(empty_delta_root)

        # Should still return a VBox
        assert isinstance(widget, ipywidgets.VBox)
        # First child should be a label with message
        assert len(widget.children) == 1
        assert isinstance(widget.children[0], ipywidgets.Label)
        assert "No pipeline" in widget.children[0].value

    def test_widget_contains_slider(self, delta_root_with_steps: Path) -> None:
        """Widget contains an IntSlider for navigation."""
        from artisan.visualization.graph import display_provenance_stepper

        widget = display_provenance_stepper(delta_root_with_steps)

        # Find slider in widget tree
        slider = None
        for child in widget.children:
            if isinstance(child, ipywidgets.HBox):
                for grandchild in child.children:
                    if isinstance(grandchild, ipywidgets.IntSlider):
                        slider = grandchild
                        break

        assert slider is not None
        assert slider.min == 0
        assert slider.max == 2  # Steps 0, 1, 2


class TestStepperModule:
    """Tests for stepper module basics."""

    def test_display_provenance_stepper_is_callable(self) -> None:
        """display_provenance_stepper function exists and is callable."""
        from artisan.visualization.graph.stepper import display_provenance_stepper

        assert callable(display_provenance_stepper)

    def test_stepper_exported_from_graph_package(self) -> None:
        """display_provenance_stepper is exported from artisan.visualization.graph."""
        from artisan.visualization.graph import display_provenance_stepper

        assert callable(display_provenance_stepper)
