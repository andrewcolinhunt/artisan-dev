"""Tests for provenance graph stepper widget."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.retained_inspection import commit_inspection_attempt, inspection_store

from artisan.schemas import CSVArtifact

try:
    import ipywidgets

    HAS_IPYWIDGETS = True
except ImportError:
    HAS_IPYWIDGETS = False


@pytest.fixture
def delta_root_with_steps(tmp_path: Path, monkeypatch) -> Path:
    """Create three completed attempts with real artifact membership."""
    monkeypatch.setenv("IPYTHONDIR", str(tmp_path / "ipython"))
    delta_root = tmp_path / "delta"
    previous = None
    for number, name in enumerate(("ingest", "creator", "calculate")):
        artifact = CSVArtifact.draft(
            f"value\n{number}\n".encode(), f"step-{number}.csv", number
        ).finalize()
        commit_inspection_attempt(
            delta_root,
            number=number,
            name=name,
            outputs={"output": [artifact]},
            inputs={} if previous is None else {"input": [previous]},
        )
        previous = artifact
    return delta_root


@pytest.fixture
def empty_delta_root(tmp_path: Path, monkeypatch) -> Path:
    """Create empty Delta Lake root directory."""
    monkeypatch.setenv("IPYTHONDIR", str(tmp_path / "ipython"))
    delta_root = tmp_path / "delta_empty"
    inspection_store(delta_root)
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

        assert output_dir.exists()
        svg_files = list(output_dir.glob("step_*.svg"))
        assert len(svg_files) == 3

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

        assert isinstance(widget, ipywidgets.VBox)
        assert len(widget.children) == 1
        assert isinstance(widget.children[0], ipywidgets.Label)
        assert "No pipeline" in widget.children[0].value

    def test_widget_contains_slider(self, delta_root_with_steps: Path) -> None:
        """Widget contains an IntSlider for navigation."""
        from artisan.visualization.graph import display_provenance_stepper

        widget = display_provenance_stepper(delta_root_with_steps)

        slider = None
        for child in widget.children:
            if isinstance(child, ipywidgets.HBox):
                for grandchild in child.children:
                    if isinstance(grandchild, ipywidgets.IntSlider):
                        slider = grandchild
                        break

        assert slider is not None
        assert slider.min == 0
        assert slider.max == 2


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
