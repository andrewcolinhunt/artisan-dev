"""Pause one tutorial input so cancellation leaves a finished sibling to recover."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from artisan.operations.base import PerArtifact
from artisan.operations.examples import DataTransformer
from artisan.schemas import ExecuteInput, PreprocessInput


class PausedTransformer(DataTransformer):
    """Transform data while holding the second tutorial input for cancellation.

    The release file changes only timing, not output content. Per-input call
    files let the notebook check whether recovery avoided executing again.
    """

    name = "tutorial_paused_transformer"
    description = "Transform tutorial data with an explicit cancellation checkpoint"

    class Params(DataTransformer.Params):
        """Keep the tutorial's release signal and invocation evidence together.

        Attributes:
            demo_root: Directory containing the release file and call counts.
        """

        demo_root: str = "."

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Keep original names alongside content-addressed materialized paths."""
        prepared = super().preprocess(inputs)
        prepared["source_name"] = PerArtifact(
            [artifact.original_name for artifact in inputs.input_artifacts["dataset"]]
        )
        return prepared

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Record invocation, wait if selected, then perform the usual transform."""
        source_names = inputs.inputs["source_name"]
        names = [source_names] if isinstance(source_names, str) else source_names
        demo_root = Path(self.params.demo_root)
        for name in names:
            with (demo_root / f"{Path(name).stem}.calls").open("a") as calls:
                calls.write("executed\n")
        if any(Path(name).stem == "dataset_00001" for name in names):
            deadline = time.monotonic() + 60
            while not (demo_root / "release").exists():
                if time.monotonic() > deadline:
                    msg = "Tutorial release signal was not supplied within 60 seconds"
                    raise TimeoutError(msg)
                time.sleep(0.02)
        return super().execute_function(inputs)
