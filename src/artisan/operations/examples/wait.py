"""Generative operation that waits a specified duration then produces a marker file."""

from __future__ import annotations

import os
import time
from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput
from artisan.schemas.specs.output_spec import OutputSpec


class Wait(OperationDefinition):
    """Wait for a configurable duration, then produce a small marker file.

    Useful for testing cancellation, timeouts, and pipeline scheduling
    without performing real computation.
    """

    # ---------- Metadata ----------
    name = "wait"
    description = "Wait a specified duration then produce a marker file"

    # ---------- Inputs ----------
    inputs: ClassVar[dict] = {}

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        output = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="data",
            description="Marker file recording the actual wait duration",
            infer_lineage_from={"inputs": []},
        ),
    }

    # ---------- Parameters ----------
    class Params(BaseModel):
        """Parameters for Wait."""

        duration: float = Field(
            default=1.0,
            ge=0.0,
            description="Seconds to wait before completing",
        )

    params: Params = Params()

    # ---------- Lifecycle ----------
    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Sleep for the configured duration."""
        output_dir = inputs.execute_dir
        os.makedirs(output_dir, exist_ok=True)

        start = time.perf_counter()
        time.sleep(self.params.duration)
        elapsed = time.perf_counter() - start

        marker = os.path.join(output_dir, "wait_marker.csv")
        with open(marker, "w") as f:
            f.write(f"requested,actual\n{self.params.duration},{elapsed:.4f}\n")

        return {"elapsed": elapsed}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build a DataArtifact from the marker file."""
        drafts: list[DataArtifact] = []
        for file_path in inputs.file_outputs:
            if file_path.endswith(".csv"):
                with open(file_path, "rb") as f:
                    content = f.read()
                drafts.append(
                    DataArtifact.draft(
                        content=content,
                        original_name=os.path.basename(file_path),
                        step_number=inputs.step_number,
                    )
                )

        return ArtifactResult(
            success=True,
            artifacts={"output": drafts},
            metadata={
                "operation": "wait",
                "duration": self.params.duration,
                "elapsed": inputs.memory_outputs.get("elapsed"),
            },
        )
