"""Generative operation that echoes numbered lines through selected command software."""

from __future__ import annotations

import os
from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import PostprocessInput
from artisan.schemas.specs.output_spec import OutputSpec


class StreamingEcho(OperationDefinition):
    """Echo numbered progress lines and write a marker through selected software.

    Bash writes each progress line to the captured command output before waiting.
    """

    name = "streaming_echo"
    version: ClassVar[str] = "2"
    description = "Echo numbered lines through selected command software"

    inputs: ClassVar[dict[str, Any]] = {}

    class OutputRole(StrEnum):
        output = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="csv",
            description="Marker recording how many lines were emitted",
            derives_from={"inputs": []},
        ),
    }

    class Params(BaseModel):
        """Parameters for StreamingEcho."""

        seconds: int = Field(
            default=5,
            ge=1,
            description="How many lines to print (one per second).",
        )

    params: Params = Params()

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        """Write numbered progress lines and one marker using selected software."""
        return [
            "bash",
            "-c",
            'for i in $(seq 1 "$1"); do '
            'echo "streaming_echo line $i / $1"; sleep 1; done; '
            'printf "lines\\n%s\\n" "$1" > streaming_echo_marker.csv',
            "streaming-echo",
            str(self.params.seconds),
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build a CSVArtifact from the marker file."""
        drafts: list[Artifact] = []
        for file_path in inputs.file_outputs:
            if file_path.endswith(".csv"):
                with open(file_path, "rb") as f:
                    content = f.read()
                drafts.append(
                    CSVArtifact.draft(
                        content=content,
                        original_name=os.path.basename(file_path),
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(
            success=True,
            artifacts={"output": drafts},
            lineage={"output": []},
            metadata={
                "operation": "streaming_echo",
                "lines": self.params.seconds,
            },
        )
