"""Generative tool operation that writes a file via bash — the build_command exemplar."""

from __future__ import annotations

import os
from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import PostprocessInput
from artisan.schemas.specs.output_spec import OutputSpec


class EchoTool(OperationDefinition):
    """Write a text file via bash — the canonical tool-op pattern.

    Declares a ``ToolSpec`` + ``build_command()`` and **no** ``execute()``:
    the framework implementation runs the command, locally as a subprocess
    or (under ``compute_provider='modal'``) on the operation's deployed
    tool endpoint. The op's products are the files the command writes to
    the execute dir; ``postprocess`` turns them into artifacts.

    Useful for: smoke-testing the tool-op convention end-to-end and as a
    template for wrapping real external tools.
    """

    # ---------- Metadata ----------
    name = "echo_tool"
    description = "Write a text file via bash (tool-op exemplar)"

    # ---------- Inputs ----------
    inputs: ClassVar[dict[str, Any]] = {}

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        output = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="data",
            description="The echoed text file",
            infer_lineage_from={"inputs": []},
        ),
    }

    # ---------- Parameters ----------
    class Params(BaseModel):
        """Parameters for EchoTool."""

        text: str = Field(
            default="hello from echo_tool",
            description="Text written to the output file.",
        )
        filename: str = Field(
            default="echo.txt",
            description="Output file name, written to the execute dir.",
        )

    params: Params = Params()

    # ---------- Tool ----------
    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    # ---------- Lifecycle ----------
    def build_command(self, inputs: dict[str, Any]) -> list[str]:
        """Assemble the bash command that writes ``text`` to ``filename``."""
        del inputs  # generative — no inputs
        return [
            *self.tool.parts(),
            "-c",
            f'printf "%s\\n" "{self.params.text}" > "{self.params.filename}"',
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build a DataArtifact from the echoed file."""
        drafts: list[Artifact] = []
        for file_path in inputs.file_outputs:
            if os.path.basename(file_path) == self.params.filename:
                with open(file_path, "rb") as f:
                    content = f.read()
                drafts.append(
                    DataArtifact.draft(
                        content=content,
                        original_name=os.path.basename(file_path),
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"output": drafts})
