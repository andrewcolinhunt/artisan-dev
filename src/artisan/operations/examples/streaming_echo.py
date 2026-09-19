"""Generative operation that echoes numbered lines via run_command for streaming demos."""

from __future__ import annotations

import os
from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.operation_config.compute import ComputeProvider, ModalComputeConfig
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.external_tools import run_command


class StreamingEcho(OperationDefinition):
    """Echo numbered lines via ``run_command(stream_output=True)``.

    Demonstrates local Python execution that wraps an external CLI tool.
    Each child stdout line reaches the parent terminal or Jupyter cell as
    it is emitted. This Python operation does not support Modal dispatch;
    use command operations such as WaitTool or CsvHead for remote execution.

    Bash avoids child-side stdout buffering when piped. Each line is also
    written to ``log_path`` and captured in the execution's ``tool_output``.
    """

    name = "streaming_echo"
    description = "Echo numbered lines via run_command for streaming demos"

    inputs: ClassVar[dict[str, Any]] = {}

    class OutputRole(StrEnum):
        output = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="data",
            description="Marker recording how many lines were emitted",
            infer_lineage_from={"inputs": []},
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

    environments: Environments = Environments(local=LocalEnvironmentSpec())

    compute_provider: ComputeProvider = ComputeProvider(
        modal=ModalComputeConfig(local_python_sources=["artisan"])
    )

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Run a bash echo loop, streaming each line to the parent's stdout."""
        env = self.environments.current()
        run_command(
            env,
            [
                *self.tool.parts(),
                "-c",
                f"for i in $(seq 1 {self.params.seconds}); do "
                f'echo "streaming_echo line $i / {self.params.seconds}"; sleep 1; done',
            ],
            cwd=inputs.execute_dir,
            stream_output=True,
            log_path=inputs.log_path,
        )

        marker = os.path.join(inputs.execute_dir, "streaming_echo_marker.csv")
        with open(marker, "w") as f:
            f.write(f"lines\n{self.params.seconds}\n")

        return {"lines": self.params.seconds}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build a DataArtifact from the marker file."""
        drafts: list[Artifact] = []
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
                "operation": "streaming_echo",
                "lines": inputs.memory_outputs.get("lines"),
            },
        )
