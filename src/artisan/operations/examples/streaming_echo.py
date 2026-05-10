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

    Demonstrates the canonical pattern for wrapping external CLI tools
    with live tool-output streaming: each child stdout line flows
    through ``_run_with_streaming`` → ``sys.stdout.write`` → the parent
    process's stdout. On the operator's terminal, in Jupyter cells,
    and on Modal's dashboard for remote-dispatched runs, lines arrive
    in real time as they're emitted (not in a burst at the end).

    Uses ``bash`` rather than ``python`` so child-side stdout doesn't
    block-buffer when piped — the streaming property is observable
    without setting ``PYTHONUNBUFFERED=1``. Each line is also written
    to ``log_path`` and surfaces in the parquet ``tool_output`` column
    after the run.

    Useful for: visually verifying live dashboard streaming on Modal,
    smoke-testing the streaming code path, and tutorials demonstrating
    the ``stream_output=True`` pattern.
    """

    # ---------- Metadata ----------
    name = "streaming_echo"
    description = "Echo numbered lines via run_command for streaming demos"

    # ---------- Inputs ----------
    inputs: ClassVar[dict[str, Any]] = {}

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        output = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="data",
            description="Marker recording how many lines were emitted",
            infer_lineage_from={"inputs": []},
        ),
    }

    # ---------- Parameters ----------
    class Params(BaseModel):
        """Parameters for StreamingEcho."""

        seconds: int = Field(
            default=5,
            ge=1,
            description="How many lines to print (one per second).",
        )

    params: Params = Params()

    # ---------- Tool ----------
    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    # ---------- Environments ----------
    environments: Environments = Environments(local=LocalEnvironmentSpec())

    # ---------- Compute ----------
    compute_provider: ComputeProvider = ComputeProvider(modal=ModalComputeConfig())

    # ---------- Lifecycle ----------
    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Run a bash echo loop, streaming each line to the parent's stdout."""
        env = self.environments.current()
        run_command(
            env,
            [
                *self.tool.parts(),
                "-c",
                f'for i in $(seq 1 {self.params.seconds}); do '
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
