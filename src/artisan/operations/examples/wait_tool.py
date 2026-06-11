"""Tool operation that counts up once per second — visible live in Modal logs."""

from __future__ import annotations

import os
from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.operation_config.compute import (
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import PostprocessInput, PreprocessInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class WaitTool(OperationDefinition):
    """Count up once per second until ``seconds`` is reached, then emit a marker.

    The tool-op sibling of ``Wait``: the delay runs as a bash loop that
    prints a tick each second, so under ``compute_provider='modal'`` the
    progress is watchable live in the Modal dashboard or via
    ``modal app logs artisan-tool-wait_tool``. Each tick and the output
    marker carry the container's task id (``MODAL_TASK_ID``; hostname
    locally) — a unit of N artifacts fans out to N containers, and the
    committed artifacts prove it.

    Useful for: verifying remote execution end-to-end, demonstrating
    per-artifact scale-out, and watching live tool output without real
    computation.
    """

    # ---------- Metadata ----------
    name = "wait_tool"
    description = "Count up once per second (live in Modal logs), then emit a marker"

    # ---------- Inputs ----------
    class InputRole(StrEnum):
        dataset = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.dataset: InputSpec(
            artifact_type="data",
            required=True,
            description="Artifacts to fan out over — one tool run per artifact",
        ),
    }

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        output = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="data",
            description="Marker recording the wait, the container hostname, and the source",
            infer_lineage_from={"inputs": ["dataset"]},
        ),
    }

    # ---------- Parameters ----------
    class Params(BaseModel):
        """Parameters for WaitTool."""

        model_config = {"extra": "forbid"}  # the endpoint's typed schema

        seconds: int = Field(
            default=10,
            ge=1,
            description="How many one-second ticks to count before finishing.",
        )

    params: Params = Params()

    # ---------- Tool ----------
    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    # ---------- Compute ----------
    compute_provider: ComputeProvider = ComputeProvider(modal=ModalComputeConfig())

    # ---------- Lifecycle ----------
    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """One materialized path per artifact — sliced per endpoint call."""
        return {
            "dataset": PerArtifact(
                [a.materialized_path for a in inputs.input_artifacts["dataset"]]
            ),
        }

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        """Assemble the bash count-up loop for one artifact.

        The marker is named ``<input-stem>_waited.csv`` so lineage stem
        matching ties each output back to the artifact that produced it.
        """
        source = inputs["dataset"]
        n = self.params.seconds
        return [
            *self.tool.parts(),
            "-c",
            (
                # MODAL_TASK_ID identifies the container (hostname is the
                # generic "modal" inside the sandbox); hostname locally
                f'src="{source}"; stem="$(basename "$src")"; stem="${{stem%.*}}"; '
                f'host="${{MODAL_TASK_ID:-$(hostname)}}"; '
                f'for i in $(seq 1 {n}); do '
                f'echo "wait_tool [$host] tick $i / {n}"; sleep 1; done; '
                f'printf "seconds,host,source\\n{n},$host,$(basename "$src")\\n" '
                f'> "${{stem}}_waited.csv"'
            ),
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build a DataArtifact per marker file."""
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
        return ArtifactResult(success=True, artifacts={"output": drafts})
