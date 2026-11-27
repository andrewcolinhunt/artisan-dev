"""Tool operation that counts up once per second — visible live in Modal logs."""

from __future__ import annotations

from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar, cast

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import PostprocessInput, PreprocessInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class WaitTool(OperationDefinition):
    """Count up once per second until ``seconds`` is reached, then emit a marker.

    The tool-op sibling of ``Wait``: the delay runs as a bash loop that
    prints a tick each second into captured command output. Each tick and output
    marker carry the worker task ID when supplied, or its hostname.

    Useful for: verifying remote execution end-to-end, demonstrating
    per-artifact scale-out, and watching live tool output without real
    computation.
    """

    name = "wait_tool"
    version: ClassVar[str] = "2"
    description = "Count up once per second (live in Modal logs), then emit a marker"

    class InputRole(StrEnum):
        dataset = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.dataset: InputSpec(
            artifact_type="csv",
            required=True,
            description="Artifacts to fan out over — one tool run per artifact",
        ),
    }

    class OutputRole(StrEnum):
        output = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="csv",
            description="Marker recording the wait, the container hostname, and the source",
            derives_from={"inputs": ["dataset"]},
        ),
    }

    class Params(BaseModel):
        """Parameters for WaitTool."""

        model_config = {"extra": "forbid"}

        seconds: int = Field(
            default=10,
            ge=1,
            description="How many one-second ticks to count before finishing.",
        )

    params: Params = Params()

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """One materialized path per artifact — sliced per command invocation."""
        return {
            "dataset": PerArtifact(
                [a.materialized_path for a in inputs.input_artifacts["dataset"]]
            ),
            "input_index": PerArtifact(range(len(inputs.input_artifacts["dataset"]))),
        }

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        """Assemble the bash count-up loop for one artifact.

        The marker records the input position so repeated artifacts retain
        distinct parent occurrences.
        """
        source = inputs["dataset"]
        n = self.params.seconds
        index = inputs["input_index"]
        return [
            *self.tool.parts(),
            "-c",
            (
                f'src="{source}"; '
                f'host="${{MODAL_TASK_ID:-$(hostname)}}"; '
                f"for i in $(seq 1 {n}); do "
                f'echo "wait_tool [$host] tick $i / {n}"; sleep 1; done; '
                f'printf "seconds,host,source\\n{n},$host,$(basename "$src")\\n" '
                f'> "{index}_waited.csv"'
            ),
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Declare each marker's exact input occurrence in its original order."""
        outputs = sorted(
            (path for path in inputs.file_outputs if path.endswith("_waited.csv")),
            key=lambda path: int(Path(path).name.removesuffix("_waited.csv")),
        )
        sources = cast(list[CSVArtifact], inputs.input_artifacts["dataset"])
        result = ArtifactResult(artifacts={"output": []}, lineage={"output": []})
        for file_path in outputs:
            index = int(Path(file_path).name.removesuffix("_waited.csv"))
            with open(file_path, "rb") as f:
                draft = CSVArtifact.draft(
                    content=f.read(),
                    original_name=f"{sources[index].original_name}_waited.csv",
                    step_number=inputs.step_number,
                )
            result.add_artifact(
                "output",
                draft,
                sources={"dataset": [inputs.ref("dataset", index=index)]},
            )
        return result
