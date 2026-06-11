"""Shared example tool ops for tool-endpoint tests."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.operation_config.compute import (
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import PreprocessInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

_OUTPUTS: dict[str, OutputSpec] = {
    "output": OutputSpec(
        artifact_type="data",
        infer_lineage_from={"inputs": []},
    ),
}


class GpuTool(OperationDefinition):
    """Tool op with a hardware spec and a required param."""

    class InputRole(StrEnum):
        reference = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "gpu_tool_test"
    description: ClassVar[str] = "Tool op with hardware spec"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.reference: InputSpec(
            artifact_type="data",
            required=False,
            description="Optional reference structure",
        ),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = _OUTPUTS

    class Params(BaseModel):
        """Parameters for GpuTool."""

        contigs: str = Field(description="Required — proves deploy never instantiates.")

    params: Params

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)
    compute_provider: ComputeProvider = ComputeProvider(
        modal=ModalComputeConfig(
            volumes={"/weights": "weights-vol"},
            secrets=["hf-read"],
            min_containers=1,
        )
    )
    compute_resources: ComputeResources = ComputeResources(
        gpu="A100", memory_gb=8, timeout=600
    )

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {}

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [*self.tool.parts(), "-c", "true"]


class NoModalTool(OperationDefinition):
    """Tool op without a modal config — not deployable."""

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "no_modal_tool_test"
    description: ClassVar[str] = "Tool op without modal config"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = _OUTPUTS

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [*self.tool.parts(), "-c", "true"]
