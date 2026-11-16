"""Installed application definitions for bounded full-worker provider qualification."""

from __future__ import annotations

import json
import os
import sys
import time
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
from pydantic import BaseModel, Field

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import (
    Artifact,
    ArtifactResult,
    Environment,
    InputSpec,
    MetricArtifact,
    OutputSpec,
)
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.storage.core.artifact_store import ArtifactStore
from modal_qualification_app.command_program import signal_ready
from modal_qualification_app.gpu_kernel import run_kernel

_PACKAGE = Path(__file__).parent
# Execute the exact installed source in explicitly prepared Python tool prefixes.
_PROGRAM = (
    (_PACKAGE / "gpu_kernel.py").read_text().split('if __name__ == "__main__":')[0]
    + "\n"
    + (_PACKAGE / "command_program.py")
    .read_text()
    .replace("from __future__ import annotations", "")
    + "\ncommand(run_kernel)\n"
)


def _metric(artifact: Artifact) -> MetricArtifact:
    assert isinstance(artifact, MetricArtifact)
    assert artifact.content is not None
    return artifact


class Seeds(OperationDefinition):
    name = "modal_qualification_seeds"

    class OutputRole(StrEnum):
        values = "values"

    outputs: ClassVar = {
        "values": OutputSpec(artifact_type="metric", derives_from={"inputs": []})
    }

    def execute_function(self, _inputs: ExecuteInput) -> dict[str, Any]:
        return {}

    def postprocess(self, _inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for value in (0, 1):
            result.add_artifact(
                "values",
                MetricArtifact.draft(
                    {"input": value, "value": value}, f"seed-{value}.json"
                ),
                sources={},
            )
        return result


class NativeProbe(OperationDefinition):
    name = "modal_qualification_native"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        values = "values"

    inputs: ClassVar = {"source": InputSpec(artifact_type="metric", materialize=False)}
    outputs: ClassVar = {
        "values": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["source"]}
        )
    }

    class Params(BaseModel):
        gpu: bool = Field(default=False, description="Run one bounded driver kernel.")
        hold: bool = Field(
            default=False, description="Hold native execution past cancellation."
        )
        signal: bool = Field(
            default=False, description="Publish exact live-call readiness."
        )

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            "items": PerArtifact(
                [
                    _metric(artifact).values
                    for artifact in inputs.input_artifacts["source"]
                ]
            )
        }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        assert len(inputs.inputs["items"]) == 1
        value = dict(inputs.inputs["items"][0])
        if self.params.hold or self.params.signal:
            signal_ready(value["input"])
        if self.params.hold:
            time.sleep(30)
        if self.params.gpu:
            value["native_gpu"] = run_kernel()
            value["value"] = 42
        value["worker_pid"] = os.getpid()
        value["worker_python"] = sys.executable
        return {"rows": [value]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        rows = inputs.memory_outputs["rows"]
        assert len(rows) == len(inputs.input_artifacts["source"])
        for index, value in enumerate(rows):
            result.add_artifact(
                "values",
                MetricArtifact.draft(value, f"native-{index}.json"),
                sources={"source": [inputs.ref("source", index=index)]},
            )
        return result


class CommandProbe(OperationDefinition):
    name = "modal_qualification_command"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        values = "values"

    inputs: ClassVar = NativeProbe.inputs
    outputs: ClassVar = NativeProbe.outputs
    tool: ToolSpec = ToolSpec(executable="python")

    class Params(BaseModel):
        manifest: str = Field(description="Canonical prepared Pixi manifest path.")
        role: str = Field(description="Selected tool feature, a or b.")
        mode: str = Field(
            default="normal", description="Normal, failing, or cancellable command."
        )
        gpu: bool = Field(default=False, description="Run one bounded driver kernel.")

    params: Params

    def model_post_init(self, _context: Any) -> None:
        self.environment = Environment.from_pixi(
            Path(self.params.manifest), name=f"tool-{self.params.role}"
        )

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        path = Path(inputs.preprocess_dir) / "input.json"
        assert len(inputs.input_artifacts["source"]) == 1
        path.write_text(json.dumps(_metric(inputs.input_artifacts["source"][0]).values))
        return {"source": PerArtifact([str(path)])}

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [
            *self.tool.parts(),
            "-c",
            _PROGRAM,
            inputs["source"],
            "result.json",
            self.params.role,
            self.params.mode,
            str(self.params.gpu).lower(),
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        path = next(
            Path(path)
            for path in inputs.file_outputs
            if Path(path).name == "result.json"
        )
        result = ArtifactResult()
        result.add_artifact(
            "values",
            MetricArtifact.draft(json.loads(path.read_text()), "command.json"),
            sources={"source": [inputs.ref("source", index=0)]},
        )
        return result


class TwoTools(CompositeDefinition):
    name = "modal_qualification_two_tools"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        values = "values"

    inputs: ClassVar = NativeProbe.inputs
    outputs: ClassVar = {"values": OutputSpec(artifact_type="metric")}

    class Params(BaseModel):
        manifest: str = Field(description="Canonical prepared Pixi manifest path.")
        gpu: bool = Field(default=False, description="Run one bounded driver kernel.")

    params: Params

    def compose(self, ctx: CompositeContext) -> None:
        value = ctx.call(
            NativeProbe,
            inputs={"source": ctx.input("source")},
            params={"gpu": self.params.gpu},
        )
        for role in ("a", "b"):
            value = ctx.call(
                CommandProbe,
                name=f"tool_{role}",
                inputs={"source": value.output("values")},
                params={
                    "manifest": self.params.manifest,
                    "role": role,
                    "gpu": self.params.gpu,
                },
            )
        ctx.output("values", value.output("values"))


class GPUCurator(OperationDefinition):
    name = "modal_qualification_gpu_curator"

    class OutputRole(StrEnum):
        values = "values"

    outputs: ClassVar = {
        "values": OutputSpec(artifact_type="metric", derives_from={"inputs": []})
    }

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        assert not inputs
        assert step_number >= 0
        del artifact_store
        result = ArtifactResult()
        result.add_artifact(
            "values", MetricArtifact.draft(run_kernel(), "gpu-curator.json"), sources={}
        )
        return result
