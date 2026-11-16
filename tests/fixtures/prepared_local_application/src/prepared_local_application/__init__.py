"""Installed application schemas and operations for prepared local qualification."""

from __future__ import annotations

import json
import os
import sys
import time
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.composites import CompositeContext
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import ArtifactResult, ArtifactTypeDef, InputSpec, OutputSpec
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.operation_config.environment import Environment
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.utils.hashing import compute_content_digest


class ApplicationMetric(MetricArtifact):
    """An installed application-defined input model crossing the live transport."""

    artifact_type: str = Field(default="prepared_application_metric", frozen=True)
    application_schema: ClassVar[str] = "prepared-local-application-v1"


class ApplicationMetricTypeDef(ArtifactTypeDef):
    """Register the installed application schema for persisted worker inputs."""

    key = "prepared_application_metric"
    table_path = "artifacts/prepared_application_metrics"
    model = ApplicationMetric
    required_references = None


class NativeIncrement(OperationDefinition):
    name = "prepared_native_increment"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = {
        "source": InputSpec(
            artifact_type="prepared_application_metric", materialize=False
        )
    }
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["source"]}
        )
    }

    class Params(BaseModel):
        add: int = Field(default=1, description="Native numeric increment.")
        fail: bool = Field(default=False, description="Fail this native invocation.")
        marker: str = Field(default="", description="Owned native-entry evidence path.")
        release: str = Field(
            default="", description="Owned bounded native-wait release path."
        )

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        source = inputs.input_artifacts["source"][0]
        assert isinstance(source, ApplicationMetric)
        assert source.application_schema == "prepared-local-application-v1"
        return {"value": source.values["value"], "preprocess_pid": os.getpid()}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        if self.params.fail:
            msg = "Requested native failure"
            raise RuntimeError(msg)
        if self.params.marker:
            Path(self.params.marker).write_text(str(os.getpid()))
        if self.params.release:
            deadline = time.monotonic() + 40
            while (
                not Path(self.params.release).exists() and time.monotonic() < deadline
            ):
                time.sleep(0.05)
        return {
            "values": [
                {
                    "value": inputs.inputs["value"] + self.params.add,
                    "preprocess_pid": inputs.inputs["preprocess_pid"],
                    "worker_pid": os.getpid(),
                    "worker_python": sys.executable,
                    "native_selected": os.environ.get("PROBE_CREDENTIAL"),
                    "native_literal": os.environ.get("PROBE_LITERAL"),
                }
            ]
        }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        inputs.memory_outputs["values"][0]["postprocess_pid"] = os.getpid()
        result = ArtifactResult()
        result.add_artifact(
            "metrics",
            MetricArtifact.draft(inputs.memory_outputs["values"][0], "native.json"),
            sources={"source": [inputs.ref("source", index=0)]},
        )
        return result


class SelectiveFailure(NativeIncrement):
    """Prove a typed failure in one packed unit preserves an independent success."""

    name = "prepared_selective_failure"

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        if inputs.inputs["value"] < 0:
            message = f"Requested negative input failure: {os.environ.get('PROBE_CREDENTIAL')}"
            raise RuntimeError(message)
        return super().execute_function(inputs)


_TOOL_PROGRAM = r"""
import json, os, pathlib, subprocess, sys, time
import packaging
source, destination, role, add, multiply, hold, marker = sys.argv[1:]
payload = json.loads(pathlib.Path(source).read_text())
if marker:
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    pathlib.Path(marker).write_text(json.dumps({"parent": os.getpid(), "child": child.pid}))
time.sleep(float(hold))
payload["value"] = (payload["value"] + int(add)) * int(multiply)
payload.setdefault("tools", []).append({"role": role, "python": sys.executable, "packaging": packaging.__version__, "activation": os.environ.get("PROBE_ACTIVATION"), "literal": os.environ.get("PROBE_LITERAL"), "selected": os.environ.get("PROBE_CREDENTIAL"), "pythonpath": os.environ.get("PYTHONPATH"), "loader": os.environ.get("LD_LIBRARY_PATH"), "cwd": str(pathlib.Path.cwd()), "scratch": os.environ.get("TMPDIR")})
pathlib.Path(destination).write_text(json.dumps(payload, sort_keys=True))
"""


class CommandTransform(OperationDefinition):
    name = "prepared_command_transform"
    tool: ToolSpec = ToolSpec(executable="python")

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        files = "files"

    inputs: ClassVar = {"source": InputSpec(artifact_type="metric", materialize=False)}
    outputs: ClassVar = {
        "files": OutputSpec(
            artifact_type="file_ref", derives_from={"inputs": ["source"]}
        )
    }

    class Params(BaseModel):
        manifest: str = Field(description="Explicit canonical environment source.")
        role: str = Field(description="Selected tool role.")
        add: int = Field(default=0, description="Added value.")
        multiply: int = Field(default=1, description="Multiplier.")
        hold: float = Field(default=0, description="Qualification cancellation delay.")
        marker: str = Field(
            default="", description="Optional owned descendant evidence path."
        )

    params: Params

    def model_post_init(self, _context: Any) -> None:
        self.environment = Environment.from_pixi(
            Path(self.params.manifest), name=f"tool-{self.params.role}"
        )

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        source = Path(inputs.preprocess_dir) / "input.json"
        source.write_bytes(inputs.input_artifacts["source"][0].content)
        return {"source": PerArtifact([str(source)])}

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [
            *self.tool.parts(),
            "-c",
            _TOOL_PROGRAM,
            inputs["source"],
            "result.json",
            self.params.role,
            str(self.params.add),
            str(self.params.multiply),
            str(self.params.hold),
            self.params.marker,
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        path = next(
            Path(path)
            for path in inputs.file_outputs
            if Path(path).name == "result.json"
        )
        content = path.read_bytes()
        result = ArtifactResult()
        result.add_artifact(
            "files",
            FileRefArtifact.draft(
                str(path), compute_content_digest(content), len(content)
            ),
            sources={"source": [inputs.ref("source", index=0)]},
        )
        return result


class ReadFile(OperationDefinition):
    name = "prepared_read_file"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = {"source": InputSpec(artifact_type="file_ref")}
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["source"]}
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            "values": PerArtifact(
                [
                    json.loads(Path(artifact.materialized_path).read_text())
                    for artifact in inputs.input_artifacts["source"]
                ]
            )
        }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {"values": inputs.inputs["values"]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        result.add_artifact(
            "metrics",
            MetricArtifact.draft(inputs.memory_outputs["values"][0], "metric.json"),
            sources={"source": [inputs.ref("source", index=0)]},
        )
        return result


class TwoToolSequence(CompositeDefinition):
    name = "prepared_two_tool_sequence"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        files = "files"
        metrics = "metrics"

    inputs: ClassVar = NativeIncrement.inputs
    outputs: ClassVar = {
        "files": OutputSpec(artifact_type="file_ref"),
        "metrics": OutputSpec(artifact_type="metric"),
    }

    class Params(BaseModel):
        manifest: str = Field(description="Explicit canonical environment source.")
        increment: int = Field(default=1, description="Native numeric increment.")
        hold: float = Field(
            default=0, description="Optional first-tool cancellation delay."
        )
        marker: str = Field(
            default="", description="Optional owned descendant evidence path."
        )

    params: Params

    def compose(self, ctx: CompositeContext) -> None:
        native = ctx.call(
            NativeIncrement,
            inputs={"source": ctx.input("source")},
            params={"add": self.params.increment},
            name="native",
        )
        first = ctx.call(
            CommandTransform,
            inputs={"source": native.output("metrics")},
            params={
                "manifest": self.params.manifest,
                "role": "a",
                "add": 2,
                "hold": self.params.hold,
                "marker": self.params.marker,
            },
            name="tool_a",
        )
        between = ctx.call(
            ReadFile, inputs={"source": first.output("files")}, name="between"
        )
        second = ctx.call(
            CommandTransform,
            inputs={"source": between.output("metrics")},
            params={"manifest": self.params.manifest, "role": "b", "multiply": 2},
            name="tool_b",
        )
        final = ctx.call(
            ReadFile, inputs={"source": second.output("files")}, name="final"
        )
        ctx.output("files", second.output("files"))
        ctx.output("metrics", final.output("metrics"))
