"""Commands used to qualify direct calls inside managed Python processes."""

from __future__ import annotations

import json
import os
import sys
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar, Literal

from pydantic import BaseModel

from artisan.execution import run_operation
from artisan.execution.environments.command_witness import BUILD_MANIFEST, COMPILER
from artisan.execution.local_software import local_software
from artisan.operations.base import OperationDefinition
from artisan.schemas import ArtifactResult, Environment, OutputSpec
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)


class CommandProbe(OperationDefinition):
    name = "lightweight_command_probe"

    class OutputRole(StrEnum):
        metrics = "metrics"

    outputs: ClassVar = {
        "metrics": OutputSpec(artifact_type="metric", derives_from={"inputs": []})
    }
    tool: ToolSpec = ToolSpec(executable="python")

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [
            "python",
            "-I",
            "-c",
            "import json,os,sys;from pathlib import Path;"
            "Path('runtime.json').write_text(json.dumps({"
            "'pid':os.getpid(),'executable':sys.executable,"
            "'token':os.environ['SERVICE_TOKEN']}))",
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        path = next(Path(p) for p in inputs.file_outputs if p.endswith("runtime.json"))
        return ArtifactResult(
            artifacts={
                "metrics": [
                    MetricArtifact.draft(json.loads(path.read_text()), path.name)
                ]
            },
            lineage={"metrics": []},
        )


class PreparedCommandProbe(CommandProbe):
    name = "lightweight_prepared_command_probe"
    tool: ToolSpec = ToolSpec(executable=COMPILER)
    environment: Environment = Environment.from_pixi(BUILD_MANIFEST)

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [COMPILER, "-dM", "-E", "-x", "c", "/dev/null", "-o", "macros.txt"]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        path = next(Path(p) for p in inputs.file_outputs if p.endswith("macros.txt"))
        macros = path.read_text()
        return ArtifactResult(
            artifacts={
                "metrics": [
                    MetricArtifact.draft(
                        {"compiler_ran": "#define __GNUC__ " in macros}, "compiler.json"
                    )
                ]
            },
            lineage={"metrics": []},
        )


class NativeParent(OperationDefinition):
    name = "lightweight_native_parent"
    OutputRole: ClassVar = CommandProbe.OutputRole
    outputs: ClassVar = CommandProbe.outputs

    class Params(BaseModel):
        """Control which phase invokes the child command.

        Attributes:
            phase: Parent phase that makes the direct call.
            work_dir: Caller-owned directory for the child command's files.
        """

        phase: Literal["preprocess", "execute", "postprocess"] = "execute"
        work_dir: str = "."

    params: Params = Params()

    def _command(self) -> dict[str, Any]:
        result = run_operation(CommandProbe, work_dir=Path(self.params.work_dir))
        return result.artifacts["metrics"][0].values

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {"probe": self._command() if self.params.phase == "preprocess" else None}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, list[Any]]:
        value = (
            self._command()
            if self.params.phase == "execute"
            else inputs.inputs["probe"]
        )
        return {"probe": [value]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        value = (
            self._command()
            if self.params.phase == "postprocess"
            else inputs.memory_outputs["probe"][0]
        )
        return ArtifactResult(
            artifacts={"metrics": [MetricArtifact.draft(value, "parent.json")]},
            lineage={"metrics": []},
        )


def main() -> None:
    definition = PreparedCommandProbe if sys.argv[1] == "prepared" else CommandProbe
    result = run_operation(definition, work_dir=Path(sys.argv[2]))
    software = local_software()
    print(
        json.dumps(
            {
                "values": result.artifacts["metrics"][0].values,
                "pid": os.getpid(),
                "python": sys.executable,
                "home": os.environ["HOME"],
                "cache": str(software.cache_root),
                "pixi": str(software.pixi),
            }
        )
    )


if __name__ == "__main__":
    main()
