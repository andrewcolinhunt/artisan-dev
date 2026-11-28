"""Small creators exercised through configured calls in real Python processes."""

from __future__ import annotations

import json
import os
import sys
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base import OperationDefinition
from artisan.schemas import ArtifactResult, MetricArtifact, OutputSpec, ToolSpec
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput


class Scale(OperationDefinition):
    name = "configured_scale"

    class OutputRole(StrEnum):
        metrics = "metrics"

    outputs: ClassVar = {
        "metrics": OutputSpec(artifact_type="metric", derives_from={"inputs": []})
    }

    class Params(BaseModel):
        value: int = Field(description="Candidate value supplied at call time.")
        factor: int = Field(default=2, description="Configured scale factor.")

    params: Params

    def execute_function(self, inputs: ExecuteInput) -> dict[str, list[int]]:
        return {"values": [self.params.value * self.params.factor]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        return ArtifactResult(
            artifacts={
                "metrics": [
                    MetricArtifact.draft(
                        {
                            "value": inputs.memory_outputs["values"][0],
                            "pid": os.getpid(),
                            "prefix": sys.prefix,
                        },
                        "metric.json",
                    )
                ]
            },
            lineage={"metrics": []},
        )


class CommandScale(Scale):
    name = "configured_command_scale"
    tool: ToolSpec = ToolSpec(executable="python")

    execute_function = OperationDefinition.execute_function

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [
            "python",
            "-I",
            "-c",
            "import json,os,sys;from pathlib import Path;"
            f"Path('metric.json').write_text(json.dumps({{'value': {self.params.value * self.params.factor},"
            "'pid':os.getpid(),'prefix':sys.prefix}))",
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        path = next(
            Path(value)
            for value in inputs.file_outputs
            if value.endswith("metric.json")
        )
        return ArtifactResult(
            artifacts={
                "metrics": [
                    MetricArtifact.draft(json.loads(path.read_text()), "metric.json")
                ]
            },
            lineage={"metrics": []},
        )


class ControllerScale(CommandScale):
    name = "configured_controller_scale"
    calls_operations = True

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        program = (
            "import json,sys;from pathlib import Path;"
            "sys.path.insert(0,sys.argv[1]);"
            "from fixtures.configured_operations import Scale;"
            "from artisan.execution import run_operation;"
            "result=run_operation(Scale,selection_key='native',params={'value':int(sys.argv[2])});"
            "Path('metric.json').write_text(json.dumps(result.artifacts['metrics'][0].values))"
        )
        return [
            "python",
            "-I",
            "-c",
            program,
            str(Path(__file__).parent.parent),
            str(self.params.value),
        ]
