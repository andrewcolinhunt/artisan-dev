"""Execute an external transform_data.py script via config artifacts."""

from __future__ import annotations

import os
from collections import defaultdict, deque
from enum import StrEnum, auto
from typing import Any, ClassVar, cast

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import (
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.external_tools import format_args


class DataTransformerScript(OperationDefinition):
    """Run an external Python script to transform CSV data.

    Pairs each dataset with a config artifact, resolves artifact references,
    and invokes the script via ``run_command()``.
    """

    name: ClassVar[str] = "data_transformer_script"
    version: ClassVar[str] = "2"
    description: ClassVar[str] = "Execute data transformation script"

    class InputRole(StrEnum):
        DATASET = "dataset"
        config = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(
            artifact_type="csv",
            materialize=True,
            description="Input CSV dataset to transform",
        ),
        InputRole.config: InputSpec(
            artifact_type=ArtifactTypes.CONFIG,
            materialize=True,
            description="Config with parameters and $artifact reference",
        ),
    }

    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="csv",
            derives_from={"inputs": ["config"]},
            description="Transformed dataset files",
        ),
    }

    group_by: GroupByStrategy | None = GroupByStrategy.LINEAGE

    tool: ToolSpec = ToolSpec(executable="python", subcommand="-m")

    runner_resources: RunnerResources = RunnerResources(  # type: ignore[call-arg]  # pydantic defaults
        cpus=1,
        memory_gb=4,
        time_limit="00:30:00",
    )

    batch_strategy: BatchStrategy = BatchStrategy(job_name="data_transformer_script")  # type: ignore[call-arg]  # pydantic defaults

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Extract materialized config paths from paired inputs."""
        prepared_inputs: list[dict[str, Any]] = []
        for group in inputs.grouped():
            config = cast(ExecutionConfigArtifact, group["config"])
            prepared_inputs.append(
                {
                    "config_path": str(config.materialized_path),
                    "design_name": config.original_name,
                    "source_artifact_id": config.artifact_id,
                }
            )

        return {"items": PerArtifact(prepared_inputs)}

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        """Run the script for one paired config under selected command software."""
        item = inputs["items"]
        return [
            *self.tool.parts(),
            "artisan.operations.examples.scripts.transform_data",
            *format_args(
                {
                    "config": item["config_path"],
                    "output-dir": ".",
                    "output-basename": item["source_artifact_id"],
                }
            ),
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Match script files to their exact config inputs and declare lineage."""
        result = ArtifactResult(artifacts={"dataset": []}, lineage={"dataset": []})
        files: dict[str, deque[str]] = defaultdict(deque)
        for path in inputs.file_outputs:
            files[os.path.basename(path)].append(path)
        configs = cast(list[ExecutionConfigArtifact], inputs.input_artifacts["config"])
        for index, config in enumerate(configs):
            path = files[f"{config.artifact_id}_variant_0.csv"].popleft()
            with open(path, "rb") as stream:
                draft = CSVArtifact.draft(
                    content=stream.read(),
                    original_name=f"{config.original_name}_variant_0.csv",
                    step_number=inputs.step_number,
                )
            result.add_artifact(
                "dataset",
                draft,
                sources={"config": [inputs.ref("config", index=index)]},
            )
        return result
