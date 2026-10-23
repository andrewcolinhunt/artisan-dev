"""Execute an external transform_data.py script via config artifacts."""

from __future__ import annotations

import os
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar, cast

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.compute import ComputeProvider, ModalComputeConfig
from artisan.schemas.operation_config.environment_spec import (
    DockerEnvironmentSpec,
    LocalEnvironmentSpec,
)
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.external_tools import format_args, run_command

SCRIPT_PATH = Path(__file__).parent / "scripts" / "transform_data.py"


class DataTransformerScript(OperationDefinition):
    """Run an external Python script to transform CSV data.

    Pairs each dataset with a config artifact, resolves artifact references,
    and invokes the script via ``run_command()``.
    """

    name: ClassVar[str] = "data_transformer_script"
    description: ClassVar[str] = "Execute data transformation script"

    class InputRole(StrEnum):
        DATASET = "dataset"
        config = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(
            artifact_type="data",
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
            artifact_type="data",
            derives_from={"inputs": ["config"]},
            description="Transformed dataset files",
        ),
    }

    group_by: GroupByStrategy | None = GroupByStrategy.LINEAGE

    tool: ToolSpec = ToolSpec(executable=str(SCRIPT_PATH), interpreter="python")

    environments: Environments = Environments(
        local=LocalEnvironmentSpec(),
        docker=DockerEnvironmentSpec(image="my-registry/transformer:latest"),
    )

    runner_resources: RunnerResources = RunnerResources(  # type: ignore[call-arg]  # pydantic defaults
        cpus=1,
        memory_gb=4,
        time_limit="00:30:00",
    )

    batch_strategy: BatchStrategy = BatchStrategy(job_name="data_transformer_script")  # type: ignore[call-arg]  # pydantic defaults

    compute_provider: ComputeProvider = ComputeProvider(
        modal=ModalComputeConfig(),
    )

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

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Invoke transform_data.py for each config in the batch."""
        execute_dir = inputs.execute_dir
        env = self.environments.current()
        outputs = []

        for item in inputs.inputs["items"]:
            config_path = item["config_path"]
            design_name = item["design_name"]
            output_basename = item["source_artifact_id"]

            args = format_args(
                {
                    "config": config_path,
                    "output-dir": execute_dir,
                    "output-basename": output_basename,
                }
            )
            run_command(
                env,
                [*self.tool.parts(), *args],
                cwd=execute_dir,
            )
            outputs.append(
                {
                    "path": os.path.join(
                        execute_dir, f"{output_basename}_variant_0.csv"
                    ),
                    "original_name": f"{design_name}_variant_0.csv",
                    "source_artifact_id": item["source_artifact_id"],
                }
            )

        return {"outputs": outputs}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build DataArtifact drafts from script-produced CSV files."""
        result = ArtifactResult(artifacts={"dataset": []}, lineage={"dataset": []})
        for output in inputs.memory_outputs["outputs"]:
            with open(output["path"], "rb") as fh:
                content = fh.read()
            draft = DataArtifact.draft(
                content=content,
                original_name=output["original_name"],
                step_number=inputs.step_number,
            )
            result.add_artifact(
                "dataset", draft, sources={"config": [output["source_artifact_id"]]}
            )

        return result
