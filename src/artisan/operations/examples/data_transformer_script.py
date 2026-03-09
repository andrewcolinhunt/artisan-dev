"""Execute an external transform_data.py script via config artifacts."""

from __future__ import annotations

from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.environment_spec import (
    DockerEnvironmentSpec,
    LocalEnvironmentSpec,
)
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.resource_config import ResourceConfig
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

    # ---------- Metadata ----------
    name: ClassVar[str] = "data_transformer_script"
    description: ClassVar[str] = "Execute data transformation script"

    # ---------- Inputs ----------
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

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["config"]},
            description="Transformed dataset files",
        ),
    }

    # ---------- Behavior ----------
    group_by: ClassVar[GroupByStrategy | None] = GroupByStrategy.LINEAGE

    # ---------- Tool ----------
    tool: ToolSpec = ToolSpec(executable=SCRIPT_PATH, interpreter="python")

    # ---------- Environments ----------
    environments: Environments = Environments(
        local=LocalEnvironmentSpec(),
        docker=DockerEnvironmentSpec(image="my-registry/transformer:latest"),
    )

    # ---------- Resources ----------
    resources: ResourceConfig = ResourceConfig(
        cpus_per_task=1,
        mem_gb=4,
        time_limit="00:30:00",
    )

    # ---------- Execution ----------
    execution: ExecutionConfig = ExecutionConfig(job_name="data_transformer_script")

    # ---------- Lifecycle ----------
    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Extract materialized config paths from paired inputs."""
        prepared_inputs = []
        for group in inputs.grouped():
            config = group["config"]
            prepared_inputs.append({
                "config_path": str(config.materialized_path),
                "design_name": config.original_name,
            })

        return {"items": prepared_inputs}

    def execute(self, inputs: ExecuteInput) -> Any:
        """Invoke transform_data.py for each config in the batch."""
        execute_dir = inputs.execute_dir
        env = self.environments.current()

        for item in inputs.inputs["items"]:
            config_path = item["config_path"]
            design_name = item["design_name"]

            args = format_args({
                "config": config_path,
                "output-dir": str(execute_dir),
                "output-basename": design_name,
            })
            run_command(
                env,
                [*self.tool.parts(), *args],
                cwd=execute_dir,
            )

        return None

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build DataArtifact drafts from script-produced CSV files."""
        drafts = []
        for f in inputs.file_outputs:
            if f.suffix != ".csv":
                continue
            draft = DataArtifact.draft(
                content=f.read_bytes(),
                original_name=f.name,
                step_number=inputs.step_number,
            )
            drafts.append(draft)

        return ArtifactResult(success=True, artifacts={"dataset": drafts})
