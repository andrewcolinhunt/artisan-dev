"""Generate repeated candidates through a run-selected operation."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import ClassVar

from pydantic import BaseModel, Field

from artisan.execution import run_operation
from artisan.operations.base import OperationDefinition
from artisan.operations.examples.data_generator import DataGenerator
from artisan.schemas import Artifact, ArtifactResult, OutputSpec
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput


class ConfiguredGenerator(OperationDefinition):
    """Call a configured generator for each seed without creating child steps."""

    name = "configured_generator"

    class OutputRole(StrEnum):
        datasets = "datasets"

    outputs: ClassVar = {
        "datasets": OutputSpec(artifact_type="csv", derives_from={"inputs": []})
    }

    class Params(BaseModel):
        seeds: tuple[int, ...] = Field(default=(1, 2), description="Candidate seeds.")

    params: Params = Params()

    def execute_function(self, inputs: ExecuteInput) -> dict[str, list[Artifact]]:
        """Use the selected fixed generator settings for each candidate seed."""
        assert inputs.execute_dir is not None
        datasets = []
        for seed in self.params.seeds:
            result = run_operation(
                DataGenerator,
                selection_key="generator",
                params={"seed": seed},
                work_dir=Path(inputs.execute_dir),
            )
            datasets.extend(result.artifacts["datasets"])
        return {"datasets": datasets}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Expose generated roots through the parent operation's normal output."""
        return ArtifactResult(
            artifacts={"datasets": inputs.memory_outputs["datasets"]},
            lineage={"datasets": []},
        )
