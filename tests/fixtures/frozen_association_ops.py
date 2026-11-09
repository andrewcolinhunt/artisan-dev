"""Importable curator exercising the public frozen-association input contract."""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar

from artisan.operations.base import OperationDefinition
from artisan.schemas import (
    ArtifactResult,
    CSVArtifact,
    CuratorInput,
    InputSpec,
    OutputSpec,
)
from artisan.storage import ArtifactStore


class AssociationCountCurator(OperationDefinition):
    """Count only the associated metrics captured for the execution."""

    name = "association_count_curator"
    description = "Exercise frozen associated inputs in a curator"

    class InputRole(StrEnum):
        primary = "primary"

    class OutputRole(StrEnum):
        result = "result"

    inputs: ClassVar[dict[str, InputSpec]] = {
        "primary": InputSpec(artifact_type="csv", with_associated=("metric",)),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "result": OutputSpec(artifact_type="csv", derives_from={"inputs": ["primary"]}),
    }

    def execute_curator(
        self, inputs: CuratorInput, step_number: int, artifact_store: ArtifactStore
    ) -> ArtifactResult:
        result = ArtifactResult(artifacts={"result": []}, lineage={"result": []})
        for artifact_id in inputs["primary"]["artifact_id"]:
            count = len(inputs.associated_artifacts(artifact_id, "metric"))
            result.add_artifact(
                "result",
                CSVArtifact.draft(
                    content=f"count\n{count}\n".encode(),
                    original_name="associated_count.csv",
                    step_number=step_number,
                ),
                sources={"primary": [artifact_id]},
            )
        return result
