"""Tiny installed application used only by fresh-interpreter registration tests."""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from artisan.operations.examples import DataGenerator
from artisan.schemas import (
    ArtifactResult,
    ArtifactTypeDef,
    DataArtifact,
    OutputSpec,
    PostprocessInput,
)


class ApplicationNote(DataArtifact):
    """A CSV payload with an application-owned artifact type."""

    artifact_type: str = Field(default="application_note", frozen=True)


class ApplicationNoteType(ArtifactTypeDef):
    key = "application_note"
    table_path = "artifacts/application_notes"
    model = ApplicationNote


class ApplicationNoteGenerator(DataGenerator):
    """Generate test application artifacts through the native creator lifecycle."""

    name = "application_note_generator"
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "datasets": OutputSpec(
            artifact_type="application_note", derives_from={"inputs": []}
        ),
    }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = super().postprocess(inputs)
        result.artifacts = {
            role: [
                ApplicationNote.model_validate(
                    item.model_dump(exclude={"artifact_type"})
                )
                for item in artifacts
            ]
            for role, artifacts in result.artifacts.items()
        }
        return result
