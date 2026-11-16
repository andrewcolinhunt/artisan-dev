"""Stable curator definition for an already evaluated interactive selection."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum, auto
from typing import ClassVar

import polars as pl
from pydantic import BaseModel, Field, JsonValue

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator.filter import Criterion
from artisan.schemas.execution.curator_result import PassthroughResult
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore


class InteractiveSelection(OperationDefinition):
    """Publish frozen IDs and diagnostic observations without evaluating new metrics."""

    name = "interactive_filter"
    description = "Publish a previously evaluated interactive artifact selection"
    cacheable = False

    class InputRole(StrEnum):
        passthrough = auto()

    class OutputRole(StrEnum):
        passthrough = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.passthrough: InputSpec(hydrate=False)
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {OutputRole.passthrough: OutputSpec()}

    class Params(BaseModel):
        selected_ids: list[str] = Field(
            description="Ordered content IDs selected by the UI"
        )
        criteria: list[Criterion] = Field(
            description="Criteria used for the frozen selection"
        )
        diagnostics: dict[str, JsonValue] = Field(
            description="Observed selection statistics"
        )

    params: Params

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        available = set(inputs["passthrough"]["artifact_id"])
        if not set(self.params.selected_ids).issubset(available):
            msg = "Interactive selection contains IDs outside its admitted inputs"
            raise ValueError(msg)
        return PassthroughResult(
            passthrough={"passthrough": self.params.selected_ids},
            metadata={"diagnostics": self.params.diagnostics},
        )
