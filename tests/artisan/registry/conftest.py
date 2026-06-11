"""Shared fixtures for registry tests.

``DocstringOnlyOp`` is the regression fixture for the docstring extraction
path: its ``Params`` fields have descriptions only in the class docstring,
no ``Field(description=...)``. Registered at module-import time via
``OperationDefinition.__pydantic_init_subclass__`` so the registry's
``describe()`` finds it.
"""

from __future__ import annotations

from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.operation_example import OperationExample
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class DocstringOnlyOp(OperationDefinition):
    """Fixture op whose Params descriptions live only in the class docstring."""

    name: ClassVar[str] = "_test_docstring_only_op"
    description: ClassVar[str] = "Test fixture for docstring extraction."
    tags: ClassVar[list[str]] = ["test", "fixture"]

    class InputRole(StrEnum):
        dataset = auto()

    class OutputRole(StrEnum):
        dataset = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.dataset: InputSpec(
            artifact_type=ArtifactTypes.DATA,
            required=True,
            description="Input dataset for the fixture.",
        ),
    }

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.dataset: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            description="Pass-through output for the fixture.",
            infer_lineage_from={"inputs": ["dataset"]},
        ),
    }

    examples: ClassVar[list[OperationExample]] = [
        OperationExample(
            description="Minimal example.",
            params={"alpha": 2},
            inputs={"dataset": "upstream_step"},
        ),
    ]

    class Params(BaseModel):
        """Params with descriptions only in this docstring.

        Attributes:
            alpha: First fixture parameter.
            beta: Second fixture parameter; spans
                multiple lines.
        """

        alpha: int = 1
        beta: str = "x"

    params: Params = Params()

    def preprocess(self, _inputs: PreprocessInput) -> dict[str, Any]:
        return {}

    def execute_function(self, _inputs: ExecuteInput) -> Any:
        return {}

    def postprocess(self, _inputs: PostprocessInput) -> ArtifactResult:
        return ArtifactResult(success=True)
