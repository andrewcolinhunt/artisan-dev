"""Tests for ``artisan.registry.schemas`` — ``params_schema_for``, ``_params_class``."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import Any, ClassVar

import pytest
from pydantic import BaseModel, Field

from artisan.operations.base._param_docs import _params_class
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator.merge import Merge
from artisan.operations.examples.data_transformer import DataTransformer
from artisan.registry.schemas import params_schema_for
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


@pytest.fixture(autouse=True)
def _populated_registry() -> None:
    """Force built-in op imports so registry is populated."""
    import artisan.operations.curator
    import artisan.operations.examples  # noqa: F401


class TestParamsClassResolution:
    def test_resolves_for_op_with_params(self) -> None:
        assert _params_class(DataTransformer) is DataTransformer.Params

    def test_returns_none_for_parameter_less_op(self) -> None:
        assert _params_class(Merge) is None


class TestParamsSchemaFor:
    def test_field_descriptions_present(self) -> None:
        """``DataTransformer.Params`` uses ``Field(description=...)``."""
        schema = params_schema_for(DataTransformer)
        scale = schema["properties"]["scale_factor"]
        assert scale["description"] == "Multiplicative scale factor for numeric columns"

    def test_docstring_descriptions_present(self) -> None:
        """Conftest fixture op has descriptions only in the class docstring."""
        cls = OperationDefinition._registry["_test_docstring_only_op"]
        schema = params_schema_for(cls)
        assert (
            schema["properties"]["alpha"]["description"] == "First fixture parameter."
        )
        beta_desc = schema["properties"]["beta"]["description"]
        assert "Second fixture parameter" in beta_desc
        assert "multiple lines" in beta_desc

    def test_parameter_less_op_returns_empty_schema(self) -> None:
        schema = params_schema_for(Merge)
        assert schema == {"type": "object", "title": "Params", "properties": {}}


class TestFieldPrecedence:
    """``Field(description=...)`` always wins over docstring extraction."""

    def test_field_precedes_docstring_when_both_present(self) -> None:
        class _PrecedenceOp(OperationDefinition):
            name: ClassVar[str] = "_test_precedence_op"
            description: ClassVar[str] = "Precedence test."

            class InputRole(StrEnum):
                dataset = auto()

            class OutputRole(StrEnum):
                dataset = auto()

            inputs: ClassVar[dict[str, InputSpec]] = {
                InputRole.dataset: InputSpec(
                    artifact_type=ArtifactTypes.DATA,
                    required=True,
                    description="x",
                ),
            }
            outputs: ClassVar[dict[str, OutputSpec]] = {
                OutputRole.dataset: OutputSpec(
                    artifact_type=ArtifactTypes.DATA,
                    description="x",
                    infer_lineage_from={"inputs": ["dataset"]},
                ),
            }

            class Params(BaseModel):
                """Both descriptions provided.

                Attributes:
                    alpha: From docstring.
                """

                alpha: int = Field(default=0, description="From Field")

            params: Params = Params()

            def preprocess(self, _inputs: PreprocessInput) -> dict[str, Any]:
                return {}

            def execute(self, _inputs: ExecuteInput) -> Any:
                return {}

            def postprocess(self, _inputs: PostprocessInput) -> ArtifactResult:
                return ArtifactResult(success=True)

        schema = params_schema_for(_PrecedenceOp)
        assert schema["properties"]["alpha"]["description"] == "From Field"
