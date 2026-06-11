"""Regression tests for ``_validate_params_documented`` (fail-fast import check).

The check runs inside ``OperationDefinition.__pydantic_init_subclass__``
before registry insertion. To trigger it in a test, define a subclass
*inside* the test body so the ``ArtisanError`` raises in the test scope.
"""

from __future__ import annotations

from enum import StrEnum, auto
from typing import Any, ClassVar

import pytest
from pydantic import BaseModel, Field

from artisan.errors import ArtisanError
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class TestImportsCleanly:
    """Ops with documentation should import without raising."""

    def test_field_described_params_imports_cleanly(self) -> None:
        class _Op(OperationDefinition):
            name: ClassVar[str] = "_test_documented_via_field"
            description: ClassVar[str] = "ok"

            class InputRole(StrEnum):
                data = auto()

            class OutputRole(StrEnum):
                data = auto()

            inputs: ClassVar[dict[str, InputSpec]] = {
                InputRole.data: InputSpec(
                    artifact_type=ArtifactTypes.DATA,
                    required=True,
                ),
            }
            outputs: ClassVar[dict[str, OutputSpec]] = {
                OutputRole.data: OutputSpec(
                    artifact_type=ArtifactTypes.DATA,
                    infer_lineage_from={"inputs": ["data"]},
                ),
            }

            class Params(BaseModel):
                """Header (no Attributes section)."""

                alpha: int = Field(default=0, description="From Field")

            params: Params = Params()

            def preprocess(self, _inputs: PreprocessInput) -> dict[str, Any]:
                return {}

            def execute_function(self, _inputs: ExecuteInput) -> Any:
                return {}

            def postprocess(self, _inputs: PostprocessInput) -> ArtifactResult:
                return ArtifactResult(success=True)

        assert OperationDefinition._registry[_Op.name] is _Op

    def test_docstring_described_params_imports_cleanly(self) -> None:
        class _Op(OperationDefinition):
            name: ClassVar[str] = "_test_documented_via_docstring"
            description: ClassVar[str] = "ok"

            class InputRole(StrEnum):
                data = auto()

            class OutputRole(StrEnum):
                data = auto()

            inputs: ClassVar[dict[str, InputSpec]] = {
                InputRole.data: InputSpec(
                    artifact_type=ArtifactTypes.DATA,
                    required=True,
                ),
            }
            outputs: ClassVar[dict[str, OutputSpec]] = {
                OutputRole.data: OutputSpec(
                    artifact_type=ArtifactTypes.DATA,
                    infer_lineage_from={"inputs": ["data"]},
                ),
            }

            class Params(BaseModel):
                """Header.

                Attributes:
                    alpha: From docstring.
                """

                alpha: int = 0

            params: Params = Params()

            def preprocess(self, _inputs: PreprocessInput) -> dict[str, Any]:
                return {}

            def execute_function(self, _inputs: ExecuteInput) -> Any:
                return {}

            def postprocess(self, _inputs: PostprocessInput) -> ArtifactResult:
                return ArtifactResult(success=True)

        assert OperationDefinition._registry[_Op.name] is _Op

    def test_parameter_less_op_imports_cleanly(self) -> None:
        class _Op(OperationDefinition):
            name: ClassVar[str] = "_test_parameter_less"
            description: ClassVar[str] = "ok"

            class InputRole(StrEnum):
                data = auto()

            class OutputRole(StrEnum):
                data = auto()

            inputs: ClassVar[dict[str, InputSpec]] = {
                InputRole.data: InputSpec(
                    artifact_type=ArtifactTypes.DATA,
                    required=True,
                ),
            }
            outputs: ClassVar[dict[str, OutputSpec]] = {
                OutputRole.data: OutputSpec(
                    artifact_type=ArtifactTypes.DATA,
                    infer_lineage_from={"inputs": ["data"]},
                ),
            }

            def preprocess(self, _inputs: PreprocessInput) -> dict[str, Any]:
                return {}

            def execute_function(self, _inputs: ExecuteInput) -> Any:
                return {}

            def postprocess(self, _inputs: PostprocessInput) -> ArtifactResult:
                return ArtifactResult(success=True)

        assert OperationDefinition._registry[_Op.name] is _Op

    def test_empty_params_class_imports_cleanly(self) -> None:
        class _Op(OperationDefinition):
            name: ClassVar[str] = "_test_empty_params"
            description: ClassVar[str] = "ok"

            class InputRole(StrEnum):
                data = auto()

            class OutputRole(StrEnum):
                data = auto()

            inputs: ClassVar[dict[str, InputSpec]] = {
                InputRole.data: InputSpec(
                    artifact_type=ArtifactTypes.DATA,
                    required=True,
                ),
            }
            outputs: ClassVar[dict[str, OutputSpec]] = {
                OutputRole.data: OutputSpec(
                    artifact_type=ArtifactTypes.DATA,
                    infer_lineage_from={"inputs": ["data"]},
                ),
            }

            class Params(BaseModel):
                """No fields."""

            params: Params = Params()

            def preprocess(self, _inputs: PreprocessInput) -> dict[str, Any]:
                return {}

            def execute_function(self, _inputs: ExecuteInput) -> Any:
                return {}

            def postprocess(self, _inputs: PostprocessInput) -> ArtifactResult:
                return ArtifactResult(success=True)

        assert OperationDefinition._registry[_Op.name] is _Op


class TestImportFailsForUndocumentedParams:
    """A Params field with no description source fails at class definition."""

    def test_raises_artisan_error_with_op_params_undocumented(self) -> None:
        with pytest.raises(ArtisanError) as exc_info:

            class _BadOp(OperationDefinition):
                name: ClassVar[str] = "_test_undocumented_should_fail"
                description: ClassVar[str] = "bad"

                class InputRole(StrEnum):
                    data = auto()

                class OutputRole(StrEnum):
                    data = auto()

                inputs: ClassVar[dict[str, InputSpec]] = {
                    InputRole.data: InputSpec(
                        artifact_type=ArtifactTypes.DATA,
                        required=True,
                    ),
                }
                outputs: ClassVar[dict[str, OutputSpec]] = {
                    OutputRole.data: OutputSpec(
                        artifact_type=ArtifactTypes.DATA,
                        infer_lineage_from={"inputs": ["data"]},
                    ),
                }

                class Params(BaseModel):
                    """Header — but no Attributes/Args section."""

                    undocumented_field: int = 0

                params: Params = Params()

                def preprocess(self, _inputs: PreprocessInput) -> dict[str, Any]:
                    return {}

                def execute_function(self, _inputs: ExecuteInput) -> Any:
                    return {}

                def postprocess(self, _inputs: PostprocessInput) -> ArtifactResult:
                    return ArtifactResult(success=True)

        envelope = exc_info.value.envelope
        assert envelope.code == "op_params_undocumented"
        assert envelope.error_type == "config"
        assert envelope.recovery_hint == "CHECK_INPUT"
        assert envelope.operation_name == "_test_undocumented_should_fail"
        assert "undocumented_field" in envelope.message

    def test_undocumented_op_never_reaches_registry(self) -> None:
        bad_name = "_test_dropped_before_registry"
        OperationDefinition._registry.pop(bad_name, None)

        with pytest.raises(ArtisanError):

            class _BadOp(OperationDefinition):
                name: ClassVar[str] = bad_name
                description: ClassVar[str] = "bad"

                class InputRole(StrEnum):
                    data = auto()

                class OutputRole(StrEnum):
                    data = auto()

                inputs: ClassVar[dict[str, InputSpec]] = {
                    InputRole.data: InputSpec(
                        artifact_type=ArtifactTypes.DATA,
                        required=True,
                    ),
                }
                outputs: ClassVar[dict[str, OutputSpec]] = {
                    OutputRole.data: OutputSpec(
                        artifact_type=ArtifactTypes.DATA,
                        infer_lineage_from={"inputs": ["data"]},
                    ),
                }

                class Params(BaseModel):
                    """Header without sections."""

                    naked_field: int = 0

                params: Params = Params()

                def preprocess(self, _inputs: PreprocessInput) -> dict[str, Any]:
                    return {}

                def execute_function(self, _inputs: ExecuteInput) -> Any:
                    return {}

                def postprocess(self, _inputs: PostprocessInput) -> ArtifactResult:
                    return ArtifactResult(success=True)

        assert bad_name not in OperationDefinition._registry
