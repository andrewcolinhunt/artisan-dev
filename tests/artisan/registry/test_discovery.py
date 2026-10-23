"""Tests for ``artisan.registry.discovery`` — discover, env-var merge, errors."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.registry import discover
from artisan.registry.discovery import _import_and_attribute, _manual_modules
from artisan.registry.models import DiscoveryError


class TestDiscoverBuiltIn:
    def test_built_in_sources_present(self) -> None:
        report = discover()
        kinds = [(s.kind, s.module) for s in report.sources]
        assert ("builtin", "artisan.operations.curator") in kinds

    def test_example_ops_not_auto_discovered(self) -> None:
        report = discover()
        modules = [s.module for s in report.sources]
        assert "artisan.operations.examples" not in modules

    def test_operations_count_matches_registry(self) -> None:
        report = discover()
        assert report.operations_count == len(OperationDefinition._registry)

    def test_idempotent_repeated_call_same_counts(self) -> None:
        first = discover()
        second = discover()
        assert first.operations_count == second.operations_count
        assert [s.module for s in first.sources] == [s.module for s in second.sources]


class TestManualModules:
    def test_extra_modules_kwarg_imported(self) -> None:
        report = discover(extra_modules=["artisan.operations.examples"])
        manual = [s for s in report.sources if s.kind == "manual"]
        assert any(s.module == "artisan.operations.examples" for s in manual)

    def test_failing_import_captured_not_raised(self) -> None:
        report = discover(extra_modules=["this.module.does.not.exist"])
        assert any(
            e.module == "this.module.does.not.exist"
            and e.error_type == "ModuleNotFoundError"
            for e in report.errors
        )

    def test_failing_import_restores_registry_and_collisions(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        module_name = "_artisan_test_partial_plugin"
        plugin_path = tmp_path / f"{module_name}.py"
        plugin_path.write_text(
            dedent(
                """
                from typing import ClassVar

                from artisan.operations.base.operation_definition import OperationDefinition
                from artisan.schemas.specs.input_spec import InputSpec
                from artisan.schemas.specs.output_spec import OutputSpec


                class PartialOperation(OperationDefinition):
                    name: ClassVar[str] = "_test_partial_import_op"
                    description: ClassVar[str] = "Must be rolled back."
                    inputs: ClassVar[dict[str, InputSpec]] = {}
                    outputs: ClassVar[dict[str, OutputSpec]] = {}

                    def execute_function(self, inputs):
                        return None


                class CollidingOperation(OperationDefinition):
                    name: ClassVar[str] = "_test_docstring_only_op"
                    description: ClassVar[str] = "Must not leave a collision."
                    inputs: ClassVar[dict[str, InputSpec]] = {}
                    outputs: ClassVar[dict[str, OutputSpec]] = {}

                    def execute_function(self, inputs):
                        return None


                raise RuntimeError("failure after class definitions")
                """
            )
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        registry_before = dict(OperationDefinition._registry)
        collisions_before = list(OperationDefinition._name_collisions)
        errors: list[DiscoveryError] = []

        source = _import_and_attribute(module_name, "manual", errors)

        assert source is None
        assert OperationDefinition._registry == registry_before
        assert OperationDefinition._name_collisions == collisions_before
        assert [(error.error_type, error.error_message) for error in errors] == [
            ("RuntimeError", "failure after class definitions")
        ]

    def test_env_var_and_kwarg_merged_and_deduped(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ARTISAN_LOAD_MODULES", "a.b,c.d")
        result = _manual_modules(["c.d", "e.f"])
        assert result == ["a.b", "c.d", "e.f"]

    def test_env_var_empty_returns_kwarg_only(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("ARTISAN_LOAD_MODULES", raising=False)
        assert _manual_modules(["a.b"]) == ["a.b"]

    def test_no_input_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ARTISAN_LOAD_MODULES", raising=False)
        assert _manual_modules(None) == []

    def test_env_var_whitespace_stripped(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ARTISAN_LOAD_MODULES", " a.b , , c.d ")
        assert _manual_modules(None) == ["a.b", "c.d"]


class TestNameCollision:
    def test_second_registration_dropped_and_recorded(self) -> None:
        from enum import StrEnum, auto
        from typing import Any, ClassVar

        from pydantic import BaseModel

        from artisan.schemas import ArtifactResult
        from artisan.schemas.artifact.types import ArtifactTypes
        from artisan.schemas.specs.input_models import (
            ExecuteInput,
            PostprocessInput,
            PreprocessInput,
        )
        from artisan.schemas.specs.input_spec import InputSpec
        from artisan.schemas.specs.output_spec import OutputSpec

        op_name = "_test_collision_op"
        OperationDefinition._registry.pop(op_name, None)
        collisions_before = len(OperationDefinition._name_collisions)

        def _make_op() -> type[OperationDefinition]:
            class _Op(OperationDefinition):
                name: ClassVar[str] = op_name
                description: ClassVar[str] = "collision test"

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
                        derives_from={"inputs": ["data"]},
                    ),
                }

                class Params(BaseModel):
                    """Empty params for the collision fixture."""

                params: Params = Params()

                def preprocess(self, _inputs: PreprocessInput) -> dict[str, Any]:
                    return {}

                def execute_function(self, _inputs: ExecuteInput) -> Any:
                    return {}

                def postprocess(self, _inputs: PostprocessInput) -> ArtifactResult:
                    return ArtifactResult(success=True)

            return _Op

        first = _make_op()
        second = _make_op()

        # First registration wins; second is dropped.
        assert OperationDefinition._registry[op_name] is first
        assert OperationDefinition._registry[op_name] is not second
        assert second is not first  # sanity: distinct classes were built

        # A collision entry was recorded for the second attempt.
        new = OperationDefinition._name_collisions[collisions_before:]
        assert any(entry[0] == op_name for entry in new)

        OperationDefinition._registry.pop(op_name, None)
        OperationDefinition._name_collisions[:] = OperationDefinition._name_collisions[
            :collisions_before
        ]
