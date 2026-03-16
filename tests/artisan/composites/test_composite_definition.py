"""Tests for CompositeDefinition base class and subclass validation."""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar

import pytest
from pydantic import BaseModel, Field

from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_valid_composite(
    name: str,
    inputs: dict[str, InputSpec] | None = None,
    outputs: dict[str, OutputSpec] | None = None,
) -> type[CompositeDefinition]:
    """Dynamically create a valid CompositeDefinition subclass."""
    if inputs is None:
        inputs = {
            "data": InputSpec(artifact_type="data", required=True),
        }
    if outputs is None:
        outputs = {
            "result": OutputSpec(artifact_type="data"),
        }

    # Build class body as a string and exec it to get proper class syntax
    # This avoids Pydantic issues with dynamically added attributes via type()
    input_roles = list(inputs.keys())
    output_roles = list(outputs.keys())

    ns: dict = {
        "CompositeDefinition": CompositeDefinition,
        "StrEnum": StrEnum,
        "ClassVar": ClassVar,
        "InputSpec": InputSpec,
        "OutputSpec": OutputSpec,
        "inputs_val": inputs,
        "outputs_val": outputs,
    }

    lines = [
        f"class TestComposite_{name}(CompositeDefinition):",
        f"    name = {name!r}",
        f"    description = 'Test composite {name}'",
    ]

    if input_roles:
        lines.append("    class InputRole(StrEnum):")
        for r in input_roles:
            lines.append(f"        {r.upper()} = {r!r}")

    if output_roles:
        lines.append("    class OutputRole(StrEnum):")
        for r in output_roles:
            lines.append(f"        {r.upper()} = {r!r}")

    lines.append("    inputs: ClassVar[dict[str, InputSpec]] = inputs_val")
    lines.append("    outputs: ClassVar[dict[str, OutputSpec]] = outputs_val")
    lines.append("    def compose(self, ctx):")
    lines.append("        pass")

    exec("\n".join(lines), ns)
    return ns[f"TestComposite_{name}"]


# ---------------------------------------------------------------------------
# Tests: Registration
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_subclass_registered(self):
        cls = _make_valid_composite("test_reg_basic")
        assert CompositeDefinition.get("test_reg_basic") is cls

    def test_get_unknown_raises(self):
        with pytest.raises(KeyError, match="Unknown composite"):
            CompositeDefinition.get("nonexistent_composite_xyz")

    def test_get_all_returns_copy(self):
        _make_valid_composite("test_reg_all")
        all_composites = CompositeDefinition.get_all()
        assert "test_reg_all" in all_composites
        # Modifying returned dict doesn't affect registry
        all_composites.pop("test_reg_all")
        assert "test_reg_all" in CompositeDefinition.get_all()

    def test_separate_from_operation_registry(self):
        """Composite registry is independent of OperationDefinition registry."""
        from artisan.operations.base.operation_definition import OperationDefinition

        _make_valid_composite("test_reg_separate")
        assert "test_reg_separate" not in OperationDefinition.get_all()


# ---------------------------------------------------------------------------
# Tests: Validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_missing_compose_raises_type_error(self):
        with pytest.raises(TypeError, match="must implement compose"):

            class BadComposite(CompositeDefinition):
                name = "bad_no_compose"
                description = "Missing compose"

                class OutputRole(StrEnum):
                    OUT = "out"

                outputs: ClassVar[dict[str, OutputSpec]] = {
                    "out": OutputSpec(artifact_type="data"),
                }

    def test_missing_output_role_enum_raises(self):
        with pytest.raises(TypeError, match="must define OutputRole"):

            class BadComposite(CompositeDefinition):
                name = "bad_no_output_role"

                outputs: ClassVar[dict[str, OutputSpec]] = {
                    "out": OutputSpec(artifact_type="data"),
                }

                def compose(self, ctx):
                    pass

    def test_mismatched_output_role_enum_raises(self):
        with pytest.raises(TypeError, match="don't match outputs keys"):

            class BadComposite(CompositeDefinition):
                name = "bad_mismatch_output"

                class OutputRole(StrEnum):
                    WRONG = "wrong"

                outputs: ClassVar[dict[str, OutputSpec]] = {
                    "out": OutputSpec(artifact_type="data"),
                }

                def compose(self, ctx):
                    pass

    def test_missing_input_role_enum_raises(self):
        with pytest.raises(TypeError, match="must define InputRole"):

            class BadComposite(CompositeDefinition):
                name = "bad_no_input_role"

                class OutputRole(StrEnum):
                    OUT = "out"

                inputs: ClassVar[dict[str, InputSpec]] = {
                    "data": InputSpec(artifact_type="data"),
                }
                outputs: ClassVar[dict[str, OutputSpec]] = {
                    "out": OutputSpec(artifact_type="data"),
                }

                def compose(self, ctx):
                    pass

    def test_mismatched_input_role_enum_raises(self):
        with pytest.raises(TypeError, match="don't match inputs keys"):

            class BadComposite(CompositeDefinition):
                name = "bad_mismatch_input"

                class InputRole(StrEnum):
                    WRONG = "wrong"

                class OutputRole(StrEnum):
                    OUT = "out"

                inputs: ClassVar[dict[str, InputSpec]] = {
                    "data": InputSpec(artifact_type="data"),
                }
                outputs: ClassVar[dict[str, OutputSpec]] = {
                    "out": OutputSpec(artifact_type="data"),
                }

                def compose(self, ctx):
                    pass

    def test_abstract_base_not_registered(self):
        """Classes with falsy name are skipped (abstract bases)."""

        class AbstractComposite(CompositeDefinition):
            pass

        assert "" not in CompositeDefinition.get_all()

    def test_no_inputs_skips_input_role_check(self):
        """Generative composites with no inputs don't need InputRole."""

        class GenerativeComposite(CompositeDefinition):
            name = "test_generative"
            description = "No inputs"

            class OutputRole(StrEnum):
                OUT = "out"

            inputs: ClassVar[dict[str, InputSpec]] = {}
            outputs: ClassVar[dict[str, OutputSpec]] = {
                "out": OutputSpec(artifact_type="data"),
            }

            def compose(self, ctx):
                pass

        assert CompositeDefinition.get("test_generative") is GenerativeComposite


# ---------------------------------------------------------------------------
# Tests: Params pattern
# ---------------------------------------------------------------------------


class TestParams:
    def test_params_field(self):
        class ParamsComposite(CompositeDefinition):
            name = "test_params"
            description = "With params"

            class InputRole(StrEnum):
                DATA = "data"

            class OutputRole(StrEnum):
                OUT = "out"

            inputs: ClassVar[dict[str, InputSpec]] = {
                "data": InputSpec(artifact_type="data"),
            }
            outputs: ClassVar[dict[str, OutputSpec]] = {
                "out": OutputSpec(artifact_type="data"),
            }

            class Params(BaseModel):
                threshold: float = Field(default=0.5)

            params: Params = Params()

            def compose(self, ctx):
                pass

        instance = ParamsComposite(params={"threshold": 0.9})
        assert instance.params.threshold == 0.9

    def test_default_params(self):
        class DefaultParamsComposite(CompositeDefinition):
            name = "test_default_params"
            description = "Default params"

            class OutputRole(StrEnum):
                OUT = "out"

            outputs: ClassVar[dict[str, OutputSpec]] = {
                "out": OutputSpec(artifact_type="data"),
            }

            class Params(BaseModel):
                mode: str = "fast"

            params: Params = Params()

            def compose(self, ctx):
                pass

        instance = DefaultParamsComposite()
        assert instance.params.mode == "fast"


# ---------------------------------------------------------------------------
# Tests: Pydantic model behavior
# ---------------------------------------------------------------------------


class TestModelBehavior:
    def test_extra_fields_forbidden(self):
        cls = _make_valid_composite("test_extra_forbid")
        with pytest.raises(Exception):  # noqa: B017
            cls(unknown_field="value")

    def test_resources_default(self):
        cls = _make_valid_composite("test_resources_default")
        instance = cls()
        assert instance.resources.cpus == 1

    def test_execution_default(self):
        cls = _make_valid_composite("test_execution_default")
        instance = cls()
        assert instance.execution.artifacts_per_unit == 1


# ---------------------------------------------------------------------------
# Tests: Docstring generation
# ---------------------------------------------------------------------------


class TestDocs:
    def test_role_docs_appended(self):
        cls = _make_valid_composite("test_role_docs")
        assert "Input Roles:" in cls.__doc__
        assert "Output Roles:" in cls.__doc__
