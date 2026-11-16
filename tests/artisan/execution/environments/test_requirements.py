"""Software selection follows compiled call paths without scientific identity keys."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import pytest
from pydantic import BaseModel, Field

from artisan.composites import CompositeContext
from artisan.composites.base.compiler import compile_definition
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.execution.environments.requirements import select_tool_environments
from artisan.operations.examples import DataTransformer
from artisan.operations.examples.wait_tool import WaitTool
from artisan.schemas import InputSpec, OutputSpec
from artisan.schemas.operation_config.environment import Environment

TOOL = Environment.from_pixi(Path("/synthetic/tool/pixi.toml"))


class DeclaredTool(WaitTool):
    name = "environment_selection_command"
    environment: Environment = TOOL


class CommandLoop(CompositeDefinition):
    name = "environment_selection_loop"

    class InputRole(StrEnum):
        dataset = "dataset"

    class OutputRole(StrEnum):
        output = "output"

    inputs: ClassVar = {"dataset": InputSpec(artifact_type="csv")}
    outputs: ClassVar = {"output": OutputSpec(artifact_type="csv")}

    class Params(BaseModel):
        count: int = Field(default=2, ge=1)
        seconds: int = Field(default=1, ge=1)

    params: Params = Field(default_factory=Params)

    def compose(self, ctx: CompositeContext) -> None:
        current = ctx.input("dataset")
        for index in range(self.params.count):
            current = ctx.call(
                DeclaredTool,
                name=f"tool_{index}",
                inputs={"dataset": current},
                params={"seconds": self.params.seconds},
            ).output("output")
        ctx.output("output", current)


class NestedCommands(CommandLoop):
    name = "environment_selection_nested"

    def compose(self, ctx: CompositeContext) -> None:
        nested = ctx.call(
            CommandLoop, name="nested", inputs={"dataset": ctx.input("dataset")}
        )
        ctx.output("output", nested.output("output"))


class AdaptedNative(DataTransformer):
    name = "environment_selection_adapter"
    execute_as_tool = True
    environment: Environment = TOOL


def test_select_distinguishes_native_inheritance_and_python_adapter() -> None:
    assert select_tool_environments(compile_definition(DataTransformer)) == ()
    inherited = select_tool_environments(compile_definition(WaitTool))
    assert inherited[0].declaration is None
    assert inherited[0].mode == "command"
    assert inherited[0].call_path == ()
    adapted = select_tool_environments(compile_definition(AdaptedNative))
    assert adapted[0].mode == "python_adapter"
    assert adapted[0].declaration == TOOL


def test_select_uses_nested_paths_and_same_software_for_numeric_or_loop_changes() -> (
    None
):
    nested = select_tool_environments(compile_definition(NestedCommands))
    assert [selection.call_path for selection in nested] == [
        ("nested", "tool_0"),
        ("nested", "tool_1"),
    ]
    longer = select_tool_environments(
        compile_definition(CommandLoop, params={"count": 4, "seconds": 2})
    )
    assert len(longer) == 4
    assert {selection.declaration for selection in nested} == {TOOL}
    assert {selection.declaration for selection in longer} == {TOOL}


def test_select_rejects_separate_software_for_native_execution() -> None:
    class InvalidNative(DataTransformer):
        name = "environment_selection_invalid_native"
        environment: Environment = TOOL

    with pytest.raises(ValueError, match="Native operations cannot declare"):
        select_tool_environments(compile_definition(InvalidNative))


def test_selected_declaration_is_not_reloaded_after_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = compile_definition(DeclaredTool)
    selected = select_tool_environments(plan)
    field = DeclaredTool.model_fields["environment"]
    original = field.default
    changed = Environment.from_pixi(Path("/synthetic/different/pixi.toml"))
    monkeypatch.setattr(field, "default", changed)
    DeclaredTool.model_rebuild(force=True)
    try:
        assert selected[0].declaration == TOOL
        assert select_tool_environments(plan)[0].declaration == changed
    finally:
        field.default = original
        DeclaredTool.model_rebuild(force=True)
