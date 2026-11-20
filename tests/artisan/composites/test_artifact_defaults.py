"""Composite default inputs are explicit captured constants in versioned recipes."""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar

import pytest
from fixtures.fixed_default_operations import (
    DefaultOffset,
    DefaultPair,
    NestedDefaultPair,
    OtherDefaultOffset,
)
from fixtures.input_contract_operations import SharedOffset, metric
from pydantic import ValidationError

from artisan.composites import CompositeContext
from artisan.composites.base.compiler import (
    compile_captured_definition,
    compile_definition,
)
from artisan.schemas import InputSpec
from artisan.schemas.composites.plan import ExecutionRecipe


def test_nested_defaults_capture_distinct_sites_in_recipe_order() -> None:
    plan = compile_definition(NestedDefaultPair)
    assert plan.recipe.schema_version == 3
    assert [(item.call_path, item.role) for item in plan.constants] == [
        (("nested", "first"), "offset"),
        (("nested", "second"), "offset"),
        (("last",), "offset"),
    ]
    assert [item.artifacts[0].values["value"] for item in plan.constants] == [
        10,
        20,
        10,
    ]
    declared = DefaultOffset.inputs["offset"].default
    assert declared is not None
    assert plan.constants[0].artifacts[0] is not declared[0]
    assert (
        ExecutionRecipe.model_validate_json(plan.recipe.model_dump_json())
        == plan.recipe
    )


def test_omitted_nested_composite_input_uses_its_own_fixed_default() -> None:
    class DefaultSourcePair(DefaultPair):
        name = "fixed_default_source_pair"
        inputs: ClassVar = {
            "source": InputSpec(
                artifact_type="metric", materialize=False, default=(metric(5),)
            )
        }

    class Wrapper(DefaultPair):
        name = "fixed_default_source_wrapper"

        def compose(self, ctx: CompositeContext) -> None:
            child = ctx.call(DefaultSourcePair, name="nested")
            ctx.output("metrics", child.output("metrics"))

    plan = compile_definition(Wrapper)
    assert [(item.call_path, item.role) for item in plan.constants] == [
        (("nested",), "source"),
        (("nested", "first"), "offset"),
        (("nested", "second"), "offset"),
    ]


def test_same_named_parent_input_does_not_implicitly_wire_children() -> None:
    class NamedParent(DefaultPair):
        name = "fixed_same_named_parent"

        class InputRole(StrEnum):
            source = "source"
            offset = "offset"

        inputs: ClassVar = {
            **DefaultPair.inputs,
            "offset": InputSpec(artifact_type="metric", materialize=False),
        }

    plan = compile_definition(NamedParent)
    assert [item.artifacts[0].values["value"] for item in plan.constants] == [10, 20]


@pytest.mark.parametrize("explicit_empty", [False, True])
def test_explicit_child_input_suppresses_default(explicit_empty: bool) -> None:
    class Wired(DefaultPair):
        name = "fixed_explicitly_wired"

        def compose(self, ctx: CompositeContext) -> None:
            child = ctx.call(
                DefaultOffset,
                inputs={
                    "source": ctx.input("source"),
                    "offset": [] if explicit_empty else ctx.input("source"),
                },
            )
            ctx.output("metrics", child.output("metrics"))

    plan = compile_definition(Wired)
    assert plan.constants == ()
    assert plan.recipe.schema_version == 2
    assert all(item.constants is None for item in plan.recipe.calls[0].inputs)


def test_empty_declared_default_remains_captured_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        DefaultOffset,
        "inputs",
        {
            **DefaultOffset.inputs,
            "offset": InputSpec(artifact_type="metric", materialize=False, default=()),
        },
    )
    plan = compile_definition(DefaultPair)
    assert plan.constants[0].artifacts == ()
    assert plan.recipe.calls[0].inputs[1].constants == ()
    assert plan.recipe.schema_version == 3


def test_captured_defaults_survive_changed_and_removed_declarations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = compile_definition(NestedDefaultPair)
    monkeypatch.setattr(DefaultOffset, "inputs", SharedOffset.inputs)
    monkeypatch.setattr(
        OtherDefaultOffset,
        "inputs",
        {
            **SharedOffset.inputs,
            "offset": InputSpec(
                artifact_type="metric", materialize=False, default=(metric(99),)
            ),
        },
    )

    def forbidden_selection(self: InputSpec) -> None:
        pytest.fail("Captured compilation consulted current artifact defaults")

    monkeypatch.setattr(InputSpec, "snapshot_default", forbidden_selection)
    restored = compile_captured_definition(NestedDefaultPair, original.recipe)
    assert restored.recipe == original.recipe
    assert restored.constants == ()
    assert restored.instantiate()


def test_new_compilation_selects_new_default_but_root_contract_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = compile_definition(DefaultOffset)
    before = compile_definition(DefaultPair)
    monkeypatch.setattr(
        DefaultOffset,
        "inputs",
        {
            **DefaultOffset.inputs,
            "offset": InputSpec(
                artifact_type="metric", materialize=False, default=(metric(30),)
            ),
        },
    )
    after = compile_definition(DefaultPair)
    assert after.recipe.digest != before.recipe.digest
    assert after.constants[0].artifacts[0].values["value"] == 30
    assert compile_definition(DefaultOffset).recipe == root.recipe
    assert all("default" not in item for item in root.recipe.model_dump()["inputs"])


def test_captured_optional_omission_does_not_adopt_a_new_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = compile_definition(DefaultPair)
    monkeypatch.setattr(
        DefaultOffset,
        "inputs",
        {
            **DefaultOffset.inputs,
            "context": InputSpec(
                artifact_type="metric",
                materialize=False,
                required=False,
                default=(metric(42),),
            ),
        },
    )
    assert (
        compile_captured_definition(DefaultPair, original.recipe).recipe
        == original.recipe
    )
    assert compile_definition(DefaultPair).recipe != original.recipe


def test_constants_cannot_be_reinterpreted_as_older_recipe_or_wrong_type() -> None:
    original = compile_definition(DefaultPair).recipe
    payload = original.model_dump(mode="json")
    payload["schema_version"] = 2
    with pytest.raises(ValidationError, match="schema version 3"):
        ExecutionRecipe.model_validate(payload)
    payload = original.model_dump(mode="json")
    payload["calls"][0]["inputs"][1]["constants"][0]["artifact_type"] = "csv"
    with pytest.raises(ValidationError, match="incompatible artifact type"):
        ExecutionRecipe.model_validate(payload)


def test_captured_compilation_still_detects_authored_wiring_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = compile_definition(DefaultPair)

    def changed(self: DefaultPair, ctx: CompositeContext) -> None:
        child = ctx.call(
            DefaultOffset,
            name="first",
            inputs={"source": ctx.input("source"), "offset": ctx.input("source")},
        )
        ctx.output("metrics", child.output("metrics"))

    monkeypatch.setattr(DefaultPair, "compose", changed)
    restored = compile_captured_definition(DefaultPair, original.recipe)
    assert restored.recipe != original.recipe
