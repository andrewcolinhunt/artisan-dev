"""Versioned recipes preserve explicit parent and shared input contracts."""

from __future__ import annotations

import json
from typing import ClassVar

import pytest
from fixtures.input_contract_operations import NestedSharedOffset, SharedOffset
from pydantic import ValidationError

from artisan.composites.base.compiler import (
    compile_captured_definition,
    compile_definition,
)
from artisan.composites.base.root_selection import RootSelection
from artisan.execution.recording.replay_snapshot import operation_behavior
from artisan.operations.examples import DataGenerator
from artisan.schemas import InputSpec
from artisan.schemas.composites.plan import ExecutionRecipe, InvocationPolicy
from artisan.schemas.enums import GroupByStrategy
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


def test_legacy_recipe_and_behavior_keep_original_canonical_shape() -> None:
    recipe = compile_definition(DataGenerator).recipe
    payload = recipe.model_dump(mode="json")
    assert recipe.schema_version == 1
    assert set(payload["policy"]) == {
        "group_by",
        "independent_input_streams",
        "per_artifact_dispatch",
        "artifacts_per_unit",
        "max_artifacts_per_unit",
    }
    for output in payload["outputs"]:
        assert set(output) == {
            "role",
            "artifact_type",
            "required",
            "parent_kind",
            "parent_roles",
        }
    canonical = canonical_json_bytes(payload)
    assert (
        ExecutionRecipe.model_validate_json(canonical).model_dump(mode="json")
        == payload
    )
    assert recipe.digest == compute_content_digest(b"execution-recipe-v1\0" + canonical)
    behavior = operation_behavior(DataGenerator())
    assert "shared_input_roles" not in behavior
    assert all(
        set(spec) == {"artifact_type", "description", "required", "derives_from"}
        for spec in behavior["outputs"].values()
    )


@pytest.mark.parametrize("definition", [SharedOffset, NestedSharedOffset])
def test_extended_recipe_json_and_registered_code_round_trip(definition: type) -> None:
    plan = compile_definition(definition)
    assert plan.recipe.schema_version == 2
    assert (
        ExecutionRecipe.model_validate_json(plan.recipe.model_dump_json())
        == plan.recipe
    )
    assert compile_captured_definition(definition, plan.recipe).recipe == plan.recipe
    assert plan.instantiate()
    payload = json.loads(plan.recipe.model_dump_json())
    payload["schema_version"] = 1
    with pytest.raises(ValidationError, match="schema version 2"):
        ExecutionRecipe.model_validate(payload)
    if plan.calls:
        assert [call.recipe.schema_version for call in plan.recipe.calls] == [1, 2]


def test_root_overrides_preserve_declared_sharing() -> None:
    plan = compile_definition(
        SharedOffset,
        group_by=GroupByStrategy.CROSS_PRODUCT,
        batch_strategy={"artifacts_per_unit": 7},
    )
    assert plan.recipe.policy.shared_input_roles == ("offset",)
    assert plan.recipe.policy.artifacts_per_unit == 7
    with pytest.raises(ValueError, match="declared invocation behavior"):
        RootSelection(InvocationPolicy()).snapshot(SharedOffset())
    assert compile_captured_definition(SharedOffset, plan.recipe).recipe == plan.recipe


def test_captured_policy_does_not_override_current_sharing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = compile_definition(SharedOffset).recipe
    monkeypatch.setattr(SharedOffset, "shared_input_roles", ())
    rebuilt = compile_captured_definition(SharedOffset, original).recipe
    assert rebuilt.policy.shared_input_roles == ()
    assert rebuilt.digest != original.digest


@pytest.mark.parametrize(
    "roles",
    [("offset", "offset"), ("missing",), ("context",), ("source", "offset", "context")],
)
def test_shared_declaration_requires_unique_required_roles(
    roles: tuple[str, ...],
) -> None:
    class InvalidSharing(SharedOffset):
        name = "invalid_sharing_contract"
        shared_input_roles = roles

    with pytest.raises(ValueError, match="Shared"):
        compile_definition(InvalidSharing)


def test_all_shared_roles_are_not_a_generative_operation() -> None:
    class AllShared(SharedOffset):
        name = "all_shared_contract"
        shared_input_roles = ("source", "offset", "context")
        inputs: ClassVar = {
            role: InputSpec(artifact_type="metric", materialize=False)
            for role in SharedOffset.inputs
        }

    with pytest.raises(ValueError, match="nonshared"):
        compile_definition(AllShared)


def test_independent_streams_cannot_hide_shared_cardinality() -> None:
    with pytest.raises(ValueError, match="independent"):
        InvocationPolicy(shared_input_roles=("offset",), independent_input_streams=True)


def test_parent_policy_alone_changes_recipe_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = compile_definition(SharedOffset).recipe
    monkeypatch.setattr(
        SharedOffset,
        "outputs",
        {
            role: spec.model_copy(update={"optional_parent_roles": ()})
            for role, spec in SharedOffset.outputs.items()
        },
    )
    changed = compile_definition(SharedOffset).recipe
    assert changed.digest != original.digest


def test_historical_v1_recipe_bytes_and_digest_remain_unchanged() -> None:
    from pathlib import Path

    encoded = (
        Path(__file__).resolve().parents[2] / "fixtures/recipes/generator-v1.json"
    ).read_bytes()
    recipe = ExecutionRecipe.model_validate_json(encoded)
    assert canonical_json_bytes(recipe.model_dump(mode="json")) == encoded
    assert recipe.digest == "043ed230f6f83f7082a628e525316b7b"
    assert compile_captured_definition(DataGenerator, recipe).recipe == recipe


def test_historical_operation_behavior_keeps_exact_bytes() -> None:
    from pathlib import Path

    encoded = (
        Path(__file__).resolve().parents[2]
        / "fixtures/recipes/generator-behavior-v1.json"
    ).read_bytes()
    assert canonical_json_bytes(operation_behavior(DataGenerator())) == encoded
