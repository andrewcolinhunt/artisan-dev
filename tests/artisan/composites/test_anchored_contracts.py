"""Anchored shape is declared while bounded group counts remain selectable."""

from __future__ import annotations

import pytest
from fixtures.input_contract_operations import (
    AnchoredOffset,
    NestedAnchoredOffset,
    SharedOffset,
)

from artisan.composites.base.compiler import (
    compile_captured_definition,
    compile_definition,
)
from artisan.schemas import AnchoredNameGrouping, GroupByStrategy
from artisan.schemas.composites.plan import ExecutionRecipe, InvocationPolicy


def test_anchored_v2_json_compile_and_nested_round_trip() -> None:
    for definition in (AnchoredOffset, NestedAnchoredOffset):
        plan = compile_definition(definition)
        assert plan.recipe.schema_version == 2
        assert (
            ExecutionRecipe.model_validate_json(plan.recipe.model_dump_json())
            == plan.recipe
        )
        assert (
            compile_captured_definition(definition, plan.recipe).recipe == plan.recipe
        )
        assert plan.instantiate()
    disabled = compile_definition(AnchoredOffset, params={"require_context": False})
    assert disabled.recipe.policy.required_nonempty_inputs == ()
    assert (
        compile_captured_definition(AnchoredOffset, disabled.recipe).recipe
        == disabled.recipe
    )


@pytest.mark.parametrize(
    "grouping",
    [
        GroupByStrategy.ZIP,
        GroupByStrategy.CROSS_PRODUCT,
        AnchoredNameGrouping(primary_role="context", matched_roles=("source",)),
    ],
)
def test_caller_cannot_replace_anchored_shape(grouping: object) -> None:
    with pytest.raises(ValueError, match="declared anchored"):
        compile_definition(AnchoredOffset, group_by=grouping)


@pytest.mark.parametrize(
    "patch",
    [
        {"max_artifacts_per_unit": None},
        {"max_artifacts_per_unit": 10},
        {"artifacts_per_unit": 6},
        {"artifacts_per_unit": 0},
    ],
)
def test_caller_cannot_remove_or_exceed_declared_bound(patch: dict) -> None:
    with pytest.raises(ValueError):
        compile_definition(AnchoredOffset, batch_strategy=patch)


def test_caller_can_reduce_positive_group_count_only() -> None:
    plan = compile_definition(AnchoredOffset, batch_strategy={"artifacts_per_unit": 2})
    assert plan.recipe.policy.artifacts_per_unit == 2
    assert plan.recipe.policy.required_nonempty_inputs == ("context",)
    assert (
        compile_captured_definition(AnchoredOffset, plan.recipe).recipe == plan.recipe
    )


def test_legacy_operation_cannot_gain_undeclared_anchor_by_override() -> None:
    with pytest.raises(ValueError, match="must be declared"):
        compile_definition(
            SharedOffset,
            group_by=AnchoredNameGrouping(
                primary_role="source", matched_roles=("context",)
            ),
        )


@pytest.mark.parametrize("roles", [("source", "source"), ("",)])
def test_nonempty_declaration_requires_valid_unique_names(
    roles: tuple[str, ...],
) -> None:
    with pytest.raises(ValueError, match="unique nonempty"):
        InvocationPolicy(required_nonempty_inputs=roles)


def test_nonempty_requirement_cannot_expand_to_legacy_pairing() -> None:
    with pytest.raises(ValueError, match="require anchored"):
        InvocationPolicy(required_nonempty_inputs=("source",))


def test_anchored_dispatch_cannot_split_primary_groups() -> None:
    with pytest.raises(ValueError, match="intact group"):
        InvocationPolicy(group_by=AnchoredNameGrouping(primary_role="source"))
