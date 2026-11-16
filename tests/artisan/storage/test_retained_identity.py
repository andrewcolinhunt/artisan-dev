"""Fresh assignments share scientific keys without losing occurrence semantics."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.retained_results import retained_case

from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.execution.assignment import AssignmentAttempt, AssignmentIdentity
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import (
    AcceptedOccurrence,
    BoundaryInputAttribution,
    BoundaryProjection,
    FrozenAssociation,
    FrozenBoundaryContext,
    FrozenReferences,
    ProjectedOccurrence,
    RoleOccurrences,
)
from artisan.storage.cache.retained_identity import retained_cache_key


def _inputs(
    owner: str,
    *,
    order: tuple[str, ...] = ("b", "a", "b"),
    metric: str = "d",
    empty_capture: bool = True,
    producer: AcceptedOccurrence | None = None,
) -> BoundaryProjection:
    addresses = tuple(
        OccurrenceAddress(invocation_id=owner, role="data", index=index)
        for index in range(len(order))
    )
    associated = OccurrenceAddress(
        invocation_id=owner + "-context", role="metric", index=9
    )
    return BoundaryProjection(
        boundary_inputs=(RoleOccurrences(role="data", occurrences=addresses),),
        occurrences=(
            *(
                ProjectedOccurrence(
                    address=address, artifact_id=identity * 32, artifact_type="csv"
                )
                for address, identity in zip(addresses, order, strict=True)
            ),
            ProjectedOccurrence(
                address=associated, artifact_id=metric * 32, artifact_type="metric"
            ),
        ),
        input_attribution=tuple(
            BoundaryInputAttribution(
                boundary=address, producer=producer if address == addresses[0] else None
            )
            for address in (*addresses, associated)
        ),
        frozen_context=FrozenBoundaryContext(
            associations=(
                FrozenAssociation(
                    primary=addresses[0],
                    artifact_type="metric",
                    members=(associated, associated),
                ),
                *(
                    (
                        FrozenAssociation(
                            primary=addresses[1], artifact_type="metric", members=()
                        ),
                    )
                    if empty_capture
                    else ()
                ),
            )
        ),
        exports=(),
    )


def _attempt(recipe: ExecutionRecipe, inputs: BoundaryProjection) -> AssignmentAttempt:
    return AssignmentAttempt(
        assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=0),
        attempt=0,
        generation=0,
        recipe_digest=recipe.digest,
        software_digest="c" * 64,
        input_context_digest=inputs.input_context_digest,
    )


def test_fresh_consuming_addresses_share_a_key_but_exact_admission_differs(
    tmp_path: Path,
) -> None:
    recipe = retained_case(tmp_path, kind="alias").recipe
    first, second = _inputs("first"), _inputs("second")
    assert first.input_context_digest != second.input_context_digest
    attempt = _attempt(recipe, second).model_copy(
        update={
            "assignment": AssignmentIdentity(step_run_id="b" * 32, unit_index=7),
            "attempt": 3,
            "generation": 4,
        }
    )
    assert retained_cache_key(
        recipe, _attempt(recipe, first), first
    ) == retained_cache_key(recipe, attempt, second)


@pytest.mark.parametrize(
    "change",
    [
        "order",
        "duplicates",
        "context-content",
        "capture-empty",
        "capture-order",
        "context-duplicates",
    ],
)
def test_input_and_frozen_context_semantics_invalidate_keys(
    tmp_path: Path, change: str
) -> None:
    recipe = retained_case(tmp_path, kind="alias").recipe
    first = _inputs("first")
    changed = {
        "order": _inputs("second", order=("a", "b", "b")),
        "duplicates": _inputs("second", order=("b", "a")),
        "context-content": _inputs("second", metric="e"),
        "capture-empty": _inputs("second", empty_capture=False),
    }.get(change, _inputs("second"))
    captures = changed.frozen_context.associations
    if change == "capture-order":
        changed = changed.model_copy(
            update={
                "frozen_context": FrozenBoundaryContext(
                    associations=tuple(reversed(captures))
                )
            }
        )
    elif change == "context-duplicates":
        changed = changed.model_copy(
            update={
                "frozen_context": FrozenBoundaryContext(
                    associations=(
                        captures[0].model_copy(
                            update={"members": captures[0].members[:1]}
                        ),
                        captures[1],
                    )
                )
            }
        )
    assert retained_cache_key(
        recipe, _attempt(recipe, first), first
    ) != retained_cache_key(recipe, _attempt(recipe, changed), changed)


def test_selected_historical_producer_and_unscoped_content_remain_distinct(
    tmp_path: Path,
) -> None:
    recipe = retained_case(tmp_path, kind="alias").recipe
    selected = AcceptedOccurrence(
        delivery_id="e" * 32,
        seal_digest="f" * 32,
        address=OccurrenceAddress(invocation_id="producer", role="data", index=0),
    )
    inputs = [
        _inputs("first"),
        _inputs("second", producer=selected),
        _inputs(
            "third", producer=selected.model_copy(update={"delivery_id": "a" * 32})
        ),
        _inputs(
            "fourth",
            producer=selected.model_copy(
                update={"address": selected.address.model_copy(update={"index": 1})}
            ),
        ),
    ]
    assert (
        len(
            {
                retained_cache_key(recipe, _attempt(recipe, item), item)
                for item in inputs
            }
        )
        == 4
    )
    equivalent = _inputs("another-consumer", producer=selected)
    assert retained_cache_key(
        recipe, _attempt(recipe, inputs[1]), inputs[1]
    ) == retained_cache_key(recipe, _attempt(recipe, equivalent), equivalent)


def test_metadata_record_order_does_not_change_scientific_key(tmp_path: Path) -> None:
    recipe = retained_case(tmp_path, kind="alias").recipe
    inputs = _inputs("first")
    reordered = inputs.model_copy(
        update={
            "occurrences": tuple(reversed(inputs.occurrences)),
            "input_attribution": tuple(reversed(inputs.input_attribution)),
        }
    )
    assert retained_cache_key(
        recipe, _attempt(recipe, inputs), inputs
    ) == retained_cache_key(recipe, _attempt(recipe, reordered), reordered)


def test_software_recipe_and_admission_are_independently_bound(tmp_path: Path) -> None:
    recipe = retained_case(tmp_path, kind="alias").recipe
    inputs = _inputs("first")
    attempt = _attempt(recipe, inputs)
    original = retained_cache_key(recipe, attempt, inputs)
    assert (
        retained_cache_key(
            recipe, attempt.model_copy(update={"software_digest": "e" * 64}), inputs
        )
        != original
    )
    changed_recipe = recipe.model_copy(update={"params_json": '{"number":2}'})
    assert (
        retained_cache_key(changed_recipe, _attempt(changed_recipe, inputs), inputs)
        != original
    )
    with pytest.raises(ValueError, match="exact admitted"):
        retained_cache_key(changed_recipe, attempt, inputs)
    with pytest.raises(ValueError, match="exact admitted"):
        retained_cache_key(recipe, attempt, _inputs("other"))


def test_diagnostics_and_noncacheable_recipes_never_get_ordinary_keys(
    tmp_path: Path,
) -> None:
    recipe = retained_case(tmp_path, kind="alias").recipe
    inputs = _inputs("first")
    attempt = _attempt(recipe, inputs).model_copy(
        update={"replay_of_execution_run_id": "e" * 32}
    )
    assert retained_cache_key(recipe, attempt, inputs) is None
    noncacheable = recipe.model_copy(update={"cacheable": False})
    assert (
        retained_cache_key(noncacheable, _attempt(noncacheable, inputs), inputs) is None
    )
    observed = _attempt(recipe, inputs).model_copy(update={"software_cacheable": False})
    assert retained_cache_key(recipe, observed, inputs) is None


def test_configuration_reference_membership_is_normalized_and_ordered(
    tmp_path: Path,
) -> None:
    recipe = retained_case(tmp_path, kind="alias").recipe
    recipe = recipe.model_copy(
        update={
            "inputs": (
                *recipe.inputs,
                recipe.inputs[0].model_copy(
                    update={
                        "role": "config",
                        "artifact_type": "config",
                        "resolve_references": True,
                    }
                ),
            ),
        }
    )

    def with_config(owner: str, reverse: bool = False) -> BoundaryProjection:
        inputs = _inputs(owner)
        config = OccurrenceAddress(invocation_id=owner, role="config", index=0)
        members = inputs.boundary_inputs[0].occurrences[:2]
        return BoundaryProjection.model_validate(
            inputs.model_copy(
                update={
                    "boundary_inputs": (
                        *inputs.boundary_inputs,
                        RoleOccurrences(role="config", occurrences=(config,)),
                    ),
                    "occurrences": (
                        *inputs.occurrences,
                        ProjectedOccurrence(
                            address=config, artifact_id="f" * 32, artifact_type="config"
                        ),
                    ),
                    "input_attribution": (
                        *inputs.input_attribution,
                        BoundaryInputAttribution(boundary=config, producer=None),
                    ),
                    "frozen_context": FrozenBoundaryContext(
                        associations=inputs.frozen_context.associations,
                        references=(
                            FrozenReferences(
                                source=config,
                                members=tuple(reversed(members))
                                if reverse
                                else members,
                            ),
                        ),
                    ),
                }
            )
        )

    first, equivalent, changed = (
        with_config("first"),
        with_config("second"),
        with_config("third", reverse=True),
    )
    key = retained_cache_key(recipe, _attempt(recipe, first), first)
    assert retained_cache_key(recipe, _attempt(recipe, equivalent), equivalent) == key
    assert retained_cache_key(recipe, _attempt(recipe, changed), changed) != key
    capture = first.frozen_context.references[0]
    changed_parents = first.model_copy(
        update={
            "frozen_context": first.frozen_context.model_copy(
                update={
                    "references": (
                        capture.model_copy(update={"direct_parents": capture.members}),
                    ),
                }
            ),
        }
    )
    assert (
        retained_cache_key(recipe, _attempt(recipe, changed_parents), changed_parents)
        != key
    )


def test_an_explicit_empty_input_role_differs_from_an_omitted_role(
    tmp_path: Path,
) -> None:
    recipe = retained_case(tmp_path, kind="alias").recipe
    recipe = recipe.model_copy(
        update={
            "inputs": (
                *recipe.inputs,
                recipe.inputs[0].model_copy(
                    update={
                        "role": "optional",
                        "required": False,
                    }
                ),
            ),
        }
    )
    inputs = _inputs("first")
    empty = inputs.model_copy(
        update={
            "boundary_inputs": (
                *inputs.boundary_inputs,
                RoleOccurrences(role="optional", occurrences=()),
            ),
        }
    )
    assert retained_cache_key(
        recipe, _attempt(recipe, inputs), inputs
    ) != retained_cache_key(recipe, _attempt(recipe, empty), empty)
