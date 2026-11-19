"""Exact-name selection validation shared by admission and local composition."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from itertools import batched

from artisan.schemas.composites.grouping import AnchoredNameGrouping
from artisan.schemas.composites.plan import InvocationPolicy


def anchored_groups[T](
    inputs: Mapping[str, Sequence[T]],
    grouping: AnchoredNameGrouping,
    name_for: Callable[[T], object],
) -> dict[str, tuple[T, ...]]:
    """Validate all context before returning the ordered complete primary groups."""
    if grouping.primary_role not in inputs or inputs.keys() - {
        grouping.primary_role,
        *grouping.matched_roles,
    }:
        msg = "Anchored selection has missing primary or undeclared matched roles"
        raise ValueError(msg)
    groups: dict[str, list[T]] = {}
    for item in inputs[grouping.primary_role]:
        groups.setdefault(exact_name(item, name_for), []).append(item)
    for role in grouping.matched_roles:
        seen = set()
        for item in inputs.get(role, ()):
            name = exact_name(item, name_for)
            if name in seen or name not in groups:
                msg = f"Anchored matched role {role!r} has duplicate or orphan name {name!r}"
                raise ValueError(msg)
            seen.add(name)
    return {name: tuple(items) for name, items in groups.items()}


def partition_anchored_inputs[T](
    inputs: Mapping[str, Sequence[T]],
    policy: InvocationPolicy,
    name_for: Callable[[T], object],
) -> Iterator[dict[str, tuple[T, ...]]]:
    """Partition complete name groups without losing sparse or empty role membership."""
    grouping = policy.group_by
    assert isinstance(grouping, AnchoredNameGrouping)
    groups = anchored_groups(inputs, grouping, name_for)
    for names in batched(groups, unit_bound(policy)):
        selected = set(names)
        yield {
            role: tuple(item for name in names for item in groups[name])
            if role == grouping.primary_role
            else tuple(item for item in items if exact_name(item, name_for) in selected)
            for role, items in inputs.items()
        }


def exact_name[T](item: T, name_for: Callable[[T], object]) -> str:
    """Require an authored exact string; never infer a filename stem."""
    name = name_for(item)
    if not isinstance(name, str) or not name:
        msg = "Anchored grouping requires nonempty string original names"
        raise ValueError(msg)
    return name


def unit_bound(policy: InvocationPolicy) -> int:
    """Return the effective scientific work-item bound."""
    return min(
        policy.artifacts_per_unit,
        policy.max_artifacts_per_unit or policy.artifacts_per_unit,
    )
