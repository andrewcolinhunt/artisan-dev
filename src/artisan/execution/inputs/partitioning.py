"""Pair and partition ordered local inputs without losing occurrence identity."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from itertools import batched, product

from artisan.schemas.composites.anchored_inputs import partition_anchored_inputs
from artisan.schemas.composites.grouping import AnchoredNameGrouping
from artisan.schemas.composites.input_contracts import (
    split_shared_inputs,
    validate_nonempty_selection,
)
from artisan.schemas.composites.plan import InvocationPolicy
from artisan.schemas.enums import GroupByStrategy


def partition_local_inputs[T](
    inputs: Mapping[str, Sequence[T]],
    policy: InvocationPolicy,
    *,
    name_for: Callable[[T], object] | None = None,
) -> Iterator[dict[str, tuple[T, ...]]]:
    """Yield child lifecycle partitions in declared pairing and input order.

    No supplied streams means one generative invocation. Supplied all-empty
    streams mean no invocation. Independent whole-stream inputs remain complete,
    including any explicitly empty role. Command splitting is a separate policy.
    """
    validate_nonempty_selection(inputs, policy)
    paired, shared = split_shared_inputs(inputs, policy.shared_input_roles)
    if shared:
        if not any(paired.values()):
            return
        policy = policy.model_copy(
            update={"shared_input_roles": (), "required_nonempty_inputs": ()}
        )
        for shared_partition in partition_local_inputs(
            paired, policy, name_for=name_for
        ):
            yield {
                role: shared[role] if role in shared else shared_partition[role]
                for role in inputs
            }
        return
    if isinstance(policy.group_by, AnchoredNameGrouping):
        if name_for is None:
            msg = "Anchored grouping requires captured original names"
            raise ValueError(msg)
        yield from partition_anchored_inputs(inputs, policy, name_for)
        return
    if not inputs:
        yield {}
        return
    if not any(inputs.values()):
        return
    if policy.independent_input_streams:
        yield {role: tuple(values) for role, values in inputs.items()}
        return
    roles = tuple(inputs)
    streams = tuple(inputs.values())
    groups: Iterator[tuple[T, ...]]
    if policy.group_by == GroupByStrategy.CROSS_PRODUCT:
        groups = product(*streams)
    elif policy.group_by in (None, GroupByStrategy.ZIP):
        if len({len(values) for values in streams}) != 1:
            msg = "Paired input streams must have equal lengths"
            raise ValueError(msg)
        groups = zip(*streams, strict=True)
    else:
        msg = "Local input pairing supports only implicit pairing, ZIP or CROSS_PRODUCT"
        raise ValueError(msg)
    size = policy.artifacts_per_unit
    if policy.max_artifacts_per_unit is not None:
        size = min(size, policy.max_artifacts_per_unit)
    for partition in batched(groups, size):
        yield {
            role: tuple(group[index] for group in partition)
            for index, role in enumerate(roles)
        }
