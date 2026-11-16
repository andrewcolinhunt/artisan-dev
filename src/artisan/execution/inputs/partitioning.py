"""Pair and partition ordered local inputs without losing occurrence identity."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from itertools import batched, product

from artisan.schemas.composites.plan import InvocationPolicy
from artisan.schemas.enums import GroupByStrategy


def partition_local_inputs[T](
    inputs: Mapping[str, Sequence[T]],
    policy: InvocationPolicy,
) -> Iterator[dict[str, tuple[T, ...]]]:
    """Yield child lifecycle partitions in declared pairing and input order.

    No supplied streams means one generative invocation. Supplied all-empty
    streams mean no invocation. Independent whole-stream inputs remain complete,
    including any explicitly empty role. Command splitting is a separate policy.
    """
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
