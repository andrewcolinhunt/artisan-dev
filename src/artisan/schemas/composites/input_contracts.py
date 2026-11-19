"""Validate singleton dependencies and already paired invocation membership."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from artisan.schemas.composites.plan import InvocationPolicy


def split_shared_inputs[T](
    inputs: Mapping[str, Sequence[T]], shared_roles: tuple[str, ...]
) -> tuple[dict[str, Sequence[T]], dict[str, tuple[T, ...]]]:
    """Validate selected singleton occurrences before pairing or empty-work checks."""
    shared = {}
    for role in shared_roles:
        values = inputs.get(role, ())
        if len(values) != 1:
            msg = f"Shared input role {role!r} requires exactly one occurrence"
            raise ValueError(msg)
        shared[role] = tuple(values)
    return {
        role: values for role, values in inputs.items() if role not in shared
    }, shared


def validate_partition_shape[T](
    inputs: Mapping[str, Sequence[T]],
    policy: InvocationPolicy,
) -> None:
    """Require admitted paired rows and separate singleton dependencies."""
    paired, shared = split_shared_inputs(inputs, policy.shared_input_roles)
    if shared and not any(paired.values()):
        msg = "Shared inputs alone contain no admitted work"
        raise ValueError(msg)
    if (
        not policy.independent_input_streams
        and len({len(values) for values in paired.values()}) > 1
    ):
        msg = "An admitted paired creator partition must have equal input lengths"
        raise ValueError(msg)


def partition_work_size[T](
    inputs: Mapping[str, Sequence[T]],
    policy: InvocationPolicy,
) -> int:
    """Count paired rows, excluding shared inputs."""
    paired, _ = split_shared_inputs(inputs, policy.shared_input_roles)
    return max((len(values) for values in paired.values()), default=1) or 1
