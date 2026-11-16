"""Local scientific partitioning preserves ordered occurrence membership."""

from __future__ import annotations

import pytest

from artisan.execution.inputs.partitioning import partition_local_inputs
from artisan.schemas.composites.plan import InvocationPolicy
from artisan.schemas.enums import GroupByStrategy


def test_pairing_caps_lifecycle_size_independently_of_command_splitting() -> None:
    inputs = {"a": ("a0", "a1", "a2"), "b": ("b0", "b1", "b2")}
    expected = [{"a": ("a0", "a1"), "b": ("b0", "b1")}, {"a": ("a2",), "b": ("b2",)}]
    for per_artifact in (False, True):
        policy = InvocationPolicy(
            artifacts_per_unit=8,
            max_artifacts_per_unit=2,
            per_artifact_dispatch=per_artifact,
        )
        assert list(partition_local_inputs(inputs, policy)) == expected


def test_cross_product_preserves_written_role_order_and_duplicate_occurrences() -> None:
    policy = InvocationPolicy(
        group_by=GroupByStrategy.CROSS_PRODUCT, artifacts_per_unit=3
    )
    assert list(partition_local_inputs({"b": ["B", "B"], "a": ["A", "C"]}, policy)) == [
        {"b": ("B", "B", "B"), "a": ("A", "C", "A")},
        {"b": ("B",), "a": ("C",)},
    ]
    assert list(partition_local_inputs({"b": [], "a": ["A"]}, policy)) == []


@pytest.mark.parametrize("strategy", [None, GroupByStrategy.ZIP])
def test_unequal_paired_streams_fail_before_yielding(
    strategy: GroupByStrategy | None,
) -> None:
    partitions = partition_local_inputs(
        {"a": [1, 2], "b": [3]}, InvocationPolicy(group_by=strategy)
    )
    with pytest.raises(ValueError, match="equal lengths"):
        next(partitions)


def test_whole_stream_reducer_keeps_unequal_and_empty_roles() -> None:
    policy = InvocationPolicy(
        independent_input_streams=True, per_artifact_dispatch=False
    )
    inputs = {"final": [1], "history": [2, 3, 4], "optional": []}
    assert list(partition_local_inputs(inputs, policy)) == [
        {"final": (1,), "history": (2, 3, 4), "optional": ()}
    ]


def test_generative_absent_and_supplied_empty_are_distinct() -> None:
    assert list(partition_local_inputs({}, InvocationPolicy())) == [{}]
    assert list(partition_local_inputs({"source": []}, InvocationPolicy())) == []
