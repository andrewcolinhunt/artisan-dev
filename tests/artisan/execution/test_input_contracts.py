"""Shared singleton inputs and optional parents retain exact invocation semantics."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.input_contract_operations import (
    NestedSharedOffset,
    SharedOffset,
    SplitSharedOffset,
    metric,
)

from artisan.composites.base.compiler import compile_definition
from artisan.execution.composition import execute_local_plan, validate_local_inputs
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.executor import Executor
from artisan.execution.inputs.partitioning import partition_local_inputs
from artisan.schemas.composites.plan import InvocationPolicy
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.provenance.projection import FrozenBoundaryContext


@pytest.mark.parametrize(
    "strategy", [None, GroupByStrategy.ZIP, GroupByStrategy.CROSS_PRODUCT]
)
def test_shared_selection_preserves_biological_partition_order(
    strategy: GroupByStrategy | None,
) -> None:
    original = {"a": ("a0", "a1", "a0"), "b": ("b0", "b1", "b2")}
    base = InvocationPolicy(group_by=strategy, artifacts_per_unit=2)
    expected = list(partition_local_inputs(original, base))
    shared = base.model_copy(update={"shared_input_roles": ("offset",)})
    actual = list(partition_local_inputs({"offset": ("one",), **original}, shared))
    assert actual == [{"offset": ("one",), **unit} for unit in expected]


@pytest.mark.parametrize("selected", [None, (), ("same", "same"), ("one", "two")])
@pytest.mark.parametrize("source", [(), ("work",)])
def test_shared_cardinality_is_checked_before_zero_work(
    selected: tuple[str, ...] | None, source: tuple[str, ...]
) -> None:
    inputs = {"source": source}
    if selected is not None:
        inputs["offset"] = selected
    policy = InvocationPolicy(shared_input_roles=("offset",))
    with pytest.raises(ValueError, match="exactly one occurrence"):
        list(partition_local_inputs(inputs, policy))


def test_shared_dependency_does_not_create_biological_work() -> None:
    policy = InvocationPolicy(shared_input_roles=("offset",))
    assert (
        list(partition_local_inputs({"source": (), "offset": ("one",)}, policy)) == []
    )


@pytest.mark.parametrize(
    "operation", [SharedOffset, SplitSharedOffset, NestedSharedOffset]
)
def test_local_execution_preserves_singleton_and_optional_exact_edges(
    tmp_path: Path, operation: type
) -> None:
    inputs = {"source": [metric(value) for value in range(3)]}
    if operation is not NestedSharedOffset:
        inputs = {
            "offset": [metric(10)],
            **inputs,
            "context": [metric(20 + value) for value in range(3)],
        }
    plan = compile_definition(operation)
    with Executor(working_root=tmp_path) as executor:
        executed = execute_local_plan(
            plan,
            inputs,
            executor.invoke,
            invocation_id="local",
            execute_adapter=lambda _op, _path: LocalExecuteRouter(),
            context_artifacts={},
            frozen_context=FrozenBoundaryContext(),
        )
    assert [
        artifact.values["value"] for artifact in executed.result.artifacts["metrics"]
    ] == [10, 11, 12]
    projection = executed.projection
    outputs = projection.export_roles()["metrics"]
    for output in outputs:
        parents = [edge.source for edge in projection.parents if edge.target == output]
        assert "source" in {parent.role for parent in parents}
        if operation is not NestedSharedOffset:
            assert [parent.index for parent in parents if parent.role == "offset"] == [
                0
            ]
    if operation is not NestedSharedOffset:
        assert [
            len(
                [
                    edge
                    for edge in projection.parents
                    if edge.target == output and edge.source.role == "context"
                ]
            )
            for output in outputs
        ] == [1, 0, 1]


def test_standalone_validation_rejects_bad_shared_selection_before_runtime() -> None:
    plan = compile_definition(SharedOffset)
    with pytest.raises(ValueError, match="exactly one occurrence"):
        validate_local_inputs(plan, {"source": [], "offset": [metric(1), metric(1)]})


def test_legacy_unit_counts_paired_rows_and_rejects_shared_only_work() -> None:
    from artisan.execution.models.execution_unit import ExecutionUnit

    unit = ExecutionUnit(
        operation=SharedOffset(),
        inputs={"offset": ["a" * 32], "source": ["b" * 32, "c" * 32]},
    )
    assert unit.get_batch_size() == 2
    with pytest.raises(ValueError, match="no admitted work"):
        ExecutionUnit(
            operation=SharedOffset(), inputs={"offset": ["a" * 32], "source": []}
        )


@pytest.mark.parametrize(("source_count", "shared_count"), [(2, 0), (2, 2), (0, 1)])
def test_admission_shape_rejects_invalid_shared_membership(
    source_count: int, shared_count: int
) -> None:
    from artisan.errors import StoreIntegrityError
    from artisan.execution.lineage.graph import OccurrenceGraph
    from artisan.storage.io.admission import _validate_input_contract

    graph = OccurrenceGraph()
    for role, count in (("source", source_count), ("offset", shared_count)):
        graph.declare_input_role(role)
        for _ in range(count):
            graph.add_input(role, metric(1))
    with pytest.raises(StoreIntegrityError):
        _validate_input_contract(
            compile_definition(SharedOffset).recipe, graph.project({}, {})
        )
