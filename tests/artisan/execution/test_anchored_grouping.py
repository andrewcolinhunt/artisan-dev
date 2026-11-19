"""Exact named groups retain all primary occurrences and sparse context."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.input_contract_operations import (
    AnchoredOffset,
    NestedAnchoredOffset,
    metric,
    named_metric,
)

from artisan.composites.base.compiler import compile_definition
from artisan.execution.composition import execute_local_plan, validate_local_inputs
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.executor import Executor
from artisan.execution.inputs.occurrence_grouping import (
    InputSelection,
    partition_occurrence_inputs,
)
from artisan.execution.inputs.partitioning import partition_local_inputs
from artisan.schemas import AnchoredNameGrouping
from artisan.schemas.composites.input_contracts import (
    partition_work_size,
    validate_partition_shape,
)
from artisan.schemas.composites.plan import InvocationPolicy
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import FrozenBoundaryContext


def policy(**updates: object) -> InvocationPolicy:
    return InvocationPolicy.model_validate(
        {
            "group_by": AnchoredNameGrouping(
                primary_role="source", matched_roles=("context",)
            ),
            "artifacts_per_unit": 5,
            "per_artifact_dispatch": False,
            "shared_input_roles": ("offset",),
            **updates,
        }
    )


def partition(inputs: dict, **updates: object) -> list[dict]:
    return list(
        partition_local_inputs(inputs, policy(**updates), name_for=lambda item: item)
    )


def test_six_queries_keep_interleaved_chains_and_sparse_supplied_context_order() -> (
    None
):
    inputs = {
        "offset": ("shared",),
        "source": ("A", "B", "C", "D", "E", "F", "A", "F"),
        "context": ("F", "B"),
    }
    units = partition(inputs, required_nonempty_inputs=("context",))
    assert units == [
        {
            "offset": ("shared",),
            "source": ("A", "A", "B", "C", "D", "E"),
            "context": ("B",),
        },
        {"offset": ("shared",), "source": ("F", "F"), "context": ("F",)},
    ]
    for expected, unit in zip((5, 1), units, strict=True):
        validate_partition_shape(unit, policy(), name_for=lambda item: item)
        assert (
            partition_work_size(unit, policy(), name_for=lambda item: item) == expected
        )


def test_sparse_context_preserves_explicit_empty_role() -> None:
    units = partition(
        {"source": tuple("ABCDEF"), "context": ("F",), "offset": ("W",)},
        required_nonempty_inputs=("context",),
    )
    assert units[0]["context"] == ()
    assert units[1]["context"] == ("F",)
    absent = partition({"source": tuple("ABCDEF"), "offset": ("W",)})
    assert all("context" not in unit for unit in absent)


@pytest.mark.parametrize("context", [("A", "A"), ("orphan",), ("F", "F")])
def test_invalid_context_fails_before_first_partition(context: tuple[str, ...]) -> None:
    stream = partition_local_inputs(
        {"source": tuple("ABCDEF"), "context": context, "offset": ("W",)},
        policy(),
        name_for=lambda item: item,
    )
    with pytest.raises(ValueError, match="duplicate or orphan"):
        next(stream)


@pytest.mark.parametrize("name", [None, "", 123])
def test_original_names_must_be_nonempty_strings(name: object) -> None:
    with pytest.raises(ValueError, match="nonempty string"):
        partition({"source": (name,), "offset": ("W",)})


def test_exact_names_preserve_extensions_and_case() -> None:
    units = partition(
        {
            "source": ("a", "A", "a.pdb", "a.cif"),
            "context": ("a.cif", "A"),
            "offset": ("W",),
        },
        artifacts_per_unit=2,
    )
    assert [unit["source"] for unit in units] == [("a", "A"), ("a.pdb", "a.cif")]
    assert [unit["context"] for unit in units] == [("A",), ("a.cif",)]


@pytest.mark.parametrize("context", [None, ()])
def test_full_selection_nonempty_requirement_precedes_empty_work(
    context: tuple | None,
) -> None:
    inputs = {"source": (), "offset": ("W",)}
    if context is not None:
        inputs["context"] = context
    with pytest.raises(ValueError, match="Complete selection"):
        partition(inputs, required_nonempty_inputs=("context",))
    assert partition(inputs) == []


def test_root_and_local_partition_membership_match_exact_occurrences() -> None:
    artifacts = {
        "source": [named_metric(name, index) for index, name in enumerate("ABCDEFA")],
        "context": [named_metric("F", 20)],
        "offset": [metric(30)],
    }
    selected = {
        role: tuple(
            InputSelection(
                artifact,
                OccurrenceAddress(invocation_id="supplied", role=role, index=index),
            )
            for index, artifact in enumerate(values)
        )
        for role, values in artifacts.items()
    }
    expected = list(
        partition_local_inputs(
            selected, policy(), name_for=lambda item: item.artifact.original_name
        )
    )
    actual = list(partition_occurrence_inputs(selected, policy()))
    assert actual == expected
    assert [item.key.index for item in actual[0]["source"]] == [0, 6, 1, 2, 3, 4]


@pytest.mark.parametrize("operation", [AnchoredOffset, NestedAnchoredOffset])
def test_local_and_nested_execution_keep_exact_sparse_parents(
    tmp_path: Path, operation: type
) -> None:
    inputs = {
        "source": [named_metric(name, index) for index, name in enumerate("ABCDEFA")]
    }
    if operation is AnchoredOffset:
        inputs.update({"offset": [metric(10)], "context": [named_metric("F", 20)]})
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
    ] == [10, 16, 11, 12, 13, 14, 15]
    if operation is AnchoredOffset:
        outputs = executed.projection.export_roles()["metrics"]
        assert [
            sum(
                edge.source.role == "context"
                for edge in executed.projection.parents
                if edge.target == output
            )
            for output in outputs
        ] == [0, 0, 0, 0, 0, 0, 1]


def test_local_complete_validation_precedes_cache_lookup() -> None:
    with pytest.raises(ValueError, match="Complete selection"):
        validate_local_inputs(
            compile_definition(AnchoredOffset),
            {"source": [named_metric("A", 0)], "offset": [metric(1)], "context": []},
        )
