"""Public direct calls capture ordinary default dependencies without extra calls."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.fixed_default_operations import DefaultOffset, NestedDefaultPair
from fixtures.input_contract_operations import metric

from artisan.execution.call import run_operation
from artisan.execution.executor import Executor
from artisan.schemas import InputSpec


@pytest.mark.parametrize("standalone", [True, False])
def test_public_calls_use_default_or_explicit_artifact(
    tmp_path: Path, standalone: bool
) -> None:
    with Executor(working_root=tmp_path / "executor") as executor:
        call = executor.run if standalone else run_operation
        omitted = call(DefaultOffset, inputs={"source": [metric(1), metric(2)]})
        explicit = call(
            DefaultOffset,
            inputs={"source": [metric(1), metric(2)], "offset": [metric(40)]},
        )
    assert [item.values["value"] for item in omitted.artifacts["metrics"]] == [11, 12]
    assert [item.values["value"] for item in explicit.artifacts["metrics"]] == [41, 42]
    assert omitted.support is not None
    assert "offset" in omitted.support.projection.input_roles()


@pytest.mark.parametrize("standalone", [True, False])
def test_nested_defaults_are_context_dependencies_with_exact_parents(
    tmp_path: Path, standalone: bool
) -> None:
    with Executor(working_root=tmp_path / "executor") as executor:
        call = executor.run if standalone else run_operation
        result = call(NestedDefaultPair, inputs={"source": [metric(1), metric(2)]})
    assert [item.values["value"] for item in result.artifacts["metrics"]] == [
        11,
        12,
        21,
        22,
        11,
        12,
    ]
    assert result.support is not None
    projection = result.support.projection
    assert list(projection.input_roles()) == ["source"]
    constants = projection.frozen_context.constants
    assert [(item.call_path, item.role) for item in constants] == [
        (("nested", "first"), "offset"),
        (("nested", "second"), "offset"),
        (("last",), "offset"),
    ]
    positions = {address for item in constants for address in item.occurrences}
    assert len(positions) == 3
    assert all(
        item.producer is None
        for item in projection.input_attribution
        if item.boundary in positions
    )
    expected_parents = [
        {source, binding.occurrences[0]}
        for binding in constants
        for source in projection.input_roles()["source"]
    ]
    actual_parents = [
        {edge.source for edge in projection.parents if edge.target == target}
        for target in projection.export_roles()["metrics"]
    ]
    assert actual_parents == expected_parents


def test_explicit_empty_never_falls_back_to_shared_default(tmp_path: Path) -> None:
    with (
        Executor(working_root=tmp_path) as executor,
        pytest.raises(ValueError, match="exactly one occurrence"),
    ):
        executor.run(DefaultOffset, inputs={"source": [metric(1)], "offset": []})


def test_explicit_input_does_not_inspect_an_unused_corrupt_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = InputSpec(artifact_type="metric", materialize=False, default=(metric(10),))
    assert spec.default is not None
    spec.default[0].metadata["changed"] = True
    monkeypatch.setattr(
        DefaultOffset, "inputs", {**DefaultOffset.inputs, "offset": spec}
    )
    result = run_operation(
        DefaultOffset, inputs={"source": [metric(1)], "offset": [metric(20)]}
    )
    assert result.artifacts["metrics"][0].values["value"] == 21
