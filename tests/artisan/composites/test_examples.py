"""Installed tutorial compositions preserve their output and nesting contracts."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

import pytest

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import compile_definition
from artisan.composites.examples import GenerateAndScore, TransformAndScore
from artisan.execution.composition import ExecuteAdapter
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.executor import Executor
from artisan.execution.executor_runtime import ExecutorRuntime
from artisan.operations.base import OperationDefinition
from artisan.schemas import Artifact


def test_nested_example_retains_final_data_and_direct_metrics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths: list[tuple[str, ...]] = []

    def software(
        self: ExecutorRuntime,
        plan: CompiledPlan,
        inventory: Sequence[Artifact],
        root: Path,
    ) -> ExecuteAdapter:
        def adapter(
            operation: OperationDefinition, path: tuple[str, ...]
        ) -> LocalExecuteRouter:
            paths.append(path)
            return LocalExecuteRouter()

        return adapter

    # Exercise the native scientific kernel; worker software is qualified separately.
    monkeypatch.setattr(ExecutorRuntime, "_software", software)
    with Executor(working_root=tmp_path) as executor:
        result = executor.run(GenerateAndScore, params={"count": 2, "seed": 42})
        assert len(result.artifacts["dataset"]) == len(result.artifacts["metrics"]) == 2
        assert [edge.source_output_index for edge in result.lineage["metrics"]] == [
            0,
            1,
        ]
        assert all(
            artifact.is_finalized
            for values in result.artifacts.values()
            for artifact in values
        )
        assert set(result.artifacts) == {"dataset", "metrics"}
    assert paths == [
        ("generate",),
        ("transform_and_score", "transform"),
        ("transform_and_score", "transform"),
        ("transform_and_score", "score"),
    ]


def test_example_compilation_keeps_nested_parameters_and_exports() -> None:
    plan = compile_definition(
        TransformAndScore, params={"scale_factor": 3.0, "seed": 7}
    )
    assert [call.name for call in plan.calls] == ["transform", "score"]
    assert json.loads(plan.calls[0].plan.recipe.params_json) == {
        "scale_factor": 3.0,
        "noise_amplitude": 0.1,
        "output_prefix": "",
        "variants": 1,
        "seed": 7,
    }
    assert [export.role for export in plan.recipe.exports] == ["dataset", "metrics"]
    nested = compile_definition(GenerateAndScore, params={"count": 2})
    assert [call.name for call in nested.calls] == ["generate", "transform_and_score"]
