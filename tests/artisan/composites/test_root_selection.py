"""Root scientific choices remain frozen without changing nested defaults."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import pytest
from fixtures.composition_operations import (
    PredictMeasure,
    SummarizeMetrics,
    Transform,
    coordinate,
    structure,
)
from fixtures.retained_acceptance import open_committer
from pydantic import ValidationError

from artisan.composites.base.compiler import (
    compile_captured_definition,
    compile_definition,
)
from artisan.composites.base.root_selection import merge_batch_strategy
from artisan.execution.composition import execute_compiled_plan
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.executor import Executor
from artisan.operations.examples.csv_head import CsvHead
from artisan.operations.examples.wait_tool import WaitTool
from artisan.orchestration.engine.retained_inputs import prepare_step_inputs
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_spec import InputSpec
from artisan.storage.core.retained_reader import RetainedReader


class CappedTransform(Transform):
    name = "root_selection_capped_transform"
    batch_strategy: BatchStrategy = BatchStrategy(
        artifacts_per_unit=8, max_artifacts_per_unit=2, units_per_worker=3
    )
    per_artifact_dispatch = False


class PairedTransform(CappedTransform):
    name = "root_selection_paired_transform"

    class InputRole(StrEnum):
        structure = "structure"
        other = "other"

    inputs: ClassVar = {
        "structure": InputSpec(artifact_type="csv", materialize=False),
        "other": InputSpec(artifact_type="csv", materialize=False),
    }


class PrefixedWait(WaitTool):
    name = "root_selection_prefixed_wait"
    tool: ToolSpec = ToolSpec(
        executable="bash", interpreter="env", subcommand="original-mode"
    )


def test_scientific_patch_keeps_explicit_null_and_detaches_caller_values() -> None:
    batch: dict[str, object] = {
        "artifacts_per_unit": 5,
        "max_artifacts_per_unit": None,
        "units_per_worker": 30,
    }
    plan = compile_definition(
        CappedTransform, group_by=GroupByStrategy.CROSS_PRODUCT, batch_strategy=batch
    )
    batch["artifacts_per_unit"] = 99
    operation = plan.instantiate()
    assert operation.group_by == GroupByStrategy.CROSS_PRODUCT
    assert operation.batch_strategy.artifacts_per_unit == 5
    assert operation.batch_strategy.max_artifacts_per_unit is None
    assert operation.batch_strategy.units_per_worker == 3
    operation.batch_strategy.artifacts_per_unit = 20
    assert plan.instantiate().batch_strategy.artifacts_per_unit == 5
    assert (
        compile_captured_definition(CappedTransform, plan.recipe).recipe == plan.recipe
    )


def test_typed_batch_patch_preserves_only_supplied_fields() -> None:
    patch = BatchStrategy(max_artifacts_per_unit=None, units_per_worker=9)
    defaults = CappedTransform().batch_strategy
    merged = merge_batch_strategy(defaults, patch)
    assert (merged.artifacts_per_unit, merged.max_artifacts_per_unit) == (8, None)
    assert merged.units_per_worker == 9
    plan = compile_definition(CappedTransform, batch_strategy=patch)
    patch.max_artifacts_per_unit = 1
    assert plan.recipe.policy.artifacts_per_unit == 8
    assert plan.recipe.policy.max_artifacts_per_unit is None
    assert defaults.max_artifacts_per_unit == 2


def test_transport_only_patch_does_not_change_recipe_or_worker_defaults() -> None:
    original = compile_definition(CappedTransform)
    selected = compile_definition(
        CappedTransform,
        batch_strategy=BatchStrategy(
            units_per_worker=15, max_workers=4, estimated_seconds=2, job_name="job"
        ),
    )
    assert selected.recipe == original.recipe
    assert selected.root_selection.policy is None
    assert selected.instantiate().batch_strategy == CappedTransform().batch_strategy


def test_composite_root_policy_never_changes_child_recipes() -> None:
    original = compile_definition(PredictMeasure)
    selected = compile_definition(
        PredictMeasure,
        group_by=GroupByStrategy.NAME,
        batch_strategy={"artifacts_per_unit": 7},
    )
    assert selected.recipe.policy.group_by == GroupByStrategy.NAME
    assert selected.recipe.policy.artifacts_per_unit == 7
    assert selected.recipe.calls == original.recipe.calls
    assert all(call.plan.root_selection.policy is None for call in selected.calls)
    selected.instantiate()
    assert (
        compile_captured_definition(PredictMeasure, selected.recipe).recipe
        == selected.recipe
    )


@pytest.mark.parametrize(
    "patch",
    [
        {"artifacts_per_unit": 1},
        {"max_artifacts_per_unit": None},
        BatchStrategy(artifacts_per_unit=1),
    ],
)
def test_whole_stream_rejects_explicit_scientific_batch_intent(
    patch: dict[str, object] | BatchStrategy,
) -> None:
    with pytest.raises(ValueError, match="Whole-stream"):
        compile_definition(SummarizeMetrics, batch_strategy=patch)


def test_whole_stream_accepts_transport_only_and_rejects_pairing() -> None:
    original = compile_definition(SummarizeMetrics)
    assert (
        compile_definition(
            SummarizeMetrics, batch_strategy={"units_per_worker": 5}
        ).recipe
        == original.recipe
    )
    with pytest.raises(ValueError, match="Whole-stream"):
        compile_definition(SummarizeMetrics, group_by=GroupByStrategy.ZIP)


def test_command_patch_is_used_by_actual_command_builder_and_replay() -> None:
    patch = ToolSpec(executable="alternate-bash", interpreter="env", subcommand=None)
    plan = compile_definition(PrefixedWait, tool=patch)
    patch.executable = "changed-later"
    operation = plan.instantiate()
    assert operation.execute_command({"dataset": "input.csv", "input_index": 0})[
        :3
    ] == [
        "env",
        "alternate-bash",
        "-c",
    ]
    restored = compile_captured_definition(PrefixedWait, plan.recipe)
    assert restored.recipe == plan.recipe
    assert restored.instantiate().tool.parts() == ["env", "alternate-bash"]
    cleared = compile_definition(
        PrefixedWait, tool={"interpreter": None, "subcommand": None}
    )
    assert cleared.instantiate().tool.parts() == ["bash"]


@pytest.mark.parametrize("definition", [Transform, PredictMeasure, CsvHead])
def test_tool_override_cannot_broadcast_or_change_execution_mode(
    definition: type[Transform] | type[PredictMeasure] | type[CsvHead],
) -> None:
    with pytest.raises(ValueError, match="ordinary command root"):
        compile_definition(definition, tool={"executable": "bash"})


def test_unknown_patches_and_untyped_grouping_fail_before_compilation() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        compile_definition(CappedTransform, batch_strategy={"wrong_field": 1})
    with pytest.raises(ValidationError, match="Extra inputs"):
        compile_definition(WaitTool, tool={"wrong_field": 1})
    with pytest.raises(TypeError, match="GroupByStrategy"):
        compile_definition(CappedTransform, group_by="zip")  # type: ignore[arg-type]


def test_root_selection_still_rejects_live_immutable_policy_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = compile_definition(CappedTransform, batch_strategy={"artifacts_per_unit": 5})
    monkeypatch.setattr(CappedTransform, "per_artifact_dispatch", True)
    with pytest.raises(ValueError, match="declared invocation"):
        plan.instantiate()
    rebuilt = compile_captured_definition(CappedTransform, plan.recipe)
    assert rebuilt.recipe != plan.recipe
    assert rebuilt.recipe.policy.per_artifact_dispatch


def test_admitted_root_override_executes_one_partition(tmp_path: Path) -> None:
    plan = compile_definition(CappedTransform, batch_strategy={"artifacts_per_unit": 4})
    CappedTransform.phase_sizes.clear()
    with Executor(working_root=tmp_path / "session") as executor:
        result = execute_compiled_plan(
            plan,
            {"structure": [structure(index) for index in range(4)]},
            executor,
            invocation_id="root",
            execute_adapter=lambda *_: LocalExecuteRouter(),
        )
    assert CappedTransform.phase_sizes == [
        (phase, 4) for phase in ("preprocess", "execute", "postprocess")
    ]
    assert len(result.result.artifacts["structure"]) == 4


def test_root_cross_product_partitions_once_in_admission_order(tmp_path: Path) -> None:
    storage = StorageConfig()
    reader = RetainedReader(
        open_committer(
            str(tmp_path / "store"), storage.filesystem(), storage, initialize=True
        )
    )
    plan = compile_definition(
        PairedTransform,
        group_by=GroupByStrategy.CROSS_PRODUCT,
        batch_strategy={"artifacts_per_unit": 2, "max_artifacts_per_unit": None},
    )
    units = prepare_step_inputs(
        plan.recipe,
        {
            "structure": [structure(0), structure(1)],
            "other": [structure(i) for i in (10, 11, 12)],
        },
        step_run_id="a" * 32,
        reader=reader,
    )
    observed = []
    PairedTransform.phase_sizes.clear()
    with Executor(working_root=tmp_path / "session") as executor:
        for unit in units:
            inputs = {
                role: [unit.artifacts[address] for address in addresses]
                for role, addresses in unit.boundary.input_roles().items()
            }
            observed.extend(
                zip(
                    map(coordinate, inputs["structure"]),
                    map(coordinate, inputs["other"]),
                    strict=True,
                )
            )
            execute_compiled_plan(
                plan,
                inputs,
                executor,
                invocation_id=unit.assignment.assignment_id,
                execute_adapter=lambda *_: LocalExecuteRouter(),
                input_attribution=unit.boundary.input_attribution,
                expected_input_context_digest=unit.boundary.input_context_digest,
            )
    assert len(units) == 3
    assert observed == [(left, right) for left in (0, 1) for right in (10, 11, 12)]
    assert PairedTransform.phase_sizes == [
        (phase, 2)
        for _ in range(3)
        for phase in ("preprocess", "execute", "postprocess")
    ]


def test_root_composite_batch_does_not_replace_child_lifecycle_size(
    tmp_path: Path,
) -> None:
    plan = compile_definition(PredictMeasure, batch_strategy={"artifacts_per_unit": 3})
    Transform.phase_sizes.clear()
    with Executor(working_root=tmp_path / "session") as executor:
        execute_compiled_plan(
            plan,
            {"structure": [structure(i) for i in range(3)]},
            executor,
            invocation_id="composite",
            execute_adapter=lambda *_: LocalExecuteRouter(),
        )
    assert Transform.phase_sizes == [
        (phase, 1)
        for _ in range(3)
        for phase in ("preprocess", "execute", "postprocess")
    ]
