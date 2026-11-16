"""Composite declarations configure outer submissions without changing nesting."""

from __future__ import annotations

import pytest
from fixtures.composition_operations import PredictMeasure

from artisan.composites import CompositeContext
from artisan.composites.base.compiler import (
    compile_captured_definition,
    compile_definition,
)
from artisan.execution.inputs.partitioning import partition_local_inputs
from artisan.orchestration.engine.pipeline_submission import freeze_submission
from artisan.schemas import BatchStrategy, GroupByStrategy, RunnerResources
from artisan.schemas.composites.plan import InvocationPolicy


class ConfiguredComposite(PredictMeasure):
    name = "defaults_configured_composite"
    runner_resources: RunnerResources = RunnerResources(
        cpus=4, memory_gb=16, gpus=1, extra={"constraints": ["accelerator"]}
    )
    batch_strategy: BatchStrategy = BatchStrategy(
        artifacts_per_unit=4, max_artifacts_per_unit=2, units_per_worker=3
    )
    group_by: GroupByStrategy | None = GroupByStrategy.ZIP


class NestedConfiguredComposite(PredictMeasure):
    name = "defaults_nested_configured_composite"

    def compose(self, ctx: CompositeContext) -> None:
        child = ctx.call(
            ConfiguredComposite,
            inputs={"structure": ctx.input("structure")},
        )
        ctx.output("structure", child.output("structure"))
        ctx.output("metrics", child.output("metrics"))


@pytest.mark.parametrize("typed", [False, True])
def test_submission_patches_preserve_composite_defaults_and_detach_values(
    typed: bool,
) -> None:
    queue = ["batch"]
    resources = (
        RunnerResources(memory_gb=32, extra={"queue": queue})
        if typed
        else {"memory_gb": 32, "extra": {"queue": queue}}
    )
    batch = (
        BatchStrategy(units_per_worker=2, max_artifacts_per_unit=None)
        if typed
        else {"units_per_worker": 2, "max_artifacts_per_unit": None}
    )
    submission = freeze_submission(
        ConfiguredComposite,
        runner_resources=resources,
        batch_strategy=batch,
    )
    queue.append("changed")
    assert submission.resources.cpus == 4
    assert submission.resources.memory_gb == 32
    assert submission.resources.gpus == 1
    assert submission.resources.extra == {"queue": ["batch"]}
    assert submission.batch_strategy.artifacts_per_unit == 4
    assert submission.batch_strategy.max_artifacts_per_unit is None
    assert submission.batch_strategy.units_per_worker == 2
    assert ConfiguredComposite().batch_strategy.max_artifacts_per_unit == 2
    assert ConfiguredComposite().runner_resources.memory_gb == 16


def test_root_defaults_determine_partitioning_and_survive_reconstruction() -> None:
    submission = freeze_submission(ConfiguredComposite)
    plan = submission.compile()
    assert plan.recipe.policy.group_by == GroupByStrategy.ZIP
    assert plan.instantiate().batch_strategy.max_artifacts_per_unit == 2
    partitions = list(
        partition_local_inputs({"structure": tuple(range(5))}, plan.recipe.policy)
    )
    assert partitions == [
        {"structure": (0, 1)},
        {"structure": (2, 3)},
        {"structure": (4,)},
    ]
    assert (
        compile_captured_definition(ConfiguredComposite, plan.recipe).recipe
        == plan.recipe
    )
    changed = compile_definition(
        ConfiguredComposite, batch_strategy={"artifacts_per_unit": 1}
    )
    assert changed.recipe.digest != plan.recipe.digest
    transport = freeze_submission(
        ConfiguredComposite,
        runner_resources={"cpus": 8},
        batch_strategy={"units_per_worker": 8},
    )
    assert transport.compile().recipe == plan.recipe


def test_nested_composite_ignores_outer_defaults_and_keeps_creator_policies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = compile_definition(NestedConfiguredComposite)
    child = original.calls[0].plan
    assert child.recipe.policy == InvocationPolicy()
    assert child.instantiate().group_by is None
    assert child.instantiate().batch_strategy.artifacts_per_unit == 1
    assert child.recipe.calls == compile_definition(PredictMeasure).recipe.calls
    assert (
        compile_captured_definition(child.definition, child.recipe).recipe
        == child.recipe
    )

    defaults = ConfiguredComposite.model_fields["batch_strategy"].default
    monkeypatch.setattr(defaults, "artifacts_per_unit", 99)
    monkeypatch.setattr(defaults, "units_per_worker", 20)
    resources = ConfiguredComposite.model_fields["runner_resources"].default
    monkeypatch.setattr(resources, "memory_gb", 64)
    assert compile_definition(NestedConfiguredComposite).recipe == original.recipe
    assert child.instantiate().batch_strategy.artifacts_per_unit == 1
    assert (
        compile_captured_definition(NestedConfiguredComposite, original.recipe).recipe
        == original.recipe
    )


def test_composite_still_rejects_scientific_fields_outside_params() -> None:
    class UnscopedParameter(PredictMeasure):
        name = "defaults_unscoped_parameter"
        scale: int = 2

    with pytest.raises(TypeError, match="inside Params"):
        compile_definition(UnscopedParameter)
