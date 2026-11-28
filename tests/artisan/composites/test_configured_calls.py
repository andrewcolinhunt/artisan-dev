"""Configuration-selected calls preserve parent identity without candidate data."""

from __future__ import annotations

from typing import ClassVar

import pytest
from pydantic import BaseModel, Field

from artisan.composites.base.compiler import compile_definition
from artisan.execution import CalledOperation
from artisan.operations.base import OperationDefinition
from artisan.operations.examples import DataGenerator
from artisan.schemas.specs.input_models import ExecuteInput


class RequiredMetric(OperationDefinition):
    name = "required_metric"
    inputs: ClassVar = {}
    outputs: ClassVar = {}

    class Params(BaseModel):
        candidate: int = Field(description="Candidate value.")
        scale: int = Field(default=2, description="Fixed scale.")

    params: Params

    def execute_function(self, inputs: ExecuteInput) -> int:
        return self.params.candidate * self.params.scale


def test_capture_without_required_candidate_and_detach_fixed_settings() -> None:
    settings = {"scale": 3}
    plan = compile_definition(
        DataGenerator,
        called_operations={"metric": CalledOperation(RequiredMetric, settings)},
    )
    settings["scale"] = 9
    assert plan.recipe.schema_version == 4
    assert plan.recipe.called_operations[0].fixed_params_json == '{"scale":3}'
    plan.instantiate()


def test_settings_change_parent_recipe() -> None:
    first = compile_definition(
        DataGenerator,
        called_operations={"metric": CalledOperation(RequiredMetric, {"scale": 3})},
    )
    second = compile_definition(
        DataGenerator,
        called_operations={"metric": CalledOperation(RequiredMetric, {"scale": 4})},
    )
    assert first.recipe.digest != second.recipe.digest


def test_empty_selection_preserves_existing_recipe_bytes() -> None:
    original = compile_definition(DataGenerator).recipe
    empty = compile_definition(DataGenerator, called_operations={}).recipe
    assert original.model_dump_json() == empty.model_dump_json()
    assert "called_operations" not in empty.model_dump()


def test_unknown_fixed_parameter_rejected() -> None:
    with pytest.raises(ValueError, match="unknown parameter"):
        compile_definition(
            DataGenerator,
            called_operations={"metric": CalledOperation(RequiredMetric, {"typo": 3})},
        )


def test_noncacheable_selection_disables_parent_reuse() -> None:
    class UncachedMetric(RequiredMetric):
        name = "uncached_configured_metric"
        cacheable = False

    plan = compile_definition(
        DataGenerator, called_operations={"metric": CalledOperation(UncachedMetric)}
    )
    assert not plan.recipe.cacheable
    plan.instantiate()


def test_submission_detaches_selection_before_scheduling() -> None:
    from artisan.orchestration.engine.pipeline_submission import freeze_submission

    settings = {"scale": 3}
    selections = {"metric": CalledOperation(RequiredMetric, settings)}
    submission = freeze_submission(DataGenerator, called_operations=selections)
    settings["scale"] = 9
    selections.clear()
    assert (
        submission.compile().recipe.called_operations[0].fixed_params_json
        == '{"scale":3}'
    )


def test_replay_restores_only_explicitly_registered_selected_code() -> None:
    from artisan.errors import ArtisanError
    from artisan.orchestration.retained_replay import _registered_plan

    plan = compile_definition(
        DataGenerator,
        called_operations={"metric": CalledOperation(RequiredMetric, {"scale": 3})},
    )
    registered = {(DataGenerator.__module__, DataGenerator.__qualname__): DataGenerator}
    with pytest.raises(ArtisanError, match="configured definition is not registered"):
        _registered_plan(plan.recipe, registered)
    registered[(RequiredMetric.__module__, RequiredMetric.__qualname__)] = (
        RequiredMetric
    )
    restored = _registered_plan(plan.recipe, registered)
    assert restored.recipe == plan.recipe
    assert restored.called_operations[0].definition is RequiredMetric
