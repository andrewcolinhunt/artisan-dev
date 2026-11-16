"""Static plans preserve scientific wiring without executing operations."""

from __future__ import annotations

import json
from datetime import date
from enum import StrEnum
from typing import ClassVar
from uuid import UUID

import pytest
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_serializer,
    field_validator,
)

from artisan.composites import CompositeContext, CompositeRef
from artisan.composites.base.compiler import compile_definition
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator.filter import Filter
from artisan.operations.examples import DataGenerator, DataTransformer, MetricCalculator
from artisan.operations.examples.wait_tool import WaitTool
from artisan.schemas import ArtifactResult, InputSpec, OutputSpec
from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)


class TransformScore(CompositeDefinition):
    name = "compiler_transform_score"

    class InputRole(StrEnum):
        dataset = "dataset"

    class OutputRole(StrEnum):
        dataset = "dataset"
        metrics = "metrics"

    inputs: ClassVar = {"dataset": InputSpec(artifact_type="csv")}
    outputs: ClassVar = {
        "dataset": OutputSpec(artifact_type="csv"),
        "metrics": OutputSpec(artifact_type="metric"),
    }

    class Params(BaseModel):
        scale_factor: float = 2.0

    params: Params = Field(default_factory=Params)

    def compose(self, ctx: CompositeContext) -> None:
        transformed = ctx.call(
            DataTransformer,
            name="transform",
            inputs={"dataset": ctx.input("dataset")},
            params={
                "scale_factor": self.params.scale_factor,
                "variants": 1,
                "seed": 100,
            },
        )
        measured = ctx.call(
            MetricCalculator,
            name="measure",
            inputs={"dataset": transformed.output("dataset")},
        )
        ctx.output("dataset", transformed.output("dataset"))
        ctx.output("metrics", measured.output("metrics"))


class Wrapper(TransformScore):
    name = "compiler_wrapper"


class HistoryReducer(OperationDefinition):
    """A compilation fixture whose scientific phases must never be called."""

    name = "compiler_history_reducer"
    independent_input_streams = True
    per_artifact_dispatch = False

    class InputRole(StrEnum):
        final = "final"
        history = "history"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = {
        "final": InputSpec(artifact_type="csv", materialize=False),
        "history": InputSpec(artifact_type="metric", materialize=False),
    }
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["final", "history"]}
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, object]:
        msg = "Compilation executed preprocessing"
        raise AssertionError(msg)

    def execute_function(self, inputs: ExecuteInput) -> dict[str, object]:
        msg = "Compilation executed scientific work"
        raise AssertionError(msg)

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        msg = "Compilation executed postprocessing"
        raise AssertionError(msg)


class IterateAndSummarize(TransformScore):
    name = "compiler_iterate"

    class Params(BaseModel):
        cycles: int = Field(default=2, ge=1)

    params: Params = Field(default_factory=Params)

    def compose(self, ctx: CompositeContext) -> None:
        current = ctx.input("dataset")
        history = []
        for index in range(self.params.cycles):
            for kind in ("a", "b"):
                current = ctx.call(
                    DataTransformer, name=f"{kind}_{index}", inputs={"dataset": current}
                ).output("dataset")
            history.append(
                ctx.call(
                    MetricCalculator,
                    name=f"metric_{index}",
                    inputs={"dataset": current},
                ).output("metrics")
            )
        summary = ctx.call(
            HistoryReducer,
            name="summary",
            inputs={"final": current, "history": history},
        )
        ctx.output("dataset", current)
        ctx.output("metrics", summary.output("metrics"))


class EmptyOutput(CompositeDefinition):
    name = "compiler_empty_output"

    class OutputRole(StrEnum):
        metrics = "metrics"

    outputs: ClassVar = {"metrics": OutputSpec(artifact_type="metric", required=False)}

    def compose(self, ctx: CompositeContext) -> None:
        ctx.output("metrics", [])


class NestedOptions(BaseModel):
    factor: float = Field(default=1, alias="gain")


class ParameterizedEmpty(EmptyOutput):
    name = "compiler_parameterized_empty"

    class Params(BaseModel):
        options: NestedOptions = Field(default_factory=NestedOptions)

    params: Params = Field(default_factory=Params)


def _context() -> CompositeContext:
    return CompositeContext(compile_definition(Wrapper).recipe, (Wrapper,))


def test_compile_prediction_and_measurement_preserves_internal_relationship() -> None:
    plan = compile_definition(TransformScore)
    assert [call.name for call in plan.calls] == ["transform", "measure"]
    assert plan.recipe.calls[1].inputs[0].sources[0].call == "transform"
    assert [binding.sources[0].call for binding in plan.recipe.exports] == [
        "transform",
        "measure",
    ]
    assert plan.instantiate().params.scale_factor == 2.0


def test_compile_iterative_recipe_preserves_history_order_and_reducer_policy() -> None:
    plan = compile_definition(IterateAndSummarize, params={"cycles": 3})
    assert len(plan.calls) == 10
    summary = plan.recipe.calls[-1]
    assert [ref.call for ref in summary.inputs[1].sources] == [
        "metric_0",
        "metric_1",
        "metric_2",
    ]
    assert summary.inputs[0].sources[0].call == "b_2"
    assert summary.recipe.policy.independent_input_streams
    assert not summary.recipe.policy.per_artifact_dispatch
    assert plan.recipe.exports[0].sources[0].call == "b_2"


def test_compile_concatenation_preserves_repetition_and_explicit_input_alias() -> None:
    context = _context()
    source = context.input("dataset")
    a = context.call(DataTransformer, name="a", inputs={"dataset": source})
    b = context.call(DataTransformer, name="b", inputs={"dataset": source})
    context.output(
        "dataset",
        [b.output("dataset"), a.output("dataset"), b.output("dataset"), source],
    )
    metric = context.call(MetricCalculator, inputs={"dataset": source})
    context.output("metrics", metric.output("metrics"))
    plan = context.finish()
    assert [ref.call for ref in plan.recipe.exports[0].sources] == ["b", "a", "b", None]


def test_compile_nested_recipe_captures_identity_and_parameters() -> None:
    context = _context()
    nested = context.call(
        TransformScore,
        name="nested",
        inputs={"dataset": context.input("dataset")},
        params=TransformScore.Params(scale_factor=4),
    )
    for role in ("dataset", "metrics"):
        context.output(role, nested.output(role))
    plan = context.finish()
    assert plan.calls[0].plan.definition is TransformScore
    assert plan.recipe.calls[0].recipe.params_json == '{"scale_factor":4.0}'
    assert len(plan.calls[0].plan.calls) == 2


def test_compile_identity_excludes_builder_uuid_and_captures_scientific_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = compile_definition(TransformScore)
    assert initial.recipe.digest == compile_definition(TransformScore).recipe.digest
    assert (
        initial.recipe.digest
        != compile_definition(TransformScore, params={"scale_factor": 3}).recipe.digest
    )
    monkeypatch.setattr(DataTransformer, "version", "changed")
    assert initial.recipe.digest != compile_definition(TransformScore).recipe.digest
    assert initial.recipe.calls[0].recipe.definition.version != "changed"


def test_compile_inherits_noncacheability_through_nested_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(MetricCalculator, "cacheable", False)
    plan = compile_definition(TransformScore)
    assert not plan.recipe.cacheable
    plan.instantiate()
    context = _context()
    child = context.call(TransformScore, inputs={"dataset": context.input("dataset")})
    context.output("dataset", child.output("dataset"))
    context.output("metrics", child.output("metrics"))
    assert not context.finish().recipe.cacheable


def test_recipe_roundtrip_is_closed_and_deeply_immutable() -> None:
    recipe = compile_definition(TransformScore).recipe
    assert ExecutionRecipe.model_validate_json(recipe.model_dump_json()) == recipe
    with pytest.raises(ValidationError, match="frozen"):
        recipe.calls[0].inputs[0].sources[0].role = "other"
    payload = recipe.model_dump()
    payload["unknown"] = True
    with pytest.raises(ValidationError, match="Extra inputs"):
        ExecutionRecipe.model_validate(payload)


def test_recipe_rejects_foreign_or_future_call_in_serialized_wiring() -> None:
    payload = compile_definition(TransformScore).recipe.model_dump(mode="json")
    payload["calls"][0]["inputs"][0]["sources"][0]["call"] = "measure"
    with pytest.raises(ValidationError, match="preceding call"):
        ExecutionRecipe.model_validate(payload)


def test_compile_rejects_unknown_params_and_unrelated_models() -> None:
    with pytest.raises(ValueError, match="Extra inputs"):
        compile_definition(TransformScore, params={"scale_factro": 3})
    with pytest.raises(TypeError, match="exact|Parameters must use"):
        compile_definition(TransformScore, params=IterateAndSummarize.Params())


def test_compile_rejects_recursive_definitions() -> None:
    class Recursive(TransformScore):
        name = "compiler_recursive"

        def compose(self, ctx: CompositeContext) -> None:
            ctx.call(type(self), inputs={"dataset": ctx.input("dataset")})

    with pytest.raises(ValueError, match="Recursive"):
        compile_definition(Recursive)


@pytest.mark.parametrize("grouping", [GroupByStrategy.NAME, GroupByStrategy.LINEAGE])
def test_compile_rejects_store_dependent_child_grouping(
    grouping: GroupByStrategy, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _context()
    original = DataTransformer.model_fields["group_by"].default
    monkeypatch.setattr(DataTransformer.model_fields["group_by"], "default", grouping)
    DataTransformer.model_rebuild(force=True)
    try:
        root = compile_definition(DataTransformer)
        assert root.recipe.policy.group_by == grouping
        assert root.instantiate().group_by == grouping
        with pytest.raises(ValueError, match="NAME or LINEAGE"):
            context.call(DataTransformer, inputs={"dataset": context.input("dataset")})
    finally:
        DataTransformer.model_fields["group_by"].default = original
        DataTransformer.model_rebuild(force=True)


def test_compile_rejects_curators() -> None:
    with pytest.raises(ValueError, match="Curators"):
        compile_definition(Filter)


def test_compile_rejects_unsupported_independent_streams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(DataTransformer, "independent_input_streams", True)
    with pytest.raises(ValueError, match="whole-stream reducer"):
        compile_definition(DataTransformer)


def test_context_rejects_foreign_refs_and_forged_producers() -> None:
    context, foreign = _context(), _context()
    with pytest.raises(ValueError, match="another composition"):
        context.output("dataset", foreign.input("dataset"))
    valid = context.input("dataset")
    with pytest.raises(ValueError, match="undeclared call"):
        context.output("dataset", CompositeRef(valid.owner, "missing", "dataset"))


def test_context_rejects_duplicate_names_and_output_bindings() -> None:
    context = _context()
    source = context.input("dataset")
    context.call(DataTransformer, name="same", inputs={"dataset": source})
    with pytest.raises(ValueError, match="Duplicate"):
        context.call(DataTransformer, name="same", inputs={"dataset": source})
    context.output("dataset", source)
    with pytest.raises(ValueError, match="already bound"):
        context.output("dataset", source)
    with pytest.raises(ValueError, match="not bound"):
        context.finish()


def test_context_rejects_type_mismatch_missing_role_and_raw_artifacts() -> None:
    context = _context()
    with pytest.raises(ValueError, match="Cannot wire"):
        context.output("metrics", context.input("dataset"))
    with pytest.raises(ValueError, match="missing"):
        context.call(DataTransformer)
    with pytest.raises(ValueError, match="another composition"):
        context.output("dataset", [object()])
    with pytest.raises(TypeError, match="map roles"):
        context.call(DataTransformer, inputs=[])


def test_compile_does_not_call_scientific_phases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        msg = "Compilation crossed an execution boundary"
        raise AssertionError(msg)

    for method in ("preprocess", "execute_function", "postprocess"):
        monkeypatch.setattr(DataTransformer, method, forbidden)
    compile_definition(TransformScore)


def test_compile_empty_optional_output_and_generative_creator() -> None:
    empty = compile_definition(EmptyOutput).recipe
    assert empty.inputs == ()
    assert empty.exports[0].sources == ()
    generator = compile_definition(DataGenerator).recipe
    assert generator.inputs == ()
    assert generator.definition.kind == "creator"


def test_compile_rejects_unknown_nested_parameters_and_snapshots_aliases() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        compile_definition(
            ParameterizedEmpty, params={"options": {"gain": 2, "unused": 3}}
        )
    params = ParameterizedEmpty.Params(options=NestedOptions(gain=4))
    plan = compile_definition(ParameterizedEmpty, params=params)
    params.options.factor = 10
    assert plan.instantiate().params.options.factor == 4
    alias_plan = compile_definition(ParameterizedEmpty, params={"options": {"gain": 4}})
    assert alias_plan.recipe.digest == plan.recipe.digest


def test_recipe_changes_with_scientific_batching_but_not_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = compile_definition(DataTransformer)
    original = plan.recipe
    defaults = DataTransformer.model_fields["batch_strategy"].default
    monkeypatch.setattr(defaults, "units_per_worker", 50)
    monkeypatch.setattr(defaults, "max_workers", 20)
    assert compile_definition(DataTransformer).recipe.digest == original.digest
    plan.instantiate()
    monkeypatch.setattr(defaults, "artifacts_per_unit", 17)
    changed = compile_definition(DataTransformer).recipe
    assert changed.policy.artifacts_per_unit == 17
    assert changed.digest != original.digest
    with pytest.raises(ValueError, match="configuration changed"):
        plan.instantiate()


def test_compile_rejects_dynamic_roles_and_composite_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(DataTransformer, "runtime_defined_inputs", True)
    with pytest.raises(ValueError, match="statically declared"):
        compile_definition(DataTransformer)
    monkeypatch.setitem(
        EmptyOutput.outputs,
        "metrics",
        OutputSpec(artifact_type="metric", required=False, derives_from={"inputs": []}),
    )
    with pytest.raises(ValueError, match="leave derives_from unset"):
        compile_definition(EmptyOutput)


def test_compile_independent_command_reducer_is_not_admitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(HistoryReducer, "execute_as_tool", True)
    with pytest.raises(ValueError, match="native whole-stream"):
        compile_definition(HistoryReducer)


def test_compile_restores_strict_json_parameter_types() -> None:
    class StrictParameters(EmptyOutput):
        name = "compiler_strict_parameters"

        class Params(BaseModel):
            model_config = ConfigDict(strict=True)

            tags: tuple[str, ...] = ("second", "first")
            day: date = date(2025, 1, 2)
            identifier: UUID = UUID("12345678-1234-5678-1234-567812345678")

        params: Params = Field(default_factory=Params)

    plan = compile_definition(StrictParameters)
    assert plan.instantiate().params == StrictParameters.Params()
    assert (
        plan.recipe.params_json
        == compile_definition(
            StrictParameters, params=StrictParameters.Params()
        ).recipe.params_json
    )


def test_compile_rejects_malformed_composite_parameter_pairs() -> None:
    class MissingField(EmptyOutput):
        class Params(BaseModel):
            count: int = 1

    class MissingModel(EmptyOutput):
        params: NestedOptions = Field(default_factory=NestedOptions)

    class WrongAnnotation(ParameterizedEmpty):
        class Params(BaseModel):
            count: int = 1

        params: NestedOptions = Field(default_factory=NestedOptions)

    class SplitInheritance(ParameterizedEmpty):
        params: ParameterizedEmpty.Params = Field(
            default_factory=ParameterizedEmpty.Params
        )

    class WrongDefault(ParameterizedEmpty):
        class Params(BaseModel):
            count: int = 1

        params: Params = Field(default_factory=dict)

    class FlatField(EmptyOutput):
        count: int = 1

    for definition in (
        MissingField,
        MissingModel,
        WrongAnnotation,
        SplitInheritance,
        WrongDefault,
        FlatField,
    ):
        with pytest.raises(TypeError, match="Params|params"):
            compile_definition(definition)


def test_compile_canonicalizes_scalar_sets_without_sorting_sequences() -> None:
    class UnorderedParameters(EmptyOutput):
        name = "compiler_unordered_parameters"

        class Params(BaseModel):
            tags: set[str] = {"pear", "apple", "plum"}
            groups: dict[str, frozenset[int]] = {"group": frozenset({3, 1, 2})}
            order: list[str] = ["second", "first"]

        params: Params = Field(default_factory=Params)

    plan = compile_definition(UnorderedParameters)
    encoded = json.loads(plan.recipe.params_json)
    assert encoded["tags"] == ["apple", "pear", "plum"]
    assert encoded["groups"] == {"group": [1, 2, 3]}
    assert encoded["order"] == ["second", "first"]
    assert plan.instantiate().params == UnorderedParameters.Params()
    reversed_plan = compile_definition(
        UnorderedParameters, params={"order": ["first", "second"]}
    )
    assert reversed_plan.recipe.digest != plan.recipe.digest


def test_compile_preserves_lossless_custom_parameter_serialization() -> None:
    class CustomParameters(EmptyOutput):
        name = "compiler_custom_parameters"

        class Params(BaseModel):
            label: str = "pear;apple"

            @field_serializer("label", when_used="json")
            def serialize_label(self, label: str) -> str:
                return f"wire:{label}"

            @field_validator("label", mode="before")
            @classmethod
            def parse_label(cls, value: object) -> object:
                return value.removeprefix("wire:") if isinstance(value, str) else value

        params: Params = Field(default_factory=Params)

    plan = compile_definition(CustomParameters, params=CustomParameters.Params())
    assert plan.recipe.params_json == '{"label":"wire:pear;apple"}'
    assert plan.instantiate().params == CustomParameters.Params()


def test_compile_rejects_lossy_serialization_and_structured_sets() -> None:
    class LossyParameters(EmptyOutput):
        name = "compiler_lossy_parameters"

        class Params(BaseModel):
            count: int = 3

            @field_serializer("count")
            def serialize_count(self, count: int) -> int:
                return count + 1

        params: Params = Field(default_factory=Params)

    class StructuredSets(EmptyOutput):
        name = "compiler_structured_sets"

        class Params(BaseModel):
            pairs: set[tuple[int, int]] = {(1, 2), (3, 4)}

        params: Params = Field(default_factory=Params)

    with pytest.raises(ValueError, match="round-trip"):
        compile_definition(LossyParameters)
    with pytest.raises(ValueError, match="scalar-item lists"):
        compile_definition(StructuredSets)


@pytest.mark.parametrize("field", ["executable", "interpreter", "subcommand"])
def test_compile_freezes_tool_prefix_and_rejects_changed_defaults(
    field: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = compile_definition(WaitTool)
    assert plan.recipe.execution.mode == "command"
    monkeypatch.setattr(WaitTool.model_fields["tool"].default, field, "different")
    changed = compile_definition(WaitTool)
    assert changed.recipe.digest != plan.recipe.digest
    with pytest.raises(ValueError, match="configuration changed"):
        plan.instantiate()


def test_compile_freezes_adapter_and_class_invocation_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = compile_definition(DataTransformer)
    monkeypatch.setattr(DataTransformer, "execute_as_tool", True)
    assert compile_definition(DataTransformer).recipe.execution.mode == "python_adapter"
    with pytest.raises(ValueError, match="configuration changed"):
        plan.instantiate()
    monkeypatch.setattr(DataTransformer, "execute_as_tool", False)
    monkeypatch.setattr(DataTransformer, "per_artifact_dispatch", False)
    with pytest.raises(ValueError, match="configuration changed"):
        plan.instantiate()


def test_recipe_rejects_internal_store_dependent_grouping() -> None:
    payload = compile_definition(TransformScore).recipe.model_dump(mode="json")
    payload["calls"][0]["recipe"]["policy"]["group_by"] = "name"
    with pytest.raises(ValidationError, match="NAME or LINEAGE"):
        ExecutionRecipe.model_validate(payload)


@pytest.mark.parametrize("field", ["inputs", "outputs", "version"])
def test_instantiate_rejects_changed_definition_contract(
    field: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = compile_definition(DataTransformer)
    if field == "inputs":
        monkeypatch.setitem(
            DataTransformer.inputs,
            "dataset",
            DataTransformer.inputs["dataset"].model_copy(update={"materialize": False}),
        )
    elif field == "outputs":
        monkeypatch.setitem(
            DataTransformer.outputs,
            "dataset",
            DataTransformer.outputs["dataset"].model_copy(update={"required": False}),
        )
    else:
        monkeypatch.setattr(DataTransformer, "version", "changed")
    with pytest.raises(ValueError, match="configuration changed"):
        plan.instantiate()


def test_recipe_rejects_execution_mode_that_disagrees_with_definition_kind() -> None:
    payload = compile_definition(TransformScore).recipe.model_dump(mode="json")
    payload["execution"]["mode"] = "native"
    with pytest.raises(ValidationError, match="kind and execution mode"):
        ExecutionRecipe.model_validate(payload)


def test_compile_rejects_nonstring_mapping_keys_before_losing_key_types() -> None:
    class NumericKeys(EmptyOutput):
        name = "compiler_numeric_keys"

        class Params(BaseModel):
            groups: dict[int, set[str]] = {1: {"pear", "apple"}}

        params: Params = Field(default_factory=Params)

    with pytest.raises(ValueError, match="string keys"):
        compile_definition(NumericKeys)


@pytest.mark.parametrize("when_used", ["always", "json"])
def test_compile_rejects_hash_seed_dependent_custom_set_serialization(
    when_used: str,
) -> None:
    class CustomSet(EmptyOutput):
        name = "compiler_custom_set"

        class Params(BaseModel):
            tags: set[str] = {"pear", "apple", "plum"}

            @field_serializer("tags", when_used=when_used)
            def serialize_tags(self, tags: set[str]) -> str:
                return ";".join(tags)

            @field_validator("tags", mode="before")
            @classmethod
            def parse_tags(cls, value: object) -> object:
                return set(value.split(";")) if isinstance(value, str) else value

        params: Params = Field(default_factory=Params)

    with pytest.raises(ValueError, match="ordered sequence for custom encodings"):
        compile_definition(CustomSet)
