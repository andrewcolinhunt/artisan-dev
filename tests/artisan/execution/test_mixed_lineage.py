"""Mixed input and sibling parents survive compilation and exact execution."""

from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import pytest
from fixtures.composition_operations import PredictMeasure, coordinate, structure
from pydantic import ValidationError

from artisan.composites import CompositeContext, CompositeDefinition
from artisan.composites.base.compiler import compile_definition
from artisan.execution.composition import execute_compiled_plan
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.executor import Executor
from artisan.operations.base import OperationDefinition
from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.schemas import (
    ArtifactResult,
    CSVArtifact,
    InputSpec,
    MetricArtifact,
    OutputSpec,
)
from artisan.schemas.composites.plan import ExecutionRecipe, MixedOutputContract
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


class GenerateAndScore(OperationDefinition):
    name = "mixed_generate_and_score"

    class InputRole(StrEnum):
        reference = "reference"

    class OutputRole(StrEnum):
        candidate = "candidate"
        score = "score"

    inputs: ClassVar = {"reference": InputSpec(artifact_type="csv", materialize=False)}
    outputs: ClassVar = {
        "candidate": OutputSpec(artifact_type="csv", derives_from={"inputs": []}),
        "score": OutputSpec(
            artifact_type="metric",
            derives_from={"inputs": ["reference"], "outputs": ["candidate"]},
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, int]:
        return {"reference": coordinate(inputs.input_artifacts["reference"][0])}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, int]:
        return {"candidate": 7, "reference": inputs.inputs["reference"]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        values = {role: results[0] for role, results in inputs.memory_outputs.items()}
        result = ArtifactResult()
        candidate = result.add_artifact(
            "candidate",
            CSVArtifact.draft(
                f"coordinate\n{values['candidate']}\n".encode(), "candidate.csv"
            ),
            sources={},
        )
        result.add_artifact(
            "score",
            MetricArtifact.draft(
                {"difference": values["candidate"] - values["reference"]}, "score.json"
            ),
            sources={
                "reference": [inputs.ref("reference", index=0)],
                "candidate": [candidate],
            },
        )
        return result


class ComposedGenerateAndScore(CompositeDefinition):
    name = "mixed_composed_generate_and_score"

    class InputRole(StrEnum):
        reference = "reference"

    class OutputRole(StrEnum):
        candidate = "candidate"
        score = "score"

    inputs: ClassVar = GenerateAndScore.inputs
    outputs: ClassVar = {
        "candidate": OutputSpec(artifact_type="csv"),
        "score": OutputSpec(artifact_type="metric"),
    }

    def compose(self, ctx: CompositeContext) -> None:
        result = ctx.call(
            GenerateAndScore, inputs={"reference": ctx.input("reference")}
        )
        ctx.output("candidate", result.output("candidate"))
        ctx.output("score", result.output("score"))


class ScoreOnly(ComposedGenerateAndScore):
    name = "mixed_score_only"

    class OutputRole(StrEnum):
        score = "score"

    outputs: ClassVar = {"score": OutputSpec(artifact_type="metric")}

    def compose(self, ctx: CompositeContext) -> None:
        result = ctx.call(
            GenerateAndScore, inputs={"reference": ctx.input("reference")}
        )
        ctx.output("score", result.output("score"))


@pytest.mark.parametrize(
    ("definition", "retain_candidate"),
    [(GenerateAndScore, True), (ComposedGenerateAndScore, True), (ScoreOnly, False)],
)
def test_mixed_parents_survive_compiled_execution_and_export_projection(
    definition: type[OperationDefinition] | type[CompositeDefinition],
    retain_candidate: bool,
    tmp_path: Path,
) -> None:
    with Executor(working_root=tmp_path / "session") as executor:
        output = execute_compiled_plan(
            compile_definition(definition),
            {"reference": [structure(2)]},
            executor,
            invocation_id="unit",
            execute_adapter=lambda _operation, _path: LocalExecuteRouter(),
        )
        exports = output.projection.export_roles()
        score = exports["score"][0]
        reference = output.projection.boundary_inputs[0].occurrences[0]
        assert output.result.artifacts["score"][0].values == {"difference": 5}
        expected_edges = {(reference, score)}
        expected_roles = {"reference"}
        assert set(exports) == (
            {"candidate", "score"} if retain_candidate else {"score"}
        )
        if retain_candidate:
            expected_edges.add((exports["candidate"][0], score))
            expected_roles.add("candidate")
        else:
            assert output.projection.support == ()
        assert {
            (edge.source, edge.target) for edge in output.projection.parents
        } == expected_edges
        assert {
            mapping.source_role for mapping in output.result.lineage["score"]
        } == expected_roles


def test_mixed_recipe_roundtrip_and_both_namespaces_affect_identity() -> None:
    recipe = compile_definition(GenerateAndScore).recipe
    restored = ExecutionRecipe.model_validate_json(recipe.model_dump_json())
    assert restored == recipe
    assert restored.digest == recipe.digest
    score = restored.outputs[1]
    assert isinstance(score, MixedOutputContract)
    assert score.input_parent_roles == ("reference",)
    assert score.output_parent_roles == ("candidate",)
    for field in ("input_parent_roles", "output_parent_roles"):
        changed = recipe.model_copy(
            update={
                "outputs": (
                    recipe.outputs[0],
                    score.model_copy(update={field: ("other",)}),
                )
            }
        )
        assert changed.digest != recipe.digest


@pytest.mark.parametrize("definition", [DataGenerator, DataTransformer, PredictMeasure])
def test_existing_output_contract_wire_shape_and_recipe_digest_are_unchanged(
    definition: type[OperationDefinition] | type[CompositeDefinition],
) -> None:
    recipe = compile_definition(definition).recipe
    historical = recipe.model_dump(mode="json")
    historical["outputs"] = [
        {
            "role": role,
            "artifact_type": spec.artifact_type,
            "required": spec.required,
            "parent_kind": next(iter(spec.derives_from)) if spec.derives_from else None,
            "parent_roles": next(iter(spec.derives_from.values()))
            if spec.derives_from
            else [],
        }
        for role, spec in definition.outputs.items()
    ]
    historical_bytes = canonical_json_bytes(historical)
    restored = ExecutionRecipe.model_validate_json(historical_bytes)
    assert canonical_json_bytes(restored.model_dump(mode="json")) == historical_bytes
    assert restored.digest == compute_content_digest(
        b"execution-recipe-v1\0" + historical_bytes
    )
    assert json.loads(recipe.model_dump_json()) == historical


@pytest.mark.parametrize(
    "changes",
    [
        {"output_parent_roles": ()},
        {"input_parent_roles": ("reference", "reference")},
        {"output_parent_roles": ("candidate", "candidate")},
        {"input_parent_roles": ("",)},
        {"parent_kind": "inputs", "parent_roles": ("reference",)},
    ],
)
def test_mixed_contract_rejects_empty_duplicate_or_ambiguous_parents(
    changes: dict[str, object],
) -> None:
    value = {
        "role": "score",
        "artifact_type": "metric",
        "required": True,
        "input_parent_roles": ("reference",),
        "output_parent_roles": ("candidate",),
    }
    with pytest.raises(ValidationError):
        MixedOutputContract.model_validate(value | changes)


def test_composite_recipe_still_rejects_authored_mixed_lineage() -> None:
    composite = compile_definition(ComposedGenerateAndScore).recipe.model_dump(
        mode="json"
    )
    composite["outputs"] = compile_definition(GenerateAndScore).recipe.model_dump(
        mode="json"
    )["outputs"]
    with pytest.raises(ValidationError, match="Composite outputs cannot declare"):
        ExecutionRecipe.model_validate(composite)
