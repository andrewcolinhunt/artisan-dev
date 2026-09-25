"""The optional result helper constructs ordinary explicit mappings atomically."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.execution.lineage.validation import (
    validate_lineage_completeness,
    validate_lineage_integrity,
)
from artisan.schemas import (
    ArtifactResult,
    ExecutionConfigArtifact,
    LineageMapping,
    MetricArtifact,
    OutputSpec,
)


def config() -> ExecutionConfigArtifact:
    return ExecutionConfigArtifact.draft(
        content={"input": {"$artifact": "a" * 32}},
        original_name="config.json",
        step_number=1,
    )


@pytest.mark.parametrize(
    "sources",
    [{"dataset": ["a" * 32]}, {"dataset": ["a" * 32, "b" * 32]}, {"dataset": [0, 1]}],
)
def test_helper_and_manual_forms_are_equivalent(
    sources: dict[str, list[str | int]],
) -> None:
    artifact = config()
    references = sources["dataset"]
    mappings = [
        LineageMapping(
            draft_index=0,
            source_role="dataset",
            source_artifact_id=ref if isinstance(ref, str) else None,
            source_output_index=ref if isinstance(ref, int) else None,
        )
        for ref in references
    ]
    manual = ArtifactResult(
        artifacts={"config": [artifact]}, lineage={"config": mappings}
    )
    built = ArtifactResult()
    assert built.add_artifact("config", artifact, sources=sources) == 0
    assert built == manual
    kind = "inputs" if isinstance(references[0], str) else "outputs"
    specs = {"config": OutputSpec(derives_from={kind: ["dataset"]})}
    if kind == "outputs":
        siblings = [config(), config()]
        specs["dataset"] = OutputSpec(derives_from={"inputs": []})
        for result in (manual, built):
            result.artifacts["dataset"] = siblings
            result.lineage["dataset"] = []
    for result in (manual, built):
        validate_lineage_integrity(
            result.lineage, {"dataset": ["a" * 32, "b" * 32]}, result.artifacts, specs
        )
        validate_lineage_completeness(result.artifacts, specs, result.lineage)


def test_indices_are_role_local_and_can_address_siblings() -> None:
    result = ArtifactResult()
    first = result.add_artifact("config", config(), sources={})
    second = result.add_artifact("config", config(), sources={})
    metric = MetricArtifact.draft(
        content={"score": 2}, original_name="metric", step_number=1
    )
    assert result.add_artifact("metric", metric, sources={"config": [second]}) == 0
    assert (first, second) == (0, 1)
    assert result.lineage == {
        "config": [],
        "metric": [
            LineageMapping(draft_index=0, source_role="config", source_output_index=1)
        ],
    }


@pytest.mark.parametrize(
    "sources",
    [
        {"dataset": []},
        {"dataset": [True]},
        {"dataset": [-1]},
        {"dataset": ["bad"]},
        {"dataset": [1.5]},
        {"dataset": [0, "a" * 32]},
        {"dataset": ["a" * 32], "other": [0]},
        {"": ["a" * 32]},
    ],
)
def test_invalid_sources_never_partially_mutate_result(sources: dict) -> None:
    result = ArtifactResult()
    result.add_artifact("config", config(), sources={"dataset": ["a" * 32]})
    before = result.model_dump()
    with pytest.raises(ValueError):
        result.add_artifact("config", config(), sources=sources)
    assert result.model_dump() == before
    with pytest.raises(ValueError):
        result.add_artifact("new_role", config(), sources=sources)
    assert result.model_dump() == before


def test_result_lineage_roundtrip_and_none_rejection() -> None:
    result = ArtifactResult()
    result.add_artifact("config", config(), sources={"dataset": ["a" * 32]})
    restored = ArtifactResult.model_validate_json(result.model_dump_json())
    assert restored.lineage == result.lineage
    assert len(restored.artifacts["config"]) == 1
    with pytest.raises(ValidationError):
        ArtifactResult(lineage=None)


def test_manual_and_helper_have_identical_contract_failures() -> None:
    artifact = config()
    manual = ArtifactResult(
        artifacts={"config": [artifact]},
        lineage={
            "config": [
                LineageMapping(
                    draft_index=0, source_role="wrong", source_artifact_id="a" * 32
                )
            ]
        },
    )
    built = ArtifactResult()
    built.add_artifact("config", artifact, sources={"wrong": ["a" * 32]})
    from artisan.execution.exceptions import LineageIntegrityError

    for result in (manual, built):
        with pytest.raises(LineageIntegrityError, match="forbidden source role"):
            validate_lineage_integrity(
                result.lineage,
                {"wrong": ["a" * 32]},
                result.artifacts,
                {"config": OutputSpec(derives_from={"inputs": ["dataset"]})},
            )
