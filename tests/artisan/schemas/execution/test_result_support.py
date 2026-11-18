"""Completed results retain exact references without retaining execution owners."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from artisan.schemas import (
    Artifact,
    ArtifactResult,
    ExecutionConfigArtifact,
    MetricArtifact,
)
from artisan.schemas.execution.result_support import ArtifactResultSupport
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import (
    BoundaryInputAttribution,
    BoundaryProjection,
    OccurrenceRelation,
    ProjectedOccurrence,
    RoleOccurrences,
)


def address(role: str, index: int = 0) -> OccurrenceAddress:
    return OccurrenceAddress(invocation_id="call", role=role, index=index)


def supported_result(*, empty: bool = False) -> ArtifactResult:
    inventory: dict[OccurrenceAddress, Artifact] = {}
    for index in range(2):
        artifact = MetricArtifact.draft(content={"value": 1}, original_name="data")
        artifact.finalize()
        config = ExecutionConfigArtifact.draft(
            content={} if empty else {"source": {"$artifact": artifact.artifact_id}},
            original_name="configuration.json",
        )
        config.finalize()
        inventory[address("data", index)] = artifact
        inventory[address("config", index)] = config
    projection = BoundaryProjection(
        boundary_inputs=(
            RoleOccurrences(
                role="data", occurrences=(address("data"), address("data", 1))
            ),
        ),
        input_attribution=tuple(
            BoundaryInputAttribution(boundary=address("data", index), producer=None)
            for index in range(2)
        ),
        occurrences=tuple(
            ProjectedOccurrence(
                address=key,
                artifact_id=artifact.artifact_id,
                artifact_type=artifact.artifact_type,
            )
            for key, artifact in inventory.items()
        ),
        exports=(
            RoleOccurrences(
                role="config", occurrences=(address("config"), address("config", 1))
            ),
        ),
        references=()
        if empty
        else tuple(
            OccurrenceRelation(
                source=address("config", index), target=address("data", index)
            )
            for index in range(2)
        ),
    )
    return ArtifactResult(
        artifacts={
            "config": [inventory[address("config", index)] for index in range(2)]
        },
        lineage={"config": []},
        support=ArtifactResultSupport(projection, inventory),
    )


def test_referenced_artifacts_selects_occurrence_before_content_identity() -> None:
    result = supported_result()
    assert result.support is not None
    first = result.support.artifacts[address("data")]
    second = result.support.artifacts[address("data", 1)]
    assert first is not second
    assert first.artifact_id == second.artifact_id
    assert (
        result.artifacts["config"][0].artifact_id
        == result.artifacts["config"][1].artifact_id
    )
    assert result.referenced_artifacts("config", index=0)[first.artifact_id] is first
    assert result.referenced_artifacts("config", index=1)[second.artifact_id] is second


def test_empty_configuration_has_known_empty_references() -> None:
    result = supported_result(empty=True)
    assert result.referenced_artifacts("config", index=0) == {}
    result.support = None
    with pytest.raises(ValueError, match="no runtime reference support"):
        result.referenced_artifacts("config", index=0)


@pytest.mark.parametrize(
    ("role", "index"), [("missing", 0), ("config", -1), ("config", 2), ("config", True)]
)
def test_referenced_artifacts_rejects_invalid_output_position(
    role: str, index: int
) -> None:
    with pytest.raises(ValueError, match="output position"):
        supported_result().referenced_artifacts(role, index=index)


def test_referenced_artifacts_rejects_replaced_equal_content_output() -> None:
    result = supported_result()
    result.artifacts["config"].reverse()
    with pytest.raises(ValueError, match="no longer matches"):
        result.referenced_artifacts("config", index=0)


def test_referenced_artifacts_rejects_non_configuration_output() -> None:
    result = supported_result()
    assert result.support is not None
    result.artifacts["config"][0] = result.support.artifacts[address("data")]
    with pytest.raises(ValueError, match="configuration output"):
        result.referenced_artifacts("config", index=0)


def test_support_copies_and_protects_inventory_membership() -> None:
    result = supported_result()
    assert result.support is not None
    inventory = dict(result.support.artifacts)
    support = ArtifactResultSupport(result.support.projection, inventory)
    inventory.clear()
    assert len(support.artifacts) == 4
    with pytest.raises(FrozenInstanceError):
        support.artifacts = {}  # type: ignore[misc]
    with pytest.raises(AttributeError):
        support.artifacts.clear()  # type: ignore[attr-defined]


def test_support_requires_complete_exact_inventory() -> None:
    result = supported_result()
    assert result.support is not None
    inventory = dict(result.support.artifacts)
    inventory.pop(address("data"))
    with pytest.raises(ValueError, match="exactly the projected occurrences"):
        ArtifactResultSupport(result.support.projection, inventory)
    inventory[address("data")] = result.artifacts["config"][0]
    with pytest.raises(ValueError, match="projected identity"):
        ArtifactResultSupport(result.support.projection, inventory)


def test_referenced_artifacts_rejects_incomplete_reference_mapping() -> None:
    result = supported_result()
    assert result.support is not None
    projection = result.support.projection.model_copy(update={"references": ()})
    result.support = ArtifactResultSupport(projection, result.support.artifacts)
    with pytest.raises(ValueError, match="does not match configuration references"):
        result.referenced_artifacts("config", index=0)


def test_referenced_artifacts_rejects_ambiguous_equal_content_referents() -> None:
    result = supported_result()
    assert result.support is not None
    projection = result.support.projection.model_copy(
        update={
            "references": (
                *result.support.projection.references,
                OccurrenceRelation(source=address("config"), target=address("data", 1)),
            ),
        }
    )
    result.support = ArtifactResultSupport(projection, result.support.artifacts)
    with pytest.raises(ValueError, match="ambiguous occurrences"):
        result.referenced_artifacts("config", index=0)


def test_support_does_not_change_serialized_result_contract() -> None:
    result = supported_result()
    plain = ArtifactResult(artifacts=result.artifacts, lineage=result.lineage)
    assert result.model_dump() == plain.model_dump()
    assert result.model_dump_json() == plain.model_dump_json()
    assert ArtifactResult.model_validate_json(result.model_dump_json()).support is None
    for mode in ("validation", "serialization"):
        assert (
            "support" not in ArtifactResult.model_json_schema(mode=mode)["properties"]
        )


def test_support_retains_transitive_references_without_flattening_access() -> None:
    result = supported_result()
    assert result.support is not None
    inventory = dict(result.support.artifacts)
    nested = result.artifacts["config"][0]
    outer = ExecutionConfigArtifact.draft(
        content={"nested": {"$artifact": nested.artifact_id}},
        original_name="outer.json",
    )
    outer.finalize()
    outer_address = address("outer")
    inventory[outer_address] = outer
    projection = result.support.projection.model_copy(
        update={
            "exports": (RoleOccurrences(role="outer", occurrences=(outer_address,)),),
            "occurrences": (
                *(
                    item
                    for item in result.support.projection.occurrences
                    if item.address != address("config", 1)
                ),
                ProjectedOccurrence(
                    address=outer_address,
                    artifact_id=outer.artifact_id,
                    artifact_type=outer.artifact_type,
                ),
            ),
            "support": (address("config"),),
            "references": (
                OccurrenceRelation(source=outer_address, target=address("config")),
                OccurrenceRelation(source=address("config"), target=address("data")),
            ),
        }
    )
    inventory.pop(address("config", 1))
    result = ArtifactResult(
        artifacts={"outer": [outer]},
        support=ArtifactResultSupport(projection, inventory),
    )
    assert result.referenced_artifacts("outer", index=0) == {nested.artifact_id: nested}
    assert result.support.artifacts[address("data")] is inventory[address("data")]
