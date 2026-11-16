"""Explicit artifact policies resolve exact semantic referents without I/O."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pydantic import Field, ValidationError

from artisan.errors import ArtifactIntegrityError
from artisan.schemas import Artifact, ExecutionConfigArtifact, MetricArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.provenance.occurrence import (
    OccurrenceAddress,
    ReferenceContext,
    RequiredReference,
)


@pytest.fixture
def policy_type(monkeypatch: pytest.MonkeyPatch) -> Iterator[type[ArtifactTypeDef]]:
    monkeypatch.setattr(ArtifactTypeDef, "_registry", dict(ArtifactTypeDef._registry))
    monkeypatch.setattr(ArtifactTypes, "_registry", dict(ArtifactTypes._registry))
    original_names = set(vars(ArtifactTypes))

    class RelationMetric(MetricArtifact):
        artifact_type: str = Field(default="_test_reference_policy", frozen=True)

    class RelationMetricType(ArtifactTypeDef):
        key = "_test_reference_policy"
        table_path = "artifacts/_test_reference_policy"
        model = RelationMetric

    yield RelationMetricType
    for name in set(vars(ArtifactTypes)) - original_names:
        delattr(ArtifactTypes, name)


def address(index: int) -> OccurrenceAddress:
    return OccurrenceAddress(invocation_id="producer", role="data", index=index)


def metric(value: int) -> MetricArtifact:
    return MetricArtifact.draft({"value": value}, "metric.json", 1).finalize()


def test_registration_can_precede_explicit_policy_admission(policy_type: type) -> None:
    assert ArtifactTypeDef.get(policy_type.key) is policy_type
    with pytest.raises(TypeError, match="no required-reference policy"):
        policy_type.reference_policy()
    policy_type.required_references = None
    assert policy_type.reference_policy() is None
    policy_type.required_references = "invalid"
    with pytest.raises(TypeError, match="callable or None"):
        policy_type.reference_policy()


def test_core_types_explicitly_declare_no_references() -> None:
    for artifact_type in ("csv", "metric", "file_ref", "appendable"):
        assert ArtifactTypeDef.get(artifact_type).reference_policy() is None
    artifact = MetricArtifact.draft({"subject": "a" * 32}, "metric.json", 1).finalize()
    assert (
        ArtifactTypeDef.get("metric").resolve_required_references(
            artifact, ReferenceContext({})
        )
        == ()
    )


def test_config_policy_resolves_markers_and_deduplicates_occurrence() -> None:
    target = metric(1)
    config = ExecutionConfigArtifact.draft(
        {
            "first": {"$artifact": target.artifact_id},
            "again": {"$artifact": target.artifact_id},
            "ordinary": "b" * 32,
        },
        "config.json",
        1,
    ).finalize()
    context = ReferenceContext({address(0): target})
    expected = RequiredReference(
        address=address(0), artifact_id=target.artifact_id, artifact_type="metric"
    )
    assert ArtifactTypeDef.get("config").resolve_required_references(
        config, context
    ) == (expected,)
    with pytest.raises(ValueError, match="missing or ambiguous"):
        ArtifactTypeDef.get("config").resolve_required_references(
            config, ReferenceContext({})
        )


def test_equal_content_config_targets_require_exact_relationship() -> None:
    target = metric(1)
    config = ExecutionConfigArtifact.draft(
        {"source": {"$artifact": target.artifact_id}}, "config.json", 1
    ).finalize()
    candidates = {address(0): target, address(1): target}
    with pytest.raises(ValueError, match="ambiguous"):
        ArtifactTypeDef.get("config").resolve_required_references(
            config, ReferenceContext(candidates)
        )
    result = ArtifactTypeDef.get("config").resolve_required_references(
        config, ReferenceContext(candidates, direct_parents=(address(1),))
    )
    assert result[0].address == address(1)


def test_relation_policy_uses_direct_parents_without_embedded_ids(
    policy_type: type,
) -> None:
    target = metric(1)
    artifact = policy_type.model.draft({"position": 7}, "annotation.json", 1).finalize()

    def policy(
        _artifact: Artifact, context: ReferenceContext
    ) -> tuple[RequiredReference, ...]:
        parent = context.direct_parents[0]
        primary = context.artifacts[parent]
        return (
            RequiredReference(
                address=parent,
                artifact_id=primary.artifact_id,
                artifact_type=primary.artifact_type,
            ),
        )

    policy_type.required_references = staticmethod(policy)
    context = ReferenceContext({address(0): target}, (address(0),))
    result = policy_type.resolve_required_references(artifact, context)
    assert result[0].address == address(0)
    assert target.artifact_id.encode() not in artifact.content


@pytest.mark.parametrize(
    "change",
    [
        {"address": address(1)},
        {"artifact_id": "b" * 32},
        {"artifact_type": "config"},
    ],
)
def test_policy_targets_are_validated(policy_type: type, change: dict) -> None:
    target = metric(1)
    artifact = policy_type.model.draft({"position": 7}, "annotation.json", 1).finalize()
    reference = RequiredReference(
        **(
            {
                "address": address(0),
                "artifact_id": target.artifact_id,
                "artifact_type": "metric",
            }
            | change
        )
    )
    policy_type.required_references = staticmethod(
        lambda _artifact, _context: (reference,)
    )
    with pytest.raises(ValueError, match="referent"):
        policy_type.resolve_required_references(
            artifact, ReferenceContext({address(0): target})
        )


def test_policy_rejects_duplicate_targets_and_unfinalized_sources(
    policy_type: type,
) -> None:
    target = metric(1)
    reference = RequiredReference(
        address=address(0), artifact_id=target.artifact_id, artifact_type="metric"
    )
    policy_type.required_references = staticmethod(
        lambda _artifact, _context: (reference, reference)
    )
    artifact = policy_type.model.draft({"position": 7}, "annotation.json", 1)
    context = ReferenceContext({address(0): target})
    with pytest.raises(ValueError, match="finalized"):
        policy_type.resolve_required_references(artifact, context)
    artifact.finalize()
    with pytest.raises(ValueError, match="duplicate"):
        policy_type.resolve_required_references(artifact, context)


def test_reference_context_snapshots_and_validates_direct_parents() -> None:
    target = metric(1)
    values = {address(0): target}
    context = ReferenceContext(values, (address(0),))
    values.clear()
    assert context.artifacts[address(0)] is target
    with pytest.raises(TypeError):
        context.artifacts[address(1)] = target
    with pytest.raises(ValueError, match="admitted"):
        ReferenceContext({}, (address(0),))
    assert address(0).key == '["producer","data",0]'
    with pytest.raises(ValidationError):
        OccurrenceAddress(invocation_id="producer", role="data", index=True)


def test_policy_cannot_mutate_protected_source_identity(policy_type: type) -> None:
    artifact = policy_type.model.draft({"position": 7}, "annotation.json", 1).finalize()

    def mutate(
        source: Artifact, _context: ReferenceContext
    ) -> tuple[RequiredReference, ...]:
        source.metadata["changed"] = True
        return ()

    policy_type.required_references = staticmethod(mutate)
    with pytest.raises(ArtifactIntegrityError):
        policy_type.resolve_required_references(artifact, ReferenceContext({}))
