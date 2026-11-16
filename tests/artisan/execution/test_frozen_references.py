"""Generic frozen dependencies preserve exact referents without retaining ancestors."""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from qualification.lineage.fixtures import metric, structure

from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.schemas import Artifact, ExecutionConfigArtifact, OutputSpec
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.provenance.occurrence import (
    OccurrenceAddress,
    ReferenceContext,
    RequiredReference,
)
from artisan.schemas.provenance.projection import (
    FrozenBoundaryContext,
    FrozenReferences,
)


def _address(name: str) -> OccurrenceAddress:
    return OccurrenceAddress(invocation_id=name, role="data", index=0)


def _reference(
    context: ReferenceContext, address: OccurrenceAddress
) -> RequiredReference:
    artifact = context.artifacts[address]
    assert artifact.artifact_id is not None
    return RequiredReference(
        address=address,
        artifact_id=artifact.artifact_id,
        artifact_type=artifact.artifact_type,
    )


def _direct_policy(
    _artifact: Artifact, context: ReferenceContext
) -> tuple[RequiredReference, ...]:
    return tuple(_reference(context, parent) for parent in context.direct_parents)


def test_bare_nonconfig_input_preserves_relation_referents_without_association(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ArtifactTypeDef.get("metric"),
        "required_references",
        staticmethod(_direct_policy),
    )
    graph = OccurrenceGraph()
    primary = graph.add_input("metric", metric({"score": 1}))
    target = graph.add_context_input(_address("structure"), structure(1))
    context = FrozenBoundaryContext(
        references=(
            FrozenReferences(
                source=primary, members=(target,), direct_parents=(target,)
            ),
        )
    )
    graph.validate_boundary_context(context)
    projection = graph.project(
        {"metric": [primary]}, {"metric": OutputSpec()}, frozen_context=context
    )
    assert projection.boundary_addresses() == {primary, target}
    assert [(edge.source, edge.target) for edge in projection.references] == [
        (primary, target)
    ]
    assert not projection.support
    assert not projection.parents
    assert not projection.associations
    wrong = FrozenBoundaryContext(
        references=(FrozenReferences(source=primary, members=(target,)),)
    )
    with pytest.raises(ValueError, match="exact type dependencies"):
        graph.validate_boundary_context(wrong)


@pytest.mark.parametrize("cyclic", [False, True])
def test_recursive_boundary_reference_closure_requires_each_capture(
    monkeypatch: pytest.MonkeyPatch, cyclic: bool
) -> None:
    graph = OccurrenceGraph()
    first = graph.add_input("metric", metric({"score": 1}))
    second = graph.add_context_input(_address("second"), metric({"score": 2}))
    target = (
        first if cyclic else graph.add_context_input(_address("last"), structure(3))
    )
    targets = {
        graph.nodes[first].artifact.artifact_id: second,
        graph.nodes[second].artifact.artifact_id: target,
    }

    def policy(
        artifact: Artifact, context: ReferenceContext
    ) -> tuple[RequiredReference, ...]:
        return (_reference(context, targets[artifact.artifact_id]),)

    monkeypatch.setattr(
        ArtifactTypeDef.get("metric"), "required_references", staticmethod(policy)
    )
    captures = (
        FrozenReferences(source=first, members=(second,)),
        FrozenReferences(source=second, members=(target,)),
    )
    context = FrozenBoundaryContext(references=captures)
    graph.validate_boundary_context(context)
    projection = graph.project(
        {"metric": [first]}, {"metric": OutputSpec()}, frozen_context=context
    )
    assert projection.boundary_addresses() == {first, second, target}
    assert not projection.parents
    with pytest.raises(ValueError, match="not captured"):
        graph.validate_boundary_context(FrozenBoundaryContext(references=captures[:1]))


def test_reference_policy_requires_captured_empty_and_parents_do_not_expand_closure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ArtifactTypeDef.get("metric"),
        "required_references",
        staticmethod(_direct_policy),
    )
    graph = OccurrenceGraph()
    primary = graph.add_input("metric", metric({"score": 1}))
    discarded = graph.add_context_input(_address("discarded"), structure(1))
    with pytest.raises(ValueError, match="not captured"):
        graph.validate_boundary_context(FrozenBoundaryContext())
    empty = FrozenBoundaryContext(
        references=(FrozenReferences(source=primary, members=()),)
    )
    graph.validate_boundary_context(empty)
    assert graph.project({}, {}, frozen_context=empty).boundary_addresses() == {primary}
    invalid = FrozenBoundaryContext(
        references=(
            FrozenReferences(source=primary, members=(), direct_parents=(discarded,)),
        )
    )
    with pytest.raises(ValueError, match="within the frozen input closure"):
        graph.validate_boundary_context(invalid)


def test_equal_content_referents_require_exact_capture_and_original_parent_evidence() -> (
    None
):
    graph = OccurrenceGraph()
    artifact = structure(1)
    first = graph.add_input("structure", artifact)
    second = graph.add_input("structure", artifact)
    config = ExecutionConfigArtifact.draft(
        {"source": {"$artifact": artifact.artifact_id}}, "config.json", 1
    ).finalize()
    primary = graph.add_input("config", config)
    ambiguous = FrozenBoundaryContext(
        references=(
            FrozenReferences(
                source=primary, members=(first, second), direct_parents=(first, second)
            ),
        )
    )
    with pytest.raises(ValueError, match="ambiguous"):
        graph.validate_boundary_context(ambiguous)
    exact = FrozenBoundaryContext(
        references=(
            FrozenReferences(
                source=primary, members=(second,), direct_parents=(second,)
            ),
        )
    )
    graph.validate_boundary_context(exact)
    assert graph.required_references(primary, frozen_context=exact)[0].address == second
    changed = FrozenBoundaryContext(
        references=(
            FrozenReferences(
                source=primary, members=(first,), direct_parents=(second,)
            ),
        )
    )
    with pytest.raises(ValueError, match="exact type dependencies"):
        graph.validate_boundary_context(changed)


def test_original_parent_evidence_cannot_contain_derivation_cycles() -> None:
    first, second = _address("first"), _address("second")
    with pytest.raises(ValidationError, match="derivation must be acyclic"):
        FrozenBoundaryContext(
            references=(
                FrozenReferences(
                    source=first, members=(second,), direct_parents=(second,)
                ),
                FrozenReferences(
                    source=second, members=(first,), direct_parents=(first,)
                ),
            )
        )
