"""Production occurrence projection compared with qualified scientific cases."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pytest
from pydantic import ValidationError
from qualification.lineage import fixtures
from qualification.lineage.projection import BoundaryProjection as QualifiedProjection

from artisan.execution.exceptions import ArtifactValidationError, LineageIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.schemas import (
    Artifact,
    ArtifactResult,
    ExecutionConfigArtifact,
    LineageMapping,
    OutputSpec,
)
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.provenance.occurrence import (
    InputReference,
    OccurrenceAddress,
    ReferenceContext,
    RequiredReference,
)
from artisan.schemas.provenance.projection import (
    AcceptedOccurrence,
    BoundaryInputAttribution,
    BoundaryProjection,
    FrozenAssociation,
    FrozenBoundaryContext,
    FrozenReferences,
)


def address(owner: str, role: str = "result", index: int = 0) -> OccurrenceAddress:
    return OccurrenceAddress(invocation_id=owner, role=role, index=index)


def derive(
    graph: OccurrenceGraph,
    invocation: str,
    artifact: Artifact,
    inputs: Mapping[str, Sequence[OccurrenceAddress]],
    *,
    role: str = "result",
) -> OccurrenceAddress:
    result = ArtifactResult()
    result.add_artifact(
        role,
        artifact,
        sources={
            source_role: [
                InputReference(
                    invocation_id=invocation,
                    role=source_role,
                    index=index,
                    artifact_id=graph.nodes[graph.original(key)].artifact.artifact_id,
                )
                for index, key in enumerate(keys)
            ]
            for source_role, keys in inputs.items()
        },
    )
    return graph.record_result(
        invocation,
        inputs,
        result,
        {
            role: OutputSpec(
                artifact_type=artifact.artifact_type,
                derives_from={"inputs": list(inputs)},
            )
        },
    )[role][0]


def specs(
    graph: OccurrenceGraph, exports: Mapping[str, Sequence[OccurrenceAddress]]
) -> dict[str, OutputSpec]:
    return {
        role: OutputSpec(artifact_type=graph.nodes[values[0]].artifact.artifact_type)
        for role, values in exports.items()
    }


class _ProductionFixtureGraph:
    """Adapt qualification fixture construction, keeping execution in production."""

    def __init__(self) -> None:
        self.graph = OccurrenceGraph()
        self.addresses: dict[str, OccurrenceAddress] = {}
        self.labels: dict[OccurrenceAddress, str] = {}
        self.projection: BoundaryProjection | None = None

    def _register(self, selected: OccurrenceAddress, label: str | None = None) -> str:
        key = label or selected.key
        self.addresses[key] = selected
        self.labels[selected] = key
        return key

    @property
    def nodes(self) -> Mapping[str, Any]:
        return {self.labels[key]: node for key, node in self.graph.nodes.items()}

    def add_input(self, role: str, artifact: Artifact) -> str:
        return self._register(self.graph.add_input(role, artifact))

    def add_context_input(self, key: str, artifact: Artifact) -> str:
        return self._register(
            self.graph.add_context_input(address("captured", key), artifact), key
        )

    def add_alias(self, invocation: str, role: str, source: str) -> str:
        return self._register(
            self.graph.add_alias(invocation, role, self.addresses[source])
        )

    def record_result(
        self,
        invocation: str,
        inputs: Mapping[str, Sequence[str]],
        result: ArtifactResult,
        output_specs: dict[str, OutputSpec],
    ) -> dict[str, tuple[str, ...]]:
        outputs = self.graph.record_result(
            invocation,
            {
                role: tuple(self.addresses[key] for key in values)
                for role, values in inputs.items()
            },
            result,
            output_specs,
        )
        return {
            role: tuple(self._register(key) for key in values)
            for role, values in outputs.items()
        }

    def project(
        self,
        exports: Mapping[str, Sequence[str]],
        output_specs: dict[str, OutputSpec],
        *,
        frozen_context: Any = None,
    ) -> QualifiedProjection:
        context = (
            FrozenBoundaryContext()
            if frozen_context is None
            else FrozenBoundaryContext(
                associations=tuple(
                    FrozenAssociation(
                        primary=self.addresses[item.primary],
                        artifact_type=item.artifact_type,
                        members=tuple(self.addresses[key] for key in item.members),
                    )
                    for item in frozen_context.associations
                ),
                references=tuple(
                    FrozenReferences(
                        source=self.addresses[item.config],
                        members=tuple(self.addresses[key] for key in item.members),
                    )
                    for item in frozen_context.references
                ),
            )
        )
        projection = self.graph.project(
            {
                role: tuple(self.addresses[key] for key in values)
                for role, values in exports.items()
            },
            output_specs,
            frozen_context=context,
        )
        self.projection = projection
        labels = self.labels
        document = {
            "boundary_inputs": {
                group.role: [labels[key] for key in group.occurrences]
                for group in projection.boundary_inputs
            },
            "occurrences": [
                {
                    "key": labels[item.address],
                    "artifact_id": item.artifact_id,
                    "artifact_type": item.artifact_type,
                }
                for item in projection.occurrences
            ],
            "exports": {
                group.role: [labels[key] for key in group.occurrences]
                for group in projection.exports
            },
            "support": [labels[key] for key in projection.support],
            **{
                name: [
                    {"source": labels[item.source], "target": labels[item.target]}
                    for item in getattr(projection, name)
                ]
                for name in ("parents", "associations", "references")
            },
            "frozen_context": {
                "associations": [
                    {
                        "primary": labels[item.primary],
                        "artifact_type": item.artifact_type,
                        "members": [labels[key] for key in item.members],
                    }
                    for item in context.associations
                ],
                "references": [
                    {
                        "config": labels[item.source],
                        "members": [labels[key] for key in item.members],
                    }
                    for item in context.references
                ],
            },
        }
        return QualifiedProjection.model_validate(document)


@pytest.mark.parametrize(
    "factory",
    [
        fixtures.prediction_measurement_case,
        fixtures.alternating_reducer_case,
        fixtures.equal_content_branches_case,
        fixtures.alias_identity_case,
        fixtures.required_support_case,
        fixtures.frozen_configuration_case,
    ],
)
def test_qualified_cases_use_production_graph(
    factory: Callable[[], fixtures.ProjectionCase],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = factory()
    created = []

    def construct(_policies: object) -> _ProductionFixtureGraph:
        instance = _ProductionFixtureGraph()
        created.append(instance)
        return instance

    monkeypatch.setattr(fixtures, "OccurrenceGraph", construct)
    actual = factory()
    assert actual.projection == expected.projection
    assert actual.payloads() == expected.payloads()
    assert isinstance(created[0].projection, BoundaryProjection)


def test_collapsed_parent_does_not_fabricate_association() -> None:
    graph = OccurrenceGraph()
    source = graph.add_input("structure", fixtures.structure(0))
    hidden = derive(graph, "predict", fixtures.structure(1), {"structure": [source]})
    metric = derive(
        graph, "measure", fixtures.metric({"score": 1}), {"structure": [hidden]}
    )
    exports = {"structure": [source], "metrics": [metric]}
    projection = graph.project(exports, specs(graph, exports))
    assert {(edge.source, edge.target) for edge in projection.parents} == {
        (source, metric)
    }
    assert not projection.associations
    assert hidden not in {item.address for item in projection.occurrences}


def test_distinct_consuming_slots_can_share_one_explicit_producer() -> None:
    graph = OccurrenceGraph()
    source = graph.add_input("structure", fixtures.structure(0))
    metric = derive(
        graph, "combine", fixtures.metric({"score": 1}), {"data": [source, source]}
    )
    assert graph.nodes[metric].parents == (source,)
    with pytest.raises(ValueError, match="ambiguous"):
        graph.visible_result(
            "outer",
            {"data": [source, source]},
            {"metrics": [metric]},
            {"metrics": OutputSpec()},
        )


def test_visible_lineage_uses_exact_inputs_and_sibling_output_aliases() -> None:
    graph = OccurrenceGraph()
    first = graph.add_input("data", fixtures.structure(0))
    second = graph.add_input("data", fixtures.structure(0))
    final = derive(graph, "predict", fixtures.structure(1), {"data": [second]})
    hidden = derive(graph, "internal", fixtures.structure(2), {"data": [final]})
    measured = derive(
        graph, "measure", fixtures.metric({"score": 1}), {"data": [hidden]}
    )
    alias = graph.add_alias("route", "structure", final)
    exports = {"structure": [alias, final], "metrics": [measured, measured]}
    result = graph.visible_result(
        "outer", {"data": [first, second]}, exports, specs(graph, exports)
    )
    assert [mapping.source_input_index for mapping in result.lineage["structure"]] == [
        1,
        1,
    ]
    assert all(
        mapping.source_invocation == "outer" for mapping in result.lineage["structure"]
    )
    assert [
        (mapping.draft_index, mapping.source_role, mapping.source_output_index)
        for mapping in result.lineage["metrics"]
    ] == [(0, "structure", 0), (1, "structure", 0)]
    assert result.artifacts["structure"] == [graph.nodes[final].artifact] * 2
    passthrough = graph.visible_result(
        "outer",
        {"data": [first, second]},
        {"output": [second]},
        {"output": OutputSpec()},
    )
    assert passthrough.lineage["output"][0].source_input_index == 1


def test_invalid_result_does_not_claim_invocation_or_change_graph() -> None:
    graph = OccurrenceGraph()
    first = graph.add_input("data", fixtures.structure(0))
    second = graph.add_input("data", fixtures.structure(0))
    result = ArtifactResult(
        artifacts={"result": [fixtures.metric({"score": 1})]},
        lineage={
            "result": [
                LineageMapping(
                    draft_index=0,
                    source_role="data",
                    source_artifact_id=graph.nodes[first].artifact.artifact_id,
                )
            ]
        },
    )
    output_specs = {"result": OutputSpec(derives_from={"inputs": ["data"]})}
    with pytest.raises(LineageIntegrityError, match="ambiguous"):
        graph.record_result("measure", {"data": [first, second]}, result, output_specs)
    assert len(graph.nodes) == 2
    derive(graph, "measure", fixtures.metric({"score": 1}), {"data": [second]})


def test_derivation_cycle_is_rejected_atomically() -> None:
    graph = OccurrenceGraph()
    result = ArtifactResult(
        artifacts={"data": [fixtures.structure(0), fixtures.structure(1)]},
        lineage={
            "data": [
                LineageMapping(
                    draft_index=0, source_role="data", source_output_index=1
                ),
                LineageMapping(
                    draft_index=1, source_role="data", source_output_index=0
                ),
            ]
        },
    )
    with pytest.raises(ValueError, match="acyclic"):
        graph.record_result(
            "cycle",
            {},
            result,
            {"data": OutputSpec(derives_from={"outputs": ["data"]})},
        )
    assert not graph.nodes


def test_relation_policy_retains_support_without_embedded_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def policy(
        _artifact: Artifact, context: ReferenceContext
    ) -> tuple[RequiredReference, ...]:
        return tuple(
            RequiredReference(
                address=key,
                artifact_id=context.artifacts[key].artifact_id,
                artifact_type="csv",
            )
            for key in context.direct_parents
        )

    monkeypatch.setattr(
        ArtifactTypeDef.get("config"), "required_references", staticmethod(policy)
    )
    graph = OccurrenceGraph()
    source = graph.add_input("structure", fixtures.structure(0))
    hidden = derive(graph, "predict", fixtures.structure(1), {"structure": [source]})
    config = ExecutionConfigArtifact.draft(
        {"selection": "best"}, "config.json", 1
    ).finalize()
    final = derive(graph, "configure", config, {"structure": [hidden]})
    projection = graph.project(
        {"config": [final]}, {"config": OutputSpec(artifact_type="config")}
    )
    assert projection.support == (hidden,)
    assert [(item.source, item.target) for item in projection.references] == [
        (final, hidden)
    ]
    assert not config.get_artifact_references()


def test_reference_cycles_terminate_without_changing_derivation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets = {}

    def policy(
        artifact: Artifact, context: ReferenceContext
    ) -> tuple[RequiredReference, ...]:
        target = targets[artifact.artifact_id]
        return (
            RequiredReference(
                address=target,
                artifact_id=context.artifacts[target].artifact_id,
                artifact_type="csv",
            ),
        )

    monkeypatch.setattr(
        ArtifactTypeDef.get("csv"), "required_references", staticmethod(policy)
    )
    graph = OccurrenceGraph()
    first = derive(graph, "first", fixtures.structure(1), {})
    second = derive(graph, "second", fixtures.structure(2), {})
    targets.update(
        {
            graph.nodes[first].artifact.artifact_id: second,
            graph.nodes[second].artifact.artifact_id: first,
        }
    )
    projection = graph.project({"structure": [first]}, {"structure": OutputSpec()})
    assert projection.support == (second,)
    assert {(edge.source, edge.target) for edge in projection.references} == {
        (first, second),
        (second, first),
    }
    assert not projection.parents


def test_frozen_context_preserves_exact_primary_and_captured_empty() -> None:
    graph = OccurrenceGraph()
    first = graph.add_input("structure", fixtures.structure(1))
    second = graph.add_input("structure", fixtures.structure(1))
    member = graph.add_context_input(
        address("upstream", "metric"), fixtures.metric({"score": 1})
    )
    context = FrozenBoundaryContext(
        associations=(
            FrozenAssociation(primary=first, artifact_type="metric", members=(member,)),
            FrozenAssociation(primary=second, artifact_type="metric", members=()),
        )
    )
    graph.validate_boundary_context(context)
    projection = graph.project(
        {"structure": [second]}, {"structure": OutputSpec()}, frozen_context=context
    )
    assert projection.frozen_context.associated(first, "metric") == (member,)
    assert projection.frozen_context.associated(second, "metric") == ()
    assert member in projection.boundary_addresses()
    with pytest.raises(ValueError, match="not captured"):
        context.associated(first, "config")
    wrong = FrozenBoundaryContext(
        associations=(
            FrozenAssociation(primary=first, artifact_type="config", members=(member,)),
        )
    )
    with pytest.raises(ValueError, match="wrong artifact type"):
        graph.validate_boundary_context(wrong)


def test_missing_or_mismatched_boundary_references_fail_before_execution() -> None:
    graph = OccurrenceGraph()
    target = graph.add_context_input(address("upstream"), fixtures.structure(1))
    config = ExecutionConfigArtifact.draft(
        {"input": {"$artifact": graph.nodes[target].artifact.artifact_id}},
        "config.json",
        0,
    ).finalize()
    primary = graph.add_input("config", config)
    with pytest.raises(ValueError, match="not captured"):
        graph.validate_boundary_context(FrozenBoundaryContext())
    wrong = FrozenBoundaryContext(
        references=(FrozenReferences(source=primary, members=()),)
    )
    with pytest.raises(ValueError, match="missing or ambiguous"):
        graph.validate_boundary_context(wrong)
    valid = FrozenBoundaryContext(
        references=(FrozenReferences(source=primary, members=(target,)),)
    )
    graph.validate_boundary_context(valid)


def test_external_producer_addresses_are_preserved_without_identity_merging() -> None:
    graph = OccurrenceGraph()
    artifact = fixtures.structure(1)
    first = graph.add_input("data", artifact, address=address("upstream", index=1))
    second = graph.add_input("data", artifact, address=address("upstream", index=2))
    exports = {"structure": [second, first, second]}
    projection = graph.project(exports, specs(graph, exports))
    assert projection.export_roles()["structure"] == (second, first, second)
    assert len(projection.occurrences) == 2
    assert not projection.content_edges()
    with pytest.raises(ValueError, match="conflicting"):
        graph.add_input("other", fixtures.structure(2), address=first)


def test_empty_optional_export_is_explicit_and_required_empty_fails() -> None:
    graph = OccurrenceGraph()
    projection = graph.project({"metrics": []}, {"metrics": OutputSpec(required=False)})
    assert projection.export_roles() == {"metrics": ()}
    assert not projection.occurrences
    assert projection != graph.project({}, {})
    assert graph.visible_result(
        "empty", {}, {"metrics": []}, {"metrics": OutputSpec(required=False)}
    ).lineage == {"metrics": []}
    with pytest.raises(ArtifactValidationError, match="Empty artifact"):
        graph.project({"metrics": []}, {"metrics": OutputSpec()})
    with pytest.raises(ArtifactValidationError, match="Missing required"):
        graph.project({}, {"metrics": OutputSpec()})


def test_projection_is_deeply_immutable_and_roundtrips() -> None:
    graph = OccurrenceGraph()
    source = graph.add_input("data", fixtures.structure(1))
    projection = graph.project({"data": [source, source]}, {"data": OutputSpec()})
    assert (
        BoundaryProjection.model_validate_json(projection.model_dump_json())
        == projection
    )
    with pytest.raises(TypeError):
        projection.export_roles()["data"] = ()
    with pytest.raises(ValidationError):
        projection.exports[0].role = "changed"
    with pytest.raises(ValidationError):
        projection.exports[0].occurrences[0].index = 7
    document = projection.model_dump(mode="json")
    document["exports"][0]["occurrences"].clear()
    assert projection.export_roles()["data"] == (source, source)


@pytest.mark.parametrize(
    "corruption",
    [
        "foreign",
        "self",
        "cycle",
        "orphan",
        "duplicate_role",
        "association_without_parent",
        "unknown_field",
    ],
)
def test_projection_rejects_invalid_membership(corruption: str) -> None:
    graph = OccurrenceGraph()
    source = graph.add_input("data", fixtures.structure(1))
    final = derive(graph, "predict", fixtures.structure(2), {"data": [source]})
    projection = graph.project({"data": [final]}, {"data": OutputSpec()})
    document = projection.model_dump(mode="json")
    if corruption == "foreign":
        document["parents"][0]["source"] = address("foreign").model_dump()
    elif corruption == "self":
        document["parents"][0]["source"] = final.model_dump()
    elif corruption == "cycle":
        document["parents"].append(
            {"source": final.model_dump(), "target": source.model_dump()}
        )
    elif corruption == "orphan":
        document["exports"] = []
        document["support"] = [final.model_dump()]
    elif corruption == "duplicate_role":
        document["exports"] *= 2
    elif corruption == "association_without_parent":
        document["parents"] = []
    else:
        document["trusted"] = True
    with pytest.raises(ValidationError):
        BoundaryProjection.model_validate(document)


def test_explicit_empty_input_role_survives_projection() -> None:
    graph = OccurrenceGraph()
    graph.declare_input_role("optional")
    source = graph.add_input("data", fixtures.structure(1))
    projection = graph.project({"data": [source]}, {"data": OutputSpec()})
    assert projection.input_roles() == {"optional": (), "data": (source,)}
    assert [group.role for group in projection.boundary_inputs] == ["optional", "data"]
    with pytest.raises(ValidationError):
        graph.declare_input_role("")


def test_consuming_reference_cannot_become_a_producer_address() -> None:
    artifact = fixtures.structure(1)
    reference = InputReference(
        invocation_id="upstream", role="data", index=0, artifact_id=artifact.artifact_id
    )
    graph = OccurrenceGraph()
    with pytest.raises(ValueError, match="not an input reference"):
        graph.add_input("data", artifact, address=reference)
    source = graph.add_input("data", artifact, address=reference.address)
    projection = graph.project({"data": [source]}, {"data": OutputSpec()})
    occurrence = projection.occurrences[0].model_copy(update={"address": reference})
    with pytest.raises(ValidationError, match="Extra inputs"):
        BoundaryProjection(**(projection.model_dump() | {"occurrences": (occurrence,)}))


@pytest.mark.parametrize("version", [True, "1", 2])
def test_projection_version_is_closed_and_strict(version: object) -> None:
    projection = OccurrenceGraph().project({}, {})
    document = projection.model_dump() | {"schema_version": version}
    with pytest.raises(ValidationError):
        BoundaryProjection.model_validate(document)


def test_graph_requires_finalized_artifacts_without_inventing_origin() -> None:
    draft = ExecutionConfigArtifact.draft({}, "config.json", None)
    graph = OccurrenceGraph()
    with pytest.raises(ValueError, match="finalized"):
        graph.add_input("config", draft)
    with pytest.raises(ValueError, match="finalized"):
        derive(graph, "create", draft, {})
    assert draft.artifact_id is None
    assert draft.origin_step_number is None
    assert not graph.nodes


def test_unregistered_reference_policy_is_rejected_at_graph_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delattr(ArtifactTypeDef.get("csv"), "required_references")
    with pytest.raises(TypeError, match="no required-reference policy"):
        OccurrenceGraph().add_input("data", fixtures.structure(1))


def test_worker_cannot_claim_uncaptured_external_support() -> None:
    graph = OccurrenceGraph()
    external = graph.add_context_input(address("external"), fixtures.structure(1))
    config = ExecutionConfigArtifact.draft(
        {"source": {"$artifact": graph.nodes[external].artifact.artifact_id}},
        "config.json",
        1,
    ).finalize()
    final = derive(graph, "configure", config, {})
    with pytest.raises(ValueError, match="outside captured input closure"):
        graph.project({"config": [final]}, {"config": OutputSpec()})
    with pytest.raises(ValueError, match="outside captured input closure"):
        graph.required_references(final)


def test_equal_content_inputs_keep_selected_accepted_producers() -> None:
    graph = OccurrenceGraph(boundary_invocation="current")
    artifact = fixtures.structure(1)
    first = graph.add_input("data", artifact)
    second = graph.add_input("data", artifact)
    attribution = tuple(
        BoundaryInputAttribution(
            boundary=slot,
            producer=AcceptedOccurrence(
                delivery_id="a" * 32,
                seal_digest="b" * 32,
                address=address("previous", index=index),
            ),
        )
        for index, slot in enumerate((first, second))
    )
    graph.validate_boundary_context(
        FrozenBoundaryContext(), input_attribution=attribution
    )
    projection = graph.project(
        {"data": [second, first, second]},
        {"data": OutputSpec()},
        input_attribution=attribution,
    )
    assert projection.input_attribution == attribution
    assert projection.export_roles()["data"] == (second, first, second)
    unscoped = graph.project({"data": [second, first, second]}, {"data": OutputSpec()})
    assert all(item.producer is None for item in unscoped.input_attribution)
    assert projection.input_context_digest != unscoped.input_context_digest
    swapped = tuple(
        BoundaryInputAttribution(
            boundary=slot, producer=attribution[1 - index].producer
        )
        for index, slot in enumerate((first, second))
    )
    changed = graph.project(
        {"data": [second, first, second]},
        {"data": OutputSpec()},
        input_attribution=swapped,
    )
    assert projection.input_context_digest != changed.input_context_digest


@pytest.mark.parametrize("change", ["missing", "duplicate", "foreign"])
def test_input_attribution_requires_exact_boundary_membership(change: str) -> None:
    graph = OccurrenceGraph()
    source = graph.add_input("data", fixtures.structure(1))
    item = BoundaryInputAttribution(boundary=source, producer=None)
    invalid = {
        "missing": (),
        "duplicate": (item, item),
        "foreign": (
            BoundaryInputAttribution(boundary=address("foreign"), producer=None),
        ),
    }[change]
    with pytest.raises(ValueError, match="every boundary occurrence exactly once"):
        graph.validate_boundary_context(
            FrozenBoundaryContext(), input_attribution=invalid
        )
    with pytest.raises(ValueError, match="every boundary occurrence exactly once"):
        graph.project(
            {"data": [source]}, {"data": OutputSpec()}, input_attribution=invalid
        )


def test_admission_digest_binds_current_slots_separately_from_cache_identity() -> None:
    first, second = (
        OccurrenceGraph(boundary_invocation="one"),
        OccurrenceGraph(boundary_invocation="two"),
    )
    artifact = fixtures.structure(1)
    first_slot, second_slot = (
        first.add_input("data", artifact),
        second.add_input("data", artifact),
    )
    first_projection = first.project({"data": [first_slot]}, {"data": OutputSpec()})
    second_projection = second.project({"data": [second_slot]}, {"data": OutputSpec()})
    assert (
        first_projection.input_context_digest != second_projection.input_context_digest
    )
    assert (
        first_projection.occurrences[0].artifact_id
        == second_projection.occurrences[0].artifact_id
    )
