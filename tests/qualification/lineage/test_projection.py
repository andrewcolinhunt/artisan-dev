"""Executable boundary semantics with real finalized Artisan artifacts."""

from __future__ import annotations

from collections.abc import Mapping

import pytest
from pydantic import ValidationError

from artisan.execution.exceptions import ArtifactValidationError
from artisan.schemas import (
    ArtifactResult,
    CSVArtifact,
    ExecutionConfigArtifact,
    LineageMapping,
    MetricArtifact,
    OutputSpec,
)

from .fixtures import (
    alternating_reducer_case,
    configuration_references,
    derive,
    equal_content_branches_case,
    metric,
    prediction_measurement_case,
    required_support_case,
    structure,
)
from .graph import Node, OccurrenceGraph, PreciseLineageMapping, RequiredReference
from .projection import (
    BoundaryProjection,
    FrozenAssociation,
    FrozenContext,
    FrozenReferences,
    Relation,
)


def pairs(relations: tuple[Relation, ...]) -> set[tuple[str, str]]:
    """Compare exact relations independently of their serialized row order."""
    return {(edge.source, edge.target) for edge in relations}


def test_prediction_retains_direct_measurement_subject() -> None:
    case = prediction_measurement_case()
    projection = case.projection
    source = projection.boundary_inputs["source"][0]
    predicted = projection.exports["structure"][0]
    measured = projection.exports["metrics"][0]
    assert len(projection.occurrences) == 3
    assert pairs(projection.parents) == {(source, predicted), (predicted, measured)}
    assert pairs(projection.associations) == pairs(projection.parents)
    assert not projection.support


def test_fixed_alternation_exports_final_geometry_and_ordinary_reducer_metric() -> None:
    case = alternating_reducer_case()
    projection = case.projection
    source = projection.boundary_inputs["structure"][0]
    final = projection.exports["structure"][0]
    summary = projection.exports["metrics"][0]
    assert len(projection.occurrences) == 3
    assert pairs(projection.parents) == {
        (source, final),
        (source, summary),
        (final, summary),
    }
    assert pairs(projection.associations) == {(final, summary)}
    metrics = [
        value for value in case.artifacts.values() if isinstance(value, MetricArtifact)
    ]
    assert len(metrics) == 1
    assert metrics[0].values == {"scores": [1, 2, 3], "maximum": 3}
    assert not projection.support


def test_equal_content_exports_preserve_order_multiplicity_and_exact_branches() -> None:
    projection = equal_content_branches_case().projection
    source_a, source_b = projection.boundary_inputs["source"]
    branch_b, branch_a, repeated_b = projection.exports["metrics"]
    assert branch_b == repeated_b
    assert branch_a != branch_b
    ids = {item.key: item.artifact_id for item in projection.occurrences}
    assert ids[branch_a] == ids[branch_b]
    assert pairs(projection.parents) == {(source_a, branch_a), (source_b, branch_b)}
    assert pairs(projection.associations) == pairs(projection.parents)
    restored = BoundaryProjection.model_validate_json(projection.model_dump_json())
    assert restored == projection
    assert len(restored.exports["metrics"]) == 3


def test_alias_normalizes_to_selected_producer_and_does_not_guess_by_content() -> None:
    graph = OccurrenceGraph({"csv": None, "metric": None})
    first_input = graph.add_input("source", structure(0, name="a", step=0))
    second_input = graph.add_input("source", structure(1, name="b", step=0))
    original = derive(graph, "original", structure(2), {"source": [first_input]})
    equal_copy = derive(graph, "copy", structure(2), {"source": [second_input]})
    measured = derive(graph, "measure", metric({"score": 1}), {"structure": [original]})
    alias = graph.add_alias("route", "structure", original)
    second_alias = graph.add_alias("route_again", "structure", alias)
    projection = graph.project(
        {"structure": [second_alias, equal_copy, alias], "metrics": [measured]},
        {
            "structure": OutputSpec(artifact_type="csv"),
            "metrics": OutputSpec(artifact_type="metric"),
        },
    )
    assert projection.exports["structure"] == (original, equal_copy, original)
    assert {item.key for item in projection.occurrences}.isdisjoint(
        {alias, second_alias}
    )
    assert (original, measured) in pairs(projection.associations)
    assert (equal_copy, measured) not in pairs(projection.associations)
    assert graph.nodes[alias].artifact is graph.nodes[original].artifact
    assert graph.nodes[alias].artifact.origin_step_number == 1


def test_identity_preserving_call_is_distinct_and_keeps_equal_content_direct_edge() -> (
    None
):
    graph = OccurrenceGraph({"csv": None})
    source = graph.add_input("structure", structure(1, step=0))
    copied = derive(graph, "copy", structure(1, step=3), {"structure": [source]})
    projection = graph.project(
        {"structure": [copied]}, {"structure": OutputSpec(artifact_type="csv")}
    )
    assert copied != source
    assert (
        graph.nodes[copied].artifact.artifact_id
        == graph.nodes[source].artifact.artifact_id
    )
    assert graph.nodes[copied].artifact.origin_step_number == 3
    assert pairs(projection.parents) == {(source, copied)}
    assert pairs(projection.associations) == {(source, copied)}
    assert not projection.content_edges()


def test_collapsed_path_does_not_manufacture_direct_association() -> None:
    graph = OccurrenceGraph({"csv": None, "metric": None})
    source = graph.add_input("structure", structure(0, step=0))
    hidden = derive(graph, "refine", structure(1), {"structure": [source]})
    measured = derive(graph, "measure", metric({"score": 1}), {"structure": [hidden]})
    projection = graph.project(
        {"metrics": [measured]}, {"metrics": OutputSpec(artifact_type="metric")}
    )
    assert pairs(projection.parents) == {(source, measured)}
    assert not projection.associations
    assert {item.key for item in projection.occurrences} == {source, measured}


def test_existing_id_lineage_rejects_ambiguous_equal_content_inputs() -> None:
    graph = OccurrenceGraph({"csv": None, "metric": None})
    first = graph.add_input("structure", structure(1))
    second = graph.add_input("structure", structure(1))
    result = ArtifactResult(
        artifacts={"metrics": [metric({"score": 1})]},
        lineage={
            "metrics": [
                LineageMapping(
                    draft_index=0,
                    source_role="structure",
                    source_artifact_id=graph.nodes[first].artifact.artifact_id,
                )
            ]
        },
    )
    with pytest.raises(ValueError, match="ambiguous"):
        graph.record_result(
            "measure",
            {"structure": [first, second]},
            result,
            {
                "metrics": OutputSpec(
                    artifact_type="metric", derives_from={"inputs": ["structure"]}
                )
            },
        )
    measured = derive(graph, "exact", metric({"score": 1}), {"structure": [second]})
    assert graph.nodes[measured].parents == (second,)
    combined = derive(
        graph, "combine", metric({"score": 2}), {"structure": [first, second]}
    )
    assert graph.nodes[combined].parents == (first, second)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"source_input_index": 1}, "outside"),
        ({"source_invocation": "foreign"}, "another invocation"),
        ({"source_artifact_id": "f" * 32}, "identity does not match"),
    ],
)
def test_precise_lineage_rejects_wrong_owner_index_or_identity(
    change: dict[str, object], message: str
) -> None:
    graph = OccurrenceGraph({"csv": None, "metric": None})
    source = graph.add_input("structure", structure(1))
    fields = {
        "draft_index": 0,
        "source_role": "structure",
        "source_artifact_id": graph.nodes[source].artifact.artifact_id,
        "source_input_index": 0,
        "source_invocation": "measure",
    } | change
    result = ArtifactResult(
        artifacts={"metrics": [metric({"score": 1})]},
        lineage={"metrics": [PreciseLineageMapping(**fields)]},
    )
    with pytest.raises(ValueError, match=message):
        graph.record_result(
            "measure",
            {"structure": [source]},
            result,
            {
                "metrics": OutputSpec(
                    artifact_type="metric", derives_from={"inputs": ["structure"]}
                )
            },
        )


def test_existing_sibling_output_indices_preserve_equal_content_producer() -> None:
    graph = OccurrenceGraph({"csv": None, "metric": None})
    result = ArtifactResult(
        artifacts={
            "structure": [structure(1), structure(1)],
            "metrics": [metric({"score": 1})],
        },
        lineage={
            "structure": [],
            "metrics": [
                LineageMapping(
                    draft_index=0, source_role="structure", source_output_index=1
                )
            ],
        },
    )
    outputs = graph.record_result(
        "predict_and_measure",
        {},
        result,
        {
            "structure": OutputSpec(artifact_type="csv", derives_from={"inputs": []}),
            "metrics": OutputSpec(
                artifact_type="metric", derives_from={"outputs": ["structure"]}
            ),
        },
    )
    assert graph.nodes[outputs["metrics"][0]].parents == (outputs["structure"][1],)


def test_required_references_retain_fixed_point_support_without_public_exports() -> (
    None
):
    case = required_support_case()
    projection = case.projection
    assert len(projection.support) == 2
    assert len(projection.exports["configuration"]) == 1
    final = projection.exports["configuration"][0]
    geometry, configuration = projection.support
    assert pairs(projection.references) == {
        (final, configuration),
        (configuration, geometry),
    }
    assert len(case.staged_payloads()) == 3
    assert len(case.boundary_payloads()) == 1


def test_reference_policy_can_use_direct_relation_without_embedded_id() -> None:
    def parent_reference(
        node: Node, nodes: Mapping[str, Node]
    ) -> tuple[RequiredReference, ...]:
        return tuple(
            RequiredReference(key, nodes[key].artifact.artifact_id, "csv")
            for key in node.parents
        )

    graph = OccurrenceGraph({"csv": None, "config": parent_reference})
    source = graph.add_input("structure", structure(0))
    hidden = derive(graph, "predict", structure(1), {"structure": [source]})
    config = ExecutionConfigArtifact.draft(
        content={"selection": "best"}, original_name="config.json", step_number=1
    ).finalize()
    result = derive(graph, "configuration", config, {"structure": [hidden]})
    projection = graph.project(
        {"configuration": [result]},
        {"configuration": OutputSpec(artifact_type="config")},
    )
    assert projection.support == (hidden,)
    assert pairs(projection.references) == {(result, hidden)}
    assert not config.get_artifact_references()


def test_arbitrary_strings_are_not_configuration_references() -> None:
    graph = OccurrenceGraph({"csv": None, "config": configuration_references})
    source = graph.add_input("structure", structure(0))
    hidden = derive(graph, "predict", structure(1), {"structure": [source]})
    config = ExecutionConfigArtifact.draft(
        content={"description": graph.nodes[hidden].artifact.artifact_id},
        original_name="config.json",
        step_number=1,
    ).finalize()
    result = derive(graph, "configuration", config, {"structure": [hidden]})
    projection = graph.project(
        {"configuration": [result]},
        {"configuration": OutputSpec(artifact_type="config")},
    )
    assert not projection.support
    assert not projection.references


@pytest.mark.parametrize(
    "reference",
    [
        RequiredReference("foreign", "f" * 32, "csv"),
        RequiredReference("target", "f" * 32, "csv"),
        RequiredReference("target", "f" * 32, "metric"),
    ],
)
def test_required_reference_rejects_unresolved_or_mismatched_referent(
    reference: RequiredReference,
) -> None:
    graph = OccurrenceGraph({"csv": lambda _node, _nodes: (reference,)})
    target = graph.add_context_input("target", structure(1))
    with pytest.raises(ValueError, match="Required referent"):
        graph.project(
            {"structure": [target]}, {"structure": OutputSpec(artifact_type="csv")}
        )


def test_required_reference_cycles_terminate_without_derivation_cycles() -> None:
    targets: dict[str, str] = {}

    def reference(
        node: Node, nodes: Mapping[str, Node]
    ) -> tuple[RequiredReference, ...]:
        key = targets[node.key]
        return (RequiredReference(key, nodes[key].artifact.artifact_id, "csv"),)

    graph = OccurrenceGraph({"csv": reference})
    first = derive(graph, "first", structure(1), {})
    second = derive(graph, "second", structure(2), {})
    targets.update({first: second, second: first})
    projection = graph.project(
        {"structure": [first]}, {"structure": OutputSpec(artifact_type="csv")}
    )
    assert projection.support == (second,)
    assert pairs(projection.references) == {(first, second), (second, first)}
    assert not projection.parents


def test_missing_type_reference_policy_is_not_implicitly_no_references() -> None:
    graph = OccurrenceGraph({})
    source = graph.add_input("structure", structure(1))
    with pytest.raises(ValueError, match="no declared required-reference policy"):
        graph.project(
            {"structure": [source]}, {"structure": OutputSpec(artifact_type="csv")}
        )


def test_empty_optional_role_is_explicit_and_required_empty_is_failure() -> None:
    graph = OccurrenceGraph({"metric": None})
    projection = graph.project(
        {"metrics": []}, {"metrics": OutputSpec(artifact_type="metric", required=False)}
    )
    assert projection.exports == {"metrics": ()}
    assert not projection.occurrences
    assert BoundaryProjection.model_validate_json(
        projection.model_dump_json()
    ).exports == {"metrics": ()}
    with pytest.raises(ArtifactValidationError, match="Empty artifact list"):
        graph.project({"metrics": []}, {"metrics": OutputSpec(artifact_type="metric")})
    with pytest.raises(ArtifactValidationError, match="Missing required"):
        graph.project({}, {"metrics": OutputSpec(artifact_type="metric")})


def test_frozen_context_distinguishes_captured_empty_from_missing() -> None:
    graph = OccurrenceGraph({"csv": None})
    primary = graph.add_input("structure", structure(1))
    config = graph.add_context_input(
        "captured_config",
        ExecutionConfigArtifact.draft(
            content={}, original_name="config.json", step_number=0
        ).finalize(),
    )
    context = FrozenContext(
        associations=(
            FrozenAssociation(primary=primary, artifact_type="metric", members=()),
        ),
        references=(FrozenReferences(config=config, members=()),),
    )
    projection = graph.project(
        {"structure": [primary]},
        {"structure": OutputSpec(artifact_type="csv")},
        frozen_context=context,
    )
    restored = BoundaryProjection.model_validate_json(projection.model_dump_json())
    assert restored.frozen_context.associated(primary, "metric") == ()
    assert restored.frozen_context.referenced(config) == ()
    assert config in restored.boundary_keys()
    assert config not in {
        key for values in restored.boundary_inputs.values() for key in values
    }
    with pytest.raises(ValueError, match="not captured"):
        restored.frozen_context.associated(primary, "config")
    with pytest.raises(ValueError, match="not captured"):
        restored.frozen_context.referenced(primary)


def test_equal_content_inputs_keep_distinct_captured_association_membership() -> None:
    graph = OccurrenceGraph({"csv": None})
    first = graph.add_input("structure", structure(1))
    second = graph.add_input("structure", structure(1))
    associated = graph.add_context_input("captured_metric", metric({"score": 1}))
    context = FrozenContext(
        associations=(
            FrozenAssociation(
                primary=first, artifact_type="metric", members=(associated,)
            ),
            FrozenAssociation(primary=second, artifact_type="metric", members=()),
        )
    )
    projection = graph.project(
        {"structure": [second]},
        {"structure": OutputSpec(artifact_type="csv")},
        frozen_context=context,
    )
    assert projection.frozen_context.associated(first, "metric") == (associated,)
    assert projection.frozen_context.associated(second, "metric") == ()
    assert associated in projection.boundary_keys()


def test_unfinalized_boundary_input_is_rejected() -> None:
    graph = OccurrenceGraph({"csv": None})
    draft = CSVArtifact.draft(
        content=b"x\n1\n", original_name="input.csv", step_number=0
    )
    with pytest.raises(ValueError, match="Only finalized"):
        graph.add_input("structure", draft)


def test_sibling_derivation_cycle_is_rejected() -> None:
    graph = OccurrenceGraph({"csv": None})
    result = ArtifactResult(
        artifacts={"structure": [structure(1), structure(2)]},
        lineage={
            "structure": [
                LineageMapping(
                    draft_index=0, source_role="structure", source_output_index=1
                ),
                LineageMapping(
                    draft_index=1, source_role="structure", source_output_index=0
                ),
            ]
        },
    )
    with pytest.raises(ValueError, match="acyclic"):
        graph.record_result(
            "cyclic",
            {},
            result,
            {
                "structure": OutputSpec(
                    artifact_type="csv", derives_from={"outputs": ["structure"]}
                )
            },
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("unknown_field", "Extra inputs"),
        ("foreign_relation", "outside"),
        ("self_relation", "self-relations"),
        ("parent_cycle", "acyclic"),
        ("conflicting_types", "conflicting artifact types"),
        ("orphan_support", "reachable"),
    ],
)
def test_projection_rejects_corrupted_transport(mutation: str, message: str) -> None:
    projection = prediction_measurement_case().projection
    document = projection.model_dump(mode="json")
    source = projection.boundary_inputs["source"][0]
    final = projection.exports["structure"][0]
    measured = projection.exports["metrics"][0]
    if mutation == "unknown_field":
        document["trusted"] = True
    elif mutation == "foreign_relation":
        document["parents"].append({"source": "foreign", "target": final})
    elif mutation == "self_relation":
        document["parents"].append({"source": final, "target": final})
    elif mutation == "parent_cycle":
        document["parents"].append({"source": final, "target": source})
    elif mutation == "conflicting_types":
        document["occurrences"][2]["artifact_id"] = document["occurrences"][1][
            "artifact_id"
        ]
    else:
        document["exports"].pop("metrics")
        document["support"] = [measured]
    with pytest.raises(ValidationError, match=message):
        BoundaryProjection.model_validate(document)
