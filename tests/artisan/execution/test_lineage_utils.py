"""Exact output resolution and deterministic declared-parent grouping."""

from __future__ import annotations

import pytest

from artisan.execution.exceptions import LineageIntegrityError
from artisan.execution.lineage.builder import build_edges
from artisan.schemas import LineageMapping, MetricArtifact
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


def artifact(value: int = 0, name: str = "same.json") -> MetricArtifact:
    return MetricArtifact.draft(
        content={"score": value}, original_name=name, step_number=1
    ).finalize()


def mapping(
    index: int = 0, role: str = "data", source: str = "a" * 32
) -> LineageMapping:
    return LineageMapping(
        draft_index=index, source_role=role, source_artifact_id=source
    )


def types_for(*artifacts: MetricArtifact) -> dict[str, str]:
    return {
        "a" * 32: "data",
        "b" * 32: "data",
        **{a.artifact_id: a.artifact_type for a in artifacts},
    }


def test_duplicate_names_resolve_to_exact_occurrences() -> None:
    first, second = artifact(1), artifact(2)
    edges = build_edges(
        {"out": [mapping(1), mapping(0, source="b" * 32)]},
        {"out": [first, second]},
        types_for(first, second),
    )
    assert {(e.source, e.target) for e in edges} == {
        ("a" * 32, second.artifact_id),
        ("b" * 32, first.artifact_id),
    }
    assert all(e.group_id is None for e in edges)


def test_same_role_fan_in_targets_one_occurrence() -> None:
    target = artifact()
    edges = build_edges(
        {"out": [mapping(), mapping(source="b" * 32)]},
        {"out": [target]},
        types_for(target),
    )
    assert {e.source for e in edges} == {"a" * 32, "b" * 32}
    assert {e.target for e in edges} == {target.artifact_id}
    expected_group = compute_content_digest(
        canonical_json_bytes(
            {
                "domain": "lineage-parents-v1",
                "parents": [("data", "data", "a" * 32), ("data", "data", "b" * 32)],
            }
        )
    )
    assert {e.group_id for e in edges} == {expected_group}


def test_sibling_indices_resolve_without_occurrence_fallback() -> None:
    first, second, target = artifact(1), artifact(2), artifact(3)
    lineage = {
        "data": [],
        "out": [
            LineageMapping(draft_index=0, source_role="data", source_output_index=1)
        ],
    }
    edges = build_edges(
        lineage,
        {"data": [first, second], "out": [target]},
        types_for(first, second, target),
    )
    assert [(e.source, e.target) for e in edges] == [
        (second.artifact_id, target.artifact_id)
    ]


@pytest.mark.parametrize("kind", ["target", "source", "unfinished", "missing_type"])
def test_bad_references_fail_instead_of_dropping_edges(kind: str) -> None:
    target = artifact()
    lineage = {"out": [mapping()]}
    outputs = {"out": [target]}
    types = types_for(target)
    if kind == "target":
        lineage["out"] = [mapping(1)]
    elif kind == "source":
        lineage["out"] = [
            LineageMapping(draft_index=0, source_role="missing", source_output_index=0)
        ]
    elif kind == "unfinished":
        outputs["out"] = [
            MetricArtifact.draft(content={}, original_name="draft", step_number=1)
        ]
    else:
        del types["a" * 32]
    with pytest.raises(LineageIntegrityError):
        build_edges(lineage, outputs, types)


def test_group_hash_depends_on_roles_and_types_but_not_order_or_unused_inputs() -> None:
    target = artifact()
    declarations = [mapping(role="primary"), mapping(role="other", source="b" * 32)]
    types = types_for(target)

    def group(mappings, artifact_types):
        return build_edges({"out": mappings}, {"out": [target]}, artifact_types)[
            0
        ].group_id

    baseline = group(declarations, types)
    assert group(declarations[::-1], types) == baseline
    assert group(declarations, {**types, "c" * 32: "reference"}) == baseline
    assert group([mapping(role="changed"), declarations[1]], types) != baseline
    assert group(declarations, {**types, "a" * 32: "metric"}) != baseline


def test_equivalent_sibling_parents_collapse_before_singleton_decision() -> None:
    parent, target = artifact(1), artifact(2)
    lineage = {
        "data": [],
        "out": [
            LineageMapping(draft_index=0, source_role="data", source_output_index=i)
            for i in range(2)
        ],
    }
    edges = build_edges(
        lineage, {"data": [parent, parent], "out": [target]}, types_for(parent, target)
    )
    assert len(edges) == 1
    assert edges[0].source == parent.artifact_id
    assert edges[0].group_id is None


def test_identical_targets_keep_distinct_parent_sets() -> None:
    target = artifact()
    lineage = {
        "out": [
            mapping(0, "primary"),
            mapping(0, "other", "b" * 32),
            mapping(1, "primary"),
            mapping(1, "other", "c" * 32),
        ]
    }
    edges = build_edges(
        lineage, {"out": [target, target]}, {**types_for(target), "c" * 32: "data"}
    )
    groups: dict[str, set[str]] = {}
    for edge in edges:
        assert edge.target == target.artifact_id
        groups.setdefault(edge.group_id, set()).add(edge.source)
    assert set(map(frozenset, groups.values())) == {
        frozenset(["a" * 32, "b" * 32]),
        frozenset(["a" * 32, "c" * 32]),
    }


def test_identity_neutral_self_id_relationship_is_preserved() -> None:
    target = artifact()
    edges = build_edges(
        {"out": [mapping(source=target.artifact_id)]},
        {"out": [target]},
        types_for(target),
    )
    assert edges[0].source == edges[0].target == target.artifact_id
