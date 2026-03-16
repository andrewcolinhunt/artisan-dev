"""Tests for composite provenance helpers."""

from __future__ import annotations

import json

import pytest

from artisan.composites.base.provenance import (
    _collect_composite_artifacts,
    _collect_composite_edges,
    update_ancestor_map,
    validate_required_roles,
)
from artisan.execution.models.execution_composite import CompositeIntermediates
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.specs.input_spec import InputSpec


def _edge(src: str, tgt: str, run_id: str = "r" * 32) -> ArtifactProvenanceEdge:
    return ArtifactProvenanceEdge(
        execution_run_id=run_id,
        source_artifact_id=src,
        target_artifact_id=tgt,
        source_artifact_type="metric",
        target_artifact_type="metric",
        source_role="data",
        target_role="data",
    )


def _art(aid: str) -> MetricArtifact:
    return MetricArtifact(
        artifact_id=aid,
        artifact_type="metric",
        content=json.dumps({"v": 1}).encode(),
        original_name="t",
        extension=".json",
        origin_step_number=0,
    )


# ---------------------------------------------------------------------------
# validate_required_roles
# ---------------------------------------------------------------------------


class TestValidateRequiredRoles:
    def test_passes_when_all_required_present(self):
        validate_required_roles(
            {"data": object()},
            {"data": InputSpec(required=True)},
        )

    def test_raises_when_required_missing(self):
        with pytest.raises(ValueError, match="Required input role 'data'"):
            validate_required_roles(
                {},
                {"data": InputSpec(required=True)},
            )

    def test_optional_role_can_be_missing(self):
        validate_required_roles(
            {},
            {"ref": InputSpec(required=False)},
        )


# ---------------------------------------------------------------------------
# update_ancestor_map
# ---------------------------------------------------------------------------


class TestUpdateAncestorMap:
    def test_first_operation(self):
        edges = [_edge("a" * 32, "b" * 32)]
        result = update_ancestor_map({}, edges, is_first=True)
        assert result["b" * 32] == ["a" * 32]

    def test_subsequent_inherits(self):
        ancestor_map = {"b" * 32: ["a" * 32]}
        edges = [_edge("b" * 32, "c" * 32)]
        result = update_ancestor_map(ancestor_map, edges, is_first=False)
        assert result["c" * 32] == ["a" * 32]

    def test_multiple_ancestors(self):
        ancestor_map = {"b" * 32: ["a" * 32, "x" * 32]}
        edges = [_edge("b" * 32, "c" * 32)]
        result = update_ancestor_map(ancestor_map, edges, is_first=False)
        assert set(result["c" * 32]) == {"a" * 32, "x" * 32}

    def test_unknown_source_ignored(self):
        result = update_ancestor_map({}, [_edge("z" * 32, "c" * 32)], is_first=False)
        assert "c" * 32 not in result

    def test_does_not_mutate_original(self):
        original = {"b" * 32: ["a" * 32]}
        result = update_ancestor_map(
            original, [_edge("b" * 32, "c" * 32)], is_first=False
        )
        assert "c" * 32 not in original
        assert "c" * 32 in result


# ---------------------------------------------------------------------------
# _collect_composite_edges
# ---------------------------------------------------------------------------


class TestCollectCompositeEdges:
    def _setup(self):
        edges = [
            [_edge("a" * 32, "b" * 32)],
            [_edge("b" * 32, "c" * 32)],
        ]
        ancestor_map = {"c" * 32: ["a" * 32]}
        art = _art("c" * 32)
        return edges, ancestor_map, {"data": [art]}

    def test_discard_returns_only_shortcuts(self):
        edges, ancestor_map, final = self._setup()
        result = _collect_composite_edges(
            edges, ancestor_map, final, "r" * 32, CompositeIntermediates.DISCARD
        )
        assert all(e.step_boundary for e in result)
        assert len(result) == 1
        assert result[0].source_role == "composite_input"

    def test_persist_internal_edges_step_boundary_false(self):
        edges, ancestor_map, final = self._setup()
        result = _collect_composite_edges(
            edges, ancestor_map, final, "r" * 32, CompositeIntermediates.PERSIST
        )
        internal = [e for e in result if e.source_role != "composite_input"]
        shortcuts = [e for e in result if e.source_role == "composite_input"]
        assert len(internal) == 2
        assert all(not e.step_boundary for e in internal)
        assert len(shortcuts) == 1
        assert all(e.step_boundary for e in shortcuts)

    def test_expose_internal_edges_step_boundary_true(self):
        """Key difference from chains: EXPOSE sets step_boundary=True on internals."""
        edges, ancestor_map, final = self._setup()
        result = _collect_composite_edges(
            edges, ancestor_map, final, "r" * 32, CompositeIntermediates.EXPOSE
        )
        internal = [e for e in result if e.source_role != "composite_input"]
        shortcuts = [e for e in result if e.source_role == "composite_input"]
        assert len(internal) == 2
        assert all(e.step_boundary for e in internal)
        assert len(shortcuts) == 1
        assert all(e.step_boundary for e in shortcuts)

    def test_persist_vs_expose_distinction(self):
        """PERSIST and EXPOSE produce different step_boundary on internal edges."""
        edges, ancestor_map, final = self._setup()
        persist_result = _collect_composite_edges(
            edges, ancestor_map, final, "r" * 32, CompositeIntermediates.PERSIST
        )
        expose_result = _collect_composite_edges(
            edges, ancestor_map, final, "r" * 32, CompositeIntermediates.EXPOSE
        )
        persist_internal = [
            e for e in persist_result if e.source_role != "composite_input"
        ]
        expose_internal = [
            e for e in expose_result if e.source_role != "composite_input"
        ]
        assert not any(e.step_boundary for e in persist_internal)
        assert all(e.step_boundary for e in expose_internal)


# ---------------------------------------------------------------------------
# _collect_composite_artifacts
# ---------------------------------------------------------------------------


class TestCollectCompositeArtifacts:
    def test_discard_returns_last_only(self):
        all_arts = [
            {"data": [_art("a" * 32)]},
            {"result": [_art("b" * 32)]},
        ]
        result = _collect_composite_artifacts(
            all_arts, "my_comp", CompositeIntermediates.DISCARD
        )
        assert "result" in result
        assert "data" not in result

    def test_persist_merges_with_prefix(self):
        all_arts = [
            {"data": [_art("a" * 32)]},
            {"result": [_art("b" * 32)]},
        ]
        result = _collect_composite_artifacts(
            all_arts, "my_comp", CompositeIntermediates.PERSIST
        )
        assert "_composite_my_comp_data" in result
        assert "result" in result
        assert len(result) == 2

    def test_expose_same_as_persist_for_artifacts(self):
        all_arts = [
            {"data": [_art("a" * 32)]},
            {"result": [_art("b" * 32)]},
        ]
        result = _collect_composite_artifacts(
            all_arts, "my_comp", CompositeIntermediates.EXPOSE
        )
        assert "_composite_my_comp_data" in result
        assert "result" in result
