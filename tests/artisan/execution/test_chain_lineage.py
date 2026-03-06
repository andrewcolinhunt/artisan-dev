"""Tests for chain lineage: ancestor map, shortcut edges, intermediates modes."""

from __future__ import annotations

from artisan.execution.executors.chain import (
    _collect_chain_edges,
    update_ancestor_map,
)
from artisan.execution.models.execution_chain import ChainIntermediates
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge


def _edge(src: str, tgt: str, run_id: str = "r" * 32) -> ArtifactProvenanceEdge:
    """Create a minimal provenance edge for testing."""
    return ArtifactProvenanceEdge(
        execution_run_id=run_id,
        source_artifact_id=src,
        target_artifact_id=tgt,
        source_artifact_type="metric",
        target_artifact_type="metric",
        source_role="data",
        target_role="data",
    )


class TestUpdateAncestorMap:
    def test_first_operation_maps_source_to_target(self) -> None:
        edges = [_edge("a" * 32, "b" * 32)]
        result = update_ancestor_map({}, edges, is_first=True)
        assert result["b" * 32] == ["a" * 32]

    def test_subsequent_operation_inherits_ancestors(self) -> None:
        ancestor_map = {"b" * 32: ["a" * 32]}
        edges = [_edge("b" * 32, "c" * 32)]
        result = update_ancestor_map(ancestor_map, edges, is_first=False)
        assert result["c" * 32] == ["a" * 32]

    def test_multiple_ancestors_propagate(self) -> None:
        ancestor_map = {"b" * 32: ["a" * 32, "x" * 32]}
        edges = [_edge("b" * 32, "c" * 32)]
        result = update_ancestor_map(ancestor_map, edges, is_first=False)
        assert set(result["c" * 32]) == {"a" * 32, "x" * 32}

    def test_subsequent_unknown_source_ignored(self) -> None:
        ancestor_map = {}
        edges = [_edge("z" * 32, "c" * 32)]
        result = update_ancestor_map(ancestor_map, edges, is_first=False)
        assert "c" * 32 not in result

    def test_does_not_mutate_original(self) -> None:
        original = {"b" * 32: ["a" * 32]}
        edges = [_edge("b" * 32, "c" * 32)]
        result = update_ancestor_map(original, edges, is_first=False)
        assert "c" * 32 not in original
        assert "c" * 32 in result


class TestCollectChainEdges:
    def _make_edges(self) -> list[list[ArtifactProvenanceEdge]]:
        return [
            [_edge("a" * 32, "b" * 32)],
            [_edge("b" * 32, "c" * 32)],
        ]

    def test_discard_returns_only_shortcut_edges(self) -> None:
        import json

        from artisan.schemas.artifact.metric import MetricArtifact

        art = MetricArtifact(
            artifact_id="c" * 32,
            artifact_type="metric",
            content=json.dumps({"v": 1}).encode(),
            original_name="t",
            extension=".json",
            origin_step_number=0,
        )
        ancestor_map = {"c" * 32: ["a" * 32]}
        edges = self._make_edges()
        result = _collect_chain_edges(
            edges,
            ancestor_map,
            {"data": [art]},
            "r" * 32,
            ChainIntermediates.DISCARD,
        )
        # Only shortcut edges (step_boundary=True)
        assert all(e.step_boundary for e in result)
        assert len(result) == 1
        assert result[0].source_artifact_id == "a" * 32
        assert result[0].target_artifact_id == "c" * 32

    def test_persist_includes_internal_edges(self) -> None:
        import json

        from artisan.schemas.artifact.metric import MetricArtifact

        art = MetricArtifact(
            artifact_id="c" * 32,
            artifact_type="metric",
            content=json.dumps({"v": 1}).encode(),
            original_name="t",
            extension=".json",
            origin_step_number=0,
        )
        ancestor_map = {"c" * 32: ["a" * 32]}
        edges = self._make_edges()
        result = _collect_chain_edges(
            edges,
            ancestor_map,
            {"data": [art]},
            "r" * 32,
            ChainIntermediates.PERSIST,
        )
        internal = [e for e in result if not e.step_boundary]
        shortcut = [e for e in result if e.step_boundary]
        assert len(internal) == 2
        assert len(shortcut) == 1

    def test_expose_same_as_persist_for_edges(self) -> None:
        import json

        from artisan.schemas.artifact.metric import MetricArtifact

        art = MetricArtifact(
            artifact_id="c" * 32,
            artifact_type="metric",
            content=json.dumps({"v": 1}).encode(),
            original_name="t",
            extension=".json",
            origin_step_number=0,
        )
        ancestor_map = {"c" * 32: ["a" * 32]}
        edges = self._make_edges()
        result = _collect_chain_edges(
            edges,
            ancestor_map,
            {"data": [art]},
            "r" * 32,
            ChainIntermediates.EXPOSE,
        )
        assert len(result) == 3  # 2 internal + 1 shortcut
