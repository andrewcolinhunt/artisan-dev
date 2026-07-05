"""Tests for the bounded provenance edge-list reader."""

from __future__ import annotations

from artisan.provenance import provenance_edges

A = "a" * 32
B = "b" * 32
C = "c" * 32
D = "d" * 32

CHAIN = [(A, B), (B, C), (C, D)]


class TestProvenanceEdges:
    """Tests for provenance_edges."""

    def test_backward_full_chain(self, tmp_path, seed_artifact_edges):
        """Depth covering the whole chain returns every edge, untruncated."""
        seed_artifact_edges(tmp_path, CHAIN)
        result = provenance_edges(str(tmp_path), D, direction="backward", depth=3)
        assert result.edges == [
            {"source_artifact_id": C, "target_artifact_id": D},
            {"source_artifact_id": B, "target_artifact_id": C},
            {"source_artifact_id": A, "target_artifact_id": B},
        ]
        assert result.truncated is False

    def test_backward_depth_bound_truncates(self, tmp_path, seed_artifact_edges):
        """Depth 1 from D returns only the C -> D edge and flags truncation."""
        seed_artifact_edges(tmp_path, CHAIN)
        result = provenance_edges(str(tmp_path), D, direction="backward", depth=1)
        assert result.edges == [{"source_artifact_id": C, "target_artifact_id": D}]
        assert result.truncated is True

    def test_forward_walk(self, tmp_path, seed_artifact_edges):
        """Forward from A collects descendant edges in hop order."""
        seed_artifact_edges(tmp_path, CHAIN)
        result = provenance_edges(str(tmp_path), A, direction="forward", depth=2)
        assert result.edges == [
            {"source_artifact_id": A, "target_artifact_id": B},
            {"source_artifact_id": B, "target_artifact_id": C},
        ]
        assert result.truncated is True
        assert result.direction == "forward"

    def test_unknown_artifact_is_empty(self, tmp_path, seed_artifact_edges):
        """An artifact with no edges yields empty edges, not an error."""
        seed_artifact_edges(tmp_path, CHAIN)
        result = provenance_edges(str(tmp_path), "f" * 32)
        assert result.edges == []
        assert result.truncated is False

    def test_missing_table_is_empty(self, tmp_path):
        """A root without an artifact_edges table degrades to empty edges."""
        result = provenance_edges(str(tmp_path), A)
        assert result.edges == []
        assert result.truncated is False

    def test_echoes_query_args(self, tmp_path, seed_artifact_edges):
        """The result carries the query parameters for machine consumers."""
        seed_artifact_edges(tmp_path, CHAIN)
        result = provenance_edges(str(tmp_path), D, direction="backward", depth=2)
        assert result.artifact_id == D
        assert result.depth == 2
