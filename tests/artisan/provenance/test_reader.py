"""Tests for the bounded provenance edge-list reader."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from artisan.provenance import provenance_edges
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import ARTIFACT_EDGES_SCHEMA

A = "a" * 32
B = "b" * 32
C = "c" * 32
D = "d" * 32


def _seed_edges(root: Path, pairs: list[tuple[str, str]]) -> None:
    """Write an artifact_edges table from (source, target) pairs."""
    n = len(pairs)
    df = pl.DataFrame(
        {
            "execution_run_id": ["run"] * n,
            "source_artifact_id": [p[0] for p in pairs],
            "target_artifact_id": [p[1] for p in pairs],
            "source_artifact_type": ["data"] * n,
            "target_artifact_type": ["data"] * n,
            "source_role": ["input"] * n,
            "target_role": ["output"] * n,
            "group_id": [None] * n,
            "step_boundary": [True] * n,
        },
        schema=ARTIFACT_EDGES_SCHEMA,
    )
    df.write_delta(str(root / TablePath.ARTIFACT_EDGES))


def _chain(root: Path) -> None:
    """Seed the linear chain A -> B -> C -> D."""
    _seed_edges(root, [(A, B), (B, C), (C, D)])


class TestProvenanceEdges:
    """Tests for provenance_edges."""

    def test_backward_full_chain(self, tmp_path):
        """Depth covering the whole chain returns every edge, untruncated."""
        _chain(tmp_path)
        result = provenance_edges(str(tmp_path), D, direction="backward", depth=3)
        assert result.edges == [
            {"source_artifact_id": C, "target_artifact_id": D},
            {"source_artifact_id": B, "target_artifact_id": C},
            {"source_artifact_id": A, "target_artifact_id": B},
        ]
        assert result.truncated is False

    def test_backward_depth_bound_truncates(self, tmp_path):
        """Depth 1 from D returns only the C -> D edge and flags truncation."""
        _chain(tmp_path)
        result = provenance_edges(str(tmp_path), D, direction="backward", depth=1)
        assert result.edges == [{"source_artifact_id": C, "target_artifact_id": D}]
        assert result.truncated is True

    def test_forward_walk(self, tmp_path):
        """Forward from A collects descendant edges in hop order."""
        _chain(tmp_path)
        result = provenance_edges(str(tmp_path), A, direction="forward", depth=2)
        assert result.edges == [
            {"source_artifact_id": A, "target_artifact_id": B},
            {"source_artifact_id": B, "target_artifact_id": C},
        ]
        assert result.truncated is True
        assert result.direction == "forward"

    def test_unknown_artifact_is_empty(self, tmp_path):
        """An artifact with no edges yields empty edges, not an error."""
        _chain(tmp_path)
        result = provenance_edges(str(tmp_path), "f" * 32)
        assert result.edges == []
        assert result.truncated is False

    def test_missing_table_is_empty(self, tmp_path):
        """A root without an artifact_edges table degrades to empty edges."""
        result = provenance_edges(str(tmp_path), A)
        assert result.edges == []
        assert result.truncated is False

    def test_echoes_query_args(self, tmp_path):
        """The result carries the query parameters for machine consumers."""
        _chain(tmp_path)
        result = provenance_edges(str(tmp_path), D, direction="backward", depth=2)
        assert result.artifact_id == D
        assert result.depth == 2
