"""Tests for ProvenanceStore transitive walk methods."""

from __future__ import annotations

import polars as pl
import pytest

from fsspec.implementations.local import LocalFileSystem

from artisan.storage.core.provenance_store import ProvenanceStore
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
)


def _write_delta(path, df):
    """Write a DataFrame as a Delta table."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_delta(str(path))


def _make_edges(pairs: list[tuple[str, str, str]]) -> pl.DataFrame:
    """Build an edges DataFrame from (source, target, target_type) triples."""
    n = len(pairs)
    return pl.DataFrame(
        {
            "execution_run_id": ["run"] * n,
            "source_artifact_id": [p[0] for p in pairs],
            "target_artifact_id": [p[1] for p in pairs],
            "source_artifact_type": ["data"] * n,
            "target_artifact_type": [p[2] for p in pairs],
            "source_role": ["input"] * n,
            "target_role": ["output"] * n,
            "group_id": [None] * n,
            "step_boundary": [True] * n,
        },
        schema=ARTIFACT_EDGES_SCHEMA,
    )


def _make_index(entries: list[tuple[str, str, int]]) -> pl.DataFrame:
    """Build an artifact_index DataFrame from (id, type, step) triples."""
    return pl.DataFrame(
        {
            "artifact_id": [e[0] for e in entries],
            "artifact_type": [e[1] for e in entries],
            "origin_step_number": [e[2] for e in entries],
            "metadata": ["{}"] * len(entries),
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )


# Artifact IDs
A = "a" * 32
B = "b" * 32
C = "c" * 32
D = "d" * 32
E = "e" * 32


class TestGetAncestorIds:
    """Tests for ProvenanceStore.get_ancestor_ids."""

    @pytest.fixture
    def store(self, tmp_path):
        return ProvenanceStore(str(tmp_path), fs=LocalFileSystem())

    def test_no_edges_table(self, store):
        """Returns empty list when artifact_edges table is missing."""
        assert store.get_ancestor_ids(A) == []

    def test_linear_chain(self, tmp_path):
        """A -> B -> C: ancestors of C are [A, B]."""
        store = ProvenanceStore(str(tmp_path), fs=LocalFileSystem())
        edges = _make_edges(
            [
                (A, B, "data"),
                (B, C, "data"),
            ]
        )
        index = _make_index(
            [
                (A, "data", 1),
                (B, "data", 2),
                (C, "data", 3),
            ]
        )
        _write_delta(tmp_path / "provenance/artifact_edges", edges)
        _write_delta(tmp_path / "artifacts/index", index)

        result = store.get_ancestor_ids(C)
        assert set(result) == {A, B}

    def test_no_ancestors(self, tmp_path):
        """Root node has no ancestors."""
        store = ProvenanceStore(str(tmp_path), fs=LocalFileSystem())
        edges = _make_edges([(A, B, "data")])
        _write_delta(tmp_path / "provenance/artifact_edges", edges)

        assert store.get_ancestor_ids(A) == []

    def test_diamond_graph(self, tmp_path):
        """A -> B, A -> C, B -> D, C -> D: ancestors of D are [A, B, C]."""
        store = ProvenanceStore(str(tmp_path), fs=LocalFileSystem())
        edges = _make_edges(
            [
                (A, B, "data"),
                (A, C, "data"),
                (B, D, "data"),
                (C, D, "data"),
            ]
        )
        index = _make_index(
            [
                (A, "data", 1),
                (B, "data", 2),
                (C, "data", 2),
                (D, "data", 3),
            ]
        )
        _write_delta(tmp_path / "provenance/artifact_edges", edges)
        _write_delta(tmp_path / "artifacts/index", index)

        result = store.get_ancestor_ids(D)
        assert set(result) == {A, B, C}

    def test_ancestor_type_filter(self, tmp_path):
        """Filter ancestors by type returns only matching types."""
        store = ProvenanceStore(str(tmp_path), fs=LocalFileSystem())
        edges = _make_edges(
            [
                (A, B, "metric"),
                (B, C, "data"),
            ]
        )
        index = _make_index(
            [
                (A, "data", 1),
                (B, "metric", 2),
                (C, "data", 3),
            ]
        )
        _write_delta(tmp_path / "provenance/artifact_edges", edges)
        _write_delta(tmp_path / "artifacts/index", index)

        # All ancestors
        assert set(store.get_ancestor_ids(C)) == {A, B}

        # Only data ancestors
        result = store.get_ancestor_ids(C, ancestor_type="data")
        assert result == [A]

        # Only metric ancestors
        result = store.get_ancestor_ids(C, ancestor_type="metric")
        assert result == [B]

    def test_unknown_artifact(self, tmp_path):
        """Artifact not in any edge returns empty list."""
        store = ProvenanceStore(str(tmp_path), fs=LocalFileSystem())
        edges = _make_edges([(A, B, "data")])
        _write_delta(tmp_path / "provenance/artifact_edges", edges)

        assert store.get_ancestor_ids(C) == []


class TestGetDescendantIds:
    """Tests for ProvenanceStore.get_descendant_ids."""

    @pytest.fixture
    def store(self, tmp_path):
        return ProvenanceStore(str(tmp_path), fs=LocalFileSystem())

    def test_no_edges_table(self, store):
        """Returns empty list when artifact_edges table is missing."""
        assert store.get_descendant_ids(A) == []

    def test_linear_chain(self, tmp_path):
        """A -> B -> C: descendants of A are [B, C]."""
        store = ProvenanceStore(str(tmp_path), fs=LocalFileSystem())
        edges = _make_edges(
            [
                (A, B, "data"),
                (B, C, "data"),
            ]
        )
        _write_delta(tmp_path / "provenance/artifact_edges", edges)

        result = store.get_descendant_ids(A)
        assert set(result) == {B, C}

    def test_no_descendants(self, tmp_path):
        """Leaf node has no descendants."""
        store = ProvenanceStore(str(tmp_path), fs=LocalFileSystem())
        edges = _make_edges([(A, B, "data")])
        _write_delta(tmp_path / "provenance/artifact_edges", edges)

        assert store.get_descendant_ids(B) == []

    def test_branching_graph(self, tmp_path):
        """A -> B, A -> C, B -> D: descendants of A are [B, C, D]."""
        store = ProvenanceStore(str(tmp_path), fs=LocalFileSystem())
        edges = _make_edges(
            [
                (A, B, "data"),
                (A, C, "metric"),
                (B, D, "data"),
            ]
        )
        _write_delta(tmp_path / "provenance/artifact_edges", edges)

        result = store.get_descendant_ids(A)
        assert set(result) == {B, C, D}

    def test_descendant_type_filter(self, tmp_path):
        """Filter descendants by type returns only matching types."""
        store = ProvenanceStore(str(tmp_path), fs=LocalFileSystem())
        edges = _make_edges(
            [
                (A, B, "data"),
                (A, C, "metric"),
                (B, D, "data"),
            ]
        )
        _write_delta(tmp_path / "provenance/artifact_edges", edges)

        # Only metric descendants
        result = store.get_descendant_ids(A, descendant_type="metric")
        assert result == [C]

        # Only data descendants
        result = store.get_descendant_ids(A, descendant_type="data")
        assert set(result) == {B, D}

    def test_unknown_artifact(self, tmp_path):
        """Artifact not in any edge returns empty list."""
        store = ProvenanceStore(str(tmp_path), fs=LocalFileSystem())
        edges = _make_edges([(A, B, "data")])
        _write_delta(tmp_path / "provenance/artifact_edges", edges)

        assert store.get_descendant_ids(C) == []
