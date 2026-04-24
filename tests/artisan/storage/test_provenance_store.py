"""Tests for ProvenanceStore transitive walk methods."""

from __future__ import annotations

import polars as pl
import pytest

from artisan.storage.core.provenance_store import ProvenanceStore
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
)


def _write_delta(uri: str, df: pl.DataFrame, storage_options: dict | None) -> None:
    """Write a DataFrame as a Delta table to the given URI."""
    df.write_delta(uri, storage_options=storage_options)


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


@pytest.fixture
def prov_env(backend_fs):
    """Yield ``(store, fs, storage_options, root)`` per ``backend_fs`` param.

    Shared by ``TestGetAncestorIds`` and ``TestGetDescendantIds`` so every
    test runs twice — once against ``LocalFileSystem``, once against MinIO.
    """
    fs, storage, root = backend_fs
    opts = storage.delta_storage_options()
    store = ProvenanceStore(root, fs=fs, storage_options=opts)
    return store, fs, opts, root


class TestGetAncestorIds:
    """Tests for ProvenanceStore.get_ancestor_ids."""

    def test_no_edges_table(self, prov_env):
        """Returns empty list when artifact_edges table is missing."""
        store, _fs, _opts, _root = prov_env
        assert store.get_ancestor_ids(A) == []

    def test_linear_chain(self, prov_env):
        """A -> B -> C: ancestors of C are [A, B]."""
        store, _fs, opts, root = prov_env
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
        _write_delta(f"{root}/provenance/artifact_edges", edges, opts)
        _write_delta(f"{root}/artifacts/index", index, opts)

        result = store.get_ancestor_ids(C)
        assert set(result) == {A, B}

    def test_no_ancestors(self, prov_env):
        """Root node has no ancestors."""
        store, _fs, opts, root = prov_env
        edges = _make_edges([(A, B, "data")])
        _write_delta(f"{root}/provenance/artifact_edges", edges, opts)

        assert store.get_ancestor_ids(A) == []

    def test_diamond_graph(self, prov_env):
        """A -> B, A -> C, B -> D, C -> D: ancestors of D are [A, B, C]."""
        store, _fs, opts, root = prov_env
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
        _write_delta(f"{root}/provenance/artifact_edges", edges, opts)
        _write_delta(f"{root}/artifacts/index", index, opts)

        result = store.get_ancestor_ids(D)
        assert set(result) == {A, B, C}

    def test_ancestor_type_filter(self, prov_env):
        """Filter ancestors by type returns only matching types."""
        store, _fs, opts, root = prov_env
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
        _write_delta(f"{root}/provenance/artifact_edges", edges, opts)
        _write_delta(f"{root}/artifacts/index", index, opts)

        # All ancestors
        assert set(store.get_ancestor_ids(C)) == {A, B}

        # Only data ancestors
        result = store.get_ancestor_ids(C, ancestor_type="data")
        assert result == [A]

        # Only metric ancestors
        result = store.get_ancestor_ids(C, ancestor_type="metric")
        assert result == [B]

    def test_unknown_artifact(self, prov_env):
        """Artifact not in any edge returns empty list."""
        store, _fs, opts, root = prov_env
        edges = _make_edges([(A, B, "data")])
        _write_delta(f"{root}/provenance/artifact_edges", edges, opts)

        assert store.get_ancestor_ids(C) == []


class TestGetDescendantIds:
    """Tests for ProvenanceStore.get_descendant_ids."""

    def test_no_edges_table(self, prov_env):
        """Returns empty list when artifact_edges table is missing."""
        store, _fs, _opts, _root = prov_env
        assert store.get_descendant_ids(A) == []

    def test_linear_chain(self, prov_env):
        """A -> B -> C: descendants of A are [B, C]."""
        store, _fs, opts, root = prov_env
        edges = _make_edges(
            [
                (A, B, "data"),
                (B, C, "data"),
            ]
        )
        _write_delta(f"{root}/provenance/artifact_edges", edges, opts)

        result = store.get_descendant_ids(A)
        assert set(result) == {B, C}

    def test_no_descendants(self, prov_env):
        """Leaf node has no descendants."""
        store, _fs, opts, root = prov_env
        edges = _make_edges([(A, B, "data")])
        _write_delta(f"{root}/provenance/artifact_edges", edges, opts)

        assert store.get_descendant_ids(B) == []

    def test_branching_graph(self, prov_env):
        """A -> B, A -> C, B -> D: descendants of A are [B, C, D]."""
        store, _fs, opts, root = prov_env
        edges = _make_edges(
            [
                (A, B, "data"),
                (A, C, "metric"),
                (B, D, "data"),
            ]
        )
        _write_delta(f"{root}/provenance/artifact_edges", edges, opts)

        result = store.get_descendant_ids(A)
        assert set(result) == {B, C, D}

    def test_descendant_type_filter(self, prov_env):
        """Filter descendants by type returns only matching types."""
        store, _fs, opts, root = prov_env
        edges = _make_edges(
            [
                (A, B, "data"),
                (A, C, "metric"),
                (B, D, "data"),
            ]
        )
        _write_delta(f"{root}/provenance/artifact_edges", edges, opts)

        # Only metric descendants
        result = store.get_descendant_ids(A, descendant_type="metric")
        assert result == [C]

        # Only data descendants
        result = store.get_descendant_ids(A, descendant_type="data")
        assert set(result) == {B, D}

    def test_unknown_artifact(self, prov_env):
        """Artifact not in any edge returns empty list."""
        store, _fs, opts, root = prov_env
        edges = _make_edges([(A, B, "data")])
        _write_delta(f"{root}/provenance/artifact_edges", edges, opts)

        assert store.get_descendant_ids(C) == []


class TestProvenanceStoreBackendParametrized:
    """Smoke test: seed a small provenance graph and read it back on each backend.

    Kept alongside the promoted classes above as an integration-level
    end-to-end check of the store's public walk API. The promoted classes
    cover more edge cases at the class-method level via ``prov_env``.
    """

    def test_seed_and_walk(self, backend_fs):
        """Seed A -> B -> C, then walk both directions via the store."""
        fs, storage_config, root = backend_fs
        storage_options = storage_config.delta_storage_options()

        edges = _make_edges([(A, B, "data"), (B, C, "data")])
        index = _make_index([(A, "data", 1), (B, "data", 2), (C, "data", 3)])

        edges.write_delta(
            f"{root}/provenance/artifact_edges",
            storage_options=storage_options,
        )
        index.write_delta(
            f"{root}/artifacts/index",
            storage_options=storage_options,
        )

        store = ProvenanceStore(root, fs=fs, storage_options=storage_options)

        assert set(store.get_ancestor_ids(C)) == {A, B}
        assert set(store.get_descendant_ids(A)) == {B, C}
