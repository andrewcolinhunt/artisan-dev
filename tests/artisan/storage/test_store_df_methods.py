"""Tests for ArtifactStore DataFrame query methods.

Tests cover:
1. load_provenance_edges_df with step scoping
2. load_metrics_df with binary content
"""

from __future__ import annotations

import json

import polars as pl

from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import TablePath
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.table_schemas import ARTIFACT_EDGES_SCHEMA, get_schema


def _write_index(root: str, entries: list[dict], storage_options: dict | None) -> None:
    path = f"{root}/{TablePath.ARTIFACT_INDEX.value}"
    pl.DataFrame(entries, schema=get_schema(TablePath.ARTIFACT_INDEX)).write_delta(
        path, storage_options=storage_options
    )


def _write_edges(root: str, edges: list[dict], storage_options: dict | None) -> None:
    path = f"{root}/{TablePath.ARTIFACT_EDGES.value}"
    pl.DataFrame(edges, schema=ARTIFACT_EDGES_SCHEMA).write_delta(
        path, storage_options=storage_options
    )


def _write_metrics(root: str, rows: list[dict], storage_options: dict | None) -> None:
    path = f"{root}/artifacts/metrics"
    pl.DataFrame(rows, schema=MetricArtifact.POLARS_SCHEMA).write_delta(
        path, storage_options=storage_options
    )


class TestLoadProvenanceEdgesDf:
    """Tests for load_provenance_edges_df."""

    def test_returns_edges_within_step_range(self, backend_fs):
        """Edges with both endpoints in range are returned."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        _write_index(
            root,
            [
                {
                    "artifact_id": "A",
                    "artifact_type": "data",
                    "origin_step_number": 1,
                    "metadata": "{}",
                },
                {
                    "artifact_id": "B",
                    "artifact_type": "metric",
                    "origin_step_number": 2,
                    "metadata": "{}",
                },
            ],
            opts,
        )
        _write_edges(
            root,
            [
                {
                    "execution_run_id": "run1",
                    "source_artifact_id": "A",
                    "target_artifact_id": "B",
                    "source_artifact_type": "data",
                    "target_artifact_type": "metric",
                    "source_role": "data",
                    "target_role": "metric",
                    "group_id": None,
                    "step_boundary": True,
                }
            ],
            opts,
        )

        store = ArtifactStore(root, fs=fs, storage_options=opts)
        result = store.load_provenance_edges_df(step_min=1, step_max=2)

        assert len(result) == 1
        assert result["source_artifact_id"][0] == "A"
        assert result["target_artifact_id"][0] == "B"

    def test_filters_out_edges_outside_range(self, backend_fs):
        """Edges where an endpoint is outside [step_min, step_max] are excluded."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        _write_index(
            root,
            [
                {
                    "artifact_id": "A",
                    "artifact_type": "data",
                    "origin_step_number": 1,
                    "metadata": "{}",
                },
                {
                    "artifact_id": "B",
                    "artifact_type": "metric",
                    "origin_step_number": 5,
                    "metadata": "{}",
                },
            ],
            opts,
        )
        _write_edges(
            root,
            [
                {
                    "execution_run_id": "run1",
                    "source_artifact_id": "A",
                    "target_artifact_id": "B",
                    "source_artifact_type": "data",
                    "target_artifact_type": "metric",
                    "source_role": "data",
                    "target_role": "metric",
                    "group_id": None,
                    "step_boundary": True,
                }
            ],
            opts,
        )

        store = ArtifactStore(root, fs=fs, storage_options=opts)
        result = store.load_provenance_edges_df(step_min=1, step_max=3)

        assert result.is_empty()
        assert result.columns == ["source_artifact_id", "target_artifact_id"]

    def test_empty_when_no_tables(self, backend_fs):
        """Returns empty DataFrame when tables don't exist."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        result = store.load_provenance_edges_df(step_min=0, step_max=10)

        assert result.is_empty()
        assert result.columns == ["source_artifact_id", "target_artifact_id"]


class TestLoadMetricsDf:
    """Tests for load_metrics_df."""

    def test_returns_metrics_by_id(self, backend_fs):
        """Loads matching metrics with binary content."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        content = json.dumps({"score": 0.95}).encode("utf-8")
        _write_metrics(
            root,
            [
                {
                    "artifact_id": "m1",
                    "origin_step_number": 1,
                    "content": content,
                    "original_name": "score",
                    "extension": ".json",
                    "metadata": "{}",
                    "external_path": None,
                }
            ],
            opts,
        )

        store = ArtifactStore(root, fs=fs, storage_options=opts)
        result = store.load_metrics_df(["m1"])

        assert len(result) == 1
        assert result["artifact_id"][0] == "m1"
        assert result["content"][0] == content

    def test_filters_to_requested_ids(self, backend_fs):
        """Only returns metrics matching the requested IDs."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        _write_metrics(
            root,
            [
                {
                    "artifact_id": "m1",
                    "origin_step_number": 1,
                    "content": b'{"a": 1}',
                    "original_name": "a",
                    "extension": ".json",
                    "metadata": "{}",
                    "external_path": None,
                },
                {
                    "artifact_id": "m2",
                    "origin_step_number": 1,
                    "content": b'{"b": 2}',
                    "original_name": "b",
                    "extension": ".json",
                    "metadata": "{}",
                    "external_path": None,
                },
            ],
            opts,
        )

        store = ArtifactStore(root, fs=fs, storage_options=opts)
        result = store.load_metrics_df(["m1"])

        assert len(result) == 1
        assert result["artifact_id"][0] == "m1"

    def test_empty_when_no_ids(self, backend_fs):
        """Returns empty DataFrame for empty ID list."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        result = store.load_metrics_df([])

        assert result.is_empty()
        assert result.columns == ["artifact_id", "content"]

    def test_empty_when_no_table(self, backend_fs):
        """Returns empty DataFrame when metrics table doesn't exist."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        result = store.load_metrics_df(["m1"])

        assert result.is_empty()
        assert result.columns == ["artifact_id", "content"]


class TestStoreDfMethodsBackendParametrized:
    """Smoke test ArtifactStore DataFrame methods on both [local, s3] backends.

    Kept alongside the promoted classes above as an additional
    integration-level round-trip check.
    """

    def test_load_metrics_df_round_trip(self, backend_fs):
        """Write a metrics Delta table and load it back via ArtifactStore."""
        fs, storage, root = backend_fs
        delta_root = f"{root}/delta"
        metrics_path = f"{delta_root}/artifacts/metrics"
        storage_options = storage.delta_storage_options()

        content = json.dumps({"score": 0.95}).encode("utf-8")
        metrics_df = pl.DataFrame(
            [
                {
                    "artifact_id": "m1",
                    "origin_step_number": 1,
                    "content": content,
                    "original_name": "score",
                    "extension": ".json",
                    "metadata": "{}",
                    "external_path": None,
                }
            ],
            schema=MetricArtifact.POLARS_SCHEMA,
        )
        metrics_df.write_delta(
            metrics_path, mode="overwrite", storage_options=storage_options
        )

        store = ArtifactStore(delta_root, fs=fs, storage_options=storage_options)
        result = store.load_metrics_df(["m1"])

        assert len(result) == 1
        assert result["artifact_id"][0] == "m1"
        assert result["content"][0] == content
