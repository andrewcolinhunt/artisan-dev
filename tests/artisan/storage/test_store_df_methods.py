"""Tests for ArtifactStore DataFrame query methods.

Tests cover:
1. provenance.load_edges_df with step scoping
2. load_metrics_df with binary content
"""

from __future__ import annotations

import json

import polars as pl
from fixtures.logical_commit_store import commit_test_tables as _commit_tables

from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import TablePath
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.table_schemas import ARTIFACT_EDGES_SCHEMA, get_schema


def _write_index(root, fs, entries, storage_options) -> None:
    _commit_tables(
        root,
        fs,
        storage_options,
        {
            TablePath.ARTIFACT_INDEX.value: pl.DataFrame(
                entries, schema=get_schema(TablePath.ARTIFACT_INDEX)
            )
        },
    )


def _write_edges(root, fs, edges, storage_options) -> None:
    _commit_tables(
        root,
        fs,
        storage_options,
        {
            TablePath.ARTIFACT_EDGES.value: pl.DataFrame(
                edges, schema=ARTIFACT_EDGES_SCHEMA
            )
        },
    )


def _write_metrics(root, fs, rows, storage_options) -> None:
    _commit_tables(
        root,
        fs,
        storage_options,
        {"artifacts/metrics": pl.DataFrame(rows, schema=MetricArtifact.POLARS_SCHEMA)},
    )


class TestLoadProvenanceEdgesDf:
    """Tests for provenance.load_edges_df via ArtifactStore."""

    def test_returns_edges_within_step_range(self, backend_fs):
        """Edges with both endpoints in range are returned."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        _write_index(
            root,
            fs,
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
            fs,
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
        result = store.provenance.load_edges_df(step_min=1, step_max=2)

        assert len(result) == 1
        assert result["source_artifact_id"][0] == "A"
        assert result["target_artifact_id"][0] == "B"

    def test_filters_out_edges_outside_range(self, backend_fs):
        """Edges where an endpoint is outside [step_min, step_max] are excluded."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        _write_index(
            root,
            fs,
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
            fs,
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
        result = store.provenance.load_edges_df(step_min=1, step_max=3)

        assert result.is_empty()
        assert result.columns == ["source_artifact_id", "target_artifact_id"]

    def test_empty_when_tables_empty(self, backend_fs):
        """Return an empty frame when the required tables are empty."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        result = store.provenance.load_edges_df(step_min=0, step_max=10)

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
            fs,
            [
                {
                    "artifact_id": "m1",
                    "origin_step_number": 1,
                    "content": content,
                    "original_name": "score",
                    "extension": ".json",
                    "metadata": "{}",
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
            fs,
            [
                {
                    "artifact_id": "m1",
                    "origin_step_number": 1,
                    "content": b'{"a": 1}',
                    "original_name": "a",
                    "extension": ".json",
                    "metadata": "{}",
                },
                {
                    "artifact_id": "m2",
                    "origin_step_number": 1,
                    "content": b'{"b": 2}',
                    "original_name": "b",
                    "extension": ".json",
                    "metadata": "{}",
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

    def test_empty_when_table_empty(self, backend_fs):
        """Return an empty frame when the metrics table is empty."""
        fs, storage, root = backend_fs
        store = ArtifactStore(
            root, fs=fs, storage_options=storage.delta_storage_options()
        )
        result = store.load_metrics_df(["m1"])

        assert result.is_empty()
        assert result.columns == ["artifact_id", "content"]


class TestStoreDfMethodsBackendParametrized:
    """Test metric frame loading through committed storage on both backends."""

    def test_load_metrics_df_round_trip(self, backend_fs):
        """Write a metrics Delta table and load it back via ArtifactStore."""
        fs, storage, root = backend_fs
        delta_root = f"{root}/delta"
        storage_options = storage.delta_storage_options()

        artifact = MetricArtifact.draft({"score": 0.95}, "score.json", 1)
        artifact.finalize()
        content = artifact.content
        metrics_df = pl.DataFrame(
            [artifact.to_row()],
            schema=MetricArtifact.POLARS_SCHEMA,
        )
        _commit_tables(
            delta_root,
            fs,
            storage_options,
            {"artifacts/metrics": metrics_df},
        )

        store = ArtifactStore(delta_root, fs=fs, storage_options=storage_options)
        result = store.load_metrics_df([artifact.artifact_id])

        assert len(result) == 1
        assert result["artifact_id"][0] == artifact.artifact_id
        assert result["content"][0] == content
