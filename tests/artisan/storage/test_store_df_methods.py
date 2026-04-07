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
from fsspec.implementations.local import LocalFileSystem

from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.table_schemas import ARTIFACT_EDGES_SCHEMA, get_schema


def _write_index(base_path, entries: list[dict]) -> None:
    path = base_path / TablePath.ARTIFACT_INDEX
    pl.DataFrame(entries, schema=get_schema(TablePath.ARTIFACT_INDEX)).write_delta(
        str(path)
    )


def _write_edges(base_path, edges: list[dict]) -> None:
    path = base_path / TablePath.ARTIFACT_EDGES
    pl.DataFrame(edges, schema=ARTIFACT_EDGES_SCHEMA).write_delta(str(path))


def _write_metrics(base_path, rows: list[dict]) -> None:
    path = base_path / "artifacts/metrics"
    pl.DataFrame(rows, schema=MetricArtifact.POLARS_SCHEMA).write_delta(str(path))


class TestLoadProvenanceEdgesDf:
    """Tests for load_provenance_edges_df."""

    def test_returns_edges_within_step_range(self, tmp_path):
        """Edges with both endpoints in range are returned."""
        _write_index(
            tmp_path,
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
        )
        _write_edges(
            tmp_path,
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
        )

        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        result = store.load_provenance_edges_df(step_min=1, step_max=2)

        assert len(result) == 1
        assert result["source_artifact_id"][0] == "A"
        assert result["target_artifact_id"][0] == "B"

    def test_filters_out_edges_outside_range(self, tmp_path):
        """Edges where an endpoint is outside [step_min, step_max] are excluded."""
        _write_index(
            tmp_path,
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
        )
        _write_edges(
            tmp_path,
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
        )

        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        result = store.load_provenance_edges_df(step_min=1, step_max=3)

        assert result.is_empty()
        assert result.columns == ["source_artifact_id", "target_artifact_id"]

    def test_empty_when_no_tables(self, tmp_path):
        """Returns empty DataFrame when tables don't exist."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        result = store.load_provenance_edges_df(step_min=0, step_max=10)

        assert result.is_empty()
        assert result.columns == ["source_artifact_id", "target_artifact_id"]


class TestLoadMetricsDf:
    """Tests for load_metrics_df."""

    def test_returns_metrics_by_id(self, tmp_path):
        """Loads matching metrics with binary content."""
        content = json.dumps({"score": 0.95}).encode("utf-8")
        _write_metrics(
            tmp_path,
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
        )

        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        result = store.load_metrics_df(["m1"])

        assert len(result) == 1
        assert result["artifact_id"][0] == "m1"
        assert result["content"][0] == content

    def test_filters_to_requested_ids(self, tmp_path):
        """Only returns metrics matching the requested IDs."""
        _write_metrics(
            tmp_path,
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
        )

        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        result = store.load_metrics_df(["m1"])

        assert len(result) == 1
        assert result["artifact_id"][0] == "m1"

    def test_empty_when_no_ids(self, tmp_path):
        """Returns empty DataFrame for empty ID list."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        result = store.load_metrics_df([])

        assert result.is_empty()
        assert result.columns == ["artifact_id", "content"]

    def test_empty_when_no_table(self, tmp_path):
        """Returns empty DataFrame when metrics table doesn't exist."""
        store = ArtifactStore(str(tmp_path), fs=LocalFileSystem())
        result = store.load_metrics_df(["m1"])

        assert result.is_empty()
        assert result.columns == ["artifact_id", "content"]
