"""Shared fixtures for execution tests."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
import xxhash

from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA


def compute_artifact_id(content: bytes) -> str:
    """Compute xxh3_128 hash for content-addressed ID."""
    return xxhash.xxh3_128(content).hexdigest()


def _setup_delta_tables(
    base_path: Path,
    metrics: list[dict] | None = None,
    file_refs: list[dict] | None = None,
    index_entries: list[dict] | None = None,
):
    """Helper to set up Delta Lake tables for testing.

    Creates Delta Lake tables with the provided data at base_path.

    Args:
        base_path: Root directory for Delta Lake tables.
        metrics: List of metric artifact data dicts.
        file_refs: List of file ref artifact data dicts.
        index_entries: List of artifact_index entries.
    """
    if metrics:
        metrics_path = base_path / "artifacts/metrics"
        df = pl.DataFrame(metrics, schema=MetricArtifact.POLARS_SCHEMA)
        df.write_delta(str(metrics_path))

    if file_refs:
        ext_path = base_path / "artifacts/file_refs"
        df = pl.DataFrame(file_refs, schema=FileRefArtifact.POLARS_SCHEMA)
        df.write_delta(str(ext_path))

    if index_entries:
        index_path = base_path / "artifacts/index"
        df = pl.DataFrame(index_entries, schema=ARTIFACT_INDEX_SCHEMA)
        df.write_delta(str(index_path))


@pytest.fixture
def metric_artifact():
    """Create a test MetricArtifact."""
    metrics = {"score": 0.95, "confidence": 0.87}
    content = json.dumps(metrics).encode("utf-8")
    artifact_id = compute_artifact_id(content)
    return MetricArtifact(
        artifact_id=artifact_id,
        origin_step_number=1,
        content=content,
    )


@pytest.fixture
def metric_artifact_2():
    """Create a second test MetricArtifact with different content."""
    metrics = {"score": 0.72, "accuracy": 1.23}
    content = json.dumps(metrics).encode("utf-8")
    artifact_id = compute_artifact_id(content)
    return MetricArtifact(
        artifact_id=artifact_id,
        origin_step_number=1,
        content=content,
    )


@pytest.fixture
def file_ref_artifact(tmp_path):
    """Create a test FileRefArtifact with actual file."""
    # Create the file ref
    file_path = tmp_path / "external" / "input.dat"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    content = b"ATOM      1  CA  GLY A   1       1.000   2.000   3.000  1.00  0.00           C\n"
    file_path.write_bytes(content)

    content_hash = compute_artifact_id(content)
    # Two-step hash: artifact_id is hash of metadata (content_hash, path, size)
    metadata_str = json.dumps(
        {
            "content_hash": content_hash,
            "path": str(file_path),
            "size_bytes": len(content),
        },
        sort_keys=True,
    )
    artifact_id = compute_artifact_id(metadata_str.encode())
    return FileRefArtifact(
        artifact_id=artifact_id,
        origin_step_number=0,
        content_hash=content_hash,
        path=str(file_path),
        size_bytes=len(content),
    )


@pytest.fixture
def artifact_store_with_metric(tmp_path, metric_artifact):
    """Create an ArtifactStore with a single metric artifact."""
    base_path = tmp_path / "delta"

    _setup_delta_tables(
        base_path,
        metrics=[
            {
                "artifact_id": metric_artifact.artifact_id,
                "origin_step_number": metric_artifact.origin_step_number,
                "content": metric_artifact.content,
                "original_name": None,
                "extension": None,
                "metadata": "{}",
                "external_path": None,
            }
        ],
        index_entries=[
            {
                "artifact_id": metric_artifact.artifact_id,
                "artifact_type": "metric",
                "origin_step_number": metric_artifact.origin_step_number,
                "metadata": "{}",
            }
        ],
    )

    return ArtifactStore(base_path)


@pytest.fixture
def artifact_store_with_file_ref(tmp_path, file_ref_artifact):
    """Create an ArtifactStore with a file ref artifact."""
    base_path = tmp_path / "delta"

    _setup_delta_tables(
        base_path,
        file_refs=[
            {
                "artifact_id": file_ref_artifact.artifact_id,
                "origin_step_number": file_ref_artifact.origin_step_number,
                "content_hash": file_ref_artifact.content_hash,
                "path": file_ref_artifact.path,
                "size_bytes": file_ref_artifact.size_bytes,
                "original_name": None,
                "extension": None,
                "metadata": "{}",
                "external_path": None,
            }
        ],
        index_entries=[
            {
                "artifact_id": file_ref_artifact.artifact_id,
                "artifact_type": "file_ref",
                "origin_step_number": file_ref_artifact.origin_step_number,
                "metadata": "{}",
            }
        ],
    )

    return ArtifactStore(base_path)


@pytest.fixture
def artifact_store_with_all_types(
    tmp_path,
    metric_artifact,
    file_ref_artifact,
):
    """Create an ArtifactStore with metric and file ref artifacts."""
    base_path = tmp_path / "delta"

    _setup_delta_tables(
        base_path,
        metrics=[
            {
                "artifact_id": metric_artifact.artifact_id,
                "origin_step_number": metric_artifact.origin_step_number,
                "content": metric_artifact.content,
                "original_name": None,
                "extension": None,
                "metadata": "{}",
                "external_path": None,
            }
        ],
        file_refs=[
            {
                "artifact_id": file_ref_artifact.artifact_id,
                "origin_step_number": file_ref_artifact.origin_step_number,
                "content_hash": file_ref_artifact.content_hash,
                "path": file_ref_artifact.path,
                "size_bytes": file_ref_artifact.size_bytes,
                "original_name": None,
                "extension": None,
                "metadata": "{}",
                "external_path": None,
            }
        ],
        index_entries=[
            {
                "artifact_id": metric_artifact.artifact_id,
                "artifact_type": "metric",
                "origin_step_number": metric_artifact.origin_step_number,
                "metadata": "{}",
            },
            {
                "artifact_id": file_ref_artifact.artifact_id,
                "artifact_type": "file_ref",
                "origin_step_number": file_ref_artifact.origin_step_number,
                "metadata": "{}",
            },
        ],
    )

    return ArtifactStore(base_path)
