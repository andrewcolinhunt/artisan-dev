"""Shared fixtures for execution tests."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA
from artisan.utils.hashing import compute_content_digest


@pytest.fixture(autouse=True)
def _format_common_delta_root(tmp_path: Path) -> None:
    """Initialize the conventional execution-test Delta root as format 2."""
    publish_test_store(str(tmp_path / "delta"), LocalFileSystem())


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
    publish_test_store(str(base_path), LocalFileSystem())
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
    return MetricArtifact.draft(metrics, "metric.json", 1).finalize()


@pytest.fixture
def metric_artifact_2():
    """Create a second test MetricArtifact with different content."""
    metrics = {"score": 0.72, "accuracy": 1.23}
    return MetricArtifact.draft(metrics, "metric_2.json", 1).finalize()


@pytest.fixture
def file_ref_artifact(tmp_path):
    """Create a test FileRefArtifact with actual file."""
    # Create the file ref
    file_path = tmp_path / "external" / "input.dat"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    content = b"ATOM      1  CA  GLY A   1       1.000   2.000   3.000  1.00  0.00           C\n"
    file_path.write_bytes(content)

    return FileRefArtifact.draft(
        path=str(file_path),
        content_hash=compute_content_digest(content),
        size_bytes=len(content),
        step_number=0,
    ).finalize()


@pytest.fixture
def artifact_store_with_metric(tmp_path, metric_artifact):
    """Create an ArtifactStore with a single metric artifact."""
    base_path = tmp_path / "delta"

    _setup_delta_tables(
        base_path,
        metrics=[metric_artifact.to_row()],
        index_entries=[
            {
                "artifact_id": metric_artifact.artifact_id,
                "artifact_type": "metric",
                "origin_step_number": metric_artifact.origin_step_number,
                "metadata": "{}",
            }
        ],
    )

    return ArtifactStore(str(base_path), fs=LocalFileSystem())


@pytest.fixture
def artifact_store_with_file_ref(tmp_path, file_ref_artifact):
    """Create an ArtifactStore with a file ref artifact."""
    base_path = tmp_path / "delta"

    _setup_delta_tables(
        base_path,
        file_refs=[file_ref_artifact.to_row()],
        index_entries=[
            {
                "artifact_id": file_ref_artifact.artifact_id,
                "artifact_type": "file_ref",
                "origin_step_number": file_ref_artifact.origin_step_number,
                "metadata": "{}",
            }
        ],
    )
    pl.DataFrame(
        [{"artifact_id": file_ref_artifact.artifact_id, "uri": file_ref_artifact.path}],
        schema={"artifact_id": pl.String, "uri": pl.String},
    ).write_delta(str(base_path / "artifacts/locations"))

    return ArtifactStore(str(base_path), fs=LocalFileSystem())


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
        metrics=[metric_artifact.to_row()],
        file_refs=[file_ref_artifact.to_row()],
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
    pl.DataFrame(
        [{"artifact_id": file_ref_artifact.artifact_id, "uri": file_ref_artifact.path}],
        schema={"artifact_id": pl.String, "uri": pl.String},
    ).write_delta(str(base_path / "artifacts/locations"))

    return ArtifactStore(str(base_path), fs=LocalFileSystem())
