"""Tests for runtime-only artifact source locations."""

from __future__ import annotations

import pytest

from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact


class TestExternalPathDefault:
    """external_path defaults to None on all artifact types."""

    def test_file_ref_default_none(self) -> None:
        artifact = FileRefArtifact.draft(
            path="/data/test.dat",
            content_hash="a" * 32,
            size_bytes=100,
            step_number=0,
        )
        assert artifact.external_path is None

    def test_metric_default_none(self) -> None:
        artifact = MetricArtifact.draft(
            content={"accuracy": 1.5}, original_name="accuracy.json", step_number=0
        )
        assert artifact.external_path is None

    def test_config_default_none(self) -> None:
        artifact = ExecutionConfigArtifact.draft(
            content={"key": "val"}, original_name="config.json", step_number=0
        )
        assert artifact.external_path is None


class TestExternalPathDraft:
    """external_path can be set on artifacts."""

    def test_external_path_can_be_set_directly(self) -> None:
        artifact = MetricArtifact.draft(
            content={"accuracy": 1.5}, original_name="accuracy.json", step_number=0
        )
        artifact.external_path = "/nfs/metrics/accuracy.json"
        assert artifact.external_path == "/nfs/metrics/accuracy.json"


class TestExternalPathSerialization:
    """Source locations stay out of artifact content rows."""

    def test_file_ref_locators_are_not_serialized(self) -> None:
        original = FileRefArtifact.draft(
            path="/data/test.dat",
            content_hash="a" * 32,
            size_bytes=100,
            step_number=0,
        )
        original.external_path = "/nfs/data/test.dat"
        original.finalize()
        row = original.to_row()
        restored = FileRefArtifact.from_row(row)
        assert "path" not in row
        assert "external_path" not in row
        assert restored.path is None
        assert restored.external_path is None

    def test_metric_source_path_is_not_serialized(self) -> None:
        original = MetricArtifact.draft(
            content={"accuracy": 1.5}, original_name="accuracy.json", step_number=0
        )
        original.external_path = "/nfs/metrics/accuracy.json"
        original.finalize()
        row = original.to_row()
        restored = MetricArtifact.from_row(row)
        assert "external_path" not in row
        assert restored.external_path is None

    def test_config_source_path_is_not_serialized(self) -> None:
        original = ExecutionConfigArtifact.draft(
            content={"key": "val"}, original_name="config.json", step_number=0
        )
        original.external_path = "/nfs/configs/config.json"
        original.finalize()
        row = original.to_row()
        restored = ExecutionConfigArtifact.from_row(row)
        assert "external_path" not in row
        assert restored.external_path is None


class TestEmbeddedExternalPathProtection:
    """Embedded source paths are non-identity semantics frozen at finalization."""

    def test_metric_paths_set_before_finalization_do_not_affect_id(self) -> None:
        a = MetricArtifact.draft(
            content={"value": 1.0},
            original_name="test.json",
            step_number=0,
        )
        a.external_path = "/path/a/test.json"
        b = MetricArtifact.draft(
            content={"value": 1.0},
            original_name="test.json",
            step_number=0,
        )
        b.external_path = "/path/b/test.json"
        assert a.finalize().artifact_id == b.finalize().artifact_id

    def test_metric_source_path_is_frozen_after_finalization(self) -> None:
        artifact = MetricArtifact.draft(
            content={"value": 1.0},
            original_name="test.json",
            step_number=0,
        ).finalize()

        with pytest.raises(TypeError, match="external_path"):
            artifact.external_path = "/path/a/test.json"


class TestExternalPathInPolarsSchema:
    """Content schemas exclude runtime source locations."""

    def test_file_ref_schema(self) -> None:
        assert "path" not in FileRefArtifact.POLARS_SCHEMA
        assert "external_path" not in FileRefArtifact.POLARS_SCHEMA

    def test_metric_schema(self) -> None:
        assert "external_path" not in MetricArtifact.POLARS_SCHEMA

    def test_config_schema(self) -> None:
        assert "external_path" not in ExecutionConfigArtifact.POLARS_SCHEMA

    def test_schema_keys_match_row_keys_file_ref(self) -> None:
        artifact = FileRefArtifact.draft(
            path="/data/test.dat",
            content_hash="a" * 32,
            size_bytes=100,
            step_number=0,
        ).finalize()
        row = artifact.to_row()
        assert set(row.keys()) == set(FileRefArtifact.POLARS_SCHEMA.keys())

    def test_schema_keys_match_row_keys_metric(self) -> None:
        artifact = MetricArtifact.draft(
            content={"accuracy": 1.5}, original_name="accuracy.json", step_number=0
        ).finalize()
        row = artifact.to_row()
        assert set(row.keys()) == set(MetricArtifact.POLARS_SCHEMA.keys())

    def test_schema_keys_match_row_keys_config(self) -> None:
        artifact = ExecutionConfigArtifact.draft(
            content={"key": "val"}, original_name="config.json", step_number=0
        ).finalize()
        row = artifact.to_row()
        assert set(row.keys()) == set(ExecutionConfigArtifact.POLARS_SCHEMA.keys())
