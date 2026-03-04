"""Tests for external_path field on Artifact base and all concrete types."""

from __future__ import annotations

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


class TestExternalPathRoundtrip:
    """external_path survives to_row()/from_row() serialization."""

    def test_file_ref_roundtrip(self) -> None:
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
        assert restored.external_path == "/nfs/data/test.dat"

    def test_metric_roundtrip(self) -> None:
        original = MetricArtifact.draft(
            content={"accuracy": 1.5}, original_name="accuracy.json", step_number=0
        )
        original.external_path = "/nfs/metrics/accuracy.json"
        original.finalize()
        row = original.to_row()
        restored = MetricArtifact.from_row(row)
        assert restored.external_path == "/nfs/metrics/accuracy.json"

    def test_config_roundtrip(self) -> None:
        original = ExecutionConfigArtifact.draft(
            content={"key": "val"}, original_name="config.json", step_number=0
        )
        original.external_path = "/nfs/configs/config.json"
        original.finalize()
        row = original.to_row()
        restored = ExecutionConfigArtifact.from_row(row)
        assert restored.external_path == "/nfs/configs/config.json"


class TestExternalPathDoesNotAffectArtifactId:
    """external_path must NOT affect artifact_id (content-addressed identity)."""

    def test_metric_same_content_different_path_same_id(self) -> None:
        a = MetricArtifact.draft(
            content={"value": 1.0},
            original_name="test.json",
            step_number=0,
        ).finalize()
        a.external_path = "/path/a/test.json"
        b = MetricArtifact.draft(
            content={"value": 1.0},
            original_name="test.json",
            step_number=0,
        ).finalize()
        b.external_path = "/path/b/test.json"
        c = MetricArtifact.draft(
            content={"value": 1.0},
            original_name="test.json",
            step_number=0,
        ).finalize()
        assert a.artifact_id == b.artifact_id == c.artifact_id


class TestExternalPathInPolarsSchema:
    """external_path appears in POLARS_SCHEMA for all artifact types."""

    def test_file_ref_schema(self) -> None:
        assert "external_path" in FileRefArtifact.POLARS_SCHEMA

    def test_metric_schema(self) -> None:
        assert "external_path" in MetricArtifact.POLARS_SCHEMA

    def test_config_schema(self) -> None:
        assert "external_path" in ExecutionConfigArtifact.POLARS_SCHEMA

    def test_schema_keys_match_row_keys_file_ref(self) -> None:
        artifact = FileRefArtifact(artifact_id="a" * 32, origin_step_number=0)
        row = artifact.to_row()
        assert set(row.keys()) == set(FileRefArtifact.POLARS_SCHEMA.keys())

    def test_schema_keys_match_row_keys_metric(self) -> None:
        artifact = MetricArtifact(artifact_id="a" * 32, origin_step_number=0)
        row = artifact.to_row()
        assert set(row.keys()) == set(MetricArtifact.POLARS_SCHEMA.keys())

    def test_schema_keys_match_row_keys_config(self) -> None:
        artifact = ExecutionConfigArtifact(artifact_id="a" * 32, origin_step_number=0)
        row = artifact.to_row()
        assert set(row.keys()) == set(ExecutionConfigArtifact.POLARS_SCHEMA.keys())
