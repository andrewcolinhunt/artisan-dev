"""Tests for model-owned serialization (to_row / from_row)."""

from __future__ import annotations

import json

import polars as pl

from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef


class TestMetricRoundtrip:
    """MetricArtifact to_row / from_row roundtrip."""

    def test_roundtrip(self) -> None:
        content = json.dumps({"accuracy": 1.5}).encode("utf-8")
        original = MetricArtifact(
            artifact_id="e" * 32,
            origin_step_number=2,
            content=content,
            original_name="accuracy",
            extension=".json",
            metadata={"unit": "angstrom"},
        )
        row = original.to_row()
        restored = MetricArtifact.from_row(row)

        assert restored.artifact_id == original.artifact_id
        assert restored.content == original.content
        assert restored.metadata == original.metadata

    def test_schema_keys_match_row_keys(self) -> None:
        artifact = MetricArtifact(artifact_id="f" * 32, origin_step_number=0)
        row = artifact.to_row()
        assert set(row.keys()) == set(MetricArtifact.POLARS_SCHEMA.keys())


class TestConfigRoundtrip:
    """ExecutionConfigArtifact to_row / from_row roundtrip."""

    def test_roundtrip(self) -> None:
        content = json.dumps({"steps": 100}).encode("utf-8")
        original = ExecutionConfigArtifact(
            artifact_id="a1" * 16,
            origin_step_number=3,
            content=content,
            original_name="config",
            extension=".json",
            metadata={"version": "1"},
        )
        row = original.to_row()
        restored = ExecutionConfigArtifact.from_row(row)

        assert restored.artifact_id == original.artifact_id
        assert restored.content == original.content
        assert restored.metadata == original.metadata

    def test_schema_keys_match_row_keys(self) -> None:
        artifact = ExecutionConfigArtifact(artifact_id="b1" * 16, origin_step_number=0)
        row = artifact.to_row()
        assert set(row.keys()) == set(ExecutionConfigArtifact.POLARS_SCHEMA.keys())


class TestFileRefRoundtrip:
    """FileRefArtifact to_row / from_row roundtrip."""

    def test_roundtrip(self) -> None:
        original = FileRefArtifact(
            artifact_id="c1" * 16,
            origin_step_number=0,
            content_hash="abc123" + "0" * 26,
            path="/data/input.dat",
            size_bytes=1024,
            original_name="input",
            extension=".dat",
            metadata={"tag": "production"},
        )
        row = original.to_row()
        restored = FileRefArtifact.from_row(row)

        assert restored.artifact_id == original.artifact_id
        assert restored.content_hash == original.content_hash
        assert restored.path == original.path
        assert restored.size_bytes == original.size_bytes
        assert restored.metadata == original.metadata

    def test_schema_keys_match_row_keys(self) -> None:
        artifact = FileRefArtifact(artifact_id="d1" * 16, origin_step_number=0)
        row = artifact.to_row()
        assert set(row.keys()) == set(FileRefArtifact.POLARS_SCHEMA.keys())


class TestConcreteTypeDefs:
    """Concrete type defs registered via __init_subclass__."""

    def test_metric_type_def(self) -> None:
        td = ArtifactTypeDef.get("metric")
        assert td.model is MetricArtifact
        assert td.table_path == "artifacts/metrics"

    def test_config_type_def(self) -> None:
        td = ArtifactTypeDef.get("config")
        assert td.model is ExecutionConfigArtifact
        assert td.table_path == "artifacts/configs"

    def test_file_ref_type_def(self) -> None:
        td = ArtifactTypeDef.get("file_ref")
        assert td.model is FileRefArtifact
        assert td.table_path == "artifacts/file_refs"

    def test_all_four_registered(self) -> None:
        all_defs = ArtifactTypeDef.get_all()
        assert set(all_defs.keys()) >= {"data", "metric", "config", "file_ref"}


class TestDataFrameIntegration:
    """Verify to_row() output works with polars DataFrame construction."""

    def test_file_ref_dataframe(self) -> None:
        artifact = FileRefArtifact(
            artifact_id="e1" * 16,
            origin_step_number=0,
            content_hash="k" * 32,
            path="/data/input.dat",
            size_bytes=1024,
            original_name="input",
            extension=".dat",
            metadata={"k": "v"},
        )
        df = pl.DataFrame([artifact.to_row()], schema=FileRefArtifact.POLARS_SCHEMA)
        assert len(df) == 1
        assert df["artifact_id"][0] == "e1" * 16

    def test_metric_dataframe(self) -> None:
        artifact = MetricArtifact(
            artifact_id="f1" * 16,
            origin_step_number=2,
            content=b'{"accuracy": 1.5}',
            original_name="accuracy",
            extension=".json",
        )
        df = pl.DataFrame([artifact.to_row()], schema=MetricArtifact.POLARS_SCHEMA)
        assert len(df) == 1
