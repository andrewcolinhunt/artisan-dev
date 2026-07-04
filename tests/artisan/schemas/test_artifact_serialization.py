"""Tests for model-owned serialization (to_row / from_row)."""

from __future__ import annotations

import json

import polars as pl
import pytest

from artisan.schemas.artifact.appendable import AppendableArtifact
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.large_file import LargeFileArtifact
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


_ALL_TYPE_INSTANCES = [
    DataArtifact(
        artifact_id="a" * 32,
        origin_step_number=0,
        content=b"x,y\n1,2\n",
        original_name="d",
        extension=".csv",
        size_bytes=8,
        columns=["x", "y"],
        row_count=1,
        metadata={"k": "v"},
        external_path="/ext/d.csv",
    ),
    MetricArtifact(
        artifact_id="b" * 32,
        origin_step_number=1,
        content=b'{"a": 1}',
        original_name="m",
        extension=".json",
        metadata={"k": "v"},
        external_path="/ext/m.json",
    ),
    ExecutionConfigArtifact(
        artifact_id="c" * 32,
        origin_step_number=2,
        content=b'{"a": 1}',
        original_name="c",
        extension=".json",
        metadata={"k": "v"},
        external_path="/ext/c.json",
    ),
    FileRefArtifact(
        artifact_id="d" * 32,
        origin_step_number=3,
        content_hash="e" * 32,
        path="/data/in.dat",
        size_bytes=1024,
        original_name="in",
        extension=".dat",
        metadata={"k": "v"},
        external_path="/ext/in.dat",
    ),
    LargeFileArtifact(
        artifact_id="f" * 32,
        origin_step_number=4,
        content_hash="0" * 32,
        size_bytes=2048,
        original_name="w",
        extension=".bin",
        metadata={"k": "v"},
        external_path="/ext/w.bin",
    ),
    AppendableArtifact(
        artifact_id="1" * 32,
        origin_step_number=5,
        record_id="r1",
        content_hash="2" * 32,
        size_bytes=64,
        original_name="rec",
        extension=".jsonl",
        metadata={"k": "v"},
        external_path="/ext/recs.jsonl",
    ),
]


class TestBaseRowSerialization:
    """Base-derived to_row/from_row across every concrete artifact type.

    Supersedes the per-type key-match guards: now that the base derives
    rows from POLARS_SCHEMA, one parametrized check covers all six types.
    """

    @pytest.mark.parametrize(
        "artifact", _ALL_TYPE_INSTANCES, ids=lambda a: type(a).__name__
    )
    def test_row_keys_match_schema(self, artifact: Artifact) -> None:
        assert set(artifact.to_row()) == set(artifact.POLARS_SCHEMA)

    @pytest.mark.parametrize(
        "artifact", _ALL_TYPE_INSTANCES, ids=lambda a: type(a).__name__
    )
    def test_roundtrip_preserves_common_fields(self, artifact: Artifact) -> None:
        restored = type(artifact).from_row(artifact.to_row())
        assert restored.artifact_id == artifact.artifact_id
        assert restored.origin_step_number == artifact.origin_step_number
        assert restored.metadata == artifact.metadata
        assert restored.external_path == artifact.external_path
