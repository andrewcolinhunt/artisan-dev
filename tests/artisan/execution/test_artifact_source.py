"""Tests for ArtifactSource."""

from __future__ import annotations

from unittest.mock import MagicMock

from artisan.execution.models.artifact_source import ArtifactSource
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.specs.input_spec import InputSpec


def _make_metric(artifact_id: str) -> MetricArtifact:
    """Create a minimal MetricArtifact for testing."""
    import json

    return MetricArtifact(
        artifact_id=artifact_id,
        artifact_type="metric",
        content=json.dumps({"value": 1.0}).encode(),
        original_name="test",
        extension=".json",
        origin_step_number=0,
    )


class TestArtifactSourceFromIds:
    def test_from_ids_creates_id_backed_source(self) -> None:
        source = ArtifactSource.from_ids(["a" * 32, "b" * 32])
        assert source._ids == ["a" * 32, "b" * 32]
        assert source._artifacts is None

    def test_from_ids_is_not_materialized(self) -> None:
        source = ArtifactSource.from_ids(["a" * 32])
        assert source.is_materialized is False

    def test_from_ids_copies_list(self) -> None:
        ids = ["a" * 32]
        source = ArtifactSource.from_ids(ids)
        ids.append("b" * 32)
        assert len(source._ids) == 1


class TestArtifactSourceFromArtifacts:
    def test_from_artifacts_creates_materialized_source(self) -> None:
        art = _make_metric("a" * 32)
        source = ArtifactSource.from_artifacts([art])
        assert source._artifacts is not None
        assert len(source._artifacts) == 1
        assert source._ids is None

    def test_from_artifacts_is_materialized(self) -> None:
        source = ArtifactSource.from_artifacts([_make_metric("a" * 32)])
        assert source.is_materialized is True

    def test_from_artifacts_copies_list(self) -> None:
        arts = [_make_metric("a" * 32)]
        source = ArtifactSource.from_artifacts(arts)
        arts.append(_make_metric("b" * 32))
        assert len(source._artifacts) == 1


class TestArtifactSourceHydrate:
    def test_hydrate_in_memory_returns_artifacts(self) -> None:
        art = _make_metric("a" * 32)
        source = ArtifactSource.from_artifacts([art])
        spec = InputSpec(artifact_type="metric")
        result = source.hydrate(MagicMock(), spec)
        assert len(result) == 1
        assert result[0].artifact_id == "a" * 32

    def test_hydrate_in_memory_returns_copy(self) -> None:
        art = _make_metric("a" * 32)
        source = ArtifactSource.from_artifacts([art])
        spec = InputSpec(artifact_type="metric")
        result1 = source.hydrate(MagicMock(), spec)
        result2 = source.hydrate(MagicMock(), spec)
        assert result1 is not result2

    def test_hydrate_empty_ids_returns_empty(self) -> None:
        source = ArtifactSource.from_ids([])
        spec = InputSpec(artifact_type="metric")
        result = source.hydrate(MagicMock(), spec)
        assert result == []

    def test_hydrate_none_ids_returns_empty(self) -> None:
        source = ArtifactSource(_ids=None, _artifacts=None)
        spec = InputSpec(artifact_type="metric")
        result = source.hydrate(MagicMock(), spec)
        assert result == []
