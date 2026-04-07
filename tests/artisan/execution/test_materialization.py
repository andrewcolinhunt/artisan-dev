"""Tests for materialize_inputs() format threading."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from artisan.execution.inputs.materialization import materialize_inputs
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.specs.input_spec import InputSpec


def _make_metric(artifact_id: str = "a" * 32) -> MetricArtifact:
    """Create a hydrated MetricArtifact for testing."""
    return MetricArtifact.draft(
        content={"score": 0.5},
        original_name="test.json",
        step_number=1,
    ).finalize()


class TestMaterializeAsForwarded:
    """Tests that materialize_as is forwarded to artifact.materialize_to()."""

    def test_materialize_as_passed_to_artifact(self, tmp_path: Path):
        """format kwarg from spec.materialize_as is forwarded."""
        artifact = MagicMock(spec=Artifact)
        artifact.is_hydrated = True
        artifact.artifact_id = "a" * 32
        artifact.materialize_to.return_value = tmp_path / "out.csv"

        specs = {"data": InputSpec(materialize=True, materialize_as=".csv")}
        artifacts = {"data": [artifact]}
        mock_store = MagicMock()

        _, materialized_ids = materialize_inputs(artifacts, specs, tmp_path, mock_store)

        artifact.materialize_to.assert_called_once_with(tmp_path, format=".csv")
        assert "a" * 32 in materialized_ids

    def test_no_materialize_as_passes_none(self, tmp_path: Path):
        """Default spec passes format=None."""
        artifact = MagicMock(spec=Artifact)
        artifact.is_hydrated = True
        artifact.artifact_id = "b" * 32
        artifact.materialize_to.return_value = tmp_path / "out.json"

        specs = {"metric": InputSpec(materialize=True)}
        artifacts = {"metric": [artifact]}
        mock_store = MagicMock()

        _, materialized_ids = materialize_inputs(artifacts, specs, tmp_path, mock_store)

        artifact.materialize_to.assert_called_once_with(tmp_path, format=None)
        assert "b" * 32 in materialized_ids

    def test_config_referenced_artifacts_get_none_format(self, tmp_path: Path):
        """Artifacts resolved from config references get format=None."""
        from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

        config = ExecutionConfigArtifact.draft(
            content={"input": {"$artifact": "c" * 32}},
            original_name="config.json",
            step_number=1,
        ).finalize()

        ref_artifact = MagicMock(spec=Artifact)
        ref_artifact.is_hydrated = True
        ref_artifact.artifact_id = "c" * 32
        ref_artifact.materialize_to.return_value = tmp_path / "ref.dat"

        mock_store = MagicMock()
        mock_store.get_artifact.return_value = ref_artifact

        specs = {"config": InputSpec(materialize=True)}
        artifacts = {"config": [config]}

        _, materialized_ids = materialize_inputs(artifacts, specs, tmp_path, mock_store)

        ref_artifact.materialize_to.assert_called_once_with(tmp_path, format=None)
        assert config.artifact_id in materialized_ids
        assert "c" * 32 in materialized_ids
