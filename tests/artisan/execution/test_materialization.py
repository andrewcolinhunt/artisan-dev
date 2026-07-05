"""Tests for materialize_inputs() format threading and endpoint routing."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import ANY, MagicMock

from artisan.execution.inputs.materialization import _is_remote, materialize_inputs
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.large_file import LargeFileArtifact
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
        artifact.materialize_to.return_value = str(tmp_path / "out.csv")

        specs = {"data": InputSpec(materialize=True, materialize_as=".csv")}
        artifacts = {"data": [artifact]}
        mock_store = MagicMock()

        directory = str(tmp_path)
        _, materialized_ids = materialize_inputs(
            artifacts, specs, directory, mock_store
        )

        artifact.materialize_to.assert_called_once_with(
            directory, format=".csv", fs=ANY
        )
        assert "a" * 32 in materialized_ids

    def test_no_materialize_as_passes_none(self, tmp_path: Path):
        """Default spec passes format=None."""
        artifact = MagicMock(spec=Artifact)
        artifact.is_hydrated = True
        artifact.artifact_id = "b" * 32
        artifact.materialize_to.return_value = str(tmp_path / "out.json")

        specs = {"metric": InputSpec(materialize=True)}
        artifacts = {"metric": [artifact]}
        mock_store = MagicMock()

        directory = str(tmp_path)
        _, materialized_ids = materialize_inputs(
            artifacts, specs, directory, mock_store
        )

        artifact.materialize_to.assert_called_once_with(directory, format=None, fs=ANY)
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
        ref_artifact.materialize_to.return_value = str(tmp_path / "ref.dat")

        mock_store = MagicMock()
        mock_store.get_artifact.return_value = ref_artifact

        specs = {"config": InputSpec(materialize=True)}
        artifacts = {"config": [config]}

        directory = str(tmp_path)
        _, materialized_ids = materialize_inputs(
            artifacts, specs, directory, mock_store
        )

        ref_artifact.materialize_to.assert_called_once_with(
            directory, format=None, fs=ANY
        )
        assert config.artifact_id in materialized_ids
        assert "c" * 32 in materialized_ids


class TestIsRemote:
    """``_is_remote`` reuses the ``://``-means-URI convention pack_inputs uses."""

    def test_cloud_uri_is_remote(self):
        assert _is_remote("s3://bucket/key.bin") is True

    def test_local_file_uri_is_not_remote(self):
        assert _is_remote("file:///tmp/x") is False

    def test_bare_local_path_is_not_remote(self):
        assert _is_remote("/tmp/x") is False

    def test_none_is_not_remote(self):
        assert _is_remote(None) is False


class TestEndpointRoutedSkip:
    """Endpoint-routed steps ship cloud inputs by reference (no download)."""

    def _cloud_large_file(self) -> LargeFileArtifact:
        return LargeFileArtifact.draft(
            content_hash="h" * 32,
            size_bytes=2_000_000_000,  # 2 GB — way past the 100 MB inline cap
            step_number=0,
            external_path="s3://bucket/weights/model.bin",
            original_name="model",
            extension=".bin",
        ).finalize()

    def test_cloud_input_skipped_when_endpoint_routed(self, tmp_path: Path):
        # a real cloud LargeFileArtifact: the skip must not touch the
        # network (materialize_to is never reached) and writes no file
        art = self._cloud_large_file()
        specs = {"weights": InputSpec(artifact_type="large_file")}

        _, materialized_ids = materialize_inputs(
            {"weights": [art]},
            specs,
            str(tmp_path),
            MagicMock(),
            endpoint_routed=True,
        )

        assert art.materialized_path == "s3://bucket/weights/model.bin"
        assert list(tmp_path.iterdir()) == []  # nothing downloaded
        # no local file entered the filesystem-passthrough match map
        assert art.artifact_id not in materialized_ids

    def test_cloud_input_downloads_by_default(self, tmp_path: Path):
        # endpoint_routed defaults False — the cloud input materializes
        # exactly as today (materialize_to is called)
        art = MagicMock(spec=Artifact)
        art.is_hydrated = True
        art.artifact_id = "a" * 32
        art.external_path = "s3://bucket/weights/model.bin"
        art.materialize_to.return_value = str(tmp_path / "model.bin")
        specs = {"weights": InputSpec(materialize=True)}

        _, materialized_ids = materialize_inputs(
            {"weights": [art]}, specs, str(tmp_path), MagicMock()
        )

        art.materialize_to.assert_called_once_with(str(tmp_path), format=None, fs=ANY)
        assert "a" * 32 in materialized_ids

    def test_cloud_input_downloads_when_endpoint_routed_false(self, tmp_path: Path):
        art = MagicMock(spec=Artifact)
        art.is_hydrated = True
        art.artifact_id = "a" * 32
        art.external_path = "s3://bucket/weights/model.bin"
        art.materialize_to.return_value = str(tmp_path / "model.bin")
        specs = {"weights": InputSpec(materialize=True)}

        materialize_inputs(
            {"weights": [art]},
            specs,
            str(tmp_path),
            MagicMock(),
            endpoint_routed=False,
        )

        art.materialize_to.assert_called_once()

    def test_local_external_path_downloads_even_when_endpoint_routed(
        self, tmp_path: Path
    ):
        # a local (bare/file://) external_path is not remote — the skip
        # does not fire, so a local-file input still materializes on an
        # endpoint step (local-file-over-cap stays out of scope for v1)
        art = MagicMock(spec=Artifact)
        art.is_hydrated = True
        art.artifact_id = "b" * 32
        art.external_path = str(tmp_path / "src.bin")
        art.materialize_to.return_value = str(tmp_path / "src.bin")
        specs = {"weights": InputSpec(materialize=True)}

        materialize_inputs(
            {"weights": [art]},
            specs,
            str(tmp_path),
            MagicMock(),
            endpoint_routed=True,
        )

        art.materialize_to.assert_called_once()

    def test_inline_data_artifact_materializes_in_both_modes(self, tmp_path: Path):
        # a DataArtifact carries no external_path — the inline path is
        # unchanged whether or not the step is endpoint-routed
        for routed in (True, False):
            sub = tmp_path / f"routed_{routed}"
            sub.mkdir()
            art = DataArtifact.draft(
                content=b"a,b\n1,2\n", original_name="d.csv", step_number=0
            ).finalize()
            specs = {"data": InputSpec(artifact_type="data")}

            _, materialized_ids = materialize_inputs(
                {"data": [art]},
                specs,
                str(sub),
                MagicMock(),
                endpoint_routed=routed,
            )

            assert art.materialized_path is not None
            assert Path(art.materialized_path).exists()
            assert Path(art.materialized_path).read_bytes() == b"a,b\n1,2\n"
            assert art.artifact_id in materialized_ids

    def test_config_artifact_unaffected_by_flag(self, tmp_path: Path):
        # config artifacts resolve through resolved_paths, not the skip —
        # the flag must not change their materialization
        from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

        config = ExecutionConfigArtifact.draft(
            content={"threshold": 5}, original_name="config.json", step_number=1
        ).finalize()
        specs = {"config": InputSpec(materialize=True)}

        _, materialized_ids = materialize_inputs(
            {"config": [config]},
            specs,
            str(tmp_path),
            MagicMock(),
            endpoint_routed=True,
        )

        assert config.artifact_id in materialized_ids
        assert config.materialized_path is not None
        assert Path(config.materialized_path).exists()
