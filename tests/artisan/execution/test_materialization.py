"""Tests for materialize_inputs() format threading and endpoint routing."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import pytest

from artisan.execution.inputs.materialization import _is_remote, materialize_inputs
from artisan.schemas.artifact.appendable import AppendableArtifact
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.large_file import LargeFileArtifact
from artisan.schemas.specs.input_spec import InputSpec


class TestMaterializeAsForwarded:
    """Tests that materialize_as is forwarded to artifact.materialize_to()."""

    def test_materialize_as_passed_to_artifact(self, tmp_path: Path):
        """format kwarg from spec.materialize_as is forwarded."""
        artifact = MagicMock(spec=Artifact)
        artifact.is_hydrated = True
        artifact.artifact_id = "a" * 32
        artifact.EXTERNALLY_BACKED = False
        artifact.materialize_to.return_value = str(tmp_path / "out.csv")

        specs = {"data": InputSpec(materialize=True, materialize_as=".csv")}
        artifacts = {"data": [artifact]}
        mock_store = MagicMock()

        directory = str(tmp_path)
        result = materialize_inputs(artifacts, specs, directory, mock_store)

        artifact.materialize_to.assert_called_once_with(
            directory, format=".csv", fs=ANY
        )
        assert result == artifacts

    def test_no_materialize_as_passes_none(self, tmp_path: Path):
        """Default spec passes format=None."""
        artifact = MagicMock(spec=Artifact)
        artifact.is_hydrated = True
        artifact.artifact_id = "b" * 32
        artifact.EXTERNALLY_BACKED = False
        artifact.materialize_to.return_value = str(tmp_path / "out.json")

        specs = {"metric": InputSpec(materialize=True)}
        artifacts = {"metric": [artifact]}
        mock_store = MagicMock()

        directory = str(tmp_path)
        result = materialize_inputs(artifacts, specs, directory, mock_store)

        artifact.materialize_to.assert_called_once_with(directory, format=None, fs=ANY)
        assert result == artifacts

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
        ref_artifact.EXTERNALLY_BACKED = False
        ref_artifact.materialize_to.return_value = str(tmp_path / "ref.dat")

        mock_store = MagicMock()
        mock_store.get_artifact.return_value = ref_artifact

        specs = {"config": InputSpec(materialize=True)}
        artifacts = {"config": [config]}

        directory = str(tmp_path)
        result = materialize_inputs(artifacts, specs, directory, mock_store)

        ref_artifact.materialize_to.assert_called_once_with(
            directory, format=None, fs=ANY
        )
        assert result is artifacts
        assert config.materialized_path is not None

    def test_missing_config_reference_fails_before_materialization(
        self, tmp_path: Path
    ) -> None:
        """A missing fixed reference fails before any input is written."""
        from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

        config = ExecutionConfigArtifact.draft(
            content={"input": {"$artifact": "c" * 32}},
            original_name="config.json",
            step_number=1,
        ).finalize()
        ordinary = MagicMock(spec=Artifact)
        ordinary.is_hydrated = True
        ordinary.artifact_id = "a" * 32
        ordinary.EXTERNALLY_BACKED = False
        store = MagicMock()
        store.get_artifact.return_value = None

        with pytest.raises(ValueError, match="Referenced artifact"):
            materialize_inputs(
                {"data": [ordinary], "config": [config]},
                {
                    "data": InputSpec(materialize=True),
                    "config": InputSpec(materialize=True),
                },
                str(tmp_path),
                store,
            )

        ordinary.materialize_to.assert_not_called()


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
        # Direct-URI transport verifies the current source without materializing it.
        art = self._cloud_large_file()
        specs = {"weights": InputSpec(artifact_type="large_file")}
        output_dir = tmp_path / "outputs"
        output_dir.mkdir()

        with patch.object(LargeFileArtifact, "verify_external_content") as verify:
            result = materialize_inputs(
                {"weights": [art]},
                specs,
                str(output_dir),
                MagicMock(),
                endpoint_routed=True,
            )

        verify.assert_called_once_with(fs=ANY)
        assert art.materialized_path == "s3://bucket/weights/model.bin"
        assert list(output_dir.iterdir()) == []  # nothing downloaded
        assert result == {"weights": [art]}

    def test_remote_file_ref_skipped_when_endpoint_routed(self, tmp_path: Path):
        art = FileRefArtifact.draft(
            path="s3://bucket/inputs/source.bin",
            content_hash="a" * 32,
            size_bytes=4,
            step_number=0,
            original_name="source",
            extension=".bin",
        ).finalize()

        with patch.object(FileRefArtifact, "verify_external_content") as verify:
            result = materialize_inputs(
                {"source": [art]},
                {"source": InputSpec(artifact_type="file_ref")},
                str(tmp_path),
                MagicMock(),
                endpoint_routed=True,
            )

        verify.assert_called_once_with(fs=ANY)
        assert art.materialized_path == "s3://bucket/inputs/source.bin"
        assert result == {"source": [art]}

    def test_appendable_record_materializes_when_endpoint_routed(self, tmp_path: Path):
        art = AppendableArtifact.draft(
            record_id="record-1",
            content_hash="a" * 32,
            size_bytes=4,
            step_number=0,
            external_path="s3://bucket/shared/records.jsonl",
        ).finalize()
        materialized = str(tmp_path / "record.json")

        with patch.object(
            AppendableArtifact,
            "materialize_to",
            return_value=materialized,
        ) as write:
            result = materialize_inputs(
                {"record": [art]},
                {"record": InputSpec(artifact_type="appendable")},
                str(tmp_path),
                MagicMock(),
                endpoint_routed=True,
            )

        write.assert_called_once_with(str(tmp_path), format=None, fs=ANY)
        assert result == {"record": [art]}

    def test_cloud_input_downloads_by_default(self, tmp_path: Path):
        # endpoint_routed defaults False — the cloud input materializes
        # exactly as today (materialize_to is called)
        art = MagicMock(spec=Artifact)
        art.is_hydrated = True
        art.artifact_id = "a" * 32
        art.external_path = "s3://bucket/weights/model.bin"
        art.EXTERNALLY_BACKED = True
        art.LOCATOR_FIELDS = frozenset({"external_path"})
        art.materialize_to.return_value = str(tmp_path / "model.bin")
        specs = {"weights": InputSpec(materialize=True)}

        result = materialize_inputs(
            {"weights": [art]}, specs, str(tmp_path), MagicMock()
        )

        art.materialize_to.assert_called_once_with(str(tmp_path), format=None, fs=ANY)
        assert result == {"weights": [art]}

    def test_cloud_input_downloads_when_endpoint_routed_false(self, tmp_path: Path):
        art = MagicMock(spec=Artifact)
        art.is_hydrated = True
        art.artifact_id = "a" * 32
        art.external_path = "s3://bucket/weights/model.bin"
        art.EXTERNALLY_BACKED = True
        art.LOCATOR_FIELDS = frozenset({"external_path"})
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
        art.EXTERNALLY_BACKED = True
        art.LOCATOR_FIELDS = frozenset({"external_path"})
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

            result = materialize_inputs(
                {"data": [art]},
                specs,
                str(sub),
                MagicMock(),
                endpoint_routed=routed,
            )

            assert art.materialized_path is not None
            assert Path(art.materialized_path).exists()
            assert Path(art.materialized_path).read_bytes() == b"a,b\n1,2\n"
            assert result == {"data": [art]}

    def test_config_artifact_unaffected_by_flag(self, tmp_path: Path):
        # config artifacts resolve through resolved_paths, not the skip —
        # the flag must not change their materialization
        from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

        config = ExecutionConfigArtifact.draft(
            content={"threshold": 5}, original_name="config.json", step_number=1
        ).finalize()
        specs = {"config": InputSpec(materialize=True)}

        result = materialize_inputs(
            {"config": [config]},
            specs,
            str(tmp_path),
            MagicMock(),
            endpoint_routed=True,
        )

        assert result == {"config": [config]}
        assert config.materialized_path is not None
        assert Path(config.materialized_path).exists()


@pytest.mark.parametrize("remote", [False, True])
def test_endpoint_config_references_fail_before_input_side_effects(
    tmp_path: Path, remote: bool
) -> None:
    from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

    ref = FileRefArtifact.draft(
        path="s3://bucket/input.bin" if remote else str(tmp_path / "input.bin"),
        content_hash="a" * 32,
        size_bytes=4,
        step_number=0,
    ).finalize()
    config = ExecutionConfigArtifact.draft(
        {"input": {"$artifact": ref.artifact_id}}, "config.json", 1
    ).finalize()
    ordinary = MagicMock(spec=Artifact)
    ordinary.is_hydrated = True
    ordinary.artifact_id = "b" * 32
    ordinary.EXTERNALLY_BACKED = False
    store = MagicMock()
    store.get_artifact.return_value = ref

    before = set(tmp_path.iterdir())
    with (
        patch.object(FileRefArtifact, "verify_external_content") as verify,
        pytest.raises(ValueError, match="Endpoint.*artifact references"),
    ):
        materialize_inputs(
            {"data": [ordinary], "config": [config]},
            {"data": InputSpec(), "config": InputSpec()},
            str(tmp_path),
            store,
            endpoint_routed=True,
        )

    store.get_artifact.assert_not_called()
    verify.assert_not_called()
    ordinary.materialize_to.assert_not_called()
    assert set(tmp_path.iterdir()) == before


def test_non_materialized_endpoint_config_keeps_references(tmp_path: Path) -> None:
    from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

    config = ExecutionConfigArtifact.draft(
        {"input": {"$artifact": "a" * 32}}, "config.json", 1
    ).finalize()
    store = MagicMock()
    before = set(tmp_path.iterdir())
    result = materialize_inputs(
        {"config": [config]},
        {"config": InputSpec(materialize=False)},
        str(tmp_path),
        store,
        endpoint_routed=True,
    )
    assert result == {"config": [config]}
    assert config.materialized_path is None
    store.get_artifact.assert_not_called()
    assert set(tmp_path.iterdir()) == before


def test_local_config_substitutes_materialized_reference(tmp_path: Path) -> None:
    import json

    from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

    artifact = DataArtifact.draft(b"value\n1\n", "input.csv", 0).finalize()
    config = ExecutionConfigArtifact.draft(
        {"input": {"$artifact": artifact.artifact_id}}, "config.json", 1
    ).finalize()
    store = MagicMock()
    store.get_artifact.return_value = artifact
    result = materialize_inputs(
        {"config": [config]}, {"config": InputSpec()}, str(tmp_path), store
    )
    assert result == {"config": [config]}
    assert json.loads(Path(config.materialized_path).read_text()) == {
        "input": artifact.materialized_path
    }
    assert Path(artifact.materialized_path).read_bytes() == b"value\n1\n"


def test_embedded_artifact_materializes_without_origin_metadata(
    tmp_path: Path,
) -> None:
    artifact = DataArtifact.draft(b"value\n1\n", "input.csv", 0)
    artifact.origin_step_number = None
    artifact.finalize()
    shell = DataArtifact(artifact_id="a" * 32)
    assert artifact.origin_step_number is None
    result = materialize_inputs(
        {"data": [artifact, shell]}, {"data": InputSpec()}, str(tmp_path), MagicMock()
    )
    assert result == {"data": [artifact, shell]}
    assert Path(artifact.materialized_path).read_bytes() == b"value\n1\n"
    assert shell.materialized_path is None
