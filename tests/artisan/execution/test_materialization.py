"""Input materialization uses captured context and explicit source filesystems."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import ANY, MagicMock

import pytest
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from artisan.errors import ArtifactIntegrityError
from artisan.execution.inputs.materialization import (
    materialize_inputs,
    resolve_input_references,
)
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.specs.input_spec import InputSpec
from artisan.utils.hashing import compute_content_digest


def test_same_basename_file_refs_materialize_without_overwriting(
    tmp_path: Path,
) -> None:
    artifacts = []
    contents = [b"first generated file", b"second generated file"]
    for index, content in enumerate(contents):
        source = tmp_path / str(index) / "output.bin"
        source.parent.mkdir()
        source.write_bytes(content)
        artifacts.append(
            FileRefArtifact.draft(
                path=str(source),
                content_hash=compute_content_digest(content),
                size_bytes=len(content),
                step_number=index,
                original_name="output",
                extension=".bin",
            ).finalize()
        )
    destination = tmp_path / "inputs"
    destination.mkdir()
    store = MagicMock()
    store._fs = LocalFileSystem()

    result = materialize_inputs(
        {"files": artifacts},
        {"files": InputSpec(artifact_type="file_ref")},
        str(destination),
        store,
    )

    assert result == {"files": artifacts}
    assert artifacts[0].materialized_path != artifacts[1].materialized_path
    for artifact, content in zip(artifacts, contents, strict=True):
        path = Path(artifact.materialized_path)
        assert path == destination / artifact.artifact_id / "output.bin"
        assert path.read_bytes() == content


def _typed_spec() -> InputSpec:
    return InputSpec(artifact_type="config", materialize=False, resolve_references=True)


def test_typed_references_preserve_occurrences_and_config_scoped_dependencies(
    tmp_path: Path,
) -> None:
    first = CSVArtifact.draft(b"value\n1\n", "same.csv", 0).finalize()
    second = CSVArtifact.draft(b"value\n2\n", "same.csv", 0).finalize()
    payload = {
        "nested": [{"$artifact": first.artifact_id}, {"$artifact": first.artifact_id}],
        "other": {"source": {"$artifact": second.artifact_id}},
    }
    config = ExecutionConfigArtifact.draft(payload, "first.json", 1).finalize()
    other = ExecutionConfigArtifact.draft(
        {"source": {"$artifact": first.artifact_id}}, "second.json", 1
    ).finalize()
    lookup = {first.artifact_id: first, second.artifact_id: second}
    store = MagicMock()
    store.get_artifact.side_effect = lambda artifact_id, **_: lookup[artifact_id]

    references = resolve_input_references(
        {"config": [config, other]}, {"config": _typed_spec()}, str(tmp_path), store
    )

    assert store.get_artifact.call_count == 2
    assert references[config.artifact_id] == lookup
    assert references[other.artifact_id] == {first.artifact_id: first}
    assert config.values == payload
    assert config.materialized_path is None
    assert config.get_artifact_references().count(first.artifact_id) == 2
    assert Path(first.materialized_path).read_bytes() == first.content
    assert Path(second.materialized_path).read_bytes() == second.content
    assert Path(first.materialized_path).parent.name == first.artifact_id
    assert Path(second.materialized_path).parent.name == second.artifact_id


@pytest.mark.parametrize("reference", [None, 17, {}, [], "invalid"])
def test_typed_references_reject_invalid_ids_before_store_lookup(
    tmp_path: Path, reference: object
) -> None:
    before = set(tmp_path.iterdir())
    config = ExecutionConfigArtifact.draft(
        {"source": {"$artifact": reference}}, "config.json", 0
    ).finalize()
    store = MagicMock()
    with pytest.raises(ValueError, match="hexadecimal artifact IDs"):
        resolve_input_references(
            {"config": [config]}, {"config": _typed_spec()}, str(tmp_path), store
        )
    store.get_artifact.assert_not_called()
    assert set(tmp_path.iterdir()) == before


def test_typed_references_reject_config_targets(tmp_path: Path) -> None:
    before = set(tmp_path.iterdir())
    target = ExecutionConfigArtifact.draft({}, "target.json", 0).finalize()
    config = ExecutionConfigArtifact.draft(
        {"source": {"$artifact": target.artifact_id}}, "config.json", 1
    ).finalize()
    store = MagicMock()
    store.get_artifact.return_value = target
    with pytest.raises(ValueError, match="cannot target another config"):
        resolve_input_references(
            {"config": [config]}, {"config": _typed_spec()}, str(tmp_path), store
        )
    assert set(tmp_path.iterdir()) == before


@pytest.mark.parametrize("corrupt", [False, True])
def test_typed_references_fail_before_writing_partial_inputs(
    tmp_path: Path, corrupt: bool
) -> None:
    before = set(tmp_path.iterdir())
    valid = CSVArtifact.draft(b"value\n1\n", "valid.csv", 0).finalize()
    config = ExecutionConfigArtifact.draft(
        [{"$artifact": valid.artifact_id}, {"$artifact": "a" * 32}],
        "config.json",
        1,
    ).finalize()
    store = MagicMock()
    store.get_artifact.side_effect = [
        valid,
        ArtifactIntegrityError("Invalid referenced content") if corrupt else None,
    ]
    with pytest.raises((ValueError, ArtifactIntegrityError)):
        resolve_input_references(
            {"config": [config]}, {"config": _typed_spec()}, str(tmp_path), store
        )
    assert set(tmp_path.iterdir()) == before


def test_typed_references_default_does_not_load_or_materialize(tmp_path: Path) -> None:
    before = set(tmp_path.iterdir())
    config = ExecutionConfigArtifact.draft(
        {"source": {"$artifact": "a" * 32}}, "config.json", 0
    ).finalize()
    store = MagicMock()
    assert (
        resolve_input_references(
            {"config": [config]},
            {"config": InputSpec(artifact_type="config", materialize=False)},
            str(tmp_path),
            store,
        )
        == {}
    )
    store.get_artifact.assert_not_called()
    assert set(tmp_path.iterdir()) == before


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


def test_non_materialized_config_keeps_references(tmp_path: Path) -> None:
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
    )
    assert result == {"config": [config]}
    assert config.materialized_path is None
    store.get_artifact.assert_not_called()
    assert set(tmp_path.iterdir()) == before


def test_local_config_substitutes_materialized_reference(tmp_path: Path) -> None:
    import json

    from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

    artifact = CSVArtifact.draft(b"value\n1\n", "input.csv", 0).finalize()
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
    artifact = CSVArtifact.draft(b"value\n1\n", "input.csv", 0)
    artifact.origin_step_number = None
    artifact.finalize()
    shell = CSVArtifact(artifact_id="a" * 32)
    assert artifact.origin_step_number is None
    result = materialize_inputs(
        {"data": [artifact, shell]}, {"data": InputSpec()}, str(tmp_path), MagicMock()
    )
    assert result == {"data": [artifact, shell]}
    assert Path(artifact.materialized_path).read_bytes() == b"value\n1\n"
    assert shell.materialized_path is None


def test_captured_references_materialize_without_store_access(tmp_path: Path) -> None:
    from artisan.execution.inputs.materialization import (
        capture_input_references,
        materialize_captured_inputs,
        materialize_captured_references,
    )

    artifact = CSVArtifact.draft(b"value\n1\n", "input.csv", 1).finalize()
    config = ExecutionConfigArtifact.draft(
        {"input": {"$artifact": artifact.artifact_id}}, "config.json", 1
    ).finalize()
    artifacts = {"config": [config]}
    specs = {"config": _typed_spec()}
    store = MagicMock()
    store.get_artifact.return_value = artifact
    captured = capture_input_references(artifacts, specs, store)
    store.get_artifact.side_effect = AssertionError("store access after capture")

    materialize_captured_inputs(artifacts, specs, str(tmp_path), captured)
    references = materialize_captured_references(
        artifacts, specs, str(tmp_path), captured
    )

    assert references == {config.artifact_id: {artifact.artifact_id: artifact}}
    assert Path(artifact.materialized_path).read_bytes() == artifact.content
    assert store.get_artifact.call_count == 1


@pytest.mark.parametrize("captured_empty", [False, True])
def test_captured_empty_config_is_distinct_from_missing_context(
    tmp_path: Path, captured_empty: bool
) -> None:
    from artisan.execution.inputs.materialization import materialize_captured_inputs

    config = ExecutionConfigArtifact.draft({}, "empty.json", 1).finalize()
    references = {config.artifact_id: {}} if captured_empty else {}
    before = set(tmp_path.iterdir())
    if not captured_empty:
        with pytest.raises(ValueError, match="not captured"):
            materialize_captured_inputs(
                {"config": [config]}, {"config": InputSpec()}, str(tmp_path), references
            )
        assert set(tmp_path.iterdir()) == before
        return
    materialize_captured_inputs(
        {"config": [config]}, {"config": InputSpec()}, str(tmp_path), references
    )
    assert Path(config.materialized_path).read_text() == "{}"


def test_captured_dependency_membership_fails_before_writing(tmp_path: Path) -> None:
    from artisan.execution.inputs.materialization import materialize_captured_inputs

    before = set(tmp_path.iterdir())
    artifact = CSVArtifact.draft(b"value\n1\n", "input.csv", 1).finalize()
    config = ExecutionConfigArtifact.draft(
        {"input": {"$artifact": artifact.artifact_id}}, "config.json", 1
    ).finalize()
    with pytest.raises(ValueError, match="do not match"):
        materialize_captured_inputs(
            {"data": [artifact], "config": [config]},
            {"data": InputSpec(), "config": InputSpec()},
            str(tmp_path),
            {config.artifact_id: {}},
        )
    assert set(tmp_path.iterdir()) == before


def test_equal_content_distinct_inputs_share_bytes_but_both_receive_paths(
    tmp_path: Path,
) -> None:
    from artisan.execution.inputs.materialization import materialize_captured_inputs

    first = CSVArtifact.draft(b"value\n1\n", "input.csv").finalize()
    second = CSVArtifact.draft(b"value\n1\n", "input.csv").finalize()
    assert first is not second
    assert first.artifact_id == second.artifact_id
    materialize_captured_inputs(
        {"data": [first, second]}, {"data": InputSpec()}, str(tmp_path), {}
    )
    assert first.materialized_path == second.materialized_path
    assert Path(second.materialized_path).read_bytes() == first.content


@pytest.mark.parametrize("remote", [False, True])
def test_external_inputs_are_verified_and_materialized_with_selected_filesystem(
    tmp_path: Path, remote: bool
) -> None:
    payload = b"bounded carrier bytes"
    fs = MemoryFileSystem() if remote else LocalFileSystem()
    source = (
        f"memory://{tmp_path.name}/source.bin"
        if remote
        else str(tmp_path / "source.bin")
    )
    with fs.open(source, "wb") as stream:
        stream.write(payload)
    artifact = FileRefArtifact.draft(
        source, compute_content_digest(payload), len(payload), 0
    ).finalize()
    store = MagicMock()
    store._fs = fs
    result = materialize_inputs(
        {"source": [artifact]},
        {"source": InputSpec(artifact_type="file_ref")},
        str(tmp_path / "materialized"),
        store,
    )
    assert result == {"source": [artifact]}
    assert artifact.materialized_path is not None
    assert "://" not in artifact.materialized_path
    assert Path(artifact.materialized_path).read_bytes() == payload
    with fs.open(source, "wb") as stream:
        stream.write(b"changed carrier bytes")
    with pytest.raises(ArtifactIntegrityError):
        materialize_inputs(
            {"source": [artifact]},
            {"source": InputSpec(artifact_type="file_ref")},
            str(tmp_path / "second-materialization"),
            store,
        )
