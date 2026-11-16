"""Private capture lifetime follows command closure rather than object finalizers."""

from __future__ import annotations

import gc
import shutil
from pathlib import Path
from typing import Any

import pytest
from fixtures.candidate_outputs import external_case
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import ArtifactIntegrityError
from artisan.execution import curator_resources as resources
from artisan.schemas.artifact import external
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.storage.io import curator_carriers
from artisan.utils.command_supervisor import CommandOwnershipError
from artisan.utils.hashing import compute_content_digest


def _capture(source: Path) -> Path:
    payload = source.read_bytes()
    digest = compute_content_digest(payload)
    artifact = FileRefArtifact.draft(str(source), digest, len(payload), 1).finalize()
    return resources.capture_curator_carrier(
        artifact,
        uri=str(source),
        digest=digest,
        size_bytes=len(payload),
        fs=LocalFileSystem(),
    )


@pytest.mark.parametrize("working", [False, True])
@pytest.mark.parametrize("preserve", [False, True])
def test_capture_owns_bytes_through_scope_then_removes_after_closure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, working: bool, preserve: bool
) -> None:
    closed: list[bool] = []

    def close(_error: BaseException | None) -> bool:
        closed.append(True)
        return True

    monkeypatch.setattr(resources, "close_command_scope", close)
    source = tmp_path / "source"
    source.write_bytes(b"source")
    with resources.curator_resources(
        str(tmp_path / "worker") if working else None, preserve_working=preserve
    ):
        target = _capture(source)
        assert _capture(source) == target
        if working:
            assert target.is_relative_to(tmp_path / "worker")
        assert resources.verify_owned_curator_file(str(target))
        source.unlink()
        assert target.read_bytes() == b"source"
    assert closed == [True]
    assert target.parent.exists() == preserve
    if preserve:
        assert target.read_bytes() == b"source"
        shutil.rmtree(target.parent)


def test_unknown_ownership_retains_copies_without_finalizer_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(resources, "close_command_scope", lambda _error: False)
    source = tmp_path / "source"
    source.write_bytes(b"source")
    with (
        pytest.raises(CommandOwnershipError),
        resources.curator_resources(str(tmp_path)),
    ):
        target = _capture(source)
        msg = "cleanup not confirmed"
        raise CommandOwnershipError(msg)
    gc.collect()
    assert target.read_bytes() == b"source"
    shutil.rmtree(target.parent)


def test_owned_copy_mutation_is_rejected_before_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(resources, "close_command_scope", lambda _error: True)
    source = tmp_path / "source"
    source.write_bytes(b"source")
    with resources.curator_resources(str(tmp_path)):
        target = _capture(source)
        target.write_bytes(b"changed")
        with pytest.raises(ArtifactIntegrityError):
            resources.verify_owned_curator_file(str(target))
        assert not resources.verify_owned_curator_file(str(source))


def test_external_capture_requires_explicit_invocation_scope(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"source")
    with pytest.raises(RuntimeError, match="active curator resource scope"):
        _capture(source)


def test_capture_uses_supplied_source_binding_without_ambient_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"bound content")
    payload = source.read_bytes()
    digest = compute_content_digest(payload)
    artifact = FileRefArtifact.draft(str(source), digest, len(payload), 1).finalize()

    class BoundSource(LocalFileSystem):
        protocol = "private-source"

        def open(self, path: str, *args: Any, **kwargs: Any) -> Any:
            assert path == str(source)
            return source.open("rb")

    monkeypatch.setattr(
        external,
        "resolve_fs",
        lambda *_args, **_kwargs: pytest.fail("Ambient source resolution is forbidden"),
    )
    monkeypatch.setattr(resources, "close_command_scope", lambda _error: True)
    with resources.curator_resources(str(tmp_path / "worker")):
        target = resources.capture_curator_carrier(
            artifact,
            uri=str(source),
            digest=digest,
            size_bytes=len(payload),
            fs=BoundSource(),
        )
        assert target.read_bytes() == payload


def test_destination_staging_rechecks_complete_capture_after_the_precheck(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = external_case(tmp_path / "source", appendable=True)
    artifact = next(iter(case.artifacts.values()))
    source = Path(artifact.external_path)
    payload = source.read_bytes()
    monkeypatch.setattr(resources, "close_command_scope", lambda _error: True)
    precheck = curator_carriers.verify_owned_curator_file

    def mutate_after_precheck(uri: str) -> tuple[str, int] | None:
        identity = precheck(uri)
        with open(uri, "ab") as stream:
            stream.write(b'{"record_id": "added", "value": 3}\n')
        return identity

    monkeypatch.setattr(
        curator_carriers, "verify_owned_curator_file", mutate_after_precheck
    )
    with resources.curator_resources(str(tmp_path / "worker")):
        target = resources.capture_curator_carrier(
            artifact,
            uri=str(source),
            digest=compute_content_digest(payload),
            size_bytes=len(payload),
            fs=LocalFileSystem(),
        )
        localized = artifact.model_copy(update={"external_path": str(target)})
        stager = curator_carriers.CuratorCarrierStager(
            str(tmp_path / "staging"), LocalFileSystem()
        )
        with pytest.raises(ArtifactIntegrityError, match="changed during"):
            stager.capture(localized)
