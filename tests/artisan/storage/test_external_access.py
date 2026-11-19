"""Explicit preparation and no-copy reuse of immutable external installations."""

from __future__ import annotations

import errno
import hashlib
from pathlib import Path
from typing import Literal

import pytest

from artisan.errors import ArtifactIntegrityError
from artisan.schemas.artifact.external_descriptor import (
    EXTERNAL_FILES_RECEIPT,
    ExternalFile,
    ExternalFilesDescriptor,
    PreparedExternalFiles,
    expected_receipt_bytes,
    expected_receipt_digest,
)
from artisan.storage.external_access import (
    prepare_external_files,
    verify_prepared_external_files,
)
from artisan.utils.hashing import compute_content_digest


def _installation(
    root: Path, hash_algorithm: Literal["xxh3_128", "sha256"] = "xxh3_128"
) -> ExternalFilesDescriptor:
    (root / "nested").mkdir()
    (root / "nested/weights.bin").write_bytes(b"abc")
    return ExternalFilesDescriptor(
        files=(
            ExternalFile(
                path="nested/weights.bin",
                hash_algorithm=hash_algorithm,
                content_hash=(
                    hashlib.sha256(b"abc").hexdigest()
                    if hash_algorithm == "sha256"
                    else compute_content_digest(b"abc")
                ),
                size_bytes=3,
            ),
        )
    )


@pytest.mark.parametrize("hash_algorithm", ["xxh3_128", "sha256"])
def test_preparation_writes_only_completion_receipt(
    tmp_path: Path, hash_algorithm: Literal["xxh3_128", "sha256"]
) -> None:
    descriptor = _installation(tmp_path, hash_algorithm)
    payload = tmp_path / descriptor.files[0].path
    before = payload.stat()
    prepared = prepare_external_files(tmp_path, descriptor)
    assert payload.stat().st_ino == before.st_ino
    assert payload.stat().st_mtime_ns == before.st_mtime_ns
    assert {path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*")} == {
        "nested",
        "nested/weights.bin",
        EXTERNAL_FILES_RECEIPT,
    }
    assert verify_prepared_external_files(prepared) == tmp_path
    assert prepare_external_files(tmp_path, descriptor) == prepared


@pytest.mark.parametrize("hash_algorithm", ["xxh3_128", "sha256"])
def test_repeated_verification_never_reads_or_hashes_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    hash_algorithm: Literal["xxh3_128", "sha256"],
) -> None:
    prepared = prepare_external_files(tmp_path, _installation(tmp_path, hash_algorithm))
    original_open = Path.open

    def guard(path: Path, *args: object, **kwargs: object):
        if path.name != EXTERNAL_FILES_RECEIPT:
            pytest.fail("Runtime verification opened payload bytes")
        return original_open(path, *args, **kwargs)

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Runtime verification hashed payload bytes")

    monkeypatch.setattr(Path, "open", guard)
    monkeypatch.setattr(
        "artisan.storage.external_access.compute_stream_digest", forbidden
    )
    assert verify_prepared_external_files(prepared) == tmp_path
    assert verify_prepared_external_files(prepared) == tmp_path


@pytest.mark.parametrize(
    "kind", ["extra_file", "extra_dir", "wrong_size", "missing_file", "symlink"]
)
def test_external_layout_changes_are_rejected(tmp_path: Path, kind: str) -> None:
    prepared = prepare_external_files(tmp_path, _installation(tmp_path))
    payload = tmp_path / "nested/weights.bin"
    if kind == "extra_file":
        (tmp_path / "extra").write_text("unexpected")
    elif kind == "extra_dir":
        (tmp_path / "extra").mkdir()
    elif kind == "wrong_size":
        payload.write_bytes(b"different size")
    elif kind == "missing_file":
        payload.unlink()
    else:
        payload.unlink()
        payload.symlink_to(tmp_path / EXTERNAL_FILES_RECEIPT)
    with pytest.raises(ArtifactIntegrityError, match="layout|regular"):
        verify_prepared_external_files(prepared)


@pytest.mark.parametrize("hash_algorithm", ["xxh3_128", "sha256"])
def test_same_size_mutation_requires_explicit_reverification(
    tmp_path: Path, hash_algorithm: Literal["xxh3_128", "sha256"]
) -> None:
    prepared = prepare_external_files(tmp_path, _installation(tmp_path, hash_algorithm))
    (tmp_path / "nested/weights.bin").write_bytes(b"xyz")
    assert verify_prepared_external_files(prepared) == tmp_path
    with pytest.raises(ArtifactIntegrityError, match="content differs"):
        prepare_external_files(tmp_path, prepared.descriptor)


@pytest.mark.parametrize("kind", ["missing", "changed", "symlink"])
def test_receipt_identity_is_checked(tmp_path: Path, kind: str) -> None:
    prepared = prepare_external_files(tmp_path, _installation(tmp_path))
    receipt = tmp_path / EXTERNAL_FILES_RECEIPT
    if kind == "changed":
        receipt.write_bytes(b"x" * receipt.stat().st_size)
    else:
        receipt.unlink()
        if kind == "symlink":
            receipt.symlink_to(tmp_path / "nested/weights.bin")
    with pytest.raises((ArtifactIntegrityError, FileNotFoundError)):
        verify_prepared_external_files(prepared)


@pytest.mark.parametrize("hash_algorithm", ["xxh3_128", "sha256"])
def test_preparation_does_not_publish_for_wrong_bytes(
    tmp_path: Path, hash_algorithm: Literal["xxh3_128", "sha256"]
) -> None:
    descriptor = _installation(tmp_path, hash_algorithm)
    (tmp_path / "nested/weights.bin").write_bytes(b"xyz")
    with pytest.raises(ArtifactIntegrityError, match="content differs"):
        prepare_external_files(tmp_path, descriptor)
    assert not (tmp_path / EXTERNAL_FILES_RECEIPT).exists()


def test_external_root_symlink_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    descriptor = _installation(source)
    alias = tmp_path / "alias"
    alias.symlink_to(source, target_is_directory=True)
    with pytest.raises(ArtifactIntegrityError, match="canonical"):
        prepare_external_files(alias, descriptor)


def test_verified_provider_mount_alias_preserves_declared_consumer_root(
    tmp_path: Path,
) -> None:
    volume = tmp_path / "volume"
    version = volume / "v1"
    version.mkdir(parents=True)
    prepared = prepare_external_files(version, _installation(version))
    mount = tmp_path / "mount"
    mount.symlink_to(volume, target_is_directory=True)
    projected = prepared.model_copy(update={"root": mount / "v1"})
    with pytest.raises(ArtifactIntegrityError, match="canonical"):
        verify_prepared_external_files(projected)
    assert (
        verify_prepared_external_files(projected, verified_mounts=(mount,))
        == mount / "v1"
    )
    with pytest.raises(ArtifactIntegrityError, match="canonical"):
        prepare_external_files(projected.root, projected.descriptor)


@pytest.mark.parametrize("alias", ["version", "nested"])
def test_verified_provider_mount_rejects_symlinks_beneath_mount(
    tmp_path: Path,
    alias: str,
) -> None:
    volume = tmp_path / "volume"
    version = volume / "v1"
    version.mkdir(parents=True)
    prepared = prepare_external_files(version, _installation(version))
    if alias == "version":
        version.rename(volume / "v2")
        version.symlink_to(volume / "v2", target_is_directory=True)
    else:
        (version / "nested").rename(tmp_path / "outside")
        (version / "nested").symlink_to(tmp_path / "outside", target_is_directory=True)
    mount = tmp_path / "mount"
    mount.symlink_to(volume, target_is_directory=True)
    projected = prepared.model_copy(update={"root": mount / "v1"})
    with pytest.raises(ArtifactIntegrityError, match="canonical|regular"):
        verify_prepared_external_files(projected, verified_mounts=(mount,))


@pytest.mark.parametrize("error_number", [errno.EPERM, errno.ENOTSUP])
def test_preparation_supports_filesystems_without_hard_links(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error_number: int
) -> None:
    descriptor = _installation(tmp_path)
    payload = tmp_path / "nested/weights.bin"
    before = payload.stat()

    def unsupported(source: Path, target: Path) -> None:
        raise OSError(error_number, "Hard links unavailable")

    monkeypatch.setattr("artisan.storage.external_access.os.link", unsupported)
    prepared = prepare_external_files(tmp_path, descriptor)
    assert verify_prepared_external_files(prepared) == tmp_path
    assert prepare_external_files(tmp_path, descriptor) == prepared
    assert payload.stat().st_ino == before.st_ino
    assert payload.stat().st_mtime_ns == before.st_mtime_ns
    assert not list(tmp_path.glob(".artisan-prepare-*"))


@pytest.mark.parametrize("content", ["same", "partial", "conflicting"])
def test_exclusive_receipt_publication_never_overwrites_existing_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, content: str
) -> None:
    descriptor = _installation(tmp_path)
    expected = expected_receipt_bytes(descriptor)
    existing = {
        "same": expected,
        "partial": expected[:5],
        "conflicting": b"x" * len(expected),
    }[content]

    def concurrent_receipt(source: Path, target: Path) -> None:
        target.write_bytes(existing)
        raise OSError(errno.EPERM, "Hard links unavailable")

    monkeypatch.setattr("artisan.storage.external_access.os.link", concurrent_receipt)
    if content == "same":
        prepared = prepare_external_files(tmp_path, descriptor)
        assert verify_prepared_external_files(prepared) == tmp_path
    else:
        with pytest.raises(ArtifactIntegrityError, match="receipt"):
            prepare_external_files(tmp_path, descriptor)
        prepared = PreparedExternalFiles(
            root=tmp_path,
            descriptor=descriptor,
            receipt_digest=expected_receipt_digest(descriptor),
        )
        with pytest.raises(ArtifactIntegrityError, match="receipt"):
            verify_prepared_external_files(prepared)
    assert (tmp_path / EXTERNAL_FILES_RECEIPT).read_bytes() == existing
    assert not list(tmp_path.glob(".artisan-prepare-*"))


def test_preparation_propagates_other_filesystem_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    descriptor = _installation(tmp_path)

    def unavailable(source: Path, target: Path) -> None:
        raise OSError(errno.EIO, "Storage unavailable")

    monkeypatch.setattr("artisan.storage.external_access.os.link", unavailable)
    with pytest.raises(OSError) as caught:
        prepare_external_files(tmp_path, descriptor)
    assert caught.value.errno == errno.EIO
    assert not (tmp_path / EXTERNAL_FILES_RECEIPT).exists()
    assert not list(tmp_path.glob(".artisan-prepare-*"))


def test_preparation_accepts_mixed_declared_hash_algorithms(tmp_path: Path) -> None:
    (tmp_path / "first.bin").write_bytes(b"abc")
    (tmp_path / "second.bin").write_bytes(b"xyz")
    descriptor = ExternalFilesDescriptor(
        files=(
            ExternalFile(
                path="first.bin",
                hash_algorithm="sha256",
                content_hash=hashlib.sha256(b"abc").hexdigest(),
                size_bytes=3,
            ),
            ExternalFile(
                path="second.bin",
                hash_algorithm="xxh3_128",
                content_hash=compute_content_digest(b"xyz"),
                size_bytes=3,
            ),
        )
    )
    prepared = prepare_external_files(tmp_path, descriptor)
    assert verify_prepared_external_files(prepared) == tmp_path
    assert (tmp_path / EXTERNAL_FILES_RECEIPT).read_bytes() == expected_receipt_bytes(
        descriptor
    )
