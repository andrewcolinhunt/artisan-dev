"""Explicit preparation and reuse of externally owned file installations."""

from __future__ import annotations

import errno
import os
import stat
import tempfile
from collections.abc import Sequence
from pathlib import Path, PurePosixPath

from artisan.errors import ArtifactIntegrityError
from artisan.schemas.artifact.external_descriptor import (
    EXTERNAL_FILES_RECEIPT,
    ExternalFilesDescriptor,
    PreparedExternalFiles,
    expected_receipt_bytes,
    expected_receipt_digest,
)
from artisan.utils.hashing import compute_stream_digest

__all__ = [
    "PreparedExternalFiles",
    "prepare_external_files",
    "verify_prepared_external_files",
]


def prepare_external_files(
    root: Path, descriptor: ExternalFilesDescriptor
) -> PreparedExternalFiles:
    """Verify an existing installation and publish its small completion receipt.

    File bytes are hashed in place, never acquired or copied. The caller owns
    this explicitly selected installation and must keep it immutable after
    preparation. Repeating preparation explicitly rechecks all payload bytes.
    Interrupted publication may leave an incomplete receipt; existing receipts
    are validated, never overwritten. Repair incomplete setup explicitly.

    Args:
        root: Canonical absolute directory containing the complete installation.
        descriptor: Expected relative paths, sizes and content hashes.

    Returns:
        Metadata suitable for binding this installation to an artifact ID.

    Raises:
        ArtifactIntegrityError: The layout, file bytes or existing receipt differ.
    """
    _verify_layout(root, descriptor)
    for member in descriptor.files:
        with (root / member.path).open("rb") as stream:
            actual = compute_stream_digest(stream)
        if actual != (member.content_hash, member.size_bytes):
            msg = f"External file content differs from descriptor: {member.path}"
            raise ArtifactIntegrityError(msg)
    _publish_receipt(root, expected_receipt_bytes(descriptor))
    return PreparedExternalFiles(
        root=root,
        descriptor=descriptor,
        receipt_digest=expected_receipt_digest(descriptor),
    )


def verify_prepared_external_files(
    prepared: PreparedExternalFiles, *, verified_mounts: Sequence[Path] = ()
) -> Path:
    """Check receipt and layout without reading payload bytes.

    This trusts the installation owner's immutability contract. Same-size byte
    mutation cannot be detected here; explicitly prepare again to rehash files.
    A provider may supply mount roots only after verifying its actual mounts.
    Platform aliases are allowed at those roots, never beneath them.
    """
    _verify_layout(prepared.root, prepared.descriptor, verified_mounts)
    receipt = prepared.root / EXTERNAL_FILES_RECEIPT
    expected = expected_receipt_bytes(prepared.descriptor)
    _verify_receipt(receipt, expected)
    return prepared.root


def _verify_layout(
    root: Path,
    descriptor: ExternalFilesDescriptor,
    verified_mounts: Sequence[Path] = (),
) -> None:
    if not _canonical_root(root, verified_mounts):
        msg = "External installation must be a canonical absolute directory"
        raise ArtifactIntegrityError(msg)
    expected_files = {item.path: item.size_bytes for item in descriptor.files}
    expected_dirs = {
        str(parent)
        for name in expected_files
        for parent in PurePosixPath(name).parents
        if str(parent) != "."
    }
    actual_files, actual_dirs = _scan_layout(root)
    if actual_files != expected_files or actual_dirs != expected_dirs:
        msg = "External installation layout or file sizes differ from descriptor"
        raise ArtifactIntegrityError(msg)


def _canonical_root(root: Path, verified_mounts: Sequence[Path]) -> bool:
    if not root.is_absolute() or not root.is_dir():
        return False
    owners = [mount for mount in verified_mounts if root.is_relative_to(mount)]
    if len(owners) > 1:
        return False
    expected = (
        owners[0].resolve(strict=True) / root.relative_to(owners[0]) if owners else root
    )
    return root.resolve(strict=True) == expected


def _scan_layout(root: Path) -> tuple[dict[str, int], set[str]]:
    files: dict[str, int] = {}
    directories: set[str] = set()
    for parent, dirs, names in os.walk(root, followlinks=False, onerror=_walk_error):
        for name in (*dirs, *names):
            path = Path(parent) / name
            relative = path.relative_to(root).as_posix()
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                directories.add(relative)
            elif stat.S_ISREG(info.st_mode):
                if relative != EXTERNAL_FILES_RECEIPT:
                    files[relative] = info.st_size
            else:
                msg = f"External installation requires regular files/directories: {relative}"
                raise ArtifactIntegrityError(msg)
    return files, directories


def _walk_error(error: OSError) -> None:
    raise error


def _verify_receipt(path: Path, expected: bytes) -> None:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size != len(expected):
        msg = "External installation receipt is not the expected regular file"
        raise ArtifactIntegrityError(msg)
    with path.open("rb") as stream:
        content = stream.read(len(expected) + 1)
    if content != expected:
        msg = "External installation receipt differs from its descriptor"
        raise ArtifactIntegrityError(msg)


def _publish_receipt(root: Path, receipt: bytes) -> None:
    target = root / EXTERNAL_FILES_RECEIPT
    if target.exists():
        _verify_receipt(target, receipt)
        return
    with tempfile.NamedTemporaryFile(
        dir=root, prefix=".artisan-prepare-", delete=False
    ) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(receipt)
            stream.flush()
            os.fsync(stream.fileno())
            try:
                os.link(temporary, target)
            except FileExistsError:
                _verify_receipt(target, receipt)
            except OSError as error:
                if error.errno not in {errno.EPERM, errno.ENOTSUP}:
                    raise
                _publish_receipt_exclusive(target, receipt)
        finally:
            temporary.unlink()


def _publish_receipt_exclusive(target: Path, receipt: bytes) -> None:
    # Some externally owned filesystems support exclusive creation but no links.
    # A partial receipt is left for explicit repair and never validates as complete.
    try:
        with target.open("xb") as stream:
            stream.write(receipt)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        _verify_receipt(target, receipt)
