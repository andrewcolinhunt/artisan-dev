"""Publish immutable objects without exposing partially written bytes."""

from __future__ import annotations

import os
import posixpath
import re
import uuid
from contextlib import suppress

from fsspec import AbstractFileSystem, get_filesystem_class
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError


def is_publication_temporary(filename: str) -> bool:
    """Recognize an atomic publisher's local temporary basename."""
    return (
        re.fullmatch(r"[^/\\]+\.(?:parquet|json|log)\.tmp-[0-9a-f]{32}", filename)
        is not None
    )


def publish_immutable_bytes(fs: AbstractFileSystem, path: str, data: bytes) -> None:
    """Publish completed bytes once, allowing only identical retries.

    Local files use a same-directory hard link after flushing their contents.
    S3 exclusive creation uses a conditional completed-object write. Other
    filesystems must not silently substitute weaker publication guarantees.

    Raises:
        StoreIntegrityError: Publication is unsupported, fails, or conflicts.
    """
    if not isinstance(fs, LocalFileSystem) and not _is_s3_filesystem(fs):
        msg = f"Immutable publication is unsupported for {type(fs).__name__}"
        raise StoreIntegrityError(msg)
    if fs.exists(path):
        _require_identical(fs, path, data)
        if isinstance(fs, LocalFileSystem):
            _sync_existing_local(fs, path)
        return
    fs.makedirs(posixpath.dirname(path), exist_ok=True)
    if isinstance(fs, LocalFileSystem):
        try:
            _publish_local(fs, path, data)
        except FileExistsError:
            _require_identical(fs, path, data)
            _sync_existing_local(fs, path)
    else:
        try:
            with fs.open(path, "xb") as stream:
                stream.write(data)
        except Exception as exc:
            if fs.exists(path):
                _require_identical(fs, path, data)
                return
            msg = f"Could not publish immutable object {posixpath.basename(path)!r}"
            raise StoreIntegrityError(msg) from exc
    _require_identical(fs, path, data)


def _is_s3_filesystem(fs: AbstractFileSystem) -> bool:
    """Resolve the optional S3 backend only for a filesystem claiming its protocol."""
    protocols = (fs.protocol,) if isinstance(fs.protocol, str) else fs.protocol
    if not {"s3", "s3a"}.intersection(protocols):
        return False
    try:
        backend = get_filesystem_class("s3")
    except ImportError as exc:
        msg = "S3 immutable publication requires the optional S3 backend"
        raise StoreIntegrityError(msg) from exc
    return isinstance(fs, backend)


def _require_identical(fs: AbstractFileSystem, path: str, data: bytes) -> None:
    """Verify the completed object, including ambiguous publication outcomes."""
    with fs.open(path, "rb") as stream:
        existing = stream.read()
    if existing != data:
        msg = f"Conflicting immutable object {posixpath.basename(path)!r}"
        raise StoreIntegrityError(msg)


def _publish_local(fs: LocalFileSystem, path: str, data: bytes) -> None:
    """Flush bytes, then link the final name without replacing existing data."""
    final = str(fs._strip_protocol(path))
    parent = os.path.dirname(final)
    temporary = f"{final}.tmp-{uuid.uuid4().hex}"
    try:
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o666)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, final)
        _sync_directory_chain(parent)
    finally:
        with suppress(FileNotFoundError):
            os.unlink(temporary)


def _sync_existing_local(fs: LocalFileSystem, path: str) -> None:
    """Make a retry durable even when the original directory fsync failed."""
    local = str(fs._strip_protocol(path))
    descriptor = os.open(local, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _sync_directory_chain(os.path.dirname(local))


def _sync_directory_chain(directory: str) -> None:
    """Persist new ancestor entries as well as the final publication link."""
    directory = os.path.abspath(directory)
    while True:
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        parent = os.path.dirname(directory)
        if parent == directory:
            return
        directory = parent
