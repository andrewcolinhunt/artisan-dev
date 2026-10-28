"""Publish immutable objects without exposing partially written bytes."""

from __future__ import annotations

import errno
import os
import posixpath
import re
import uuid
from contextlib import suppress

from fsspec import AbstractFileSystem, get_filesystem_class
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.utils.hashing import compute_stream_digest


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


def publish_immutable_file(
    fs: AbstractFileSystem,
    path: str,
    source_path: str,
    *,
    source_fs: AbstractFileSystem | None = None,
) -> None:
    """Publish a completed file with bounded transfer and identical-only retries.

    The source remains available on every path. Local publication links a
    flushed file; S3 completes an exclusive multipart write with one bounded
    upload buffer. An uncertain publication never removes the destination.
    """
    source_fs = source_fs or fs
    if not isinstance(fs, LocalFileSystem) and not _is_s3_filesystem(fs):
        msg = f"Immutable publication is unsupported for {type(fs).__name__}"
        raise StoreIntegrityError(msg)
    expected = _file_digest(source_fs, source_path)
    if fs.exists(path):
        _require_file_digest(fs, path, expected)
        if isinstance(fs, LocalFileSystem):
            _sync_existing_local(fs, path)
        return
    fs.makedirs(posixpath.dirname(path), exist_ok=True)
    if isinstance(fs, LocalFileSystem):
        try:
            _publish_local_file(fs, path, source_fs, source_path, expected)
        except FileExistsError:
            _require_file_digest(fs, path, expected)
            _sync_existing_local(fs, path)
    else:
        try:
            if expected[1] == 0:
                # s3fs' zero-length commit uses touch(), which is not exclusive.
                bucket, key, _ = fs.split_path(path)
                fs.call_s3(
                    "put_object", Bucket=bucket, Key=key, Body=b"", IfNoneMatch="*"
                )
            else:
                _copy_file(source_fs, source_path, fs, path, expected)
        except Exception as exc:
            fs.invalidate_cache(path)
            if fs.exists(path):
                _require_file_digest(fs, path, expected)
                return
            msg = f"Could not publish immutable object {posixpath.basename(path)!r}"
            raise StoreIntegrityError(msg) from exc
    _require_file_digest(fs, path, expected)


def _file_digest(fs: AbstractFileSystem, path: str) -> tuple[str, int]:
    with fs.open(path, "rb", cache_type="none") as stream:
        return compute_stream_digest(stream)


def _require_file_digest(
    fs: AbstractFileSystem, path: str, expected: tuple[str, int]
) -> None:
    if _file_digest(fs, path) != expected:
        msg = f"Conflicting immutable object {posixpath.basename(path)!r}"
        raise StoreIntegrityError(msg)


def _copy_file(
    source_fs: AbstractFileSystem,
    source_path: str,
    fs: AbstractFileSystem,
    path: str,
    expected: tuple[str, int],
) -> None:
    multipart = _is_s3_filesystem(fs)
    with source_fs.open(source_path, "rb", cache_type="none") as source:
        destination = fs.open(path, "xb", block_size=5 * 1024 * 1024)
        if multipart:
            # Set this after open to avoid fsspec retaining the file in its
            # filesystem-wide transaction queue. Completion belongs here.
            destination.autocommit = False
        try:
            if compute_stream_digest(source, destination=destination) != expected:
                msg = "Immutable publication source changed while being copied"
                raise StoreIntegrityError(msg)
            destination.close()
            if multipart:
                destination.commit()
        except BaseException:
            if multipart:
                with suppress(Exception):
                    destination.discard()
            raise
        finally:
            with suppress(Exception):
                destination.close()


def _publish_local_file(
    fs: LocalFileSystem,
    path: str,
    source_fs: AbstractFileSystem,
    source_path: str,
    expected: tuple[str, int],
) -> None:
    final = str(fs._strip_protocol(path))
    if isinstance(source_fs, LocalFileSystem):
        source = str(source_fs._strip_protocol(source_path))
        with open(source, "rb") as stream:
            os.fsync(stream.fileno())
        try:
            os.link(source, final)
        except OSError as exc:
            if exc.errno != errno.EXDEV:
                raise
        else:
            _sync_directory_chain(os.path.dirname(final))
            return
    temporary = f"{final}.tmp-{uuid.uuid4().hex}"
    _copy_file(source_fs, source_path, fs, temporary, expected)
    with open(temporary, "rb") as stream:
        os.fsync(stream.fileno())
    os.link(temporary, final)
    _sync_directory_chain(os.path.dirname(final))
    with suppress(OSError):
        os.unlink(temporary)


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
