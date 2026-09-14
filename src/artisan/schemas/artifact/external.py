"""Shared bounded I/O helpers for externally backed artifacts."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any, BinaryIO, cast
from urllib.parse import urlsplit, urlunsplit

import xxhash

from artisan.errors import ArtifactIntegrityError
from artisan.schemas.execution.fs import resolve_fs
from artisan.utils.hashing import STREAM_CHUNK_BYTES


def sanitized_uri(uri: str) -> str:
    """Remove credentials and query capabilities from a URI for errors."""
    parts = urlsplit(uri)
    if not parts.scheme:
        return uri
    host = parts.hostname or ""
    if parts.port is not None:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


def validate_persistable_uri(uri: str) -> None:
    """Reject credentials and transient query capabilities in a location."""
    parts = urlsplit(uri)
    if parts.username or parts.password or parts.query:
        msg = "Artifact locations cannot contain credentials or query strings"
        raise ValueError(msg)


def open_external(uri: str, fs: Any = None) -> BinaryIO:
    """Open an external URI as a binary stream."""
    scheme = urlsplit(uri).scheme
    protocols = getattr(fs, "protocol", ()) if fs is not None else ()
    if isinstance(protocols, str):
        protocols = (protocols,)
    use_explicit = fs is not None and (
        (scheme and scheme in protocols)
        or (not scheme and (not uri.startswith("/") or "file" in protocols))
    )
    if not use_explicit:
        resolved_fs, path = resolve_fs(uri, storage=None)
    else:
        resolved_fs, path = fs, uri
    return cast(BinaryIO, resolved_fs.open(path, "rb"))


def verify_complete_file(
    *,
    artifact_id: str | None,
    artifact_type: str,
    uri: str,
    expected_digest: str | None,
    expected_size: int | None,
    fs: Any = None,
) -> None:
    """Stream and verify a complete externally stored file."""
    with open_external(uri, fs) as source:
        _consume_verified(
            source,
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            uri=uri,
            expected_digest=expected_digest,
            expected_size=expected_size,
        )


def copy_verified_file(
    *,
    artifact_id: str | None,
    artifact_type: str,
    uri: str,
    destination: str,
    expected_digest: str | None,
    expected_size: int | None,
    fs: Any = None,
) -> None:
    """Copy and verify external bytes in one bounded pass."""
    try:
        with open_external(uri, fs) as source, open(destination, "wb") as target:
            _consume_verified(
                source,
                artifact_id=artifact_id,
                artifact_type=artifact_type,
                uri=uri,
                expected_digest=expected_digest,
                expected_size=expected_size,
                target=target,
            )
    except Exception:
        Path(destination).unlink(missing_ok=True)
        raise


def copy_verified_chunks(
    chunks: Iterable[bytes],
    *,
    artifact_id: str | None,
    artifact_type: str,
    uri: str,
    destination: str,
    expected_digest: str | None,
    expected_size: int | None,
) -> None:
    """Copy streamed chunks while verifying the complete byte contract."""
    try:
        with open(destination, "wb") as target:
            _consume_verified_chunks(
                chunks,
                artifact_id=artifact_id,
                artifact_type=artifact_type,
                uri=uri,
                expected_digest=expected_digest,
                expected_size=expected_size,
                target=target,
            )
    except Exception:
        Path(destination).unlink(missing_ok=True)
        raise


def read_verified_file(
    *,
    artifact_id: str | None,
    artifact_type: str,
    uri: str,
    expected_digest: str | None,
    expected_size: int | None,
    fs: Any = None,
) -> bytes:
    """Read complete external bytes while verifying digest and size."""
    chunks: list[bytes] = []
    with open_external(uri, fs) as source:
        _consume_verified(
            source,
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            uri=uri,
            expected_digest=expected_digest,
            expected_size=expected_size,
            chunks=chunks,
        )
    return b"".join(chunks)


def _consume_verified(
    source: BinaryIO,
    *,
    artifact_id: str | None,
    artifact_type: str,
    uri: str,
    expected_digest: str | None,
    expected_size: int | None,
    target: BinaryIO | None = None,
    chunks: list[bytes] | None = None,
) -> None:
    """Consume a stream once, optionally copying or retaining its chunks."""
    source_chunks = iter(lambda: source.read(STREAM_CHUNK_BYTES), b"")
    _consume_verified_chunks(
        source_chunks,
        artifact_id=artifact_id,
        artifact_type=artifact_type,
        uri=uri,
        expected_digest=expected_digest,
        expected_size=expected_size,
        target=target,
        chunks=chunks,
    )


def _consume_verified_chunks(
    source_chunks: Iterable[bytes],
    *,
    artifact_id: str | None,
    artifact_type: str,
    uri: str,
    expected_digest: str | None,
    expected_size: int | None,
    target: BinaryIO | None = None,
    chunks: list[bytes] | None = None,
) -> None:
    """Verify one bounded-pass byte stream and optionally copy its chunks."""
    if expected_digest is None or expected_size is None:
        msg = f"{artifact_type} artifact {artifact_id} lacks digest or size"
        raise ArtifactIntegrityError(msg)
    hasher = xxhash.xxh3_128()
    size_bytes = 0
    for chunk in source_chunks:
        hasher.update(chunk)
        size_bytes += len(chunk)
        if target is not None:
            target.write(chunk)
        if chunks is not None:
            chunks.append(chunk)
    actual_digest = hasher.hexdigest()
    if (actual_digest, size_bytes) != (expected_digest, expected_size):
        msg = (
            f"External {artifact_type} artifact {artifact_id} failed integrity "
            f"at {sanitized_uri(uri)!r}: expected {expected_digest}/{expected_size} "
            f"bytes, got {actual_digest}/{size_bytes}"
        )
        raise ArtifactIntegrityError(msg)
