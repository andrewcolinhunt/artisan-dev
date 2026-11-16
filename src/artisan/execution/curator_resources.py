"""Own private source captures until curator sealing and command closure finish."""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import mkdtemp

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external import copy_verified_chunks, verify_complete_file
from artisan.utils.command_supervisor import close_command_scope
from artisan.utils.hashing import STREAM_CHUNK_BYTES


@dataclass
class _Resources:
    working_root: str | None = None
    root: Path | None = None
    captures: dict[tuple[int, str, str, int], Path] = field(default_factory=dict)
    files: dict[str, tuple[str, int]] = field(default_factory=dict)


_current: ContextVar[_Resources | None] = ContextVar("curator_resources", default=None)


@contextmanager
def curator_resources(
    working_root: str | None = None, *, preserve_working: bool = False
) -> Iterator[None]:
    """Retain captures when requested or when command ownership remains unknown.

    This scope must be inside the existing managed invocation. It does not create
    a command owner and never treats an exception or a scope exit as closure.
    """
    resources = _Resources(working_root=working_root)
    token = _current.set(resources)
    error: BaseException | None = None
    try:
        yield
    except BaseException as caught:
        error = caught
        raise
    finally:
        try:
            if (
                resources.root is not None
                and close_command_scope(error)
                and not preserve_working
            ):
                shutil.rmtree(resources.root)
        finally:
            _current.reset(token)


def capture_curator_carrier(
    artifact: Artifact,
    *,
    uri: str,
    digest: str,
    size_bytes: int,
    fs: AbstractFileSystem,
) -> Path:
    """Copy one complete authoritative carrier into the active invocation scope."""
    resources = _current.get()
    if resources is None:
        msg = "Source import requires an active curator resource scope"
        raise RuntimeError(msg)
    key = (id(fs), uri, digest, size_bytes)
    if key in resources.captures:
        return resources.captures[key]
    if resources.root is None:
        if resources.working_root is not None:
            Path(resources.working_root).mkdir(parents=True, exist_ok=True)
        resources.root = Path(
            mkdtemp(prefix="artisan-curator-source-", dir=resources.working_root)
        )
    path = resources.root / str(len(resources.captures))
    assert artifact.artifact_id is not None
    with fs.open(uri, "rb", cache_type="none") as stream:
        copy_verified_chunks(
            iter(lambda: stream.read(STREAM_CHUNK_BYTES), b""),
            artifact_id=artifact.artifact_id,
            artifact_type=artifact.artifact_type,
            uri=uri,
            destination=str(path),
            expected_digest=digest,
            expected_size=size_bytes,
        )
    resources.captures[key] = path
    resources.files[str(path)] = (digest, size_bytes)
    return path


def verify_owned_curator_file(uri: str) -> tuple[str, int] | None:
    """Verify a private copy and return the full identity destination staging owes."""
    resources = _current.get()
    if resources is None or uri not in resources.files:
        return None
    digest, size_bytes = resources.files[uri]
    verify_complete_file(
        artifact_id="curator source capture",
        artifact_type="carrier",
        uri=uri,
        expected_digest=digest,
        expected_size=size_bytes,
        fs=LocalFileSystem(),
    )
    return digest, size_bytes
