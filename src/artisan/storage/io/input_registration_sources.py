"""Capture raw acquisitions before publishing managed registration evidence."""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from concurrent.futures import CancelledError
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, cast

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.utils import get_protocol

from artisan.errors import ArtifactIntegrityError
from artisan.schemas.artifact.external import sanitized_uri, validate_persistable_uri
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.execution.admission import ManagedCarrier
from artisan.schemas.execution.fs import resolve_fs
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.io.input_registration_plan import RegisteredInput
from artisan.utils.filename import strip_extensions
from artisan.utils.hashing import compute_stream_digest


@dataclass(frozen=True)
class RawInputSource:
    """An individually resolved source; credentials remain outside durable evidence."""

    uri: str
    fs: AbstractFileSystem
    path: str


@dataclass(frozen=True)
class CapturedRawInputs:
    """Finalized descriptors and owned complete copies valid within caller scratch."""

    inputs: tuple[RegisteredInput, ...]
    artifacts: dict[str, FileRefArtifact]
    carriers: dict[str, ManagedCarrier]
    paths: dict[str, Path]


def normalize_raw_uri(uri: str) -> str:
    """Freeze local resolution without reopening a previously captured source."""
    validate_persistable_uri(uri)
    if get_protocol(uri) in {"file", "local"}:
        return os.path.abspath(str(LocalFileSystem._strip_protocol(uri)))
    return uri


def resolve_raw_inputs(
    uris: Sequence[str], storage: StorageConfig
) -> tuple[RawInputSource, ...]:
    """Validate all sources with per-URI configured or ambient filesystem resolution."""
    if not uris:
        msg = "Raw registration requires at least one input"
        raise ValueError(msg)
    sources, failures = [], []
    for uri in uris:
        try:
            validate_persistable_uri(uri)
            fs, path = resolve_fs(uri, storage)
            if not fs.isfile(path):
                msg = "Input is absent or is not a file"
                raise FileNotFoundError(msg)
            normalized = normalize_raw_uri(uri)
            sources.append(RawInputSource(normalized, fs, path))
        except Exception as error:
            failures.append(f"{sanitized_uri(uri)} ({type(error).__name__})")
    if failures:
        raise ArtifactIntegrityError("Invalid raw inputs: " + "; ".join(failures))
    return tuple(sources)


def capture_raw_inputs(
    sources: Sequence[RawInputSource],
    scratch: Path,
    *,
    step_number: int,
    check_cancelled: Callable[[], None],
) -> CapturedRawInputs:
    """Hash only private copies, preserving ordered duplicate input descriptors."""
    inputs: list[RegisteredInput] = []
    artifacts: dict[str, FileRefArtifact] = {}
    carriers: dict[str, ManagedCarrier] = {}
    paths: dict[str, Path] = {}
    captured: dict[str, FileRefArtifact] = {}
    for source in sources:
        check_cancelled()
        artifact = captured.get(source.uri)
        if artifact is None:
            path = scratch / f"source-{len(captured)}"
            carrier = _copy_source(source, path, check_cancelled)
            artifact = _file_ref(source.uri, path, carrier, step_number)
            captured[source.uri] = artifact
            carriers[carrier.digest] = carrier
            paths.setdefault(carrier.digest, path)
        artifact_id = cast(str, artifact.artifact_id)
        artifacts.setdefault(artifact_id, artifact)
        inputs.append(RegisteredInput(uri=source.uri, artifact_id=artifact_id))
    check_cancelled()
    return CapturedRawInputs(tuple(inputs), artifacts, carriers, paths)


@dataclass(frozen=True)
class _CheckedReader:
    stream: BinaryIO
    check_cancelled: Callable[[], None]

    def read(self, size: int = -1) -> bytes:
        self.check_cancelled()
        chunk = self.stream.read(size)
        self.check_cancelled()
        return chunk


def _copy_source(
    source: RawInputSource, path: Path, check_cancelled: Callable[[], None]
) -> ManagedCarrier:
    try:
        with (
            source.fs.open(source.path, "rb", cache_type="none") as stream,
            path.open("xb") as target,
        ):
            digest, size = compute_stream_digest(
                cast(BinaryIO, _CheckedReader(stream, check_cancelled)),
                destination=target,
            )
    except CancelledError:
        raise
    except Exception as error:
        msg = f"Cannot capture raw input {sanitized_uri(source.uri)} ({type(error).__name__})"
        raise ArtifactIntegrityError(msg) from None
    return ManagedCarrier(digest=digest, size_bytes=size)


def _file_ref(
    uri: str, path: Path, carrier: ManagedCarrier, step_number: int
) -> FileRefArtifact:
    basename = os.path.basename(uri)
    stem = strip_extensions(basename)
    artifact = cast(
        FileRefArtifact,
        FileRefArtifact.draft(
            path=str(path),
            content_hash=carrier.digest,
            size_bytes=carrier.size_bytes,
            step_number=step_number,
            original_name=stem,
            extension=basename[len(stem) :],
        ).finalize(),
    )
    artifact.verify_external_content(fs=LocalFileSystem())
    return artifact
