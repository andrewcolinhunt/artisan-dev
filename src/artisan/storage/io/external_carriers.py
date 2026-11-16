"""Bounded staging and capture of complete external carrier files."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external import open_external
from artisan.schemas.execution.retained_result import ExternalBinding, ExternalCarrier
from artisan.storage.io.publication import publish_immutable_file
from artisan.utils.hashing import compute_stream_digest
from artisan.utils.path import uri_join


def stage_external_carriers(
    content: Iterable[tuple[str, Artifact]],
    directory: str,
    scratch: Path,
    fs: AbstractFileSystem,
    artifact_fs: AbstractFileSystem | None,
) -> tuple[tuple[ExternalCarrier, ...], tuple[ExternalBinding, ...]]:
    """Verify each artifact against a captured carrier and publish each file once."""
    sources: dict[str, tuple[Path, ExternalCarrier]] = {}
    carriers: dict[str, ExternalCarrier] = {}
    bindings: dict[str, ExternalBinding] = {}
    for artifact_id, artifact in content:
        if not artifact.EXTERNALLY_BACKED:
            continue
        locator = next(iter(artifact.LOCATOR_FIELDS))
        source = getattr(artifact, locator)
        if not isinstance(source, str) or not source:
            msg = "Externally backed artifact has no source location"
            raise StoreIntegrityError(msg)
        if source not in sources:
            path = scratch / f"carrier-source-{len(sources)}"
            with (
                open_external(source, artifact_fs) as stream,
                path.open("xb") as target,
            ):
                digest, size = compute_stream_digest(stream, destination=target)
            carrier = ExternalCarrier(
                filename=f"carrier-{digest}.bin", digest=digest, size_bytes=size
            )
            sources[source] = path, carrier
        path, carrier = sources[source]
        verification = artifact.model_copy(deep=True)
        setattr(verification, locator, str(path))
        verification.to_row()
        verification.verify_external_content()
        binding = ExternalBinding(artifact_id=artifact_id, filename=carrier.filename)
        if bindings.setdefault(artifact_id, binding) != binding:
            msg = "One artifact identity names conflicting complete external carriers"
            raise StoreIntegrityError(msg)
        if carrier.filename not in carriers:
            publish_immutable_file(
                fs,
                uri_join(directory, carrier.filename),
                str(path),
                source_fs=LocalFileSystem(),
            )
            carriers[carrier.filename] = carrier
        elif carriers[carrier.filename] != carrier:
            msg = "Conflicting complete carrier identity"
            raise StoreIntegrityError(msg)
    return (
        tuple(carriers[name] for name in sorted(carriers)),
        tuple(bindings[key] for key in sorted(bindings)),
    )


def capture_external_carriers(
    directory: str,
    inventory: Iterable[ExternalCarrier],
    scratch: Path,
    fs: AbstractFileSystem,
) -> dict[str, str]:
    """Copy exact carrier bytes to lifetime-owned private files and verify them."""
    carriers = {}
    for item in inventory:
        path = scratch / item.filename
        with (
            fs.open(
                uri_join(directory, item.filename), "rb", cache_type="none"
            ) as source,
            path.open("xb") as target,
        ):
            digest, size = compute_stream_digest(source, destination=target)
        if (digest, size) != (item.digest, item.size_bytes):
            msg = "External carrier bytes changed"
            raise StoreIntegrityError(msg)
        carriers[item.filename] = str(path)
    return carriers
