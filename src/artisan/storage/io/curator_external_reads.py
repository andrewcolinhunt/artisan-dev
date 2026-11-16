"""Capture and revalidate selected complete carriers for supported curator reads."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import polars as pl
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import ArtifactIntegrityError, StoreIntegrityError
from artisan.schemas.artifact.external import copy_verified_file, verify_complete_file
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_cache import CuratorCarrierRead
from artisan.storage.core.committed_scan import verify_snapshot_effect
from artisan.storage.core.table_schemas import get_physical_schema_for_path
from artisan.storage.io.curator_plan import CuratorCommitPlan
from artisan.storage.io.delta_files import DeltaSnapshot
from artisan.storage.io.input_registration_plan import InputRegistrationPlan
from artisan.storage.io.retained_plan import RetainedCommitPlan
from artisan.utils.path import uri_join

if TYPE_CHECKING:
    from artisan.storage.core.curator_view import CuratorView


@dataclass(frozen=True)
class CapturedCuratorFile:
    """Original descriptor plus a private, lifetime-owned copy of its carrier."""

    artifact: FileRefArtifact
    path: Path
    witness: CuratorCarrierRead


def curator_carrier_read(
    view: CuratorView, artifact_id: str, uri: str
) -> CuratorCarrierRead:
    """Bind a selected location to its exact completed physical owner's inventory."""
    path = TablePath.ARTIFACT_LOCATIONS.value
    rows = (
        view.physical(path)
        .filter((pl.col("artifact_id") == artifact_id) & (pl.col("uri") == uri))
        .collect()
    )
    if rows.height != 1:
        msg = "Curator carrier read needs one exact captured location"
        raise StoreIntegrityError(msg)
    control = view.controls.filter(
        pl.col("logical_commit_id") == rows["logical_commit_id"][0]
    )
    if control.height != 1 or control["state"][0] != "complete":
        msg = "Curator carrier read lacks completed location ownership"
        raise StoreIntegrityError(msg)
    plan = view.committer._control_plan(control.row(0, named=True))
    if not isinstance(
        plan, (RetainedCommitPlan, CuratorCommitPlan, InputRegistrationPlan)
    ):
        msg = "Curator carrier read lacks a complete managed inventory"
        raise StoreIntegrityError(msg)
    version = next(
        item.version for item in view.snapshot.tables if item.table_path == path
    )
    snapshot = DeltaSnapshot(
        uri_join(view.committer.delta_root, path),
        get_physical_schema_for_path(path),
        fs=view.committer.fs,
        storage_options=view.committer.storage_options,
        version=version,
    )
    verify_snapshot_effect(plan, path, snapshot, view.controls)
    carrier = next(
        (
            item
            for item in plan.carriers
            if uri_join(view.committer.files_root, item.relative_path) == uri
        ),
        None,
    )
    table = plan.table(path)
    if carrier is None or table is None or (artifact_id, uri) not in table.row_keys:
        msg = "Curator read differs from its completed carrier/location membership"
        raise StoreIntegrityError(msg)
    return CuratorCarrierRead(artifact_id=artifact_id, uri=uri, carrier=carrier)


def verify_curator_carrier_read(view: CuratorView, read: CuratorCarrierRead) -> None:
    """Recheck exact location authority and complete bytes at acceptance or reuse."""
    if curator_carrier_read(view, read.artifact_id, read.uri) != read:
        msg = "Curator carrier witness changed its completed inventory"
        raise StoreIntegrityError(msg)
    _verify_bytes(read, read.uri, view.committer.fs)


@contextmanager
def capture_curator_file(
    view: CuratorView, artifact: FileRefArtifact
) -> Iterator[CapturedCuratorFile]:
    """Preserve the original URI while owning verified bytes through conversion.

    This captures supported filesystem reads; it does not confine arbitrary
    authored Python I/O. Recheck both the capture and original on normal close.
    """
    assert artifact.artifact_id is not None
    _verify_descriptor(view, artifact)
    locations = (
        view.scan(TablePath.ARTIFACT_LOCATIONS)
        .filter(pl.col("artifact_id") == artifact.artifact_id)
        .select("uri")
        .collect()
    )
    with TemporaryDirectory(prefix="artisan-curator-read-") as directory:
        target = Path(directory) / "content"
        for uri in sorted(
            set(locations["uri"]),
            key=lambda item: (
                not item.startswith(view.committer.files_root.rstrip("/") + "/"),
                item,
            ),
        ):
            read = curator_carrier_read(view, artifact.artifact_id, uri)
            try:
                copy_verified_file(
                    artifact_id=artifact.artifact_id,
                    artifact_type=artifact.artifact_type,
                    uri=uri,
                    destination=str(target),
                    expected_digest=read.carrier.digest,
                    expected_size=read.carrier.size_bytes,
                    fs=view.committer.fs,
                )
            except ArtifactIntegrityError:
                raise
            except OSError:
                continue
            disposable = artifact.model_copy(update={"path": uri})
            disposable._cached_content = None
            verification = disposable.model_copy(update={"path": str(target)})
            verification.verify_external_content(fs=LocalFileSystem())
            yield CapturedCuratorFile(disposable, target, read)
            _verify_bytes(read, str(target), LocalFileSystem())
            _verify_bytes(read, uri, view.committer.fs)
            return
    msg = "Curator file input has no readable verified managed carrier"
    raise ArtifactIntegrityError(msg)


def _verify_bytes(read: CuratorCarrierRead, uri: str, fs: AbstractFileSystem) -> None:
    verify_complete_file(
        artifact_id=read.artifact_id,
        artifact_type="curator carrier",
        uri=uri,
        expected_digest=read.carrier.digest,
        expected_size=read.carrier.size_bytes,
        fs=fs,
    )


def _verify_descriptor(view: CuratorView, artifact: FileRefArtifact) -> None:
    path = ArtifactTypeDef.get_table_path(artifact.artifact_type)
    rows = (
        view.physical(path)
        .filter(pl.col("artifact_id") == artifact.artifact_id)
        .collect()
    )
    if rows.height != 1:
        msg = "Curator file descriptor lacks unique captured content"
        raise StoreIntegrityError(msg)
    control = view.controls.filter(
        pl.col("logical_commit_id") == rows["logical_commit_id"][0]
    )
    if control.height != 1 or control["state"][0] != "complete":
        msg = "Curator file descriptor lacks completed content ownership"
        raise StoreIntegrityError(msg)
    plan = view.committer._control_plan(control.row(0, named=True))
    if artifact.origin_step_number != plan.expected_origin(
        path, (artifact.artifact_id,)
    ):
        msg = "Curator file descriptor changed its protected original origin"
        raise StoreIntegrityError(msg)
    verify_snapshot_effect(plan, path, view.committer._snapshot(path), view.controls)
