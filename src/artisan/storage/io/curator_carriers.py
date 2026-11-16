"""Stage curator external bytes outside the closed Parquet execution shard."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import ArtifactIntegrityError, StoreIntegrityError
from artisan.execution.curator_resources import verify_owned_curator_file
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.execution.curator_carriers import CuratorCarrierInventory
from artisan.schemas.execution.retained_result import ExternalBinding, ExternalCarrier
from artisan.storage.io.bounded_io import iter_parquet_frames
from artisan.storage.io.external_carriers import stage_external_carriers
from artisan.utils.path import uri_join


def curator_carrier_directory(staging_path: str) -> str:
    """Return an execution-owned sibling, never a worker-managed global location."""
    return f"{staging_path.rstrip('/')}.carriers"


def stage_curator_carriers(
    staging_path: str, fs: AbstractFileSystem, *, files_root: str | None = None
) -> CuratorCarrierInventory:
    """Capture successful eager or drained-stream external content before sealing.

    Payload models are read sequentially from finalized staging. Carrier buffers
    are bounded, and no global artifact location is published by this helper.
    """
    return CuratorCarrierStager(staging_path, fs, files_root=files_root).finish()


class CuratorCarrierStager:
    """Capture external emissions without retaining their artifact models.

    Capture runs before the iterator advances. Finalization selects the same
    managed-first, URI-ordered location as ordinary hydration, after all staged
    descriptors are complete. Only the final selected inventory can be sealed.
    """

    def __init__(
        self,
        staging_path: str,
        fs: AbstractFileSystem,
        *,
        files_root: str | None = None,
    ) -> None:
        self.staging_path, self.fs = staging_path, fs
        self.directory = curator_carrier_directory(staging_path)
        self.files_root = files_root.rstrip("/") if files_root is not None else None
        self._sources: dict[str, ExternalCarrier] = {}
        self._published: dict[str, ExternalCarrier] = {}
        self._finished = False

    def capture(self, artifact: Artifact) -> None:
        """Snapshot this emission while its external file still exists.

        A shared file may grow between emissions, so each capture rereads its
        complete bytes. Identical snapshots share one published staging object.
        """
        if self._finished:
            msg = "Curator carrier staging has already finished"
            raise StoreIntegrityError(msg)
        if not artifact.EXTERNALLY_BACKED:
            return
        assert artifact.artifact_id is not None
        uri = getattr(artifact, next(iter(artifact.LOCATOR_FIELDS)))
        captured = verify_owned_curator_file(uri)
        source_fs = LocalFileSystem() if captured is not None else self.fs
        with TemporaryDirectory(prefix="artisan-curator-carriers-") as scratch:
            carriers, _ = stage_external_carriers(
                [(artifact.artifact_id, artifact)],
                self.directory,
                Path(scratch),
                self.fs,
                source_fs,
            )
        if captured is not None and captured != (
            carriers[0].digest,
            carriers[0].size_bytes,
        ):
            msg = "Curator source capture changed during destination staging"
            raise ArtifactIntegrityError(msg)
        self._sources[uri] = carriers[0]
        self._published[carriers[0].filename] = carriers[0]

    def finish(self) -> CuratorCarrierInventory:
        """Select complete validated carrier bindings after all tables are staged."""
        if self._finished:
            msg = "Curator carrier staging has already finished"
            raise StoreIntegrityError(msg)
        carriers: dict[str, ExternalCarrier] = {}
        bindings: dict[str, ExternalBinding] = {}
        for artifact, locations in _external_artifacts(self.staging_path, self.fs):
            carrier = self._select(artifact, locations)
            assert artifact.artifact_id is not None
            binding = ExternalBinding(
                artifact_id=artifact.artifact_id, filename=carrier.filename
            )
            if bindings.setdefault(artifact.artifact_id, binding) != binding:
                msg = "Curator content has inconsistent selected external locations"
                raise StoreIntegrityError(msg)
            carriers[carrier.filename] = carrier
        inventory = CuratorCarrierInventory(
            carriers=tuple(carriers[name] for name in sorted(carriers)),
            external_bindings=tuple(bindings[key] for key in sorted(bindings)),
        )
        for carrier in set(self._published.values()) - set(inventory.carriers):
            self.fs.rm(uri_join(self.directory, carrier.filename))
        self._finished = True
        return inventory

    def _select(self, artifact: Artifact, locations: set[str]) -> ExternalCarrier:
        locator = next(iter(artifact.LOCATOR_FIELDS))
        for uri in sorted(locations, key=self._location_key):
            candidate = artifact.model_copy(deep=True)
            try:
                if uri not in self._sources:
                    setattr(candidate, locator, uri)
                    self.capture(candidate)
                carrier = self._sources[uri]
                setattr(candidate, locator, uri_join(self.directory, carrier.filename))
                candidate.verify_external_content(fs=self.fs)
            except ArtifactIntegrityError:
                raise
            except OSError:
                continue
            return carrier
        msg = "Curator external artifact has no readable verified staged carrier"
        raise StoreIntegrityError(msg)

    def _location_key(self, uri: str) -> tuple[bool, str]:
        managed = self.files_root is not None and (
            uri == self.files_root or uri.startswith(f"{self.files_root}/")
        )
        return not managed, uri

    def abort(self) -> None:
        """Remove this stager's own snapshots when no successful seal was written."""
        for carrier in self._published.values():
            path = uri_join(self.directory, carrier.filename)
            if self.fs.exists(path):
                self.fs.rm(path)
        self._finished = True


def _external_artifacts(
    staging_path: str, fs: AbstractFileSystem
) -> Iterator[tuple[Artifact, set[str]]]:
    locations: dict[str, set[str]] = {}
    location_path = uri_join(staging_path, "locations.parquet")
    if fs.exists(location_path):
        for frame in iter_parquet_frames(location_path, fs=fs):
            for artifact_id, uri in frame.select("artifact_id", "uri").iter_rows():
                locations.setdefault(artifact_id, set()).add(uri)
    observed: set[str] = set()
    for definition in ArtifactTypeDef.get_all().values():
        model = cast(type[Artifact], definition.model)
        path = uri_join(staging_path, definition.parquet_filename())
        if not model.EXTERNALLY_BACKED or not fs.exists(path):
            continue
        for frame in iter_parquet_frames(path, fs=fs):
            for row in frame.iter_rows(named=True):
                artifact = model.from_row(row)
                artifact_id = artifact.artifact_id
                if artifact_id is None or artifact_id not in locations:
                    msg = "Curator external artifact lacks its exact staged location"
                    raise StoreIntegrityError(msg)
                observed.add(artifact_id)
                yield artifact, locations[artifact_id]
    if observed != set(locations):
        msg = "Curator staged locations lack matching external artifact rows"
        raise StoreIntegrityError(msg)
