"""Closed payload inventories carried by immutable worker execution seals."""

from __future__ import annotations

import posixpath
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from types import TracebackType
from uuid import uuid4
from weakref import finalize

import polars as pl
import pyarrow.parquet as pq
from fsspec import AbstractFileSystem
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from artisan.errors import ApplicationRegistrationError, StoreIntegrityError
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import get_schema
from artisan.storage.io.bounded_io import (
    CapturedParquet,
    capture_parquet,
    iter_parquet_frames,
)
from artisan.storage.io.publication import is_publication_temporary
from artisan.utils.hashing import (
    canonical_json_bytes,
    compute_stream_digest,
)
from artisan.utils.path import uri_join

STAGING_INVENTORY_KEY = "artisan.staging_inventory"
EXECUTION_SEAL_FILENAME = "executions.parquet"


class _InventoryFile(BaseModel):
    """Exact bytes of one finalized worker payload."""

    filename: str
    size_bytes: int = Field(gt=0)
    digest: str = Field(pattern=r"^[0-9a-f]{32}$")

    model_config = ConfigDict(extra="forbid", strict=True)


class _StagingInventory(BaseModel):
    """Versioned closed file set, embedded in the execution Parquet footer."""

    version: int
    files: list[_InventoryFile]

    model_config = ConfigDict(extra="forbid", strict=True)

    @model_validator(mode="after")
    def _validate_files(self) -> _StagingInventory:
        names = [item.filename for item in self.files]
        if self.version != 1 or names != sorted(set(names)):
            msg = "Invalid worker inventory version or repeated/unsorted filenames"
            raise ValueError(msg)
        for name in set(names) - _payload_filenames():
            if (
                not name.endswith(".parquet")
                or name == EXECUTION_SEAL_FILENAME
                or posixpath.basename(name) != name
                or not name.removesuffix(".parquet").replace("-", "_").isidentifier()
            ):
                msg = "Worker inventory contains an invalid payload filename"
                raise ValueError(msg)
            msg = f"Worker payload {name!r} has no installed artifact registration"
            raise ApplicationRegistrationError(msg)
        return self


def _payload_filenames() -> set[str]:
    """Return registered artifact and worker-owned framework filenames."""
    return {
        definition.parquet_filename()
        for definition in ArtifactTypeDef.get_all().values()
    } | {
        f"{table.table_name}.parquet"
        for table in (
            TablePath.ARTIFACT_INDEX,
            TablePath.ARTIFACT_LOCATIONS,
            TablePath.EXECUTION_EDGES,
            TablePath.ARTIFACT_EDGES,
        )
    }


def ensure_unsealed(staging_path: str, fs: AbstractFileSystem) -> None:
    """Reject a new recording attempt before it can change a sealed shard."""
    if fs.exists(uri_join(staging_path, EXECUTION_SEAL_FILENAME)):
        msg = f"Worker execution shard is already sealed: {posixpath.basename(staging_path)}"
        raise StoreIntegrityError(msg)


def _payload_paths(staging_path: str, fs: AbstractFileSystem) -> dict[str, str]:
    """Inspect all siblings, excluding only the seal and recognized temporaries."""
    paths: dict[str, str] = {}
    # Empty S3 prefixes have no directory object before their first seal.
    if not fs.exists(staging_path):
        return paths
    for entry in fs.ls(staging_path, detail=False):
        path = str(entry).rstrip("/")
        name = posixpath.basename(path)
        if name == EXECUTION_SEAL_FILENAME or is_publication_temporary(name):
            continue
        if fs.isdir(path) or name not in _payload_filenames():
            msg = f"Unexpected worker staging object {name!r}"
            raise StoreIntegrityError(msg)
        paths[name] = path
    return paths


def build_staging_inventory(staging_path: str, fs: AbstractFileSystem) -> bytes:
    """Describe every finalized sibling payload for execution-seal metadata."""
    files = []
    for name, path in sorted(_payload_paths(staging_path, fs).items()):
        with fs.open(path, "rb", cache_type="none") as stream:
            digest, size_bytes = compute_stream_digest(stream)
        files.append(
            _InventoryFile(
                filename=name,
                size_bytes=size_bytes,
                digest=digest,
            )
        )
    inventory = _StagingInventory(version=1, files=files)
    return canonical_json_bytes(inventory.model_dump())


@dataclass
class CapturedWorkerFiles:
    """Verified local worker files with explicit ownership of temporary storage."""

    files: dict[str, CapturedParquet]
    _scratch: TemporaryDirectory[str] | None = None

    def __post_init__(self) -> None:
        if self._scratch is not None:
            finalize(self, self._scratch.cleanup)

    def close(self) -> None:
        """Release this capture's private storage when it owns the directory."""
        if self._scratch is not None:
            self._scratch.cleanup()

    def __enter__(self) -> CapturedWorkerFiles:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        self.close()

    def iter_frames(
        self, filename: str, columns: list[str] | None = None
    ) -> Iterator[pl.DataFrame]:
        """Read a verified payload with an optional metadata-only projection."""
        yield from iter_parquet_frames(self.files[filename].path, columns=columns)

    def read_frame(
        self, filename: str, columns: list[str] | None = None
    ) -> pl.DataFrame:
        """Collect an explicit metadata projection or the single execution row."""
        captured = self.files[filename]
        schema = captured.schema
        if columns is not None:
            schema = {name: schema[name] for name in columns}
        frames = list(self.iter_frames(filename, columns))
        return pl.concat(frames) if frames else pl.DataFrame(schema=schema)


def verify_worker_seal(staging_path: str, fs: AbstractFileSystem) -> pl.DataFrame:
    """Verify the closed inventory and return its single execution row."""
    with capture_worker_files(staging_path, fs) as captured:
        return captured.read_frame(EXECUTION_SEAL_FILENAME)


def capture_worker_files(
    staging_path: str,
    fs: AbstractFileSystem,
    *,
    destination: str | None = None,
) -> CapturedWorkerFiles:
    """Capture every sealed object without retaining compressed or decoded payload."""
    scratch = (
        TemporaryDirectory(prefix="artisan-worker-") if destination is None else None
    )
    directory_name = scratch.name if scratch is not None else destination
    assert directory_name is not None
    directory = Path(directory_name) / uuid4().hex
    try:
        directory.mkdir()
        files = _capture_worker_files(staging_path, fs, directory)
        return CapturedWorkerFiles(files, scratch)
    except BaseException:
        if scratch is not None:
            scratch.cleanup()
        raise


def _capture_worker_files(
    staging_path: str, fs: AbstractFileSystem, destination: Path
) -> dict[str, CapturedParquet]:
    seal, inventory = _capture_seal(staging_path, fs, destination)
    paths = _payload_paths(staging_path, fs)
    expected = {item.filename: item for item in inventory.files}
    if paths.keys() != expected.keys():
        raise _inventory_changed(staging_path)
    files = {EXECUTION_SEAL_FILENAME: seal}
    for name, path in sorted(paths.items()):
        try:
            captured = capture_parquet(path, fs, str(destination / name))
        except StoreIntegrityError as exc:
            raise _inventory_changed(staging_path) from exc
        item = expected[name]
        if captured.size_bytes != item.size_bytes or captured.digest != item.digest:
            raise _inventory_changed(staging_path)
        files[name] = captured
    return files


def _capture_seal(
    staging_path: str, fs: AbstractFileSystem, destination: Path
) -> tuple[CapturedParquet, _StagingInventory]:
    try:
        seal = capture_parquet(
            uri_join(staging_path, EXECUTION_SEAL_FILENAME),
            fs,
            str(destination / EXECUTION_SEAL_FILENAME),
        )
        metadata = pq.read_metadata(seal.path).metadata or {}
        inventory = _StagingInventory.model_validate_json(
            metadata[STAGING_INVENTORY_KEY.encode()]
        )
    except (OSError, KeyError, ValueError, ValidationError, StoreIntegrityError) as exc:
        msg = (
            f"Unreadable worker seal or inventory in {posixpath.basename(staging_path)}"
        )
        raise StoreIntegrityError(msg) from exc
    if seal.row_count != 1 or seal.schema != get_schema(TablePath.EXECUTIONS):
        msg = (
            "Worker seal must contain exactly one execution with the registered schema"
        )
        raise StoreIntegrityError(msg)
    return seal, inventory


def _inventory_changed(staging_path: str) -> StoreIntegrityError:
    msg = f"Missing or changed worker inventory payloads in {posixpath.basename(staging_path)}"
    return StoreIntegrityError(msg)
