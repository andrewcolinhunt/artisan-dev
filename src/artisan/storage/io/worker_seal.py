"""Closed payload inventories carried by immutable worker execution seals."""

from __future__ import annotations

import io
import posixpath

import polars as pl
from fsspec import AbstractFileSystem
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import get_schema
from artisan.storage.io.publication import is_publication_temporary
from artisan.utils.hashing import (
    canonical_json_bytes,
    compute_content_digest,
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
        if not set(names) <= _payload_filenames():
            msg = "Worker inventory contains an unregistered payload filename"
            raise ValueError(msg)
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
        with fs.open(path, "rb") as stream:
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


def verify_worker_seal(staging_path: str, fs: AbstractFileSystem) -> pl.DataFrame:
    """Verify the closed inventory and return its single execution row."""
    return read_worker_files(staging_path, fs)[EXECUTION_SEAL_FILENAME][1]


def read_worker_files(
    staging_path: str, fs: AbstractFileSystem
) -> dict[str, tuple[bytes, pl.DataFrame]]:
    """Capture and verify each sealed worker object once for batch planning."""
    try:
        with fs.open(uri_join(staging_path, EXECUTION_SEAL_FILENAME), "rb") as stream:
            data = stream.read()
        metadata = pl.read_parquet_metadata(io.BytesIO(data))
        inventory = _StagingInventory.model_validate_json(
            metadata[STAGING_INVENTORY_KEY]
        )
        frame = pl.read_parquet(io.BytesIO(data))
    except (
        OSError,
        KeyError,
        ValueError,
        ValidationError,
        pl.exceptions.PolarsError,
    ) as exc:
        msg = (
            f"Unreadable worker seal or inventory in {posixpath.basename(staging_path)}"
        )
        raise StoreIntegrityError(msg) from exc
    if frame.height != 1 or dict(frame.schema) != get_schema(TablePath.EXECUTIONS):
        msg = (
            "Worker seal must contain exactly one execution with the registered schema"
        )
        raise StoreIntegrityError(msg)
    paths = _payload_paths(staging_path, fs)
    expected = {item.filename: item for item in inventory.files}
    if paths.keys() != expected.keys():
        msg = f"Missing or changed worker inventory payloads in {posixpath.basename(staging_path)}"
        raise StoreIntegrityError(msg)
    files = {EXECUTION_SEAL_FILENAME: (data, frame)}
    for name, path in sorted(paths.items()):
        with fs.open(path, "rb") as stream:
            payload = stream.read()
        item = expected[name]
        if (
            len(payload) != item.size_bytes
            or compute_content_digest(payload) != item.digest
        ):
            msg = f"Missing or changed worker inventory payloads in {posixpath.basename(staging_path)}"
            raise StoreIntegrityError(msg)
        try:
            files[name] = (payload, pl.read_parquet(io.BytesIO(payload)))
        except pl.exceptions.PolarsError as exc:
            msg = f"Unreadable worker payload {name!r}"
            raise StoreIntegrityError(msg) from exc
    return files
