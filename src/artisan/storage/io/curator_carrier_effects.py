"""Capture sealed curator carriers and derive controller-owned location effects."""

from __future__ import annotations

import json
import posixpath
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.admission import ManagedCarrier
from artisan.schemas.execution.curator_carriers import (
    CURATOR_CARRIER_INVENTORY_KEY,
    CuratorCarrierInventory,
)
from artisan.storage.core.table_schemas import ARTIFACT_LOCATIONS_SCHEMA
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.bounded_io import capture_parquet, write_parquet_rows
from artisan.storage.io.curator_carriers import curator_carrier_directory
from artisan.storage.io.curator_effects import CapturedCuratorEffects, CuratorEffects
from artisan.storage.io.external_carriers import capture_external_carriers
from artisan.storage.io.plan_evidence import PlannedFile, PlannedTable
from artisan.storage.io.publication import publish_immutable_file
from artisan.utils.hashing import compute_stream_digest
from artisan.utils.path import step_dir_name, uri_join


@dataclass(frozen=True)
class CuratorCarrierEvidence:
    """Private captured files and derived tables valid within the capture scope."""

    inventory: CuratorCarrierInventory | None
    carriers: Mapping[str, str]
    validation_tables: dict[str, DiskTable]
    tables: dict[str, DiskTable]
    managed_locations: PlannedTable | None


def sealed_curator_carriers(
    metadata: str | None, *, success: bool
) -> CuratorCarrierInventory | None:
    """Read carrier evidence only from a verified exact execution row."""
    payload = {} if metadata is None else json.loads(metadata)
    if not isinstance(payload, dict):
        msg = "Curator execution metadata must be an object"
        raise StoreIntegrityError(msg)
    if CURATOR_CARRIER_INVENTORY_KEY not in payload:
        return None
    if not success:
        msg = "Failed curator execution cannot seal external carrier inventory"
        raise StoreIntegrityError(msg)
    return CuratorCarrierInventory.model_validate(
        payload[CURATOR_CARRIER_INVENTORY_KEY]
    )


def carrier_directory(effects: CuratorEffects, staging_root: str) -> str:
    execution = effects.table(TablePath.EXECUTIONS.value)
    if execution is None or len(execution.files) != 1:
        msg = "Curator carrier inventory requires one exact execution shard"
        raise StoreIntegrityError(msg)
    return curator_carrier_directory(
        posixpath.dirname(uri_join(staging_root, execution.files[0].relative_path))
    )


@contextmanager
def capture_curator_carriers(
    staged: CapturedCuratorEffects, *, files_root: str, fs: AbstractFileSystem
) -> Iterator[CuratorCarrierEvidence]:
    """Validate sealed bytes without depending on original worker source paths."""
    execution = staged.tables.get(TablePath.EXECUTIONS.value)
    inventory = None
    if execution is not None:
        if len(execution.keys) != 1:
            msg = "Curator carriers require one exact execution"
            raise StoreIntegrityError(msg)
        row = execution.get_row(execution.keys[0])
        inventory = sealed_curator_carriers(row["metadata"], success=row["success"])
    external = {
        key[0]
        for definition in ArtifactTypeDef.get_all().values()
        if cast(type[Artifact], definition.model).EXTERNALLY_BACKED
        for key in (
            staged.tables[definition.table_path].keys
            if definition.table_path in staged.tables
            else ()
        )
    }
    bindings = inventory.external_bindings if inventory is not None else ()
    locations = staged.tables.get(TablePath.ARTIFACT_LOCATIONS.value)
    located = {key[0] for key in locations.keys} if locations is not None else set()
    if external != {item.artifact_id for item in bindings} or external != located:
        msg = "Curator external content lacks its exact sealed carrier inventory"
        raise StoreIntegrityError(msg)
    if inventory is None:
        yield CuratorCarrierEvidence(inventory, {}, staged.tables, staged.tables, None)
        return
    directory = carrier_directory(staged.plan, staged.staging_root)
    actual = (
        {posixpath.basename(str(path)) for path in fs.ls(directory, detail=False)}
        if fs.exists(directory)
        else set()
    )
    if actual != {item.filename for item in inventory.carriers}:
        msg = "Curator external staging differs from its complete sealed inventory"
        raise StoreIntegrityError(msg)
    if not inventory.carriers:
        yield CuratorCarrierEvidence(inventory, {}, staged.tables, staged.tables, None)
        return
    with TemporaryDirectory(prefix="artisan-curator-carrier-capture-") as scratch:
        carriers = capture_external_carriers(
            directory, inventory.carriers, Path(scratch), fs
        )
        validation = dict(staged.tables)
        validation[TablePath.ARTIFACT_LOCATIONS.value] = _locations(
            ((item.artifact_id, carriers[item.filename]) for item in bindings),
            Path(scratch) / "local.parquet",
        )
        by_name = {item.filename: item for item in inventory.carriers}
        managed = _locations(
            (
                (
                    item.artifact_id,
                    uri_join(
                        files_root,
                        ManagedCarrier(
                            digest=by_name[item.filename].digest,
                            size_bytes=by_name[item.filename].size_bytes,
                        ).relative_path,
                    ),
                )
                for item in bindings
            ),
            Path(scratch) / "managed.parquet",
        )
        effect = _publish_locations(staged, managed, scratch, fs)
        tables = {**staged.tables, TablePath.ARTIFACT_LOCATIONS.value: managed}
        yield CuratorCarrierEvidence(inventory, carriers, validation, tables, effect)


def _locations(rows: Iterator[tuple[str, str]], path: Path) -> DiskTable:
    write_parquet_rows(
        str(path),
        ({"artifact_id": artifact_id, "uri": uri} for artifact_id, uri in rows),
        ARTIFACT_LOCATIONS_SCHEMA,
    )
    return DiskTable(TablePath.ARTIFACT_LOCATIONS.value, [str(path)], str(path.parent))


def _publish_locations(
    staged: CapturedCuratorEffects,
    table: DiskTable,
    scratch: str,
    fs: AbstractFileSystem,
) -> PlannedTable:
    relative = uri_join(
        step_dir_name(staged.plan.step_number, staged.plan.operation_name),
        "_orchestrator",
        staged.plan.step_run_id,
        "curator_carriers",
        "locations.parquet",
    )
    captured = capture_parquet(
        str(Path(scratch) / "managed.parquet"),
        LocalFileSystem(),
        str(Path(scratch) / "managed-effect.parquet"),
    )
    planned = PlannedTable(
        table_path=table.table_path,
        natural_key=table.natural_key,
        row_keys=table.keys,
        row_count=table.row_count,
        table_plan_key=table.canonical_key(staged.plan.logical_commit_id),
        files=(
            PlannedFile(
                relative_path=relative,
                size_bytes=captured.size_bytes,
                digest=captured.digest,
                row_count=captured.row_count,
                schema_signature=tuple(
                    (name, str(dtype)) for name, dtype in captured.schema.items()
                ),
            ),
        ),
    )
    publish_immutable_file(
        fs,
        uri_join(staged.staging_root, relative),
        captured.path,
        source_fs=LocalFileSystem(),
    )
    return planned


def publish_curator_carriers(
    captured: CuratorCarrierEvidence, *, files_root: str, fs: AbstractFileSystem
) -> None:
    """Apply declared managed copies only after immutable controller selection."""
    if captured.inventory is None:
        return
    for carrier in captured.inventory.carriers:
        managed = ManagedCarrier(digest=carrier.digest, size_bytes=carrier.size_bytes)
        publish_immutable_file(
            fs,
            uri_join(files_root, managed.relative_path),
            captured.carriers[carrier.filename],
            source_fs=LocalFileSystem(),
        )


def verify_curator_carriers(
    inventory: CuratorCarrierInventory | None,
    locations: PlannedTable | None,
    *,
    files_root: str,
    fs: AbstractFileSystem,
) -> None:
    """Verify exact location bindings and complete managed bytes after publication."""
    if inventory is None or not inventory.carriers:
        if locations is not None:
            msg = "Curator managed locations have no full carrier inventory"
            raise StoreIntegrityError(msg)
        return
    carriers = {item.filename: item for item in inventory.carriers}
    expected = {
        (
            item.artifact_id,
            uri_join(
                files_root,
                ManagedCarrier(
                    digest=carriers[item.filename].digest,
                    size_bytes=carriers[item.filename].size_bytes,
                ).relative_path,
            ),
        )
        for item in inventory.external_bindings
    }
    if locations is None or set(locations.row_keys) != expected:
        msg = "Curator managed location plan disagrees with sealed carrier bindings"
        raise StoreIntegrityError(msg)
    for carrier in inventory.carriers:
        managed = ManagedCarrier(digest=carrier.digest, size_bytes=carrier.size_bytes)
        with fs.open(
            uri_join(files_root, managed.relative_path), "rb", cache_type="none"
        ) as stream:
            if compute_stream_digest(stream) != (carrier.digest, carrier.size_bytes):
                msg = "Curator managed full carrier changed"
                raise StoreIntegrityError(msg)
