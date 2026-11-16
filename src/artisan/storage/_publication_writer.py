"""Rebuild publication stores from audited committed effects, never raw copies."""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

import polars as pl
from deltalake import DeltaTable
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import ArtifactIntegrityError, StoreIntegrityError
from artisan.schemas import Artifact, FileRefArtifact
from artisan.schemas.artifact.external import copy_verified_file
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage._publication_policy import PublicationPolicy
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.committed_scan import (
    read_committed,
    read_logical_commits,
    scan_plan_keys,
    verify_snapshot_effect,
)
from artisan.storage.core.table_schemas import get_physical_schema_for_path, get_schema
from artisan.storage.io.bounded_io import iter_parquet_frames, write_parquet_rows
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import (
    CommitPlan,
    prepare_commit_evidence,
    publish_commit_plan,
    read_commit_plan,
)
from artisan.storage.io.delta_files import DeltaSnapshot, append_table_files
from artisan.storage.io.repair import repair_store
from artisan.storage.io.staging import StagingManager
from artisan.storage.io.worker_seal import (
    STAGING_INVENTORY_KEY,
    build_staging_inventory,
)
from artisan.storage.publication import _File, _PublicationFileSystem
from artisan.utils.path import shard_uri, step_dir_name, uri_join


# Fsspec does not provide type stubs for its filesystem base classes.
class _WriterFileSystem(LocalFileSystem):  # type: ignore[misc]
    """Private write adapter; only the export transaction receives this object."""

    cachable = False  # codespell:ignore cachable

    def __init__(self, directory: Path, files: list[_File]) -> None:
        super().__init__()
        self.mapping = _PublicationFileSystem(directory, files)

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        return super().open(self.mapping._mapped(path), mode=mode, **kwargs)

    def exists(self, path: str, **kwargs: Any) -> bool:
        return cast(bool, super().exists(self.mapping._mapped(path), **kwargs))

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        return cast(dict[str, Any], super().info(self.mapping._mapped(path), **kwargs))


class _Source:
    """Freeze table versions and validate complete source plans before export."""

    def __init__(self, store: ArtifactStore) -> None:
        self.store = store
        self.controls = read_logical_commits(
            store.base_path, fs=store.filesystem, storage_options=store._storage_options
        )
        if self.controls.filter(pl.col("state") != "complete").height:
            msg = "Publication requires all logical commits complete"
            raise StoreIntegrityError(msg)
        _audit(
            store, uri_join(store.base_path, "_artisan", "publication-audit-staging")
        )
        paths = {table.value for table in TablePath} | {
            definition.table_path for definition in ArtifactTypeDef.get_all().values()
        }
        self.snapshots = {
            path: DeltaSnapshot(
                uri_join(store.base_path, path),
                get_physical_schema_for_path(path),
                fs=store.filesystem,
                storage_options=store._storage_options,
            )
            for path in paths
        }
        control_frames = list(self.snapshots[TablePath.LOGICAL_COMMITS].iter_frames())
        frozen_controls = (
            pl.concat(control_frames) if control_frames else self.controls.clear()
        )
        if not self.controls.sort("logical_commit_id").equals(
            frozen_controls.sort("logical_commit_id")
        ):
            msg = "Source store changed during publication capture"
            raise StoreIntegrityError(msg)
        self.plans = [
            read_commit_plan(
                store.base_path,
                store.filesystem,
                row["step_run_id"],
                row["commit_kind"],
                row["recovery_batch_id"],
            )
            for row in self.controls.sort(
                "completed_at", "logical_commit_id"
            ).iter_rows(named=True)
        ]
        self.steps = read_committed(
            store.base_path,
            TablePath.STEPS,
            fs=store.filesystem,
            storage_options=store._storage_options,
        )
        latest = self.steps.sort("state_sequence").unique(
            subset="step_run_id", keep="last"
        )
        if latest.filter(pl.col("status").is_in(["pending", "running"])).height:
            msg = "Publication requires quiescent terminal steps"
            raise StoreIntegrityError(msg)

    def unchanged(self) -> None:
        """Reject concurrent source writes instead of exporting mixed versions."""
        for snapshot in self.snapshots.values():
            current = DeltaTable(
                snapshot.path, storage_options=self.store._storage_options
            )
            if current.version() != snapshot.delta.version():
                msg = "Source store changed during publication export"
                raise StoreIntegrityError(msg)

    def rows(self, plan: CommitPlan, table_path: str) -> Iterator[dict[str, Any]]:
        """Yield one exact, verified effect without collecting scientific payloads."""
        table = plan.table(table_path)
        assert table is not None
        snapshot = self.snapshots[table_path]
        verify_snapshot_effect(plan, table_path, snapshot, self.controls)
        selection = scan_plan_keys(plan, table, snapshot, self.controls)
        columns = [name for name in snapshot.schema if name != "logical_commit_id"]
        for frame in snapshot.iter_frames(
            columns=columns, rows_by_file=selection.eligible
        ):
            for row in frame.iter_rows(named=True):
                if "origin_step_number" in row:
                    row["origin_step_number"] = plan.step_number
                yield row


def _audit(store: ArtifactStore, staging_root: str) -> None:
    report = repair_store(
        delta_root=store.base_path,
        staging_root=staging_root,
        fs=store.filesystem,
        storage_options=store._storage_options,
        files_root=store.files_root,
    )
    if report.blocking or report.unresolved:
        msg = "Publication store failed the complete-evidence audit"
        raise StoreIntegrityError(msg)


def _copy_files(
    source: _Source, directory: Path, policy: PublicationPolicy
) -> list[_File]:
    files = []
    store = source.store
    locations = read_committed(
        store.base_path,
        TablePath.ARTIFACT_LOCATIONS,
        fs=store.filesystem,
        storage_options=store._storage_options,
    )
    for definition in ArtifactTypeDef.get_all().values():
        model = cast(type[Artifact], definition.model)
        if not model.EXTERNALLY_BACKED:
            continue
        seen: set[str] = set()
        for frame in source.snapshots[definition.table_path].iter_frames():
            for row in frame.iter_rows(named=True):
                if definition.model is not FileRefArtifact:
                    msg = "Publication supports FileRef external artifacts only"
                    raise ValueError(msg)
                policy.protected(row, definition.table_path)
                reference = FileRefArtifact.from_row(row)
                reference.finalize()
                artifact_id = cast(str, reference.artifact_id)
                if artifact_id in seen:
                    continue
                seen.add(artifact_id)
                candidates = locations.filter(
                    pl.col("artifact_id") == reference.artifact_id
                )["uri"]
                files.append(
                    _copy_reference(
                        reference, candidates.to_list(), store, directory, policy
                    )
                )
    return files


def _copy_reference(
    reference: FileRefArtifact,
    candidates: list[str],
    store: ArtifactStore,
    directory: Path,
    policy: PublicationPolicy,
) -> _File:
    name = (
        reference.original_name + (reference.extension or "")
        if reference.original_name
        else Path(candidates[0]).name
        if candidates
        else "content"
    )
    policy.protected(name, "external file basename")
    if Path(name).name != name or name in {".", ".."} or "\\" in name or "\x00" in name:
        msg = "External file basename is not portable"
        raise ValueError(msg)
    relative = f"files/{reference.artifact_id}/{name}"
    target = directory / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    for uri in candidates:
        try:
            copy_verified_file(
                artifact_id=reference.artifact_id,
                artifact_type=reference.artifact_type,
                uri=uri,
                destination=str(target),
                expected_digest=reference.content_hash,
                expected_size=reference.size_bytes,
                fs=store.filesystem,
            )
            break
        except (OSError, ArtifactIntegrityError):
            continue
    else:
        msg = "No verified source location for publication artifact"
        raise ArtifactIntegrityError(msg)
    overlap = max((len(value.encode()) for value in policy.redact), default=1) - 1
    tail = b""
    with target.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            policy.protected(tail + chunk, "external artifact bytes")
            tail = (tail + chunk)[-overlap:] if overlap else b""
    return _File(
        artifact_id=cast(str, reference.artifact_id),
        path=relative,
        uri=f"/publication/{relative}",
        content_hash=cast(str, reference.content_hash),
        size_bytes=cast(int, reference.size_bytes),
    )


def _schema(table_path: str) -> dict[str, Any]:
    if table_path in {table.value for table in TablePath}:
        return get_schema(TablePath(table_path))
    return next(
        definition.polars_schema()
        for definition in ArtifactTypeDef.get_all().values()
        if definition.table_path == table_path
    )


def _effect_rows(
    source: _Source,
    plan: CommitPlan,
    table: str,
    policy: PublicationPolicy,
    locations: dict[str, str],
) -> Iterator[dict[str, Any]]:
    models = {
        definition.table_path: cast(type[Artifact], definition.model)
        for definition in ArtifactTypeDef.get_all().values()
    }
    for original in source.rows(plan, table):
        row = policy.row(table, original, locations)
        if table in models:
            models[table].from_row(row).finalize()
        yield row


def _write_effects(
    source: _Source,
    plan: CommitPlan,
    scratch: Path,
    policy: PublicationPolicy,
    locations: dict[str, str],
) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for table in plan.tables:
        path = scratch / f"{len(paths)}.parquet"
        write_parquet_rows(
            str(path),
            _effect_rows(source, plan, table.table_path, policy, locations),
            _schema(table.table_path),
        )
        paths[table.table_path] = path
    return paths


def _rows(path: Path) -> Iterator[dict[str, Any]]:
    for frame in iter_parquet_frames(str(path)):
        yield from frame.iter_rows(named=True)


def _stage_plan(
    plan: CommitPlan, effects: dict[str, Path], staging_root: Path
) -> list[str]:
    """Reconstruct worker ownership from preserved edges and seal new bytes."""
    LocalFileSystem()
    execution_path = effects.get(TablePath.EXECUTIONS)
    executions = list(_rows(execution_path)) if execution_path else []
    execution_ids = [row["execution_run_id"] for row in executions]
    step = staging_root / step_dir_name(plan.step_number, plan.operation_name)
    if plan.commit_kind == "execution_recovery":
        common = Path(
            shard_uri(
                str(staging_root),
                execution_ids[0],
                step_number=plan.step_number,
                operation_name=plan.operation_name,
            )
        )
    else:
        common = step / "_orchestrator" / plan.step_run_id / plan.commit_kind
    common.mkdir(parents=True, exist_ok=True)
    worker_tables = {
        TablePath.EXECUTIONS,
        TablePath.EXECUTION_EDGES,
        TablePath.ARTIFACT_EDGES,
    }
    for table, source in effects.items():
        if table not in worker_tables:
            shutil.copyfile(source, common / f"{table.rsplit('/', 1)[-1]}.parquet")
    for execution in executions:
        _stage_worker(plan, effects, staging_root, execution, common)
    return execution_ids


def _stage_worker(
    plan: CommitPlan,
    effects: dict[str, Path],
    staging_root: Path,
    execution: dict[str, Any],
    common: Path,
) -> None:
    execution_id = execution["execution_run_id"]
    directory = Path(
        shard_uri(
            str(staging_root),
            execution_id,
            step_number=plan.step_number,
            operation_name=plan.operation_name,
        )
    )
    directory.mkdir(parents=True, exist_ok=True)
    referenced: set[str] = set()
    for table in (TablePath.EXECUTION_EDGES, TablePath.ARTIFACT_EDGES):
        if table not in effects:
            continue
        rows = [
            row
            for row in _rows(effects[table])
            if row["execution_run_id"] == execution_id
        ]
        if table == TablePath.EXECUTION_EDGES:
            referenced.update(row["artifact_id"] for row in rows)
        write_parquet_rows(
            str(directory / f"{table.table_name}.parquet"), rows, get_schema(table)
        )
    if TablePath.ARTIFACT_INDEX in effects and directory != common:
        index_rows = (
            row
            for row in _rows(effects[TablePath.ARTIFACT_INDEX])
            if row["artifact_id"] in referenced
        )
        write_parquet_rows(
            str(directory / "index.parquet"),
            index_rows,
            get_schema(TablePath.ARTIFACT_INDEX),
        )
    inventory = build_staging_inventory(str(directory), LocalFileSystem())
    write_parquet_rows(
        str(directory / "executions.parquet"),
        [execution],
        get_schema(TablePath.EXECUTIONS),
        metadata={STAGING_INVENTORY_KEY.encode(): inventory},
    )


def _lifecycle(source: _Source, destination: Path, policy: PublicationPolicy) -> None:
    snapshot = source.snapshots[TablePath.STEPS]
    schema = get_physical_schema_for_path(TablePath.STEPS)
    target = DeltaSnapshot(
        str(destination / "delta" / TablePath.STEPS), schema, fs=LocalFileSystem()
    )

    def frames() -> Iterator[pl.DataFrame]:
        for frame in snapshot.iter_frames():
            rows = [
                policy.row(TablePath.STEPS, row, {})
                for row in frame.filter(
                    pl.col("logical_commit_id").is_null()
                ).iter_rows(named=True)
            ]
            if rows:
                yield pl.DataFrame(rows, schema=schema)

    append_table_files(
        target, frames(), step_number=0, statistics_columns=["step_run_id"]
    )


def _commit(
    source: _Source,
    original: CommitPlan,
    destination: Path,
    writer: DeltaCommitter,
    policy: PublicationPolicy,
    locations: dict[str, str],
) -> None:
    policy.protected(original.operation_name, "operation name")
    with TemporaryDirectory(prefix="artisan-publication-") as scratch:
        effects = _write_effects(source, original, Path(scratch), policy, locations)
        execution_ids = _stage_plan(original, effects, destination / "staging")
    with prepare_commit_evidence(
        staging_root=str(destination / "staging"),
        fs=writer._fs,
        commit_kind=original.commit_kind,
        step_run_id=original.step_run_id,
        step_number=original.step_number,
        operation_name=original.operation_name,
        execution_run_ids=execution_ids,
    ) as evidence:
        plan = evidence.plan
        created_at = source.controls.filter(
            pl.col("logical_commit_id") == original.logical_commit_id
        ).item(0, "created_at")
        history = source.steps.filter(
            (pl.col("step_run_id") == plan.step_run_id)
            & (pl.col("timestamp") <= created_at)
        )
        terminal = evidence.tables.get(TablePath.STEPS)
        if terminal is not None:
            sequence = terminal.get_row(terminal.keys[0])["state_sequence"]
            history = history.filter(pl.col("state_sequence") < sequence)
        history = pl.DataFrame(
            [
                policy.row(TablePath.STEPS, row, {})
                for row in history.iter_rows(named=True)
            ],
            schema=history.schema,
        )
        prepared = writer.prepare_logical(plan, staged=evidence, source_rows=history)
        publish_commit_plan(str(destination / "delta"), writer._fs, plan)
        writer.commit_logical(plan, prepared=prepared, preserve_staging=True)


def write_publication(
    store: ArtifactStore, destination: Path, policy: PublicationPolicy
) -> list[_File]:
    """Build and audit fresh evidence, keeping source snapshots read-only."""
    source = _Source(store)
    files = _copy_files(source, destination, policy)
    filesystem = _WriterFileSystem(destination, files)
    writer = DeltaCommitter(
        str(destination / "delta"),
        StagingManager(str(destination / "staging"), filesystem),
        fs=filesystem,
        files_root=str(destination / "files"),
    )
    try:
        writer.initialize_tables()
        _lifecycle(source, destination, policy)
        locations = {item.artifact_id: item.uri for item in files}
        for plan in source.plans:
            _commit(source, plan, destination, writer, policy, locations)
        exported = ArtifactStore(
            str(destination / "delta"),
            fs=filesystem,
            files_root=str(destination / "files"),
        )
        _audit(exported, str(destination / "staging"))
        source.unchanged()
    finally:
        writer.close()
    return files
