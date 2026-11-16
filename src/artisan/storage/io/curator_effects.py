"""Capture curator staging effects under their separately admitted authority.

Only the typed outer curator plan publishes completion authority. This module
freezes and verifies its inner file/table effects without publishing a plan.
"""

from __future__ import annotations

import posixpath
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import Any, Literal
from uuid import uuid4
from weakref import finalize

import polars as pl
from fsspec import AbstractFileSystem
from pydantic import BaseModel, ConfigDict, model_validator

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import get_schema
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.bounded_io import (
    CapturedParquet,
    capture_parquet,
    iter_parquet_frames,
)
from artisan.storage.io.plan_evidence import (
    PlannedFile,
    PlannedTable,
    _is_content_digest,
    ordered_table_paths,
    staging_table_paths,
)
from artisan.storage.io.publication import is_publication_temporary
from artisan.storage.io.worker_seal import capture_worker_files
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest
from artisan.utils.path import shard_uri, step_dir_name, uri_join


class CuratorEffects(BaseModel):
    """Exact curator terminal effects; authorization belongs to the outer plan."""

    logical_commit_id: str
    commit_kind: Literal["step_result"]
    step_run_id: str
    recovery_batch_id: None = None
    step_number: int
    operation_name: str
    tables: tuple[PlannedTable, ...]
    plan_digest: str

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def _verify_digest(self) -> CuratorEffects:
        expected_id = f"step_result:{self.step_run_id}"
        if self.logical_commit_id != expected_id:
            msg = f"Commit plan ID does not match its owner: {self.logical_commit_id}"
            raise ValueError(msg)
        if self.step_number < 0 or not self.operation_name:
            msg = f"Commit plan {self.logical_commit_id} has invalid step metadata"
            raise ValueError(msg)
        table_paths = [table.table_path for table in self.tables]
        if len(table_paths) != len(set(table_paths)):
            msg = f"Commit plan {self.logical_commit_id} repeats a table"
            raise ValueError(msg)
        file_paths = [
            file.relative_path for table in self.tables for file in table.files
        ]
        if len(file_paths) != len(set(file_paths)):
            msg = f"Commit plan {self.logical_commit_id} repeats a staged file"
            raise ValueError(msg)
        order = [path for path in ordered_table_paths() if path in table_paths]
        if table_paths != order:
            msg = f"Commit plan {self.logical_commit_id} has invalid table order"
            raise ValueError(msg)
        terminal = self.table(TablePath.STEPS.value)
        if terminal is None or terminal.row_count != 1:
            msg = f"Step-result commit {self.logical_commit_id} requires one terminal snapshot"
            raise ValueError(msg)
        if self.plan_digest != curator_effects_digest(
            self.model_dump(exclude={"plan_digest"})
        ):
            msg = f"Commit plan {self.logical_commit_id} has an invalid digest"
            raise ValueError(msg)
        return self

    def table(self, table_path: str) -> PlannedTable | None:
        """Return the planned effect for ``table_path``, when present."""
        return next(
            (table for table in self.tables if table.table_path == table_path),
            None,
        )

    def expected_origin(self, table_path: str, key: tuple[Any, ...]) -> int:
        """Require the admitted curator step's uniform origin for new content."""
        return self.step_number


@dataclass(frozen=True)
class CapturedCuratorEffects:
    """Captured exact files and per-worker artifact declarations for one plan."""

    plan: CuratorEffects
    staging_root: str
    tables: dict[str, DiskTable]
    per_execution_artifact_ids: dict[str, set[str]]
    _scratch: TemporaryDirectory[str]

    def __post_init__(self) -> None:
        finalize(self, self._scratch.cleanup)

    def close(self) -> None:
        """Release captured payloads and canonical spools on every exit path."""
        self._scratch.cleanup()

    def __enter__(self) -> CapturedCuratorEffects:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        self.close()


def prepare_curator_effects(
    *,
    staging_root: str,
    fs: AbstractFileSystem,
    step_run_id: str,
    step_number: int,
    operation_name: str,
    execution_run_ids: list[str] | tuple[str, ...] = (),
) -> CapturedCuratorEffects:
    """Read exact staging evidence once for planning and immediate application."""
    if len(set(execution_run_ids)) != len(execution_run_ids):
        msg = f"Duplicate execution IDs in curator effects {step_run_id}"
        raise StoreIntegrityError(msg)
    logical_commit_id = f"step_result:{step_run_id}"
    scratch = TemporaryDirectory(prefix="artisan-commit-")
    try:
        table_files = _inspect_staging(
            staging_root=staging_root,
            fs=fs,
            step_run_id=step_run_id,
            step_number=step_number,
            operation_name=operation_name,
            execution_run_ids=sorted(execution_run_ids),
            scratch_dir=scratch.name,
        )
        disk_tables = _disk_tables(table_files, scratch.name)
        tables = tuple(
            _build_table(logical_commit_id, table_path, files, disk_tables[table_path])
            for table_path, files in _ordered_table_files(table_files)
        )
        if not tables:
            msg = f"Logical commit {logical_commit_id} has no staged effects"
            raise StoreIntegrityError(msg)
        payload: dict[str, Any] = {
            "logical_commit_id": logical_commit_id,
            "commit_kind": "step_result",
            "step_run_id": step_run_id,
            "recovery_batch_id": None,
            "step_number": step_number,
            "operation_name": operation_name,
            "tables": [table.model_dump(mode="json") for table in tables],
        }
        plan = CuratorEffects(**payload, plan_digest=curator_effects_digest(payload))
        return _staged_evidence(plan, staging_root, table_files, disk_tables, scratch)
    except BaseException:
        scratch.cleanup()
        raise


def capture_curator_effects(
    plan: CuratorEffects,
    staging_root: str,
    fs: AbstractFileSystem,
) -> CapturedCuratorEffects:
    """Capture exact planned files, preserving each shard's artifact membership."""
    scratch = TemporaryDirectory(prefix="artisan-commit-")
    try:
        table_files: dict[str, list[tuple[PlannedFile, CapturedParquet]]] = {}
        for table in plan.tables:
            files = []
            for planned_file in table.files:
                path = _resolve_relative(staging_root, planned_file.relative_path)
                captured = capture_parquet(
                    path, fs, f"{scratch.name}/{uuid4().hex}.parquet"
                )
                if (
                    captured.size_bytes != planned_file.size_bytes
                    or captured.digest != planned_file.digest
                    or captured.row_count != planned_file.row_count
                    or _schema_signature(captured.schema)
                    != planned_file.schema_signature
                ):
                    msg = f"Staged object changed after planning: {_safe(path)}"
                    raise StoreIntegrityError(msg)
                files.append((planned_file, captured))
            table_files[table.table_path] = files
        tables = _disk_tables(table_files, scratch.name)
        return _staged_evidence(plan, staging_root, table_files, tables, scratch)
    except BaseException:
        scratch.cleanup()
        raise


def _disk_tables(
    table_files: dict[str, list[tuple[PlannedFile, CapturedParquet]]], scratch_dir: str
) -> dict[str, DiskTable]:
    return {
        table_path: DiskTable(table_path, [file.path for _, file in files], scratch_dir)
        for table_path, files in _ordered_table_files(table_files)
    }


def _staged_evidence(
    plan: CuratorEffects,
    staging_root: str,
    table_files: dict[str, list[tuple[PlannedFile, CapturedParquet]]],
    tables: dict[str, DiskTable],
    scratch: TemporaryDirectory[str],
) -> CapturedCuratorEffects:
    membership: dict[str, set[str]] = {}
    for table in plan.tables:
        _validate_table_rows(plan, table, tables[table.table_path])
        if table.table_path == TablePath.ARTIFACT_INDEX.value:
            for file, captured in table_files[table.table_path]:
                execution_id = posixpath.basename(posixpath.dirname(file.relative_path))
                if _is_content_digest(execution_id):
                    ids = membership.setdefault(execution_id, set())
                    for frame in iter_parquet_frames(
                        captured.path, columns=["artifact_id"]
                    ):
                        ids.update(frame["artifact_id"].to_list())
    executions = plan.table(TablePath.EXECUTIONS.value)
    if executions is not None:
        for (execution_id,) in executions.row_keys:
            membership.setdefault(execution_id, set())
    return CapturedCuratorEffects(plan, staging_root, tables, membership, scratch)


def _inspect_staging(
    *,
    staging_root: str,
    fs: AbstractFileSystem,
    step_run_id: str,
    step_number: int,
    operation_name: str,
    execution_run_ids: list[str] | tuple[str, ...],
    scratch_dir: str,
) -> dict[str, list[tuple[PlannedFile, CapturedParquet]]]:
    """Capture only named directories, validating ownership through projections."""
    allowed = staging_table_paths()
    directories: list[tuple[str, str | None]] = [
        (
            shard_uri(
                staging_root,
                execution_id,
                step_number=step_number,
                operation_name=operation_name,
            ),
            execution_id,
        )
        for execution_id in execution_run_ids
    ]
    orchestrator = uri_join(
        staging_root,
        step_dir_name(step_number, operation_name),
        "_orchestrator",
        step_run_id,
        "step_result",
    )
    if fs.exists(orchestrator):
        directories.append((orchestrator, None))
    found: dict[str, list[tuple[PlannedFile, CapturedParquet]]] = {}
    for directory, execution_id in directories:
        if not fs.exists(directory):
            msg = f"Missing staging directory {_safe(directory)}"
            raise StoreIntegrityError(msg)
        if execution_id is not None:
            captured_files = capture_worker_files(
                directory, fs, destination=scratch_dir
            ).files
        else:
            captured_files = _capture_orchestrator(directory, fs, allowed, scratch_dir)
        for filename, captured in sorted(captured_files.items()):
            path = uri_join(directory, filename)
            table_path = allowed[filename]
            _validate_staged_schema(table_path, captured.schema)
            _validate_captured_ownership(
                captured,
                table_path,
                step_run_id,
                step_number,
                operation_name,
                execution_id,
            )
            planned_file = PlannedFile(
                relative_path=_relative_path(staging_root, path, fs),
                size_bytes=captured.size_bytes,
                digest=captured.digest,
                row_count=captured.row_count,
                schema_signature=_schema_signature(captured.schema),
            )
            found.setdefault(table_path, []).append((planned_file, captured))
    return found


def _capture_orchestrator(
    directory: str, fs: AbstractFileSystem, allowed: dict[str, str], scratch_dir: str
) -> dict[str, CapturedParquet]:
    captured = {}
    for entry in fs.ls(directory, detail=False):
        path = str(entry).rstrip("/")
        filename = posixpath.basename(path)
        if is_publication_temporary(filename):
            continue
        if fs.isdir(path) or filename not in allowed:
            msg = f"Unexpected staged object {_safe(path)}"
            raise StoreIntegrityError(msg)
        captured[filename] = capture_parquet(
            path, fs, f"{scratch_dir}/{uuid4().hex}.parquet"
        )
    return captured


def _build_table(
    logical_commit_id: str,
    table_path: str,
    files: list[tuple[PlannedFile, CapturedParquet]],
    effect: DiskTable,
) -> PlannedTable:
    return PlannedTable(
        table_path=table_path,
        natural_key=effect.natural_key,
        row_keys=effect.keys,
        row_count=effect.row_count,
        table_plan_key=effect.canonical_key(logical_commit_id),
        files=tuple(planned for planned, _ in files),
    )


def _validate_table_rows(
    plan: CuratorEffects, table: PlannedTable, effect: DiskTable
) -> None:
    if effect.row_count != table.row_count or effect.keys != table.row_keys:
        msg = f"Row count or keys changed for {plan.logical_commit_id} table {table.table_path}"
        raise StoreIntegrityError(msg)
    if effect.canonical_key(plan.logical_commit_id) != table.table_plan_key:
        msg = f"Table plan key changed for {plan.logical_commit_id} table {table.table_path}"
        raise StoreIntegrityError(msg)


def _validate_staged_schema(table_path: str, schema: dict[str, Any]) -> None:
    """Require the exact ownerless schema at the staging boundary."""
    if table_path in {member.value for member in TablePath}:
        expected = get_schema(TablePath(table_path))
    else:
        type_def = next(
            definition
            for definition in ArtifactTypeDef.get_all().values()
            if definition.table_path == table_path
        )
        expected = type_def.polars_schema()
    if schema != expected:
        msg = f"Invalid staged schema for {table_path}: expected {expected!r}, found {schema!r}"
        raise StoreIntegrityError(msg)


def _validate_captured_ownership(
    captured: CapturedParquet,
    table_path: str,
    step_run_id: str,
    step_number: int,
    operation_name: str,
    execution_run_id: str | None,
) -> None:
    columns = [
        name
        for name in captured.schema
        if name
        in {
            "execution_run_id",
            "operation_name",
            "step_run_id",
            "origin_step_number",
            "current_step_run_id",
            "success",
            "replay_of_execution_run_id",
            "status",
        }
    ]
    if table_path == TablePath.STEPS.value and captured.row_count != 1:
        msg = f"Terminal candidate ownership mismatch for {step_run_id}"
        raise StoreIntegrityError(msg)
    if columns and captured.row_count:
        frames = iter_parquet_frames(captured.path, columns=columns)
    else:
        frames = iter(
            [pl.DataFrame(schema={name: captured.schema[name] for name in columns})]
        )
    for frame in frames:
        _validate_ownership(
            frame,
            table_path,
            step_run_id,
            step_number,
            operation_name,
            execution_run_id,
        )


def _validate_ownership(
    frame: pl.DataFrame,
    table_path: str,
    step_run_id: str,
    step_number: int,
    operation_name: str,
    execution_run_id: str | None,
) -> None:
    """Validate directory-derived execution and step ownership."""
    if execution_run_id is not None:
        if table_path == TablePath.EXECUTIONS.value:
            values = set(frame["execution_run_id"].to_list())
            if values != {execution_run_id}:
                msg = f"Execution seal ownership mismatch for {execution_run_id}"
                raise StoreIntegrityError(msg)
            operations = set(frame["operation_name"].to_list())
            if operations != {operation_name}:
                msg = f"Execution seal operation mismatch for {execution_run_id}"
                raise StoreIntegrityError(msg)
        if "execution_run_id" in frame.columns:
            values = set(frame["execution_run_id"].to_list())
            if values != {execution_run_id}:
                msg = f"Staged execution ownership mismatch for {execution_run_id}"
                raise StoreIntegrityError(msg)
    if "step_run_id" in frame.columns:
        values = set(frame["step_run_id"].to_list())
        if values != {step_run_id}:
            msg = f"Staged step ownership mismatch for {step_run_id}"
            raise StoreIntegrityError(msg)
    if "origin_step_number" in frame.columns:
        values = set(frame["origin_step_number"].to_list())
        if values != {step_number}:
            msg = f"Staged step number mismatch for {step_run_id}"
            raise StoreIntegrityError(msg)
    if table_path == TablePath.CACHE_REUSE.value and set(
        frame["current_step_run_id"].to_list()
    ) != {step_run_id}:
        msg = f"Cache reuse ownership mismatch for {step_run_id}"
        raise StoreIntegrityError(msg)
    if table_path == TablePath.STEPS.value and (
        set(frame["step_run_id"].to_list()) != {step_run_id}
        or frame.height != 1
        or frame["status"][0] not in {"succeeded", "partial", "failed"}
    ):
        msg = f"Terminal candidate ownership mismatch for {step_run_id}"
        raise StoreIntegrityError(msg)


def _ordered_table_files(
    files: dict[str, list[tuple[PlannedFile, CapturedParquet]]],
) -> list[tuple[str, list[tuple[PlannedFile, CapturedParquet]]]]:
    return [(table, files[table]) for table in ordered_table_paths() if table in files]


def _schema_signature(schema: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple((name, str(dtype)) for name, dtype in schema.items())


def curator_effects_digest(payload: dict[str, Any]) -> str:
    return compute_content_digest(canonical_json_bytes(payload))


def _relative_path(root: str, path: str, fs: AbstractFileSystem) -> str:
    stripped_root = str(fs._strip_protocol(root)).rstrip("/")
    stripped_path = str(fs._strip_protocol(path))
    relative = posixpath.relpath(stripped_path, stripped_root)
    if relative == ".." or relative.startswith("../"):
        msg = f"Staged path escapes its root: {_safe(path)}"
        raise StoreIntegrityError(msg)
    return relative


def _resolve_relative(
    root: str,
    relative: str,
) -> str:
    if posixpath.isabs(relative) or relative == ".." or relative.startswith("../"):
        msg = f"Invalid relative staging path {relative!r}"
        raise StoreIntegrityError(msg)
    return uri_join(root, relative)


def _safe(path: str) -> str:
    from artisan.schemas.artifact.external import sanitized_uri

    return sanitized_uri(path)
