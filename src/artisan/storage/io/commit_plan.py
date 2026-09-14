"""Immutable logical-commit plans built from exact sealed staging evidence."""

from __future__ import annotations

import io
import json
import posixpath
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Literal

import polars as pl
from fsspec import AbstractFileSystem
from pydantic import BaseModel, ConfigDict, model_validator

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import get_natural_key, get_schema
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest
from artisan.utils.path import shard_uri, step_dir_name, uri_join

CommitKind = Literal["step_result", "input_registration"]


class PlannedFile(BaseModel):
    """One exact staged Parquet object referenced by a commit plan."""

    relative_path: str
    size_bytes: int
    digest: str
    row_count: int
    schema_signature: tuple[tuple[str, str], ...]

    model_config = ConfigDict(frozen=True)


class PlannedTable(BaseModel):
    """One table effect and the evidence needed to prove exact retry."""

    table_path: str
    natural_key: tuple[str, ...]
    row_keys: tuple[tuple[Any, ...], ...]
    row_count: int
    table_plan_key: str
    files: tuple[PlannedFile, ...]

    model_config = ConfigDict(frozen=True)


class CommitPlan(BaseModel):
    """Durable plan for one step result or input registration."""

    logical_commit_id: str
    commit_kind: CommitKind
    step_run_id: str
    step_number: int
    operation_name: str
    tables: tuple[PlannedTable, ...]
    plan_digest: str

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def _verify_digest(self) -> CommitPlan:
        if self.plan_digest != _plan_digest(self.model_dump(exclude={"plan_digest"})):
            msg = f"Commit plan {self.logical_commit_id} has an invalid digest"
            raise ValueError(msg)
        return self

    def table(self, table_path: str) -> PlannedTable | None:
        """Return the planned effect for ``table_path``, when present."""
        return next(
            (table for table in self.tables if table.table_path == table_path),
            None,
        )


def build_commit_plan(
    *,
    delta_root: str,
    staging_root: str,
    fs: AbstractFileSystem,
    commit_kind: CommitKind,
    step_run_id: str,
    step_number: int,
    operation_name: str,
    execution_run_ids: list[str] | tuple[str, ...] = (),
) -> CommitPlan:
    """Validate exact staging directories and publish one immutable plan."""
    if len(set(execution_run_ids)) != len(execution_run_ids):
        msg = f"Duplicate execution IDs in commit {commit_kind}:{step_run_id}"
        raise StoreIntegrityError(msg)
    logical_commit_id = f"{commit_kind}:{step_run_id}"
    table_files = _inspect_staging(
        staging_root=staging_root,
        fs=fs,
        step_run_id=step_run_id,
        step_number=step_number,
        operation_name=operation_name,
        execution_run_ids=execution_run_ids,
        commit_kind=commit_kind,
    )
    tables = tuple(
        _build_table(logical_commit_id, table_path, files)
        for table_path, files in _ordered_table_files(table_files)
    )
    if not tables:
        msg = f"Logical commit {logical_commit_id} has no staged effects"
        raise StoreIntegrityError(msg)
    terminal = next(
        (table for table in tables if table.table_path == TablePath.STEPS.value),
        None,
    )
    if commit_kind == "step_result" and (terminal is None or terminal.row_count != 1):
        msg = f"Step-result commit {logical_commit_id} requires one terminal snapshot"
        raise StoreIntegrityError(msg)
    if commit_kind == "input_registration" and terminal is not None:
        msg = f"Input registration {logical_commit_id} cannot contain a step snapshot"
        raise StoreIntegrityError(msg)
    payload: dict[str, Any] = {
        "logical_commit_id": logical_commit_id,
        "commit_kind": commit_kind,
        "step_run_id": step_run_id,
        "step_number": step_number,
        "operation_name": operation_name,
        "tables": [table.model_dump(mode="json") for table in tables],
    }
    plan = CommitPlan(**payload, plan_digest=_plan_digest(payload))
    return publish_commit_plan(delta_root, fs, plan)


def publish_commit_plan(
    delta_root: str,
    fs: AbstractFileSystem,
    plan: CommitPlan,
) -> CommitPlan:
    """Publish a plan through a temporary object, then verify the final bytes."""
    final_path = commit_plan_path(delta_root, plan.step_run_id, plan.commit_kind)
    if fs.exists(final_path):
        existing = read_commit_plan(delta_root, fs, plan.step_run_id, plan.commit_kind)
        if existing != plan:
            msg = f"Conflicting immutable plan for {plan.logical_commit_id}"
            raise StoreIntegrityError(msg)
        return existing

    parent = posixpath.dirname(final_path)
    fs.makedirs(parent, exist_ok=True)
    temporary = f"{final_path}.tmp-{uuid.uuid4().hex}"
    encoded = canonical_json_bytes(plan.model_dump(mode="json"))
    try:
        with fs.open(temporary, "wb") as stream:
            stream.write(encoded)
        fs.mv(temporary, final_path)
    finally:
        if fs.exists(temporary):
            fs.rm(temporary)
    published = read_commit_plan(delta_root, fs, plan.step_run_id, plan.commit_kind)
    if published != plan:
        msg = f"Published plan changed for {plan.logical_commit_id}"
        raise StoreIntegrityError(msg)
    return published


def read_commit_plan(
    delta_root: str,
    fs: AbstractFileSystem,
    step_run_id: str,
    commit_kind: CommitKind,
) -> CommitPlan:
    """Read and digest-validate one exact plan."""
    path = commit_plan_path(delta_root, step_run_id, commit_kind)
    try:
        with fs.open(path, "rb") as stream:
            raw = json.load(stream)
        return CommitPlan.model_validate(raw)
    except StoreIntegrityError:
        raise
    except Exception as exc:
        msg = f"Unreadable commit plan {commit_kind}:{step_run_id} at {_safe(path)}"
        raise StoreIntegrityError(msg) from exc


def verify_plan_files(
    plan: CommitPlan,
    staging_root: str,
    fs: AbstractFileSystem,
) -> dict[str, pl.DataFrame]:
    """Reread every planned object and reject changed bytes or rows."""
    tables: dict[str, pl.DataFrame] = {}
    for table in plan.tables:
        frames: list[pl.DataFrame] = []
        for planned_file in table.files:
            path = _resolve_relative(staging_root, planned_file.relative_path, fs)
            data, frame = _read_parquet_bytes(path, fs)
            if (
                len(data) != planned_file.size_bytes
                or compute_content_digest(data) != planned_file.digest
                or frame.height != planned_file.row_count
                or _schema_signature(frame) != planned_file.schema_signature
            ):
                msg = f"Staged object changed after planning: {_safe(path)}"
                raise StoreIntegrityError(msg)
            frames.append(frame)
        combined = pl.concat(frames, how="vertical_relaxed", rechunk=True)
        _validate_table_rows(plan, table, combined)
        tables[table.table_path] = combined
    return tables


def commit_plan_path(
    delta_root: str,
    step_run_id: str,
    commit_kind: CommitKind,
) -> str:
    """Return the durable path for one logical commit plan."""
    return uri_join(
        delta_root,
        "_artisan",
        "commit_plans",
        step_run_id,
        f"{commit_kind}.json",
    )


def canonical_table_plan_key(
    logical_commit_id: str,
    table_path: str,
    frame: pl.DataFrame,
) -> str:
    """Hash a table's canonical ownerless planned rows."""
    rows = _canonical_rows(frame, get_natural_key(table_path))
    return compute_content_digest(
        canonical_json_bytes(
            {
                "logical_commit_id": logical_commit_id,
                "table_path": table_path,
                "rows": rows,
            }
        )
    )


def _inspect_staging(
    *,
    staging_root: str,
    fs: AbstractFileSystem,
    step_run_id: str,
    step_number: int,
    operation_name: str,
    execution_run_ids: list[str] | tuple[str, ...],
    commit_kind: CommitKind,
) -> dict[str, list[tuple[PlannedFile, pl.DataFrame]]]:
    """Inspect only the named worker and orchestrator directories."""
    allowed = _staging_table_paths()
    directories = [
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
    )
    if fs.exists(orchestrator):
        directories.append((orchestrator, None))

    found: dict[str, list[tuple[PlannedFile, pl.DataFrame]]] = {}
    for directory, execution_id in directories:
        if not fs.exists(directory):
            msg = f"Missing staging directory {_safe(directory)}"
            raise StoreIntegrityError(msg)
        entries = list(fs.ls(directory, detail=False))
        names = {posixpath.basename(str(entry).rstrip("/")) for entry in entries}
        if execution_id is not None and "executions.parquet" not in names:
            msg = f"Missing execution seal in {_safe(directory)}"
            raise StoreIntegrityError(msg)
        for entry in entries:
            path = str(entry).rstrip("/")
            if fs.isdir(path):
                msg = f"Unexpected staging directory {_safe(path)}"
                raise StoreIntegrityError(msg)
            filename = posixpath.basename(path)
            table_path = allowed.get(filename)
            if table_path is None:
                msg = f"Unexpected staged object {_safe(path)}"
                raise StoreIntegrityError(msg)
            data, frame = _read_parquet_bytes(path, fs)
            _validate_staged_schema(table_path, frame)
            _validate_ownership(
                frame,
                table_path,
                step_run_id,
                step_number,
                execution_id,
                commit_kind,
            )
            planned_file = PlannedFile(
                relative_path=_relative_path(staging_root, path, fs),
                size_bytes=len(data),
                digest=compute_content_digest(data),
                row_count=frame.height,
                schema_signature=_schema_signature(frame),
            )
            found.setdefault(table_path, []).append((planned_file, frame))
    return found


def _build_table(
    logical_commit_id: str,
    table_path: str,
    files: list[tuple[PlannedFile, pl.DataFrame]],
) -> PlannedTable:
    frames = [frame for _, frame in files]
    combined = pl.concat(frames, how="vertical_relaxed", rechunk=True)
    natural_key = get_natural_key(table_path)
    _require_unique_keys(table_path, combined, natural_key)
    canonical_rows = _canonical_rows(combined, natural_key)
    row_keys = tuple(
        tuple(row[column] for column in natural_key) for row in canonical_rows
    )
    return PlannedTable(
        table_path=table_path,
        natural_key=natural_key,
        row_keys=row_keys,
        row_count=combined.height,
        table_plan_key=canonical_table_plan_key(
            logical_commit_id,
            table_path,
            combined,
        ),
        files=tuple(planned for planned, _ in files),
    )


def _validate_table_rows(
    plan: CommitPlan,
    table: PlannedTable,
    frame: pl.DataFrame,
) -> None:
    _require_unique_keys(table.table_path, frame, table.natural_key)
    if frame.height != table.row_count:
        msg = f"Row count changed for {plan.logical_commit_id} table {table.table_path}"
        raise StoreIntegrityError(msg)
    if (
        canonical_table_plan_key(plan.logical_commit_id, table.table_path, frame)
        != table.table_plan_key
    ):
        msg = f"Table plan key changed for {plan.logical_commit_id} table {table.table_path}"
        raise StoreIntegrityError(msg)


def _validate_staged_schema(table_path: str, frame: pl.DataFrame) -> None:
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
    if dict(frame.schema) != expected:
        msg = (
            f"Invalid staged schema for {table_path}: expected {expected!r}, "
            f"found {dict(frame.schema)!r}"
        )
        raise StoreIntegrityError(msg)


def _validate_ownership(
    frame: pl.DataFrame,
    table_path: str,
    step_run_id: str,
    step_number: int,
    execution_run_id: str | None,
    commit_kind: CommitKind,
) -> None:
    """Validate directory-derived execution and step ownership."""
    if execution_run_id is not None:
        if table_path == TablePath.EXECUTIONS.value:
            values = set(frame["execution_run_id"].to_list())
            if values != {execution_run_id}:
                msg = f"Execution seal ownership mismatch for {execution_run_id}"
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
        commit_kind != "step_result"
        or set(frame["step_run_id"].to_list()) != {step_run_id}
        or frame.height != 1
        or frame["status"][0] not in {"succeeded", "partial", "failed"}
    ):
        msg = f"Terminal candidate ownership mismatch for {step_run_id}"
        raise StoreIntegrityError(msg)


def _staging_table_paths() -> dict[str, str]:
    paths = {
        definition.parquet_filename(): definition.table_path
        for definition in ArtifactTypeDef.get_all().values()
    }
    for table in (
        TablePath.ARTIFACT_INDEX,
        TablePath.ARTIFACT_LOCATIONS,
        TablePath.EXECUTIONS,
        TablePath.EXECUTION_EDGES,
        TablePath.ARTIFACT_EDGES,
        TablePath.CACHE_REUSE,
        TablePath.STEPS,
    ):
        paths[f"{table.table_name}.parquet"] = table.value
    return paths


def _ordered_table_files(
    files: dict[str, list[tuple[PlannedFile, pl.DataFrame]]],
) -> list[tuple[str, list[tuple[PlannedFile, pl.DataFrame]]]]:
    artifact_paths = sorted(
        definition.table_path for definition in ArtifactTypeDef.get_all().values()
    )
    order = [
        *artifact_paths,
        TablePath.ARTIFACT_INDEX.value,
        TablePath.ARTIFACT_LOCATIONS.value,
        TablePath.EXECUTIONS.value,
        TablePath.EXECUTION_EDGES.value,
        TablePath.ARTIFACT_EDGES.value,
        TablePath.CACHE_REUSE.value,
        TablePath.STEPS.value,
    ]
    return [(table, files[table]) for table in order if table in files]


def _read_parquet_bytes(
    path: str,
    fs: AbstractFileSystem,
) -> tuple[bytes, pl.DataFrame]:
    try:
        with fs.open(path, "rb") as stream:
            data = stream.read()
        return data, pl.read_parquet(io.BytesIO(data))
    except Exception as exc:
        msg = f"Unreadable staged Parquet object {_safe(path)}"
        raise StoreIntegrityError(msg) from exc


def _require_unique_keys(
    table_path: str,
    frame: pl.DataFrame,
    natural_key: tuple[str, ...],
) -> None:
    if any(column not in frame.columns for column in natural_key):
        msg = f"Missing natural key {natural_key!r} in staged table {table_path}"
        raise StoreIntegrityError(msg)
    if frame.height != frame.unique(subset=list(natural_key)).height:
        msg = f"Duplicate natural keys in staged table {table_path}"
        raise StoreIntegrityError(msg)


def _canonical_rows(
    frame: pl.DataFrame,
    natural_key: tuple[str, ...],
) -> list[dict[str, Any]]:
    ordered = frame.sort(list(natural_key)) if frame.height > 1 else frame
    return [
        {column: _json_value(row[column]) for column in frame.columns}
        for row in ordered.iter_rows(named=True)
    ]


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"$binary": value.hex()}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def _schema_signature(frame: pl.DataFrame) -> tuple[tuple[str, str], ...]:
    return tuple((name, str(dtype)) for name, dtype in frame.schema.items())


def _plan_digest(payload: dict[str, Any]) -> str:
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
    fs: AbstractFileSystem,
) -> str:
    if posixpath.isabs(relative) or relative == ".." or relative.startswith("../"):
        msg = f"Invalid relative staging path {relative!r}"
        raise StoreIntegrityError(msg)
    return uri_join(root, relative)


def _safe(path: str) -> str:
    from artisan.schemas.artifact.external import sanitized_uri

    return sanitized_uri(path)
