"""Immutable logical-commit plans built from exact sealed staging evidence."""

from __future__ import annotations

import io
import json
import posixpath
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Literal

import polars as pl
from fsspec import AbstractFileSystem
from pydantic import BaseModel, ConfigDict, model_validator

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    get_natural_key,
    get_schema,
    is_global_artifact_table,
)
from artisan.storage.io.publication import (
    is_publication_temporary,
    publish_immutable_bytes,
)
from artisan.storage.io.worker_seal import read_worker_files
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest
from artisan.utils.path import shard_uri, step_dir_name, uri_join

CommitKind = Literal["step_result", "input_registration", "execution_recovery"]


def logical_commit_identity(
    commit_kind: str, step_run_id: str, recovery_batch_id: str | None = None
) -> str:
    """Validate the kind-specific owner and return its durable commit ID."""
    if commit_kind not in {"step_result", "input_registration", "execution_recovery"}:
        msg = f"Unknown commit kind {commit_kind!r}"
        raise ValueError(msg)
    if (commit_kind == "execution_recovery") != (recovery_batch_id is not None):
        msg = "Only execution recovery requires a batch owner"
        raise ValueError(msg)
    if recovery_batch_id is not None and not _is_content_digest(recovery_batch_id):
        msg = "Invalid recovery batch ID"
        raise ValueError(msg)
    return f"{commit_kind}:{recovery_batch_id or step_run_id}"


class PlannedFile(BaseModel):
    """One exact staged Parquet object referenced by a commit plan."""

    relative_path: str
    size_bytes: int
    digest: str
    row_count: int
    schema_signature: tuple[tuple[str, str], ...]

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def _validate_evidence(self) -> PlannedFile:
        normalized = posixpath.normpath(self.relative_path)
        if (
            not self.relative_path
            or posixpath.isabs(self.relative_path)
            or normalized != self.relative_path
            or normalized == ".."
            or normalized.startswith("../")
        ):
            msg = f"Invalid relative staged path {self.relative_path!r}"
            raise ValueError(msg)
        if self.size_bytes <= 0 or self.row_count < 0:
            msg = f"Invalid staged evidence sizes for {self.relative_path!r}"
            raise ValueError(msg)
        if not _is_content_digest(self.digest):
            msg = f"Invalid staged digest for {self.relative_path!r}"
            raise ValueError(msg)
        return self


class PlannedTable(BaseModel):
    """One table effect and the evidence needed to prove exact retry."""

    table_path: str
    natural_key: tuple[str, ...]
    row_keys: tuple[tuple[Any, ...], ...]
    row_count: int
    table_plan_key: str
    files: tuple[PlannedFile, ...]

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def _validate_effect(self) -> PlannedTable:
        if self.table_path not in set(_staging_table_paths().values()):
            msg = f"Unknown planned table {self.table_path!r}"
            raise ValueError(msg)
        if self.natural_key != get_natural_key(self.table_path):
            msg = f"Invalid natural key for planned table {self.table_path!r}"
            raise ValueError(msg)
        if not self.files or len({file.relative_path for file in self.files}) != len(
            self.files
        ):
            msg = f"Invalid staged file set for planned table {self.table_path!r}"
            raise ValueError(msg)
        staged_row_count = sum(file.row_count for file in self.files)
        if self.row_count > staged_row_count or (
            not is_global_artifact_table(self.table_path)
            and self.row_count != staged_row_count
        ):
            msg = f"Invalid row count for planned table {self.table_path!r}"
            raise ValueError(msg)
        if self.row_count != len(self.row_keys) or len(set(self.row_keys)) != len(
            self.row_keys
        ):
            msg = f"Invalid row keys for planned table {self.table_path!r}"
            raise ValueError(msg)
        if any(len(key) != len(self.natural_key) for key in self.row_keys):
            msg = f"Malformed row key for planned table {self.table_path!r}"
            raise ValueError(msg)
        if not _is_content_digest(self.table_plan_key):
            msg = f"Invalid table plan key for {self.table_path!r}"
            raise ValueError(msg)
        return self


class CommitPlan(BaseModel):
    """Durable plan for a step result, input registration, or recovered execution."""

    logical_commit_id: str
    commit_kind: CommitKind
    step_run_id: str
    recovery_batch_id: str | None = None
    step_number: int
    operation_name: str
    tables: tuple[PlannedTable, ...]
    plan_digest: str

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def _verify_digest(self) -> CommitPlan:
        expected_id = logical_commit_identity(
            self.commit_kind, self.step_run_id, self.recovery_batch_id
        )
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
        order = [
            table
            for table, _ in _ordered_table_files({path: [] for path in table_paths})
        ]
        if table_paths != order:
            msg = f"Commit plan {self.logical_commit_id} has invalid table order"
            raise ValueError(msg)
        terminal = self.table(TablePath.STEPS.value)
        if self.commit_kind == "step_result" and (
            terminal is None or terminal.row_count != 1
        ):
            msg = f"Step-result commit {self.logical_commit_id} requires one terminal snapshot"
            raise ValueError(msg)
        if self.commit_kind == "input_registration" and terminal is not None:
            msg = f"Input registration {self.logical_commit_id} cannot contain a step snapshot"
            raise ValueError(msg)
        if self.commit_kind == "execution_recovery":
            execution = self.table(TablePath.EXECUTIONS.value)
            if (
                terminal is not None
                or self.table(TablePath.CACHE_REUSE.value) is not None
                or execution is None
                or execution.row_count == 0
                or self.recovery_batch_id
                != recovery_batch_identity(
                    self.step_run_id, [key[0] for key in execution.row_keys]
                )
            ):
                msg = (
                    "Recovery requires its exact execution batch and no step/reuse rows"
                )
                raise ValueError(msg)
            directories = {
                shard_uri(
                    "",
                    key[0],
                    step_number=self.step_number,
                    operation_name=self.operation_name,
                ).lstrip("/")
                for key in execution.row_keys
            }
            allowed_files = _staging_table_paths()
            for table in self.tables:
                for file in table.files:
                    if (
                        posixpath.dirname(file.relative_path) not in directories
                        or allowed_files.get(posixpath.basename(file.relative_path))
                        != table.table_path
                    ):
                        msg = "Recovery file path does not match its exact execution batch"
                        raise ValueError(msg)
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


@dataclass(frozen=True)
class StagedPlanEvidence:
    """Captured exact files and per-worker artifact declarations for one plan."""

    plan: CommitPlan
    staging_root: str
    frames: dict[str, pl.DataFrame]
    per_execution_artifact_ids: dict[str, set[str]]


def recovery_batch_identity(
    step_run_id: str, execution_run_ids: list[str] | tuple[str, ...]
) -> str:
    """Identify a finite recovery snapshot independently of discovery order."""
    if not execution_run_ids or any(
        not _is_content_digest(value) for value in execution_run_ids
    ):
        msg = "Recovery requires valid execution IDs"
        raise ValueError(msg)
    if len(set(execution_run_ids)) != len(execution_run_ids):
        msg = "Recovery contains duplicate execution IDs"
        raise ValueError(msg)
    return compute_content_digest(
        canonical_json_bytes(
            {
                "step_run_id": step_run_id,
                "execution_run_ids": sorted(execution_run_ids),
            }
        )
    )


def prepare_commit_evidence(
    *,
    staging_root: str,
    fs: AbstractFileSystem,
    commit_kind: CommitKind,
    step_run_id: str,
    step_number: int,
    operation_name: str,
    execution_run_ids: list[str] | tuple[str, ...] = (),
) -> StagedPlanEvidence:
    """Read exact staging evidence once for planning and immediate application."""
    if len(set(execution_run_ids)) != len(execution_run_ids):
        msg = f"Duplicate execution IDs in commit {commit_kind}:{step_run_id}"
        raise StoreIntegrityError(msg)
    recovery_batch_id = (
        recovery_batch_identity(step_run_id, execution_run_ids)
        if commit_kind == "execution_recovery"
        else None
    )
    logical_commit_id = logical_commit_identity(
        commit_kind, step_run_id, recovery_batch_id
    )
    table_files = _inspect_staging(
        staging_root=staging_root,
        fs=fs,
        step_run_id=step_run_id,
        step_number=step_number,
        operation_name=operation_name,
        execution_run_ids=sorted(execution_run_ids),
        commit_kind=commit_kind,
    )
    tables = tuple(
        _build_table(logical_commit_id, table_path, files)
        for table_path, files in _ordered_table_files(table_files)
    )
    if not tables:
        msg = f"Logical commit {logical_commit_id} has no staged effects"
        raise StoreIntegrityError(msg)
    payload: dict[str, Any] = {
        "logical_commit_id": logical_commit_id,
        "commit_kind": commit_kind,
        "step_run_id": step_run_id,
        "recovery_batch_id": recovery_batch_id,
        "step_number": step_number,
        "operation_name": operation_name,
        "tables": [table.model_dump(mode="json") for table in tables],
    }
    plan = CommitPlan(**payload, plan_digest=_plan_digest(payload))
    return _staged_evidence(plan, staging_root, table_files)


def prepare_commit_plan(
    *,
    staging_root: str,
    fs: AbstractFileSystem,
    commit_kind: CommitKind,
    step_run_id: str,
    step_number: int,
    operation_name: str,
    execution_run_ids: list[str] | tuple[str, ...] = (),
) -> CommitPlan:
    """Construct an immutable plan without publishing or applying it."""
    return prepare_commit_evidence(
        staging_root=staging_root,
        fs=fs,
        commit_kind=commit_kind,
        step_run_id=step_run_id,
        step_number=step_number,
        operation_name=operation_name,
        execution_run_ids=execution_run_ids,
    ).plan


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
    """Prepare and publish one immutable plan from exact staging evidence."""
    plan = prepare_commit_plan(
        staging_root=staging_root,
        fs=fs,
        commit_kind=commit_kind,
        step_run_id=step_run_id,
        step_number=step_number,
        operation_name=operation_name,
        execution_run_ids=execution_run_ids,
    )
    return publish_commit_plan(delta_root, fs, plan)


def publish_commit_plan(
    delta_root: str,
    fs: AbstractFileSystem,
    plan: CommitPlan,
) -> CommitPlan:
    """Publish exact plan bytes once, then verify the durable object."""
    final_path = commit_plan_path(
        delta_root, plan.step_run_id, plan.commit_kind, plan.recovery_batch_id
    )
    encoded = canonical_json_bytes(plan.model_dump(mode="json"))
    publish_immutable_bytes(fs, final_path, encoded)
    published = read_commit_plan(
        delta_root, fs, plan.step_run_id, plan.commit_kind, plan.recovery_batch_id
    )
    if published != plan:
        msg = f"Published plan changed for {plan.logical_commit_id}"
        raise StoreIntegrityError(msg)
    return published


def read_commit_plan(
    delta_root: str,
    fs: AbstractFileSystem,
    step_run_id: str,
    commit_kind: CommitKind,
    recovery_batch_id: str | None = None,
) -> CommitPlan:
    """Read and digest-validate one exact plan."""
    path = commit_plan_path(delta_root, step_run_id, commit_kind, recovery_batch_id)
    try:
        with fs.open(path, "rb") as stream:
            raw = json.load(stream)
        plan = CommitPlan.model_validate(raw)
        if (
            plan.step_run_id != step_run_id
            or plan.commit_kind != commit_kind
            or plan.recovery_batch_id != recovery_batch_id
        ):
            msg = f"Commit plan path does not match {commit_kind}:{step_run_id}"
            raise StoreIntegrityError(msg)
        return plan
    except StoreIntegrityError:
        raise
    except Exception as exc:
        msg = f"Unreadable commit plan {commit_kind}:{step_run_id} at {_safe(path)}"
        raise StoreIntegrityError(msg) from exc


def read_plan_evidence(
    plan: CommitPlan,
    staging_root: str,
    fs: AbstractFileSystem,
) -> StagedPlanEvidence:
    """Reread exact planned files, preserving each shard's artifact membership."""
    table_files: dict[str, list[tuple[PlannedFile, pl.DataFrame]]] = {}
    for table in plan.tables:
        files = []
        for planned_file in table.files:
            path = _resolve_relative(staging_root, planned_file.relative_path)
            data, frame = _read_parquet_bytes(path, fs)
            if (
                len(data) != planned_file.size_bytes
                or compute_content_digest(data) != planned_file.digest
                or frame.height != planned_file.row_count
                or _schema_signature(frame) != planned_file.schema_signature
            ):
                msg = f"Staged object changed after planning: {_safe(path)}"
                raise StoreIntegrityError(msg)
            files.append((planned_file, frame))
        table_files[table.table_path] = files
    return _staged_evidence(plan, staging_root, table_files)


def verify_plan_files(
    plan: CommitPlan, staging_root: str, fs: AbstractFileSystem
) -> dict[str, pl.DataFrame]:
    """Read and verify one persisted plan's exact staged table effects."""
    return read_plan_evidence(plan, staging_root, fs).frames


def _staged_evidence(
    plan: CommitPlan,
    staging_root: str,
    table_files: dict[str, list[tuple[PlannedFile, pl.DataFrame]]],
) -> StagedPlanEvidence:
    frames = {}
    membership: dict[str, set[str]] = {}
    for table in plan.tables:
        files = table_files[table.table_path]
        combined = pl.concat(
            [frame for _, frame in files], how="vertical_relaxed", rechunk=True
        )
        frames[table.table_path] = _validate_table_rows(plan, table, combined)
        if table.table_path == TablePath.ARTIFACT_INDEX.value:
            for file, frame in files:
                execution_id = posixpath.basename(posixpath.dirname(file.relative_path))
                if _is_content_digest(execution_id):
                    membership.setdefault(execution_id, set()).update(
                        frame["artifact_id"].to_list()
                    )
    executions = plan.table(TablePath.EXECUTIONS.value)
    if executions is not None:
        for (execution_id,) in executions.row_keys:
            membership.setdefault(execution_id, set())
    return StagedPlanEvidence(plan, staging_root, frames, membership)


def commit_plan_path(
    delta_root: str,
    step_run_id: str,
    commit_kind: CommitKind,
    recovery_batch_id: str | None = None,
) -> str:
    """Return the durable path for one logical commit plan."""
    logical_commit_identity(commit_kind, step_run_id, recovery_batch_id)
    if recovery_batch_id is not None:
        return uri_join(
            delta_root,
            "_artisan",
            "commit_plans",
            step_run_id,
            commit_kind,
            f"{recovery_batch_id}.json",
        )
    return uri_join(
        delta_root,
        "_artisan",
        "commit_plans",
        step_run_id,
        f"{commit_kind}.json",
    )


def comparable_effect_rows(table_path: str, frame: pl.DataFrame) -> pl.DataFrame:
    """Separate reusable artifact values from their first commit's origin."""
    if is_global_artifact_table(table_path) and "origin_step_number" in frame.columns:
        return frame.drop("origin_step_number")
    return frame


def canonical_table_plan_key(
    logical_commit_id: str,
    table_path: str,
    frame: pl.DataFrame,
) -> str:
    """Hash a table's canonical ownerless planned rows."""
    natural_key = get_natural_key(table_path)
    effect = _planned_effect_rows(table_path, frame, natural_key)
    rows = _canonical_rows(comparable_effect_rows(table_path, effect), natural_key)
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
        commit_kind,
    )
    if commit_kind != "execution_recovery" and fs.exists(orchestrator):
        directories.append((orchestrator, None))

    found: dict[str, list[tuple[PlannedFile, pl.DataFrame]]] = {}
    for directory, execution_id in directories:
        if not fs.exists(directory):
            msg = f"Missing staging directory {_safe(directory)}"
            raise StoreIntegrityError(msg)
        if execution_id is not None:
            captured = read_worker_files(directory, fs)
        else:
            captured = {}
            for entry in fs.ls(directory, detail=False):
                path = str(entry).rstrip("/")
                filename = posixpath.basename(path)
                if is_publication_temporary(filename):
                    continue
                if fs.isdir(path) or filename not in allowed:
                    msg = f"Unexpected staged object {_safe(path)}"
                    raise StoreIntegrityError(msg)
                captured[filename] = _read_parquet_bytes(path, fs)
        for filename, (data, frame) in sorted(captured.items()):
            path = uri_join(directory, filename)
            table_path = allowed[filename]
            _validate_staged_schema(table_path, frame)
            _validate_ownership(
                frame,
                table_path,
                step_run_id,
                step_number,
                operation_name,
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
    effect = _planned_effect_rows(table_path, combined, natural_key)
    canonical_rows = _canonical_rows(effect, natural_key)
    row_keys = tuple(
        tuple(row[column] for column in natural_key) for row in canonical_rows
    )
    return PlannedTable(
        table_path=table_path,
        natural_key=natural_key,
        row_keys=row_keys,
        row_count=effect.height,
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
) -> pl.DataFrame:
    effect = _planned_effect_rows(table.table_path, frame, table.natural_key)
    if effect.height != table.row_count:
        msg = f"Row count changed for {plan.logical_commit_id} table {table.table_path}"
        raise StoreIntegrityError(msg)
    if (
        canonical_table_plan_key(plan.logical_commit_id, table.table_path, effect)
        != table.table_plan_key
    ):
        msg = f"Table plan key changed for {plan.logical_commit_id} table {table.table_path}"
        raise StoreIntegrityError(msg)
    return effect


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
    operation_name: str,
    execution_run_id: str | None,
    commit_kind: CommitKind,
) -> None:
    """Validate directory-derived execution and step ownership."""
    if commit_kind == "execution_recovery" and table_path in {
        TablePath.STEPS.value,
        TablePath.CACHE_REUSE.value,
    }:
        msg = "Recovery cannot contain step or cache-reuse rows"
        raise StoreIntegrityError(msg)
    if execution_run_id is not None:
        if table_path == TablePath.EXECUTIONS.value:
            if commit_kind == "execution_recovery" and (
                frame.height != 1
                or frame["success"][0] is not True
                or frame["replay_of_execution_run_id"][0] is not None
            ):
                msg = "Recovery requires one successful ordinary execution"
                raise StoreIntegrityError(msg)
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


def _planned_effect_rows(
    table_path: str,
    frame: pl.DataFrame,
    natural_key: tuple[str, ...],
) -> pl.DataFrame:
    if any(column not in frame.columns for column in natural_key):
        msg = f"Missing natural key {natural_key!r} in staged table {table_path}"
        raise StoreIntegrityError(msg)

    unique = frame.unique(subset=list(natural_key), maintain_order=True)
    if frame.height == unique.height:
        return frame
    if not is_global_artifact_table(table_path):
        msg = f"Duplicate natural keys in staged table {table_path}"
        raise StoreIntegrityError(msg)

    data_columns = [column for column in frame.columns if column not in natural_key]
    conflicts = (
        frame.group_by(list(natural_key))
        .agg(pl.struct(data_columns).n_unique().alias("variants"))
        .filter(pl.col("variants") != 1)
    )
    if not conflicts.is_empty():
        msg = f"Conflicting natural keys in staged table {table_path}"
        raise StoreIntegrityError(msg)
    return unique


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


def _is_content_digest(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 32:
        return False
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


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
