"""Exact staged file/table evidence and canonical effect identities."""

from __future__ import annotations

import posixpath
from datetime import datetime
from enum import Enum
from typing import Any

import polars as pl
from pydantic import BaseModel, ConfigDict, model_validator

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    get_natural_key,
    get_physical_schema_for_path,
    is_global_artifact_table,
)
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


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
        if self.table_path not in set(staging_table_paths().values()):
            get_physical_schema_for_path(self.table_path)
            msg = f"Unknown planned table {self.table_path!r}"
            raise ValueError(msg)
        return self


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


def staging_table_paths() -> dict[str, str]:
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


def _is_content_digest(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 32:
        return False
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


def ordered_table_paths() -> list[str]:
    """Keep content before relationships and the terminal lifecycle effect."""
    return [
        *sorted(item.table_path for item in ArtifactTypeDef.get_all().values()),
        TablePath.ARTIFACT_INDEX.value,
        TablePath.ARTIFACT_LOCATIONS.value,
        TablePath.EXECUTIONS.value,
        TablePath.EXECUTION_EDGES.value,
        TablePath.ARTIFACT_EDGES.value,
        TablePath.CACHE_REUSE.value,
        TablePath.STEPS.value,
    ]
