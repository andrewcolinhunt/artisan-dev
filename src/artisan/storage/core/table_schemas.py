"""Polars schemas for framework Delta Lake tables.

Define ownerless staged schemas and centrally owned physical schemas for
Artisan's framework tables. Artifact content staging schemas remain owned by
their models; the storage layer adds internal commit ownership at persistence.

Key exports:
    FRAMEWORK_SCHEMAS: Registry mapping ``TablePath`` to ownerless schemas.
    get_physical_schema: Add internal commit ownership for Delta persistence.
    NON_PARTITIONED_TABLES: Tables not partitioned by origin_step_number.
    get_schema: Look up a schema by ``TablePath``.
    create_empty_dataframe: Build an empty DataFrame with the correct schema.
"""

from __future__ import annotations

import posixpath
from typing import Any

import polars as pl
from polars.datatypes import DataType, DataTypeClass

from artisan.errors import ApplicationRegistrationError, StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.projection_schemas import PROJECTION_TABLES_BY_PATH

EXECUTIONS_SCHEMA = {
    "execution_run_id": pl.String,
    "execution_spec_id": pl.String,
    "step_run_id": pl.String,
    "origin_step_number": pl.Int32,
    "operation_name": pl.String,
    "params": pl.String,
    "user_overrides": pl.String,
    "timestamp_start": pl.Datetime("us", "UTC"),
    "timestamp_end": pl.Datetime("us", "UTC"),
    "source_worker": pl.Int32,
    "compute_backend": pl.String,
    "success": pl.Boolean,
    "error": pl.String,
    "error_envelope": pl.String,
    "tool_output": pl.String,
    "worker_log": pl.String,
    "command_recording": pl.String,
    "replay_snapshot": pl.String,
    "replay_of_execution_run_id": pl.String,
    "metadata": pl.String,
}


EXECUTION_EDGES_SCHEMA = {
    "execution_run_id": pl.String,
    "direction": pl.String,
    "role": pl.String,
    "artifact_id": pl.String,
}


ARTIFACT_EDGES_SCHEMA = {
    "execution_run_id": pl.String,
    "source_artifact_id": pl.String,
    "target_artifact_id": pl.String,
    "source_artifact_type": pl.String,
    "target_artifact_type": pl.String,
    "source_role": pl.String,
    "target_role": pl.String,
    "group_id": pl.String,
    "step_boundary": pl.Boolean,
}


ARTIFACT_INDEX_SCHEMA = {
    "artifact_id": pl.String,
    "artifact_type": pl.String,
    "origin_step_number": pl.Int32,
    "metadata": pl.String,
}

ARTIFACT_LOCATIONS_SCHEMA = {
    "artifact_id": pl.String,
    "uri": pl.String,
}


CACHE_REUSE_SCHEMA = {
    "current_step_run_id": pl.String,
    "cached_execution_run_id": pl.String,
}


STEPS_SCHEMA: dict[str, DataType | DataTypeClass] = {
    "step_run_id": pl.String,
    "step_spec_id": pl.String,
    "pipeline_run_id": pl.String,
    "replay_of_execution_run_id": pl.String,
    "step_number": pl.Int32,
    "step_name": pl.String,
    "status": pl.String,
    "state_sequence": pl.UInt32,
    "disposition": pl.String,
    "cancellation_status": pl.String,
    "operation_class": pl.String,
    "params_json": pl.String,
    "input_refs_json": pl.String,
    "compute_backend": pl.String,
    "compute_options_json": pl.String,
    "output_roles_json": pl.String,
    "output_types_json": pl.String,
    "total_count": pl.Int32,
    "succeeded_count": pl.Int32,
    "failed_count": pl.Int32,
    "timestamp": pl.Datetime("us", "UTC"),
    "duration_seconds": pl.Float64,
    "error": pl.String,
    "metadata": pl.String,
}

LOGICAL_COMMITS_SCHEMA: dict[str, DataType | DataTypeClass] = {
    "logical_commit_id": pl.String,
    "commit_kind": pl.String,
    "step_run_id": pl.String,
    "recovery_batch_id": pl.String,
    "state": pl.String,
    "plan_digest": pl.String,
    "created_at": pl.Datetime("us", "UTC"),
    "completed_at": pl.Datetime("us", "UTC"),
    "abandon_reason": pl.String,
}


FRAMEWORK_SCHEMAS: dict[TablePath, dict[str, Any]] = {
    TablePath.EXECUTIONS: EXECUTIONS_SCHEMA,
    TablePath.EXECUTION_EDGES: EXECUTION_EDGES_SCHEMA,
    TablePath.ARTIFACT_EDGES: ARTIFACT_EDGES_SCHEMA,
    TablePath.ARTIFACT_INDEX: ARTIFACT_INDEX_SCHEMA,
    TablePath.ARTIFACT_LOCATIONS: ARTIFACT_LOCATIONS_SCHEMA,
    TablePath.CACHE_REUSE: CACHE_REUSE_SCHEMA,
    TablePath.LOGICAL_COMMITS: LOGICAL_COMMITS_SCHEMA,
    TablePath.STEPS: STEPS_SCHEMA,
}

COMMIT_OWNED_TABLES: frozenset[TablePath] = frozenset(
    {
        TablePath.ARTIFACT_INDEX,
        TablePath.ARTIFACT_LOCATIONS,
        TablePath.EXECUTIONS,
        TablePath.EXECUTION_EDGES,
        TablePath.ARTIFACT_EDGES,
        TablePath.STEPS,
    }
)

NATURAL_KEYS: dict[TablePath, tuple[str, ...]] = {
    TablePath.ARTIFACT_INDEX: ("artifact_id",),
    TablePath.ARTIFACT_LOCATIONS: ("artifact_id", "uri"),
    TablePath.EXECUTIONS: ("execution_run_id",),
    TablePath.EXECUTION_EDGES: (
        "execution_run_id",
        "direction",
        "role",
        "artifact_id",
    ),
    TablePath.ARTIFACT_EDGES: tuple(ARTIFACT_EDGES_SCHEMA),
    TablePath.CACHE_REUSE: (
        "current_step_run_id",
        "cached_execution_run_id",
    ),
    TablePath.STEPS: ("step_run_id", "state_sequence"),
    TablePath.LOGICAL_COMMITS: ("logical_commit_id",),
}

NON_PARTITIONED_TABLES: frozenset[TablePath] = frozenset(
    {
        TablePath.ARTIFACT_INDEX,
        TablePath.ARTIFACT_LOCATIONS,
        TablePath.ARTIFACT_EDGES,
        TablePath.EXECUTION_EDGES,
        TablePath.CACHE_REUSE,
        TablePath.LOGICAL_COMMITS,
        TablePath.STEPS,
    }
)


def get_schema(table: TablePath) -> dict[str, Any]:
    """Return the Polars schema dict for a framework table.

    Args:
        table: Identifies which framework table schema to retrieve.

    Returns:
        Mapping of column names to Polars data types.

    Raises:
        KeyError: If ``table`` is not a registered framework table.
    """
    if table not in FRAMEWORK_SCHEMAS:
        msg = f"Unknown table: {table}. Valid tables: {list(FRAMEWORK_SCHEMAS.keys())}"
        raise KeyError(msg)
    return FRAMEWORK_SCHEMAS[table]


def create_empty_dataframe(table: TablePath) -> pl.DataFrame:
    """Create an empty DataFrame matching a framework table schema.

    Args:
        table: Identifies which framework table schema to use.

    Returns:
        Empty Polars DataFrame with the correct column types.
    """
    schema = get_schema(table)
    return pl.DataFrame(schema=schema)


def get_physical_schema(table: TablePath) -> dict[str, Any]:
    """Return the Delta schema, including internal commit ownership."""
    schema = {
        name: pl.Int32 if dtype == pl.UInt32 else dtype
        for name, dtype in get_schema(table).items()
    }
    if table in COMMIT_OWNED_TABLES:
        schema["logical_commit_id"] = pl.String
    return schema


def get_physical_schema_for_path(table: str | TablePath) -> dict[str, Any]:
    """Return the physical schema for a registered table path."""
    if isinstance(table, TablePath):
        return get_physical_schema(table)
    try:
        return get_physical_schema(TablePath(table))
    except ValueError:
        if candidate := PROJECTION_TABLES_BY_PATH.get(table):
            return {**candidate.schema, "logical_commit_id": pl.String}
        from artisan.schemas.artifact.registry import ArtifactTypeDef

        for definition in ArtifactTypeDef.get_all().values():
            if definition.table_path == table:
                return {**definition.polars_schema(), "logical_commit_id": pl.String}
        if (
            not table.startswith("artifacts/")
            or any(part in {"", ".", ".."} for part in table.split("/"))
            or posixpath.normpath(table) != table
            or "\\" in table
        ):
            msg = "Invalid artifact table path"
            raise StoreIntegrityError(msg) from None
        msg = f"No installed artifact definition is registered for table {table!r}"
        raise ApplicationRegistrationError(msg) from None


def get_natural_key(table: str | TablePath) -> tuple[str, ...]:
    """Return the exact retry key for a framework or artifact table."""
    if not isinstance(table, TablePath):
        try:
            table = TablePath(table)
        except ValueError:
            if candidate := PROJECTION_TABLES_BY_PATH.get(table):
                return candidate.natural_key
            return ("artifact_id",)
    return NATURAL_KEYS[table]


def is_global_artifact_table(table_path: str) -> bool:
    """Return whether a table holds artifact rows reusable across commits."""
    return table_path.startswith("artifacts/")
