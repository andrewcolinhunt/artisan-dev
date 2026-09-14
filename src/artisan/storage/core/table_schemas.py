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

from typing import Any

import polars as pl
from polars.datatypes import DataType, DataTypeClass

from artisan.schemas.enums import TablePath

# =============================================================================
# executions table
# =============================================================================
# Lightweight execution log with success/error/timestamps. Stores
# execution metadata only; input/output edges live in the
# execution_edges table.

# Note on nullability: Polars DataFrames allow null values by default for all
# column types. Fields like timestamp_end, error, and metadata may contain nulls.

EXECUTIONS_SCHEMA = {
    "execution_run_id": pl.String,  # PK - unique per execution attempt
    "execution_spec_id": pl.String,  # Deterministic ID for caching (indexed)
    "step_run_id": pl.String,  # Links execution to step attempt (nullable)
    "origin_step_number": pl.Int32,  # Partition key
    "operation_name": pl.String,  # From OperationDefinition.name
    "params": pl.String,  # JSON - full instantiated params
    "user_overrides": pl.String,  # JSON - original user-provided overrides
    "timestamp_start": pl.Datetime("us", "UTC"),  # Execution start (microseconds, UTC)
    "timestamp_end": pl.Datetime(
        "us", "UTC"
    ),  # Execution end (microseconds, UTC); nullable
    "source_worker": pl.Int32,  # Worker ID
    "compute_backend": pl.String,  # Resolved step-runner name
    "success": pl.Boolean,  # Whether execution succeeded (row-level)
    "error": pl.String,  # Error message if failed (row-level)
    "error_envelope": pl.String,  # JSON - structured ArtisanError envelope (nullable)
    "tool_output": pl.String,  # Captured stdout+stderr from external command
    "worker_log": pl.String,  # Provider-captured worker stdout+stderr
    "metadata": pl.String,  # JSON - additional data
}

# =============================================================================
# execution_edges table
# =============================================================================
# Normalized input/output edges for execution provenance.
# One row per edge (input or output artifact), which keeps queries
# simple and avoids array columns in the executions table.

EXECUTION_EDGES_SCHEMA = {
    "execution_run_id": pl.String,  # FK to executions
    "direction": pl.String,  # "input" or "output"
    "role": pl.String,  # Role name
    "artifact_id": pl.String,  # Artifact ID
}

# =============================================================================
# artifact_edges table
# =============================================================================
# Directed source->target derivation edges for artifact provenance.
# This is the entity-centric provenance table (vs activity-centric executions).
#
# Terminology: Uses source/target (graph-centric) for artifact provenance,
# distinct from inputs/outputs (operation-centric) in executions.
#
# Multi-input grouping: Edges sharing the same group_id and target_artifact_id
# were co-inputs to a single derivation. group_id is null for independent
# (single-input) derivation.

ARTIFACT_EDGES_SCHEMA = {
    "execution_run_id": pl.String,  # Which execution established this edge
    "source_artifact_id": pl.String,  # Source artifact (derivation origin)
    "target_artifact_id": pl.String,  # Target artifact (derived)
    "source_artifact_type": pl.String,  # Artifact type of source (denormalized)
    "target_artifact_type": pl.String,  # Artifact type of target (denormalized)
    "source_role": pl.String,  # Role name of source artifact
    "target_role": pl.String,  # Role name of target artifact
    "group_id": pl.String,  # Links jointly-necessary input edges (nullable)
    "step_boundary": pl.Boolean,  # True = crosses step boundary, False = composite-internal
}

# =============================================================================
# artifact_index table
# =============================================================================
# Global registry resolving artifact_id -> type + location.
# Speed optimization for artifact lookups without scanning type-specific tables.

ARTIFACT_INDEX_SCHEMA = {
    "artifact_id": pl.String,  # PK
    "artifact_type": pl.String,  # data, metric, file_ref, config
    "origin_step_number": pl.Int32,  # Where produced
    "metadata": pl.String,  # JSON - additional data
}

ARTIFACT_LOCATIONS_SCHEMA = {
    "artifact_id": pl.String,
    "uri": pl.String,
}

# =============================================================================
# cache_reuse table
# =============================================================================
# Minimal relation between a current logical step and an execution accepted
# from cache. Every other fact is derived from steps, executions, and edges.

CACHE_REUSE_SCHEMA = {
    "current_step_run_id": pl.String,
    "cached_execution_run_id": pl.String,
}

# =============================================================================
# steps table
# =============================================================================
# Append-only event log of step state transitions.
# Snapshots are ordered by state_sequence. The unique latest authoritative
# snapshot is the lifecycle source of truth for one step attempt.
# Not partitioned (small table, few rows per step per run).
# Written directly by StepTracker, not through staging path.

STEPS_SCHEMA: dict[str, DataType | DataTypeClass] = {
    "step_run_id": pl.String,
    "step_spec_id": pl.String,
    "pipeline_run_id": pl.String,
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
    "state": pl.String,
    "plan_digest": pl.String,
    "created_at": pl.Datetime("us", "UTC"),
    "completed_at": pl.Datetime("us", "UTC"),
    "abandon_reason": pl.String,
}

# =============================================================================
# Framework Schema Registry
# =============================================================================
# Mapping from TablePath to schema for framework tables only.
# Artifact content table schemas are managed by ArtifactTypeDef.

# Schemas use Polars DataType classes (e.g. ``pl.String``) as well as
# instances (e.g. ``pl.Datetime(...)``), so the value type is widened to
# ``Any`` rather than ``pl.DataType``.
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

# Tables that are NOT partitioned by origin_step_number
# Used by commit.py to avoid setting partition_by for these tables
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
        from artisan.schemas.artifact.registry import ArtifactTypeDef

        type_def = next(
            definition
            for definition in ArtifactTypeDef.get_all().values()
            if definition.table_path == table
        )
        return {**type_def.polars_schema(), "logical_commit_id": pl.String}


def get_natural_key(table: str | TablePath) -> tuple[str, ...]:
    """Return the exact retry key for a framework or artifact table."""
    if not isinstance(table, TablePath):
        try:
            table = TablePath(table)
        except ValueError:
            return ("artifact_id",)
    return NATURAL_KEYS[table]
