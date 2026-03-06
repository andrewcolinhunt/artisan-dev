"""Polars schemas for framework Delta Lake tables.

Define column schemas for the five framework tables: executions,
execution_edges, artifact_edges, artifact_index, and steps.  Artifact
content table schemas are owned by their respective models via
``ArtifactTypeDef``.

Key exports:
    FRAMEWORK_SCHEMAS: Registry mapping ``TablePath`` to schema dicts.
    NON_PARTITIONED_TABLES: Tables not partitioned by origin_step_number.
    get_schema: Look up a schema by ``TablePath``.
    create_empty_dataframe: Build an empty DataFrame with the correct schema.
"""

from __future__ import annotations

import polars as pl

from artisan.schemas.enums import TablePath

# =============================================================================
# executions table
# =============================================================================
# Lightweight execution log with success/error/timestamps.
# Input/output edges have been moved to execution_edges table.
#
#
# Semantic shift: executions stores execution metadata only,
# while execution_edges stores normalized input/output edges.

# Note on nullability: Polars DataFrames allow null values by default for all
# column types. Fields like timestamp_end, error, and metadata may contain nulls.
# This aligns with Phase 1 models where these fields are Optional.

EXECUTIONS_SCHEMA = {
    "execution_run_id": pl.String,  # PK - unique per execution attempt
    "execution_spec_id": pl.String,  # Deterministic ID for caching (indexed)
    "origin_step_number": pl.Int32,  # Partition key
    "operation_name": pl.String,  # From OperationDefinition.name
    "params": pl.String,  # JSON - full instantiated params
    "user_overrides": pl.String,  # JSON - original user-provided overrides
    "timestamp_start": pl.Datetime("us", "UTC"),  # Execution start (microseconds, UTC)
    "timestamp_end": pl.Datetime(
        "us", "UTC"
    ),  # Execution end (microseconds, UTC); nullable
    "source_worker": pl.Int32,  # Worker ID
    "compute_backend": pl.String,  # local or slurm
    "success": pl.Boolean,  # Whether execution succeeded (row-level)
    "error": pl.String,  # Error message if failed (row-level)
    "tool_output": pl.String,  # Captured stdout+stderr from external command
    "worker_log": pl.String,  # SLURM job or worker stdout+stderr
    "metadata": pl.String,  # JSON - additional data
}

# =============================================================================
# execution_edges table
# =============================================================================
# Normalized input/output edges for execution provenance.
# One row per edge (input or output artifact).
#
# Benefits over arrays in executions:
# - No batching logic needed - one row per edge, Parquet handles the rest
# - Clean normalized schema - no arrays in cells
# - Easy queries - WHERE execution_run_id = ? AND direction = 'output'
# - Scales naturally - 1M edges = 1M rows (Parquet optimized for this)
# - Works for both creator and curator ops - same table structure

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
# Reference: design_provenance_model_v3.md
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
    "step_boundary": pl.Boolean,  # True = crosses step boundary, False = chain-internal
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

# =============================================================================
# steps table
# =============================================================================
# Append-only event log of step state transitions.
# Each step produces two rows: one at start (status=running)
# and one at end (status=completed or failed).
# Not partitioned (small table, few rows per step per run).
# Written directly by StepTracker, not through staging path.

STEPS_SCHEMA = {
    "step_run_id": pl.String,
    "step_spec_id": pl.String,
    "pipeline_run_id": pl.String,
    "step_number": pl.Int32,
    "step_name": pl.String,
    "status": pl.String,
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
    "dispatch_error": pl.String,
    "commit_error": pl.String,
    "metadata": pl.String,
}

# =============================================================================
# Framework Schema Registry
# =============================================================================
# Mapping from TablePath to schema for framework tables only.
# Artifact content table schemas are managed by ArtifactTypeDef.

FRAMEWORK_SCHEMAS: dict[TablePath, dict[str, pl.DataType]] = {
    TablePath.EXECUTIONS: EXECUTIONS_SCHEMA,
    TablePath.EXECUTION_EDGES: EXECUTION_EDGES_SCHEMA,
    TablePath.ARTIFACT_EDGES: ARTIFACT_EDGES_SCHEMA,
    TablePath.ARTIFACT_INDEX: ARTIFACT_INDEX_SCHEMA,
    TablePath.STEPS: STEPS_SCHEMA,
}

# Tables that are NOT partitioned by origin_step_number
# Used by commit.py to avoid setting partition_by for these tables
NON_PARTITIONED_TABLES: frozenset[TablePath] = frozenset(
    {
        TablePath.ARTIFACT_INDEX,
        TablePath.ARTIFACT_EDGES,
        TablePath.EXECUTION_EDGES,
        TablePath.STEPS,
    }
)


def get_schema(table: TablePath) -> dict[str, pl.DataType]:
    """Return the Polars schema dict for a framework table.

    Args:
        table: Identifies which framework table schema to retrieve.

    Returns:
        Mapping of column names to Polars data types.

    Raises:
        KeyError: If ``table`` is not a registered framework table.
    """
    if table not in FRAMEWORK_SCHEMAS:
        raise KeyError(
            f"Unknown table: {table}. "
            f"Valid tables: {list(FRAMEWORK_SCHEMAS.keys())}"
        )
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
