"""Core storage models and artifact persistence APIs."""

from __future__ import annotations

from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
    FRAMEWORK_SCHEMAS,
    NON_PARTITIONED_TABLES,
    STEPS_SCHEMA,
    create_empty_dataframe,
    get_schema,
)

__all__ = [
    "ArtifactStore",
    "EXECUTIONS_SCHEMA",
    "EXECUTION_EDGES_SCHEMA",
    "ARTIFACT_EDGES_SCHEMA",
    "ARTIFACT_INDEX_SCHEMA",
    "STEPS_SCHEMA",
    "FRAMEWORK_SCHEMAS",
    "NON_PARTITIONED_TABLES",
    "get_schema",
    "create_empty_dataframe",
]
