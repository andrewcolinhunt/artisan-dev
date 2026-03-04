"""Artisan storage layer for Delta Lake persistence."""

from __future__ import annotations

from artisan.storage.cache.cache_lookup import (
    CacheHit,
    CacheMiss,
    cache_lookup,
)
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
    FRAMEWORK_SCHEMAS,
    NON_PARTITIONED_TABLES,
    STEPS_SCHEMA,
    get_schema,
)
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.staging import StagingArea, StagingManager

__all__ = [
    # Schemas
    "ARTIFACT_EDGES_SCHEMA",
    "ARTIFACT_INDEX_SCHEMA",
    "EXECUTIONS_SCHEMA",
    "EXECUTION_EDGES_SCHEMA",
    "FRAMEWORK_SCHEMAS",
    "NON_PARTITIONED_TABLES",
    "STEPS_SCHEMA",
    "get_schema",
    # Stores
    "ArtifactStore",
    # Staging
    "StagingArea",
    "StagingManager",
    # Commit
    "DeltaCommitter",
    # Cache lookup
    "CacheHit",
    "CacheMiss",
    "cache_lookup",
]
