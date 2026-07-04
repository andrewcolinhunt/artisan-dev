"""Commit staged Parquet files to Delta Lake tables.

The orchestrator is the single writer; workers stage Parquet files (see
``staging.py``) and ``DeltaCommitter`` merges them into Delta tables
with content-addressed deduplication, partitioning, and optional
compaction/vacuum.
"""

from __future__ import annotations

import logging
from typing import Any

import polars as pl
from deltalake import DeltaTable, WriterProperties
from fsspec import AbstractFileSystem

from artisan.errors import CommitError
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    FRAMEWORK_SCHEMAS,
    NON_PARTITIONED_TABLES,
)
from artisan.storage.io.staging import StagingManager
from artisan.utils.path import uri_join

logger = logging.getLogger(__name__)

# Default writer properties for Delta Lake writes
# Using zstd compression for good compression ratio and performance
DEFAULT_WRITER_PROPERTIES = WriterProperties(compression="ZSTD")


def _normalize_table(table: str | TablePath) -> str:
    """Coerce a table identifier to its plain string path."""
    return table.value if isinstance(table, TablePath) else table


def _table_name_from_path(table_path: str) -> str:
    """Extract table name (last segment) from a table path string."""
    return table_path.rsplit("/", 1)[-1]


def _get_commit_order() -> list[str]:
    """Build the table commit order from the current artifact registry.

    Order ensures referential integrity on partial failure: content
    tables first, then index, then provenance edges, then executions.
    """
    artifact_paths = [td.table_path for td in ArtifactTypeDef.get_all().values()]
    framework_paths = [
        TablePath.ARTIFACT_INDEX.value,
        TablePath.ARTIFACT_EDGES.value,
        TablePath.EXECUTION_EDGES.value,
        TablePath.EXECUTIONS.value,
    ]
    return [*artifact_paths, *framework_paths]


class DeltaCommitter:
    """Commit staged Parquet files to Delta Lake tables.

    Attributes:
        delta_base_path: Root URI/path for Delta Lake tables.
        staging_manager: Manages staged Parquet files.
    """

    def __init__(
        self,
        delta_base_path: str,
        staging_manager: StagingManager,
        *,
        fs: AbstractFileSystem,
        storage_options: dict[str, str] | None = None,
    ):
        """Initialize with Delta Lake root and a staging manager.

        Args:
            delta_base_path: Root URI/path for Delta Lake tables.
            staging_manager: Pre-constructed staging manager instance.
            fs: Filesystem implementation (LocalFileSystem, S3FileSystem, etc.).
            storage_options: Credentials/config passed to delta-rs calls.
        """
        self.delta_base_path = delta_base_path
        self.staging_manager = staging_manager
        self._fs = fs
        self._storage_options = storage_options or {}

    def _table_path(self, table: str) -> str:
        """Resolve the URI for a Delta table."""
        return uri_join(self.delta_base_path, table)

    def _is_non_partitioned(self, table: str) -> bool:
        """Check if a table should not be partitioned."""
        return table in NON_PARTITIONED_TABLES

    def _has_artifact_id(self, table: str) -> bool:
        """Check if the table supports artifact_id deduplication."""
        return table != TablePath.EXECUTION_EDGES.value

    # -------------------------------------------------------------------------
    # Commit operations
    # -------------------------------------------------------------------------

    def commit_table(
        self,
        table: str | TablePath,
        deduplicate: bool = True,
        partition_by: list[str] | None = None,
        step_number: int | None = None,
        operation_name: str | None = None,
    ) -> int:
        """Commit staged Parquet data for one table to Delta Lake.

        Args:
            table: Table path (e.g. ``"artifacts/data"`` or a
                ``TablePath`` member).
            deduplicate: Skip rows whose ``artifact_id`` already
                exists in the target table.
            partition_by: Partition columns. Defaults to
                ``["origin_step_number"]`` for partitioned tables.
            step_number: Restrict to staged files from this step
                directory. None commits from all directories.
            operation_name: Human-readable step directory suffix used
                alongside ``step_number``.

        Returns:
            Number of rows written. Zero when nothing was staged or
            all rows were deduplicated.
        """
        table = _normalize_table(table)
        table_name = _table_name_from_path(table)

        # Read all staged files for this table
        staged_df = self.staging_manager.read_all_staged_for_table(
            table_name, step_number=step_number, operation_name=operation_name
        )
        if staged_df is None or staged_df.is_empty():
            return 0

        table_path = self._table_path(table)

        # Default partition: all tables use origin_step_number except
        # non-partitioned tables
        if partition_by is None:
            if self._is_non_partitioned(table):
                partition_by = None
            else:
                partition_by = ["origin_step_number"]

        # Handle deduplication for artifact tables
        if (
            deduplicate
            and "artifact_id" in staged_df.columns
            and self._has_artifact_id(table)
        ):
            staged_df = self._deduplicate_artifacts(staged_df, table_path)
            if staged_df.is_empty():
                return 0

        self._write_df(staged_df, table_path, partition_by=partition_by)

        return staged_df.shape[0]

    def commit_all_tables(
        self,
        cleanup_staging: bool = True,
        step_number: int | None = None,
        operation_name: str | None = None,
    ) -> dict[str, int]:
        """Commit all staged data across every table to Delta Lake.

        Each table is committed independently; Delta Lake does not
        support multi-table transactions. The commit order (content
        tables, index, edges, executions) minimises referential
        integrity issues on partial failure.

        Args:
            cleanup_staging: Remove staging files after a successful
                commit. Ignored when any table fails — staging is
                preserved so the commit can be retried.
            step_number: Restrict to staged files from this step
                directory. None commits from all directories.
            operation_name: Human-readable step directory suffix used
                alongside ``step_number``.

        Returns:
            Mapping of table name to rows committed. Tables with zero
            rows are omitted.

        Raises:
            CommitError: If any table's commit failed. Earlier tables
                may already be committed; staging is preserved so the
                commit can be safely retried (``recover_staged`` dedups
                via anti-join).
        """
        results: dict[str, int] = {}
        failed_tables: list[str] = []
        first_error: Exception | None = None

        for table in _get_commit_order():
            table_name = _table_name_from_path(table)
            try:
                rows_committed = self.commit_table(
                    table,
                    step_number=step_number,
                    operation_name=operation_name,
                )
                if rows_committed > 0:
                    results[table_name] = rows_committed
            except Exception as exc:
                logger.error(
                    "Failed to commit table %s: %s: %s",
                    table_name,
                    type(exc).__name__,
                    exc,
                )
                failed_tables.append(table_name)
                if first_error is None:
                    first_error = exc

        if results:
            parts = [f"{name}={count}" for name, count in results.items()]
            logger.debug("Step %d commit: %s", step_number or 0, ", ".join(parts))

        # A partial failure leaves the store inconsistent and the staged
        # Parquet is the only recovery source, so never clean up staging
        # when a table failed — recover_staged re-commits it idempotently.
        if failed_tables:
            raise CommitError(failed_tables) from first_error

        if cleanup_staging:
            if step_number is not None:
                self.staging_manager.cleanup_step(
                    step_number, operation_name=operation_name
                )
            else:
                self.staging_manager.cleanup_all()

        return results

    def recover_staged(self, *, preserve_staging: bool = False) -> dict[str, int]:
        """Commit leftover staging files from a prior crashed run.

        Idempotent: content-addressed deduplication skips rows that
        already exist in Delta.

        Recovery is best-effort: a partial commit failure is logged and
        swallowed rather than raised, because this runs at pipeline
        startup against debris from a *prior* run — raising would block
        every subsequent start on the same bad file. The failed tables'
        staging is preserved, so a later recovery can retry.

        Args:
            preserve_staging: Keep staging files after commit instead
                of cleaning them up.

        Returns:
            Mapping of table name to rows committed. Empty dict when
            no leftover staging files are found or recovery failed.
        """
        if not self._fs.exists(self.staging_manager.staging_dir):
            return {}

        probe = self.staging_manager.get_staged_files_for_table("executions")
        if not probe:
            return {}

        logger.debug(
            "Staged recovery: found %d leftover execution file(s), committing...",
            len(probe),
        )

        try:
            results = self.commit_all_tables(
                cleanup_staging=not preserve_staging,
                step_number=None,
            )
        except CommitError as exc:
            logger.error(
                "Staged recovery failed (%s); staging preserved for retry", exc
            )
            return {}

        if results:
            parts = [f"{name}={count}" for name, count in results.items()]
            logger.debug("Staged recovery committed: %s", ", ".join(parts))
        else:
            logger.debug("Staged recovery: no new rows to commit")

        return results

    def commit_batch(self, batch_id: str, cleanup_after: bool = True) -> dict[str, int]:
        """Commit a single staging batch to Delta Lake.

        Args:
            batch_id: Identifies the batch subdirectory to commit.
            cleanup_after: Remove the batch staging directory after a
                successful commit.

        Returns:
            Mapping of table name to rows committed. Tables with zero
            rows are omitted.
        """
        results: dict[str, int] = {}
        batch_dir = f"{self.staging_manager.staging_dir}/{batch_id}"

        if not self._fs.exists(batch_dir):
            return results

        for table in _get_commit_order():
            table_name = _table_name_from_path(table)
            parquet_uri = f"{batch_dir}/{table_name}.parquet"
            if self._fs.exists(parquet_uri):
                with self._fs.open(parquet_uri, "rb") as f:
                    df = pl.read_parquet(f)
                if not df.is_empty():
                    rows = self.commit_dataframe(df, table)
                    if rows > 0:
                        results[table_name] = rows

        if cleanup_after:
            self.staging_manager.cleanup_batch(batch_id)

        return results

    # -------------------------------------------------------------------------
    # Helper methods
    # -------------------------------------------------------------------------

    def _deduplicate_artifacts(self, df: pl.DataFrame, table_path: str) -> pl.DataFrame:
        """Remove rows whose artifact_id already exists in Delta."""
        if not self._fs.exists(table_path):
            return df

        existing_ids = (
            pl.scan_delta(table_path, storage_options=self._storage_options)
            .select("artifact_id")
            .collect()
        )

        if existing_ids.is_empty():
            return df

        return df.join(existing_ids, on="artifact_id", how="anti")

    def _write_df(
        self,
        df: pl.DataFrame,
        table_path: str,
        partition_by: list[str] | None = None,
    ) -> None:
        """Append to an existing Delta table or create it via overwrite.

        Existing tables are appended with ``schema_mode="merge"`` so new
        columns are tolerated; a new table is created with the given
        partitioning. All writes use zstd compression.

        Args:
            df: Rows to write.
            table_path: Resolved Delta table URI.
            partition_by: Partition columns applied only when creating
                the table. Ignored on append — partitioning is fixed at
                table creation.
        """
        if self._fs.exists(table_path):
            df.write_delta(
                table_path,
                mode="append",
                delta_write_options={
                    "writer_properties": DEFAULT_WRITER_PROPERTIES,
                    "schema_mode": "merge",
                },
                storage_options=self._storage_options,
            )
        else:
            write_opts: dict[str, Any] = {
                "writer_properties": DEFAULT_WRITER_PROPERTIES
            }
            if partition_by:
                write_opts["partition_by"] = partition_by
            df.write_delta(
                table_path,
                mode="overwrite",
                delta_write_options=write_opts,
                storage_options=self._storage_options,
            )

    def commit_dataframe(
        self,
        df: pl.DataFrame,
        table: str | TablePath,
        deduplicate: bool = True,
    ) -> int:
        """Write a single DataFrame directly to a Delta table.

        Args:
            df: Data to write (appended to existing table or creates
                a new one).
            table: Target table path string or ``TablePath`` member.
            deduplicate: Skip rows whose ``artifact_id`` already
                exists in the target table.

        Returns:
            Number of rows written. Zero when the DataFrame is empty
            or all rows were deduplicated.
        """
        table = _normalize_table(table)
        table_path = self._table_path(table)

        if deduplicate and "artifact_id" in df.columns and self._has_artifact_id(table):
            df = self._deduplicate_artifacts(df, table_path)
            if df.is_empty():
                return 0

        partition_by = (
            ["origin_step_number"] if not self._is_non_partitioned(table) else None
        )

        self._write_df(df, table_path, partition_by=partition_by)

        return df.shape[0]

    # -------------------------------------------------------------------------
    # Table management
    # -------------------------------------------------------------------------

    def initialize_tables(self) -> None:
        """Create empty Delta tables for all framework and artifact types.

        Skip tables that already exist. Useful for bootstrapping a new
        pipeline database.
        """
        # Initialize framework tables
        for table, schema in FRAMEWORK_SCHEMAS.items():
            table_str = _normalize_table(table)
            table_path = self._table_path(table_str)
            if not self._fs.exists(table_path):
                empty_df = pl.DataFrame(schema=schema)
                partition_by = (
                    None
                    if self._is_non_partitioned(table_str)
                    else ["origin_step_number"]
                )
                self._write_df(empty_df, table_path, partition_by=partition_by)

        # Initialize artifact content tables from registry
        for type_def in ArtifactTypeDef.get_all().values():
            table_path = self._table_path(type_def.table_path)
            if not self._fs.exists(table_path):
                empty_df = pl.DataFrame(schema=type_def.polars_schema())
                self._write_df(
                    empty_df, table_path, partition_by=["origin_step_number"]
                )

    def compact_table(
        self,
        table: str | TablePath,
        z_order_columns: list[str] | None = None,
        step_number: int | None = None,
    ) -> dict[str, int]:
        """Compact a Delta table, optionally applying Z-ORDER clustering.

        Args:
            table: Table path string or ``TablePath`` member.
            z_order_columns: Columns to cluster by. None performs a
                simple file compaction without ordering.
            step_number: Restrict compaction to this partition. None
                compacts the entire table.

        Returns:
            Dict with ``files_added`` and ``files_removed`` counts.
        """
        table = _normalize_table(table)
        table_path = self._table_path(table)
        if not self._fs.exists(table_path):
            return {"files_added": 0, "files_removed": 0}

        dt = DeltaTable(table_path, storage_options=self._storage_options)

        partition_filters = None
        if step_number is not None and not self._is_non_partitioned(table):
            partition_filters = [("origin_step_number", "=", str(step_number))]

        if z_order_columns:
            result = dt.optimize.z_order(
                columns=z_order_columns,
                partition_filters=partition_filters,
            )
        else:
            result = dt.optimize.compact(partition_filters=partition_filters)

        return {
            "files_added": result.get("numFilesAdded", 0),
            "files_removed": result.get("numFilesRemoved", 0),
        }

    def compact_all_tables(
        self,
        z_order: bool = True,
        step_number: int | None = None,
    ) -> dict[str, dict[str, int]]:
        """Compact every Delta table, optionally with Z-ORDER clustering.

        Args:
            z_order: Apply Z-ORDER clustering on each table's key
                columns during compaction.
            step_number: Restrict compaction to this partition. None
                compacts all partitions.

        Returns:
            Mapping of table name to compaction statistics. Tables
            with no file changes are omitted.
        """
        results = {}

        # Build Z-ORDER config from registry + framework tables
        zorder_config: dict[str, list[str]] = {}

        # Artifact tables from registry
        for type_def in ArtifactTypeDef.get_all().values():
            zorder_config[type_def.table_path] = ["artifact_id"]

        # Framework tables
        zorder_config[TablePath.EXECUTIONS.value] = ["execution_spec_id"]
        zorder_config[TablePath.ARTIFACT_INDEX.value] = ["artifact_id"]
        zorder_config[TablePath.ARTIFACT_EDGES.value] = [
            "source_artifact_id",
            "target_artifact_id",
        ]
        zorder_config[TablePath.EXECUTION_EDGES.value] = ["execution_run_id"]
        zorder_config[TablePath.STEPS.value] = ["step_spec_id"]

        for table, z_order_cols in zorder_config.items():
            table_name = _table_name_from_path(table)
            stats = self.compact_table(
                table,
                z_order_columns=z_order_cols if z_order else None,
                step_number=step_number,
            )
            if stats["files_added"] > 0 or stats["files_removed"] > 0:
                results[table_name] = stats

        return results

    def vacuum_table(self, table: str | TablePath, retention_hours: int = 168) -> None:
        """Remove stale data files from a Delta table.

        Args:
            table: Table path string or ``TablePath`` member.
            retention_hours: Keep files newer than this threshold.
                Defaults to 168 (7 days).
        """
        table_path = self._table_path(_normalize_table(table))
        if not self._fs.exists(table_path):
            return

        dt = DeltaTable(table_path, storage_options=self._storage_options)
        dt.vacuum(retention_hours=retention_hours, enforce_retention_duration=False)
