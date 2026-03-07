"""Commit staged Parquet files to Delta Lake tables.

The orchestrator is the single writer; workers stage Parquet files (see
``staging.py``) and ``DeltaCommitter`` merges them into Delta tables
with content-addressed deduplication, partitioning, and optional
compaction/vacuum.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl
from deltalake import DeltaTable, WriterProperties

logger = logging.getLogger(__name__)

from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import (
    FRAMEWORK_SCHEMAS,
    NON_PARTITIONED_TABLES,
)
from artisan.storage.io.staging import StagingManager

# Default writer properties for Delta Lake writes
# Using zstd compression for good compression ratio and performance
DEFAULT_WRITER_PROPERTIES = WriterProperties(compression="ZSTD")


def _to_str(table: str) -> str:
    """Coerce a TablePath enum or plain string to its string value."""
    return table.value if hasattr(table, "value") else table


def _table_name_from_path(table_path: str) -> str:
    """Extract table name (last segment) from a table path string."""
    return Path(table_path).name


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
        delta_base_path (Path): Root directory for Delta Lake tables.
        staging_manager (StagingManager): Manages staged Parquet files.
    """

    def __init__(self, delta_base_path: Path | str, staging_dir: Path | str):
        """Initialize with Delta Lake root and staging directories.

        Args:
            delta_base_path: Root directory for Delta Lake tables.
            staging_dir: Root directory containing worker-staged
                Parquet files.
        """
        self.delta_base_path = Path(delta_base_path)
        self.staging_manager = StagingManager(staging_dir)

    def _table_path(self, table: str) -> Path:
        """Resolve the filesystem path for a Delta table."""
        return self.delta_base_path / table

    def _is_non_partitioned(self, table: str) -> bool:
        """Check if a table should not be partitioned."""
        for npt in NON_PARTITIONED_TABLES:
            if _to_str(npt) == _to_str(table):
                return True
        return False

    def _has_artifact_id(self, table: str) -> bool:
        """Check if the table supports artifact_id deduplication."""
        if _to_str(table) == _to_str(TablePath.EXECUTION_EDGES):
            return False
        return True

    # -------------------------------------------------------------------------
    # Commit operations
    # -------------------------------------------------------------------------

    def commit_table(
        self,
        table: str,
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
        table_name = _table_name_from_path(_to_str(table))

        # Read all staged files for this table
        staged_df = self.staging_manager.read_all_staged_for_table(
            table_name, step_number=step_number, operation_name=operation_name
        )
        if staged_df is None or staged_df.is_empty():
            return 0

        table_path = self._table_path(_to_str(table))

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

        # Write to Delta Lake with zstd compression
        if table_path.exists():
            # Append to existing table (schema_mode=merge for column evolution)
            staged_df.write_delta(
                str(table_path),
                mode="append",
                delta_write_options={
                    "writer_properties": DEFAULT_WRITER_PROPERTIES,
                    "schema_mode": "merge",
                },
            )
        else:
            # Create new table with partitioning
            write_opts = {"writer_properties": DEFAULT_WRITER_PROPERTIES}
            if partition_by:
                write_opts["partition_by"] = partition_by
            staged_df.write_delta(
                str(table_path),
                mode="overwrite",
                delta_write_options=write_opts,
            )

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
                commit.
            step_number: Restrict to staged files from this step
                directory. None commits from all directories.
            operation_name: Human-readable step directory suffix used
                alongside ``step_number``.

        Returns:
            Mapping of table name to rows committed. Tables with zero
            rows are omitted. A ``_failed_tables`` key is present when
            any table commit raised an exception.
        """
        results = {}
        failed_tables = []

        for table in _get_commit_order():
            table_name = _table_name_from_path(_to_str(table))
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
                failed_tables.append((table_name, str(exc)))

        if failed_tables:
            results["_failed_tables"] = failed_tables

        if results:
            parts = [
                f"{name}={count}"
                for name, count in results.items()
                if not name.startswith("_")
            ]
            if parts:
                logger.debug("Step %d commit: %s", step_number or 0, ", ".join(parts))

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

        Args:
            preserve_staging: Keep staging files after commit instead
                of cleaning them up.

        Returns:
            Mapping of table name to rows committed. Empty dict when
            no leftover staging files are found.
        """
        if not self.staging_manager.staging_dir.exists():
            return {}

        probe = self.staging_manager.get_staged_files_for_table("executions")
        if not probe:
            return {}

        logger.debug(
            "Staged recovery: found %d leftover execution file(s), committing...",
            len(probe),
        )

        results = self.commit_all_tables(
            cleanup_staging=not preserve_staging,
            step_number=None,
        )

        committed = {k: v for k, v in results.items() if not k.startswith("_")}
        if committed:
            parts = [f"{name}={count}" for name, count in committed.items()]
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
        results = {}
        batch_dir = self.staging_manager.staging_dir / batch_id

        if not batch_dir.exists():
            return results

        for table in _get_commit_order():
            table_name = _table_name_from_path(_to_str(table))
            parquet_path = batch_dir / f"{table_name}.parquet"
            if parquet_path.exists():
                df = pl.read_parquet(parquet_path)
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

    def _deduplicate_artifacts(
        self, df: pl.DataFrame, table_path: Path
    ) -> pl.DataFrame:
        """Remove rows whose artifact_id already exists in Delta."""
        if not table_path.exists():
            return df

        existing_ids = pl.scan_delta(str(table_path)).select("artifact_id").collect()

        if existing_ids.is_empty():
            return df

        return df.join(existing_ids, on="artifact_id", how="anti")

    def commit_dataframe(
        self,
        df: pl.DataFrame,
        table: str,
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
        table_path = self._table_path(_to_str(table))

        if deduplicate and "artifact_id" in df.columns and self._has_artifact_id(table):
            df = self._deduplicate_artifacts(df, table_path)
            if df.is_empty():
                return 0

        partition_by = (
            ["origin_step_number"] if not self._is_non_partitioned(table) else None
        )

        if table_path.exists():
            df.write_delta(
                str(table_path),
                mode="append",
                delta_write_options={
                    "writer_properties": DEFAULT_WRITER_PROPERTIES,
                    "schema_mode": "merge",
                },
            )
        else:
            write_opts = {"writer_properties": DEFAULT_WRITER_PROPERTIES}
            if partition_by:
                write_opts["partition_by"] = partition_by
            df.write_delta(
                str(table_path),
                mode="overwrite",
                delta_write_options=write_opts,
            )

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
            table_path = self._table_path(_to_str(table))
            if not table_path.exists():
                empty_df = pl.DataFrame(schema=schema)
                if self._is_non_partitioned(table):
                    partition_by = None
                else:
                    partition_by = ["origin_step_number"]
                write_opts = {"writer_properties": DEFAULT_WRITER_PROPERTIES}
                if partition_by:
                    write_opts["partition_by"] = partition_by
                empty_df.write_delta(
                    str(table_path),
                    mode="overwrite",
                    delta_write_options=write_opts,
                )

        # Initialize artifact content tables from registry
        for type_def in ArtifactTypeDef.get_all().values():
            table_path = self._table_path(type_def.table_path)
            if not table_path.exists():
                empty_df = pl.DataFrame(schema=type_def.polars_schema())
                write_opts = {
                    "writer_properties": DEFAULT_WRITER_PROPERTIES,
                    "partition_by": ["origin_step_number"],
                }
                empty_df.write_delta(
                    str(table_path),
                    mode="overwrite",
                    delta_write_options=write_opts,
                )

    def compact_table(
        self,
        table: str,
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
        table_path = self._table_path(_to_str(table))
        if not table_path.exists():
            return {"files_added": 0, "files_removed": 0}

        dt = DeltaTable(str(table_path))

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
        zorder_config[_to_str(TablePath.EXECUTIONS)] = ["execution_spec_id"]
        zorder_config[_to_str(TablePath.ARTIFACT_INDEX)] = ["artifact_id"]
        zorder_config[_to_str(TablePath.ARTIFACT_EDGES)] = [
            "source_artifact_id",
            "target_artifact_id",
        ]
        zorder_config[_to_str(TablePath.EXECUTION_EDGES)] = ["execution_run_id"]
        zorder_config[_to_str(TablePath.STEPS)] = ["step_spec_id"]

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

    def vacuum_table(self, table: str, retention_hours: int = 168) -> None:
        """Remove stale data files from a Delta table.

        Args:
            table: Table path string or ``TablePath`` member.
            retention_hours: Keep files newer than this threshold.
                Defaults to 168 (7 days).
        """
        table_path = self._table_path(_to_str(table))
        if not table_path.exists():
            return

        dt = DeltaTable(str(table_path))
        dt.vacuum(retention_hours=retention_hours, enforce_retention_duration=False)
