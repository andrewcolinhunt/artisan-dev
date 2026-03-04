"""Worker staging directory for Parquet files.

Workers write Parquet files here instead of directly to Delta Lake,
avoiding transaction conflicts on shared filesystems. The orchestrator
later commits staged files via ``DeltaCommitter`` (see ``commit.py``).

Staging directory layout::

    staging_dir/
        {step_number}/
            {hash[0:2]}/{hash[2:4]}/{execution_run_id}/
                data.parquet
                metrics.parquet
                ...
"""

from __future__ import annotations

import logging
import os
import shutil
import uuid
from pathlib import Path

import polars as pl

from artisan.utils.path import step_dir_name

logger = logging.getLogger(__name__)


class StagingArea:
    """Per-worker staging area for writing Parquet files.

    Attributes:
        staging_dir (Path): Root staging directory.
        batch_id (str): Unique identifier for this batch of writes.
    """

    def __init__(
        self,
        staging_dir: Path | str,
        batch_id: str | None = None,
        worker_id: int = 0,
    ):
        """Initialize a staging area for one batch of writes.

        Args:
            staging_dir: Root directory for staging files.
            batch_id: Unique batch identifier. Generated from
                ``worker_id`` and a UUID fragment when None.
            worker_id: Numeric worker identifier embedded in the
                auto-generated ``batch_id``.
        """
        self.staging_dir = Path(staging_dir)
        self.worker_id = worker_id

        if batch_id is None:
            # Generate unique batch ID including worker and timestamp
            self.batch_id = f"w{worker_id}_{uuid.uuid4().hex[:12]}"
        else:
            self.batch_id = batch_id

        # Create batch directory
        self._batch_dir = self.staging_dir / self.batch_id
        self._batch_dir.mkdir(parents=True, exist_ok=True)

        # Track what has been staged
        self._staged_tables: set[str] = set()

    @property
    def batch_dir(self) -> Path:
        """Batch-specific staging directory."""
        return self._batch_dir

    def stage_dataframe(self, df: pl.DataFrame, table_name: str) -> Path:
        """Write a DataFrame to a staged Parquet file.

        If a file for ``table_name`` was already staged in this batch,
        the new rows are concatenated with the existing file.

        Args:
            df: Data to stage. An empty DataFrame is a no-op that
                returns the expected path without writing.
            table_name: Target Delta table name (used as the filename
                stem, e.g. ``"data"`` becomes ``data.parquet``).

        Returns:
            Path to the staged Parquet file.
        """
        if df.is_empty():
            return self._batch_dir / f"{table_name}.parquet"

        parquet_path = self._batch_dir / f"{table_name}.parquet"

        # If we already staged this table, append by reading existing and concat
        # Note: rechunk=True ensures contiguous memory after concat (behavior
        # changed in Polars v0.20.26 where rechunk=False became the default)
        if table_name in self._staged_tables and parquet_path.exists():
            existing = pl.read_parquet(parquet_path)
            df = pl.concat([existing, df], rechunk=True)

        # Use zstd compression for good compression ratio and performance
        df.write_parquet(parquet_path, compression="zstd")
        self._staged_tables.add(table_name)

        return parquet_path

    def stage_artifacts(self, artifacts_by_table: dict[str, pl.DataFrame]) -> None:
        """Stage multiple artifact DataFrames in one call.

        Args:
            artifacts_by_table: Mapping of table name to DataFrame.
                Empty DataFrames are skipped.
        """
        for table_name, df in artifacts_by_table.items():
            if not df.is_empty():
                self.stage_dataframe(df, table_name)

    def get_staged_file(self, table_name: str) -> Path | None:
        """Return the path to a previously staged Parquet file.

        Args:
            table_name: Delta table name to look up.

        Returns:
            Path to the staged file, or None if nothing was staged
            for this table or the file no longer exists.
        """
        if table_name not in self._staged_tables:
            return None

        parquet_path = self._batch_dir / f"{table_name}.parquet"
        if parquet_path.exists():
            return parquet_path

        return None

    def list_staged_tables(self) -> list[str]:
        """Return table names that have staged Parquet files."""
        return list(self._staged_tables)

    def cleanup(self) -> None:
        """Remove this batch's staging directory and reset state."""
        if self._batch_dir.exists():
            shutil.rmtree(self._batch_dir)
        self._staged_tables.clear()

    def __enter__(self) -> StagingArea:
        """Enter the staging context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the staging context, preserving files for debugging on error."""
        if exc_type is None:
            # No exception - cleanup is typically handled by orchestrator
            # after commit, so we don't auto-cleanup here
            pass
        # On exception, leave staging files for debugging


class StagingManager:
    """Discover and read staged Parquet files from multiple workers.

    Attributes:
        staging_dir (Path): Root staging directory containing worker
            batch subdirectories.
    """

    def __init__(self, staging_dir: Path | str):
        """Initialize with the root staging directory.

        Args:
            staging_dir: Directory containing per-worker batch
                subdirectories.
        """
        self.staging_dir = Path(staging_dir)

    def list_batch_ids(self) -> list[str]:
        """Return batch IDs present in the staging directory."""
        if not self.staging_dir.exists():
            return []

        return [
            d.name
            for d in self.staging_dir.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ]

    @staticmethod
    def _invalidate_nfs_cache(directory: Path) -> None:
        """Force NFS to refresh its directory entry cache."""
        try:
            os.listdir(directory)
        except (FileNotFoundError, PermissionError, OSError):
            pass

    def get_staged_files_for_table(
        self,
        table_name: str,
        step_number: int | None = None,
        operation_name: str | None = None,
    ) -> list[Path]:
        """Collect staged Parquet files for a table across all batches.

        Supports both flat (``{batch_id}/{table}.parquet``) and sharded
        (``{step}/{hash_prefix}/{run_id}/{table}.parquet``) layouts.

        Args:
            table_name: Delta table name (filename stem to glob for).
            step_number: Restrict the search to this step directory.
                None searches the entire staging tree.
            operation_name: Human-readable step directory suffix used
                alongside ``step_number``.

        Returns:
            Paths to matching Parquet files. Empty list when the
            staging directory does not exist.
        """
        # Invalidate NFS directory cache so rglob() sees files from workers
        self._invalidate_nfs_cache(self.staging_dir)

        if not self.staging_dir.exists():
            return []

        if step_number is not None:
            if operation_name is not None:
                step_dir = self.staging_dir / step_dir_name(step_number, operation_name)
            else:
                step_dir = self.staging_dir / str(step_number)
            self._invalidate_nfs_cache(step_dir)
            if not step_dir.exists():
                return []
            return list(step_dir.rglob(f"{table_name}.parquet"))

        # Backward compatible: search all directories
        return list(self.staging_dir.rglob(f"{table_name}.parquet"))

    def read_all_staged_for_table(
        self,
        table_name: str,
        step_number: int | None = None,
        operation_name: str | None = None,
    ) -> pl.DataFrame | None:
        """Read and concatenate all staged Parquet files for a table.

        Corrupted files are logged and skipped rather than raising.

        Args:
            table_name: Delta table name to collect files for.
            step_number: Restrict to files within this step directory.
                None reads from all directories.
            operation_name: Human-readable step directory suffix used
                alongside ``step_number``.

        Returns:
            Combined DataFrame, or None when no readable files exist.
        """
        files = self.get_staged_files_for_table(
            table_name, step_number=step_number, operation_name=operation_name
        )
        if not files:
            return None

        # rechunk=True ensures contiguous memory (default changed in Polars v0.20.26)
        dfs = []
        for f in files:
            try:
                dfs.append(pl.read_parquet(f))
            except Exception as exc:
                logger.warning(
                    "Skipping corrupted staging file %s: %s: %s",
                    f,
                    type(exc).__name__,
                    exc,
                )
        if not dfs:
            return None
        return pl.concat(dfs, rechunk=True)

    def cleanup_batch(self, batch_id: str) -> None:
        """Remove a batch's staging directory.

        Args:
            batch_id: Batch directory to delete. No-op if missing.
        """
        batch_dir = self.staging_dir / batch_id
        if batch_dir.exists():
            shutil.rmtree(batch_dir)

    def cleanup_step(self, step_number: int, operation_name: str | None = None) -> None:
        """Remove all staging directories for a given step.

        Args:
            step_number: Step whose staging tree to delete.
            operation_name: Human-readable step directory suffix, if
                the step directory uses the ``{number}_{name}`` format.
        """
        if operation_name is not None:
            step_dir = self.staging_dir / step_dir_name(step_number, operation_name)
        else:
            step_dir = self.staging_dir / str(step_number)
        if step_dir.exists():
            shutil.rmtree(step_dir)

    def cleanup_all(self) -> None:
        """Remove every batch directory under the staging root."""
        for batch_id in self.list_batch_ids():
            self.cleanup_batch(batch_id)
