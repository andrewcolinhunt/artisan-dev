"""Stage orchestrator effects and clean completed logical-commit evidence.

Workers seal Parquet files under execution shards in
``{step_number}_{operation_name}/{id[:2]}/{id[2:4]}/{execution_run_id}/``.
Orchestrator effects use the same step directory followed by
``_orchestrator/{step_run_id}/{commit_kind}/``. Commit plans select exact
files from these directories; cleanup follows only completed plans.
"""

from __future__ import annotations

import posixpath
import re

import polars as pl
from fsspec import AbstractFileSystem

from artisan.storage.core.table_schemas import CACHE_REUSE_SCHEMA
from artisan.utils.path import step_dir_name

_HEX_ID = re.compile(r"[0-9a-f]{32}")


class StagingManager:
    """Stage orchestrator tables and clean directories named by completed plans.

    Attributes:
        staging_dir: Root staging URI/path shared by workers and the orchestrator.
    """

    def __init__(self, staging_dir: str, fs: AbstractFileSystem) -> None:
        """Initialize with the root staging directory.

        Args:
            staging_dir: Root URI/path containing execution and orchestrator staging.
            fs: Filesystem implementation (LocalFileSystem, S3FileSystem, etc.).
        """
        self.staging_dir = staging_dir
        self._fs = fs

    def stage_cache_reuse(
        self,
        current_step_run_id: str,
        cached_execution_run_ids: set[str] | list[str],
        *,
        step_number: int,
        operation_name: str,
    ) -> str | None:
        """Stage sorted, deduplicated cache-reuse pairs for one step attempt.

        Args:
            current_step_run_id: Current run-owned logical step identifier.
            cached_execution_run_ids: Existing execution identifiers accepted
                from cache.
            step_number: Current logical step number.
            operation_name: Current operation name used in the staging path.

        Returns:
            Staged Parquet URI, or None when no execution IDs were supplied.

        Raises:
            ValueError: If either identifier is not lowercase 32-character hex.
        """
        execution_ids = sorted(set(cached_execution_run_ids))
        if not execution_ids:
            return None
        _require_hex_id(current_step_run_id, "current_step_run_id")
        for execution_id in execution_ids:
            _require_hex_id(execution_id, "cached_execution_run_id")

        df = pl.DataFrame(
            {
                "current_step_run_id": [current_step_run_id] * len(execution_ids),
                "cached_execution_run_id": execution_ids,
            },
            schema=CACHE_REUSE_SCHEMA,
        )
        step_dir = step_dir_name(step_number, operation_name)
        orchestrator_dir = (
            f"{self.staging_dir}/{step_dir}/_orchestrator/{current_step_run_id}"
            "/step_result"
        )
        self._fs.makedirs(orchestrator_dir, exist_ok=True)
        parquet_uri = f"{orchestrator_dir}/cache_reuse.parquet"
        if self._fs.exists(parquet_uri):
            with self._fs.open(parquet_uri, "rb") as stream:
                existing = pl.read_parquet(stream)
            df = pl.concat([existing, df], rechunk=True).unique(
                subset=list(CACHE_REUSE_SCHEMA), maintain_order=True
            )
        with self._fs.open(parquet_uri, "wb") as stream:
            df.write_parquet(stream, compression="zstd")
        return parquet_uri

    def stage_orchestrator_dataframe(
        self,
        df: pl.DataFrame,
        table_path: str,
        *,
        commit_kind: str,
        step_run_id: str,
        step_number: int,
        operation_name: str,
    ) -> str | None:
        """Stage one ownerless table inside the exact logical-commit directory."""
        if commit_kind not in {"step_result", "input_registration"}:
            msg = f"Unknown logical commit kind {commit_kind!r}"
            raise ValueError(msg)
        if df.is_empty():
            return None
        if "logical_commit_id" in df.columns:
            msg = "Staged rows must not carry logical_commit_id"
            raise ValueError(msg)
        orchestrator_dir = (
            f"{self.staging_dir}/{step_dir_name(step_number, operation_name)}"
            f"/_orchestrator/{step_run_id}/{commit_kind}"
        )
        self._fs.makedirs(orchestrator_dir, exist_ok=True)
        table_name = table_path.rsplit("/", 1)[-1]
        parquet_uri = f"{orchestrator_dir}/{table_name}.parquet"
        if self._fs.exists(parquet_uri):
            with self._fs.open(parquet_uri, "rb") as stream:
                existing = pl.read_parquet(stream)
            if not existing.equals(df, null_equal=True):
                msg = f"Conflicting staged retry for {table_path}"
                raise ValueError(msg)
            return parquet_uri
        with self._fs.open(parquet_uri, "wb") as stream:
            df.write_parquet(stream, compression="zstd")
        return parquet_uri

    def cleanup_plan(self, relative_paths: list[str]) -> None:
        """Delete only staging directories named by a completed plan."""
        directories = {
            posixpath.dirname(relative_path) for relative_path in relative_paths
        }
        for relative_dir in sorted(directories, reverse=True):
            directory = f"{self.staging_dir}/{relative_dir}"
            if self._fs.exists(directory):
                self._fs.rm(directory, recursive=True)


def _require_hex_id(value: str, field: str) -> None:
    """Require the occurrence-ID representation shared by reuse relations."""
    if _HEX_ID.fullmatch(value) is None:
        msg = f"{field} must be a 32-character lowercase hexadecimal ID"
        raise ValueError(msg)
