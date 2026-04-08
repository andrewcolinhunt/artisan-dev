"""Step-level state persistence via the steps delta table.

Handles step caching (``check_cache``), state recording
(``record_step_*``), and resume (``load_completed_steps``).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import polars as pl
from deltalake import WriterProperties
from fsspec import AbstractFileSystem

from artisan.schemas.enums import CachePolicy, TablePath
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.core.table_schemas import STEPS_SCHEMA
from artisan.utils.path import uri_join

WRITER_PROPS = WriterProperties(compression="ZSTD")


class StepTracker:
    """Read and write step-level state to the steps delta table."""

    def __init__(
        self,
        delta_root: str,
        pipeline_run_id: str = "",
        storage_options: dict[str, str] | None = None,
        fs: AbstractFileSystem | None = None,
    ) -> None:
        """Initialize with a delta root and optional run identifier.

        Args:
            delta_root: Root URI for Delta Lake tables.
            pipeline_run_id: Run identifier for this pipeline session.
            storage_options: Delta-rs storage options for cloud backends.
            fs: Filesystem implementation for path operations.
        """
        if fs is None:
            from fsspec.implementations.local import LocalFileSystem

            fs = LocalFileSystem()
        self._fs = fs
        self._steps_path = uri_join(delta_root, TablePath.STEPS)
        self._pipeline_run_id = pipeline_run_id
        self._storage_options = storage_options

    def check_cache(
        self,
        step_spec_id: str,
        cache_policy: CachePolicy = CachePolicy.ALL_SUCCEEDED,
    ) -> StepResult | None:
        """Check for a completed step with this spec_id.

        Args:
            step_spec_id: Deterministic hash for the step.
            cache_policy: Controls which completed steps qualify as cache hits.

        Returns:
            StepResult if a completed step exists, None otherwise.
        """
        if not self._fs.exists(self._steps_path):
            return None

        query = (
            pl.scan_delta(self._steps_path, storage_options=self._storage_options)
            .filter(pl.col("step_spec_id") == step_spec_id)
            .filter(pl.col("status") == "completed")
            .filter(pl.col("dispatch_error").is_null())
            .filter(pl.col("commit_error").is_null())
        )
        if cache_policy == CachePolicy.ALL_SUCCEEDED:
            query = query.filter(pl.col("failed_count") == 0)

        result = query.sort("timestamp", descending=True).limit(1).collect()

        if result.is_empty():
            return None

        return _row_to_step_result(result.row(0, named=True))

    def _base_row(self, record: StepStartRecord, status: str) -> dict:
        """Build the common row dict shared by all record_step_* methods."""
        return {
            "step_run_id": record.step_run_id,
            "step_spec_id": record.step_spec_id,
            "pipeline_run_id": self._pipeline_run_id,
            "step_number": record.step_number,
            "step_name": record.step_name,
            "status": status,
            "operation_class": record.operation_class,
            "params_json": record.params_json,
            "input_refs_json": record.input_refs_json,
            "compute_backend": record.compute_backend,
            "compute_options_json": record.compute_options_json,
            "output_roles_json": record.output_roles_json,
            "output_types_json": record.output_types_json,
            "total_count": None,
            "succeeded_count": None,
            "failed_count": None,
            "timestamp": datetime.now(UTC),
            "duration_seconds": None,
            "error": None,
            "dispatch_error": None,
            "commit_error": None,
            "metadata": None,
        }

    def record_step_start(self, record: StepStartRecord) -> None:
        """Write a 'running' row for a step that is about to execute.

        Args:
            record: StepStartRecord with all metadata fields.
        """
        row = self._base_row(record, "running")
        df = pl.DataFrame([row], schema=STEPS_SCHEMA)
        self._write_row(df)

    def record_step_completed(
        self,
        start_record: StepStartRecord,
        result: StepResult,
    ) -> None:
        """Write a 'completed' row after successful step execution.

        Args:
            start_record: The original StepStartRecord for metadata.
            result: The StepResult from execute_step().
        """
        row = self._base_row(start_record, "completed")
        row.update(
            output_roles_json=json.dumps(sorted(result.output_roles)),
            output_types_json=json.dumps(result.output_types),
            total_count=result.total_count,
            succeeded_count=result.succeeded_count,
            failed_count=result.failed_count,
            duration_seconds=result.duration_seconds,
            dispatch_error=(
                result.metadata.get("dispatch_error") if result.metadata else None
            ),
            commit_error=(
                result.metadata.get("commit_error") if result.metadata else None
            ),
            metadata=json.dumps(result.metadata) if result.metadata else None,
        )
        df = pl.DataFrame([row], schema=STEPS_SCHEMA)
        self._write_row(df)

    def record_step_skipped(
        self,
        start_record: StepStartRecord,
        result: StepResult,
    ) -> None:
        """Write a 'skipped' row for a step that had empty inputs.

        Skipped rows are excluded from cache lookups but included in
        ``load_completed_steps`` for resume.

        Args:
            start_record: The original StepStartRecord for metadata.
            result: The StepResult from execute_step().
        """
        row = self._base_row(start_record, "skipped")
        row.update(
            output_roles_json=json.dumps(sorted(result.output_roles)),
            output_types_json=json.dumps(result.output_types),
            total_count=result.total_count,
            succeeded_count=result.succeeded_count,
            failed_count=result.failed_count,
            duration_seconds=result.duration_seconds,
            metadata=json.dumps(result.metadata) if result.metadata else None,
        )
        df = pl.DataFrame([row], schema=STEPS_SCHEMA)
        self._write_row(df)

    def record_step_failed(
        self,
        start_record: StepStartRecord,
        error: str,
    ) -> None:
        """Write a 'failed' row after step failure.

        Args:
            start_record: The original StepStartRecord for metadata.
            error: Error message string.
        """
        row = self._base_row(start_record, "failed")
        row["error"] = error
        df = pl.DataFrame([row], schema=STEPS_SCHEMA)
        self._write_row(df)

    def record_step_cancelled(
        self,
        start_record: StepStartRecord,
        reason: str = "Pipeline cancelled",
    ) -> None:
        """Write a 'cancelled' row when a step is aborted mid-pipeline.

        Cancelled steps are excluded from both ``check_cache`` (filters
        ``status == "completed"``) and ``load_completed_steps`` (filters
        ``["completed", "skipped"]``).

        Args:
            start_record: The original StepStartRecord for metadata.
            reason: Human-readable cancellation reason.
        """
        row = self._base_row(start_record, "cancelled")
        row["error"] = reason
        df = pl.DataFrame([row], schema=STEPS_SCHEMA)
        self._write_row(df)

    def load_completed_steps(
        self, pipeline_run_id: str | None = None
    ) -> list[StepState]:
        """Load completed steps for a pipeline run.

        Args:
            pipeline_run_id: Run to load. If None, loads the most recent run.

        Returns:
            List of StepState objects ordered by step_number.
        """
        if not self._fs.exists(self._steps_path):
            return []

        lf = pl.scan_delta(
            self._steps_path, storage_options=self._storage_options
        ).filter(pl.col("status").is_in(["completed", "skipped"]))

        if pipeline_run_id is not None:
            lf = lf.filter(pl.col("pipeline_run_id") == pipeline_run_id)
        else:
            # Find the most recent pipeline_run_id
            latest = (
                pl.scan_delta(self._steps_path, storage_options=self._storage_options)
                .filter(pl.col("status").is_in(["completed", "skipped"]))
                .sort("timestamp", descending=True)
                .limit(1)
                .select("pipeline_run_id")
                .collect()
            )
            if latest.is_empty():
                return []
            run_id = latest.item(0, 0)
            lf = lf.filter(pl.col("pipeline_run_id") == run_id)

        # For each step_number, take the most recent completed row
        df = (
            lf.sort("timestamp", descending=True)
            .unique(subset=["step_number"], keep="first")
            .sort("step_number")
            .collect()
        )

        if df.is_empty():
            return []

        steps = []
        for row in df.iter_rows(named=True):
            steps.append(
                StepState(
                    pipeline_run_id=row["pipeline_run_id"],
                    step_run_id=row.get("step_run_id", ""),
                    step_number=row["step_number"],
                    step_name=row["step_name"],
                    step_spec_id=row["step_spec_id"],
                    status=row["status"],
                    operation_class=row["operation_class"],
                    params_json=row["params_json"],
                    input_refs_json=row["input_refs_json"],
                    compute_backend=row["compute_backend"],
                    compute_options_json=row["compute_options_json"],
                    total_count=row["total_count"],
                    succeeded_count=row["succeeded_count"],
                    failed_count=row["failed_count"],
                    duration_seconds=row["duration_seconds"],
                    output_roles=frozenset(json.loads(row["output_roles_json"])),
                    output_types=json.loads(row["output_types_json"]),
                )
            )
        return steps

    def list_runs(self) -> pl.DataFrame:
        """List all pipeline runs in the delta table.

        Returns:
            DataFrame with pipeline_run_id, step_count, last_status,
            min_timestamp, max_timestamp — one row per run.
            Empty DataFrame if no table exists.
        """
        if not self._fs.exists(self._steps_path):
            return pl.DataFrame(
                schema={
                    "pipeline_run_id": pl.String,
                    "step_count": pl.UInt32,
                    "last_status": pl.String,
                    "started_at": pl.Datetime("us", "UTC"),
                    "ended_at": pl.Datetime("us", "UTC"),
                }
            )

        return (
            pl.scan_delta(self._steps_path, storage_options=self._storage_options)
            .sort("timestamp")
            .group_by("pipeline_run_id")
            .agg(
                pl.col("step_number").n_unique().alias("step_count"),
                pl.col("status").last().alias("last_status"),
                pl.col("timestamp").first().alias("started_at"),
                pl.col("timestamp").last().alias("ended_at"),
            )
            .sort("started_at", descending=True)
            .collect()
        )

    def _write_row(self, df: pl.DataFrame) -> None:
        """Append a single-row DataFrame to the steps delta table."""
        if self._fs.exists(self._steps_path):
            df.write_delta(
                self._steps_path,
                mode="append",
                storage_options=self._storage_options,
                delta_write_options={"writer_properties": WRITER_PROPS},
            )
        else:
            df.write_delta(
                self._steps_path,
                mode="overwrite",
                storage_options=self._storage_options,
                delta_write_options={"writer_properties": WRITER_PROPS},
            )


def _row_to_step_result(row: dict) -> StepResult:
    """Reconstruct a StepResult from a steps delta table row."""
    return StepResult(
        step_name=row["step_name"],
        step_number=row["step_number"],
        success=row["failed_count"] == 0,
        total_count=row["total_count"],
        succeeded_count=row["succeeded_count"],
        failed_count=row["failed_count"],
        output_roles=frozenset(json.loads(row["output_roles_json"])),
        output_types=json.loads(row["output_types_json"]),
        duration_seconds=row["duration_seconds"],
        step_run_id=row.get("step_run_id"),
    )
