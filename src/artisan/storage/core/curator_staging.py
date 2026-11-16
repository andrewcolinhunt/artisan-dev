"""Derive curator terminal summaries from closed worker evidence."""

from __future__ import annotations

import json
import re

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.core.curator_result import (
    curator_result_summary,
    dynamic_output_roles,
)
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.core.table_schemas import get_schema
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.worker_seal import CapturedWorkerFiles, capture_worker_files
from artisan.utils.path import shard_uri


def read_staged_curator_result(
    admission: CuratorAdmission,
    execution_run_id: str,
    *,
    staging_root: str,
    committer: RetainedCommitter,
    duration_seconds: float | None = None,
) -> StepResult:
    """Summarize one sealed invocation; acceptance still verifies all table effects.

    The transport supplies only an execution locator. Its success flag, output
    IDs and staging path do not authorize the controller's terminal result.
    """
    if re.fullmatch(r"[0-9a-f]{32}", execution_run_id) is None:
        msg = "Curator delivery requires a valid execution identity"
        raise StoreIntegrityError(msg)
    record = admission.record
    path = shard_uri(
        staging_root,
        execution_run_id,
        step_number=record.step_number,
        operation_name=admission.operation_name,
    )
    with capture_worker_files(path, committer.fs) as captured:
        executions = _frame(captured, TablePath.EXECUTIONS)
        execution = executions.row(0, named=True)
        expected = {
            "execution_run_id": execution_run_id,
            "execution_spec_id": admission.digest,
            "step_run_id": record.step_run_id,
            "origin_step_number": record.step_number,
            "operation_name": admission.operation_name,
            "replay_of_execution_run_id": record.replay_of_execution_run_id,
        }
        if any(execution[name] != value for name, value in expected.items()):
            msg = "Curator execution seal differs from its admitted owner"
            raise StoreIntegrityError(msg)
        success = execution["success"]
        output_types: dict[str, str | None]
        if success:
            frames = _summary_frames(captured, executions, admission, committer)
            actual_types, metadata = curator_result_summary(frames, {execution_run_id})
            output_types = (
                dict(actual_types)
                if dynamic_output_roles(record.compute_options_json)
                else json.loads(record.output_types_json)
            )
        else:
            output_types, metadata = {}, {}
        return StepResult(
            step_run_id=record.step_run_id,
            step_number=record.step_number,
            step_name=record.step_name,
            status=StepStatus.SUCCEEDED if success else StepStatus.FAILED,
            disposition=StepDisposition.EXECUTED if success else None,
            total_count=1,
            succeeded_count=int(success),
            failed_count=int(not success),
            output_roles=frozenset(output_types),
            output_types=output_types,
            metadata=metadata,
            error=None if success else execution["error"] or "Curator execution failed",
            duration_seconds=duration_seconds,
        )


def _frame(captured: CapturedWorkerFiles, table: TablePath) -> pl.DataFrame:
    filename = f"{table.table_name}.parquet"
    if filename not in captured.files:
        return pl.DataFrame(schema=get_schema(table))
    if captured.files[filename].schema != get_schema(table):
        msg = f"Curator staged {table.value} has an invalid schema"
        raise StoreIntegrityError(msg)
    columns = {
        TablePath.ARTIFACT_INDEX: ["artifact_id", "artifact_type"],
        TablePath.EXECUTION_EDGES: [
            "execution_run_id",
            "direction",
            "role",
            "artifact_id",
        ],
    }.get(table)
    return captured.read_frame(filename, columns)


def _summary_frames(
    captured: CapturedWorkerFiles,
    executions: pl.DataFrame,
    admission: CuratorAdmission,
    committer: RetainedCommitter,
) -> dict[str, pl.DataFrame]:
    edges = _frame(captured, TablePath.EXECUTION_EDGES)
    selected = edges.filter(pl.col("direction") == "output")["artifact_id"].unique()
    view = CuratorView(committer, admission.view)
    existing = (
        view.scan(TablePath.ARTIFACT_INDEX)
        .filter(pl.col("artifact_id").is_in(selected))
        .select("artifact_id", "artifact_type")
        .collect()
    )
    staged = _frame(captured, TablePath.ARTIFACT_INDEX).select(
        "artifact_id", "artifact_type"
    )
    return {
        TablePath.EXECUTIONS.value: executions,
        TablePath.EXECUTION_EDGES.value: edges,
        TablePath.ARTIFACT_INDEX.value: pl.concat([existing, staged]).unique(),
    }
