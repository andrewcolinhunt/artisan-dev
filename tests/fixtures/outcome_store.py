"""Committed, identity-valid metric populations for outcome and comparison tests."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.storage.core.table_schemas import (
    ARTIFACT_INDEX_SCHEMA,
    CACHE_REUSE_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
    STEPS_SCHEMA,
)
from artisan.utils.hashing import digest_utf8
from artisan.visualization.outcomes import OutcomeSelection
from fixtures.logical_commit_store import commit_test_step


def append_metric_attempt(
    root: Path,
    artifacts: list[Artifact],
    *,
    pipeline_run_id: str = "run",
    step_name: str = "score",
    step_number: int = 0,
    role: str = "metrics",
    declared_type: str | None = "metric",
    status: str = "succeeded",
    step_run_id: str | None = None,
    reuse_execution_ids: list[str] | None = None,
) -> OutcomeSelection:
    """Append one attempt; supplied artifacts become successful output evidence."""
    step_run_id = step_run_id or uuid.uuid4().hex
    execution_id = digest_utf8(f"{step_run_id}:execution")
    now = datetime.now(UTC)
    usable = status in {"succeeded", "partial"}
    terminal = dict.fromkeys(STEPS_SCHEMA)
    terminal.update(
        pipeline_run_id=pipeline_run_id,
        step_run_id=step_run_id,
        step_number=step_number,
        step_name=step_name,
        step_spec_id=digest_utf8(f"{step_run_id}:spec"),
        status=status,
        state_sequence=2,
        operation_class="fixtures.Score",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json="{}",
        output_roles_json=json.dumps([role] if usable else []),
        output_types_json=json.dumps({role: declared_type} if usable else {}),
        disposition=("cache_hit" if reuse_execution_ids else "executed")
        if usable
        else None,
        total_count=2 if status == "partial" else 1,
        succeeded_count=1 if usable else 0,
        failed_count=1 if status in {"failed", "partial"} else 0,
        timestamp=now,
        duration_seconds=1.0,
        metadata="{}",
        error="fixture failure" if status == "failed" else None,
    )
    tables: dict[str, pl.DataFrame] = {}
    if usable:
        tables = _artifact_tables(artifacts)
        if reuse_execution_ids:
            tables[TablePath.CACHE_REUSE.value] = pl.DataFrame(
                [
                    {
                        "current_step_run_id": step_run_id,
                        "cached_execution_run_id": value,
                    }
                    for value in reuse_execution_ids
                ],
                schema=CACHE_REUSE_SCHEMA,
            )
        else:
            execution = dict.fromkeys(EXECUTIONS_SCHEMA)
            execution.update(
                execution_run_id=execution_id,
                execution_spec_id=digest_utf8(f"{execution_id}:spec"),
                step_run_id=step_run_id,
                origin_step_number=step_number,
                operation_name=step_name,
                params="{}",
                user_overrides="{}",
                timestamp_start=now,
                timestamp_end=now,
                source_worker=0,
                compute_backend="local",
                success=True,
                metadata="{}",
                command_recording=CommandRecording.empty().model_dump_json(),
                replay_snapshot=ReplaySnapshot.unavailable(
                    "outcome_fixture"
                ).model_dump_json(),
            )
            tables[TablePath.EXECUTIONS.value] = pl.DataFrame(
                [execution], schema=EXECUTIONS_SCHEMA
            )
            tables[TablePath.EXECUTION_EDGES.value] = pl.DataFrame(
                [
                    {
                        "execution_run_id": execution_id,
                        "direction": "output",
                        "role": role,
                        "artifact_id": artifact.artifact_id,
                    }
                    for artifact in artifacts
                ],
                schema=EXECUTION_EDGES_SCHEMA,
            )
    commit_test_step(root, root.parent / "outcome-staging", [terminal], tables)
    return OutcomeSelection(
        pipeline_run_id=pipeline_run_id,
        step_name=step_name,
        role=role,
        step_number=step_number,
        step_run_id=step_run_id,
    )


def _artifact_tables(artifacts: list[Artifact]) -> dict[str, pl.DataFrame]:
    tables: dict[str, pl.DataFrame] = {}
    for artifact_type in {artifact.artifact_type for artifact in artifacts}:
        selected = [item for item in artifacts if item.artifact_type == artifact_type]
        tables[ArtifactTypeDef.get_table_path(artifact_type)] = pl.DataFrame(
            [item.to_row() for item in selected], schema=type(selected[0]).POLARS_SCHEMA
        )
    if artifacts:
        tables[TablePath.ARTIFACT_INDEX.value] = pl.DataFrame(
            [
                {
                    "artifact_id": item.artifact_id,
                    "artifact_type": item.artifact_type,
                    "origin_step_number": item.origin_step_number,
                    "metadata": json.dumps(item.metadata),
                }
                for item in artifacts
            ],
            schema=ARTIFACT_INDEX_SCHEMA,
        )
    return tables
