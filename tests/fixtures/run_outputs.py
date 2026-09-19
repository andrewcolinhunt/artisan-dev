"""Seed accepted outputs through the real lifecycle and logical commit boundary."""

from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

import polars as pl
from fsspec import AbstractFileSystem

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import EXECUTIONS_SCHEMA, get_schema
from fixtures.logical_commit_store import commit_test_step


def commit_outputs(
    delta_root: str,
    *,
    run_id: str = "source-run",
    number: int = 0,
    artifacts: Sequence[Artifact] = (),
    output_ids: dict[str, list[str]] | None = None,
    status: str = "succeeded",
    execution_success: bool = True,
    failed_output_ids: Sequence[str] = (),
    reused_execution_id: str | None = None,
    fs: AbstractFileSystem | None = None,
    storage_options: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Commit one attempt with typed artifacts, output edges, or cache reuse."""
    step_id, execution_id = uuid.uuid4().hex, uuid.uuid4().hex
    outputs = (
        output_ids
        if output_ids is not None
        else {"output": [artifact.artifact_id for artifact in artifacts]}
    )
    now = datetime.now(UTC)
    terminal = {
        "step_run_id": step_id,
        "step_spec_id": "f" * 32,
        "pipeline_run_id": run_id,
        "step_number": number,
        "step_name": f"step-{number}",
        "status": status,
        "state_sequence": 2,
        "disposition": "executed" if status in {"succeeded", "partial"} else None,
        "operation_class": "example.Operation",
        "params_json": "{}",
        "input_refs_json": "{}",
        "compute_backend": "local",
        "compute_options_json": "{}",
        "output_roles_json": json.dumps(
            list(outputs) if status in {"succeeded", "partial"} else []
        ),
        "output_types_json": "{}",
        "total_count": 1 + bool(failed_output_ids),
        "succeeded_count": int(execution_success),
        "failed_count": int(not execution_success) + bool(failed_output_ids),
        "timestamp": now,
        "duration_seconds": 1.0,
        "error": "fixture failure" if status == "failed" else None,
    }
    tables: dict[str, pl.DataFrame] = {}
    if status in {"succeeded", "partial", "failed"}:
        execution = {
            "execution_run_id": execution_id,
            "execution_spec_id": "e" * 32,
            "step_run_id": step_id,
            "origin_step_number": number,
            "operation_name": "fixture",
            "params": "{}",
            "user_overrides": "{}",
            "timestamp_start": now,
            "timestamp_end": now,
            "source_worker": 0,
            "compute_backend": "local",
            "success": execution_success,
            "metadata": "{}",
        }
        if reused_execution_id is None:
            execution_rows = [execution]
            failed_id = uuid.uuid4().hex
            if failed_output_ids:
                execution_rows.append(
                    {
                        **execution,
                        "execution_run_id": failed_id,
                        "success": False,
                        "error": "fixture failure",
                    }
                )
            tables[TablePath.EXECUTIONS.value] = pl.DataFrame(
                execution_rows, schema=EXECUTIONS_SCHEMA
            )
            edges = [
                {
                    "execution_run_id": execution_id,
                    "direction": "output",
                    "role": role,
                    "artifact_id": artifact_id,
                }
                for role, ids in outputs.items()
                for artifact_id in ids
            ]
            edges.extend(
                {
                    "execution_run_id": failed_id,
                    "direction": "output",
                    "role": "failed",
                    "artifact_id": artifact_id,
                }
                for artifact_id in failed_output_ids
            )
            if edges:
                tables[TablePath.EXECUTION_EDGES.value] = pl.DataFrame(
                    edges, schema=get_schema(TablePath.EXECUTION_EDGES)
                )
        else:
            tables[TablePath.CACHE_REUSE.value] = pl.DataFrame(
                [
                    {
                        "current_step_run_id": step_id,
                        "cached_execution_run_id": reused_execution_id,
                    }
                ],
                schema=get_schema(TablePath.CACHE_REUSE),
            )
    for type_key in {artifact.artifact_type for artifact in artifacts}:
        type_def = ArtifactTypeDef.get(type_key)
        tables[type_def.table_path] = pl.DataFrame(
            [
                artifact.to_row()
                for artifact in artifacts
                if artifact.artifact_type == type_key
            ],
            schema=type_def.polars_schema(),
        )
    if artifacts:
        tables[TablePath.ARTIFACT_INDEX.value] = pl.DataFrame(
            [
                {
                    "artifact_id": artifact.artifact_id,
                    "artifact_type": artifact.artifact_type,
                    "origin_step_number": artifact.origin_step_number,
                    "metadata": json.dumps(artifact.metadata),
                }
                for artifact in artifacts
            ],
            schema=get_schema(TablePath.ARTIFACT_INDEX),
        )
        locations = [
            {
                "artifact_id": artifact.artifact_id,
                "uri": getattr(artifact, next(iter(artifact.LOCATOR_FIELDS))),
            }
            for artifact in artifacts
            if artifact.EXTERNALLY_BACKED
        ]
        if locations:
            tables[TablePath.ARTIFACT_LOCATIONS.value] = pl.DataFrame(
                locations, schema=get_schema(TablePath.ARTIFACT_LOCATIONS)
            )
    commit_test_step(
        delta_root,
        f"{delta_root}-staging",
        [terminal],
        tables,
        fs=fs,
        storage_options=storage_options,
    )
    return step_id, execution_id
