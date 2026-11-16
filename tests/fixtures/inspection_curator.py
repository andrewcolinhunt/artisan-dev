"""Sealed curator execution evidence for inspection field and ordering tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl

from artisan.execution.recording.parquet_writer import _stage_execution
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.curator_staging import read_staged_curator_result
from artisan.storage.core.curator_view import capture_curator_view
from artisan.storage.core.table_schemas import EXECUTION_EDGES_SCHEMA
from artisan.utils.path import shard_uri
from fixtures.curator_cache import DynamicCurator
from fixtures.retained_inspection import inspection_store


def commit_curator_record(
    root: Path,
    *,
    run_id: str = "run1",
    number: int = 0,
    name: str | None = None,
    success: bool = False,
    error: str | None = "fixture failure",
    envelope: dict[str, Any] | None = None,
    worker_log: str | None = None,
    commands: CommandRecording | None = None,
    timestamp: datetime | None = None,
) -> str:
    """Commit exact closed evidence through admission, worker seal and terminal plan."""
    committer = inspection_store(root)
    operation = DynamicCurator(
        params=DynamicCurator.Params(empty=True, fail=not success)
    )
    coordinator = CuratorResults(
        RetainedStepTracker(committer, run_id), f"{root}/staging"
    )
    record = StepStartRecord(
        step_run_id=uuid4().hex,
        step_number=number,
        step_name=name or operation.name,
        operation_class=f"{type(operation).__module__}.{type(operation).__qualname__}",
        params_json=operation.params.model_dump_json(),
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json=json.dumps({"dynamic_output_roles": True}),
        output_roles_json="[]",
        output_types_json="{}",
    )
    admission = CuratorAdmission(
        record=record,
        operation_name=operation.name,
        operation_version=operation.version,
        view=capture_curator_view(committer),
        inputs=(),
    )
    reference = coordinator.admit(admission)
    execution = uuid4().hex
    now = timestamp or datetime.now(UTC)
    staging_path = shard_uri(
        coordinator.staging.staging_dir,
        execution,
        step_number=number,
        operation_name=operation.name,
    )
    committer.fs.makedirs(staging_path, exist_ok=True)
    _stage_execution(
        execution,
        admission.digest,
        operation.name,
        number,
        pl.DataFrame(schema=EXECUTION_EDGES_SCHEMA),
        staging_path,
        committer.fs,
        success,
        None if success else error,
        now,
        now,
        0,
        operation.params.model_dump(),
        "local",
        commands or CommandRecording.empty(),
        ReplaySnapshot.unavailable("inspection_fixture"),
        None,
        worker_log=worker_log,
        step_run_id=record.step_run_id,
        error_envelope=envelope,
    )
    terminal = read_staged_curator_result(
        admission,
        execution,
        staging_root=coordinator.staging.staging_dir,
        committer=committer,
    )
    coordinator.finish(reference, execution, terminal)
    return execution
