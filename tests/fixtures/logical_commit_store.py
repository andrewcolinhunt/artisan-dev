"""Helpers for seeding tests through the format-2 commit boundary."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import polars as pl
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.table_schemas import STEPS_SCHEMA
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.commit_plan import build_commit_plan
from artisan.storage.io.staging import StagingManager


def commit_test_step(
    delta_root: Path | str,
    staging_root: Path | str,
    step_rows: list[dict[str, object]],
    tables: dict[str, pl.DataFrame],
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict[str, str] | None = None,
) -> None:
    """Persist one fixture step and its evidence as a complete logical commit."""
    fs = fs or LocalFileSystem()
    storage_options = storage_options or {}
    terminal = {**step_rows[-1], "state_sequence": 2}
    step_run_id = str(terminal["step_run_id"])
    step_spec_id = str(terminal["step_spec_id"])
    step_number = int(terminal["step_number"])
    operation_name = str(terminal["step_name"])
    DeltaCommitter(
        str(delta_root),
        StagingManager(str(staging_root), fs),
        fs=fs,
        storage_options=storage_options,
    ).initialize_tables()
    tracker = StepTracker(
        str(delta_root),
        str(terminal["pipeline_run_id"]),
        fs=fs,
        storage_options=storage_options,
    )
    tracker.create_attempt(
        StepStartRecord(
            step_run_id=step_run_id,
            step_spec_id=None,
            step_number=step_number,
            step_name=operation_name,
            operation_class=str(terminal["operation_class"]),
            params_json=str(terminal["params_json"]),
            input_refs_json=str(terminal["input_refs_json"]),
            compute_backend=str(terminal["compute_backend"]),
            compute_options_json=str(terminal["compute_options_json"]),
            output_roles_json=str(terminal["output_roles_json"]),
            output_types_json=str(terminal["output_types_json"]),
        )
    )
    target = StepStatus(str(terminal["status"]))
    if target == StepStatus.PENDING:
        return
    if target in {StepStatus.SKIPPED, StepStatus.CANCELLED}:
        if tables:
            msg = f"Direct {target.value} fixture cannot carry committed evidence"
            raise ValueError(msg)
        cancellation = None
        if target == StepStatus.CANCELLED:
            tracker.record_cancellation(
                step_run_id,
                StepStatus.PENDING,
                CancellationAcknowledgement(CancellationStatus.REQUESTED),
            )
            tracker.record_cancellation(
                step_run_id,
                StepStatus.PENDING,
                CancellationAcknowledgement(CancellationStatus.CONFIRMED),
            )
            cancellation = CancellationStatus.CONFIRMED
        tracker.transition(
            step_run_id,
            StepStatus.PENDING,
            target,
            step_spec_id=step_spec_id,
            result=_terminal_result(terminal, target, cancellation),
        )
        return
    tracker.transition(
        step_run_id,
        StepStatus.PENDING,
        StepStatus.RUNNING,
        step_spec_id=step_spec_id,
    )
    if target == StepStatus.RUNNING:
        return
    staging = StagingManager(str(staging_root), fs)
    for table_path, frame in tables.items():
        staging.stage_orchestrator_dataframe(
            frame,
            table_path,
            commit_kind="step_result",
            step_run_id=step_run_id,
            step_number=step_number,
            operation_name=operation_name,
        )
    terminal_frame = pl.DataFrame([terminal], schema=STEPS_SCHEMA)
    staging.stage_orchestrator_dataframe(
        terminal_frame,
        TablePath.STEPS.value,
        commit_kind="step_result",
        step_run_id=step_run_id,
        step_number=step_number,
        operation_name=operation_name,
    )
    plan = build_commit_plan(
        delta_root=str(delta_root),
        staging_root=str(staging_root),
        fs=fs,
        commit_kind="step_result",
        step_run_id=step_run_id,
        step_number=step_number,
        operation_name=operation_name,
    )
    DeltaCommitter(
        str(delta_root),
        staging,
        fs=fs,
        storage_options=storage_options,
    ).commit_logical(plan)


def _terminal_result(
    terminal: dict[str, object],
    status: StepStatus,
    cancellation_status: CancellationStatus | None = None,
) -> StepResult:
    """Build the exact terminal payload needed by guarded fixture transitions."""
    empty = status in {StepStatus.SKIPPED, StepStatus.CANCELLED}
    return StepResult(
        step_name=str(terminal["step_name"]),
        step_number=int(terminal["step_number"]),
        status=status,
        disposition=None if empty else terminal.get("disposition"),
        cancellation_status=cancellation_status,
        total_count=0 if empty else int(terminal.get("total_count") or 0),
        succeeded_count=0 if empty else int(terminal.get("succeeded_count") or 0),
        failed_count=0 if empty else int(terminal.get("failed_count") or 0),
        output_roles=frozenset(json.loads(str(terminal["output_roles_json"]))),
        output_types=json.loads(str(terminal["output_types_json"])),
        duration_seconds=None if empty else terminal.get("duration_seconds"),
        error=terminal.get("error"),
        step_run_id=str(terminal["step_run_id"]),
    )


def commit_test_inputs(
    delta_root: str | Path,
    staging_root: str | Path,
    tables: dict[str, pl.DataFrame],
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict[str, Any] | None = None,
    step_run_id: str | None = None,
) -> None:
    """Persist ownerless artifact fixture rows as input registrations."""
    fs = fs or LocalFileSystem()
    delta_root_text = str(delta_root)
    staging_root_text = str(staging_root)
    DeltaCommitter(
        delta_root_text,
        StagingManager(staging_root_text, fs),
        fs=fs,
        storage_options=storage_options,
    ).initialize_tables()
    step_numbers = sorted(
        {
            int(value)
            for frame in tables.values()
            if "origin_step_number" in frame.columns
            for value in frame["origin_step_number"].unique().to_list()
        }
    ) or [0]
    if step_run_id is not None and len(step_numbers) != 1:
        msg = "An explicit fixture owner requires exactly one origin step"
        raise ValueError(msg)
    for step_number in step_numbers:
        owner = step_run_id or uuid.uuid4().hex
        staging = StagingManager(staging_root_text, fs)
        staged_any = False
        for table_path, frame in tables.items():
            selected = (
                frame.filter(pl.col("origin_step_number") == step_number)
                if "origin_step_number" in frame.columns
                else (frame if step_number == step_numbers[0] else frame.clear())
            )
            if selected.is_empty():
                continue
            staged_any = True
            staging.stage_orchestrator_dataframe(
                selected,
                table_path,
                commit_kind="input_registration",
                step_run_id=owner,
                step_number=step_number,
                operation_name="test_input_registration",
            )
        if not staged_any:
            continue
        plan = build_commit_plan(
            delta_root=delta_root_text,
            staging_root=staging_root_text,
            fs=fs,
            commit_kind="input_registration",
            step_run_id=owner,
            step_number=step_number,
            operation_name="test_input_registration",
        )
        DeltaCommitter(
            delta_root_text,
            staging,
            fs=fs,
            storage_options=storage_options,
        ).commit_logical(plan)


def commit_test_tables(
    delta_root: str | Path,
    fs: AbstractFileSystem,
    storage_options: dict[str, Any] | None,
    tables: dict[str, pl.DataFrame],
) -> None:
    """Seed reader fixtures through a complete logical input registration."""
    commit_test_inputs(
        delta_root,
        f"{delta_root}/_test_staging",
        tables,
        fs=fs,
        storage_options=storage_options,
    )
