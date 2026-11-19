"""Resolve an explicit execution selector to its original completed authority."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.admission import AnyAdmissionRecord
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.execution.step_admission import ADMISSION_DIGEST
from artisan.storage.io.admission import read_admission_record
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.curator_plan import CuratorCommitPlan
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_metadata import (
    read_curator_source_metadata,
    read_terminal_metadata,
)
from artisan.storage.io.retained_plan import RetainedCommitPlan
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.step_admission import read_step_admission
from artisan.storage.io.step_result_plan import (
    CurrentStepUnits,
    StepResultPlan,
    StepUnitResolution,
)


@dataclass(frozen=True)
class ScientificExecutionSource:
    """An accepted delivery and its exact required recipe and input metadata."""

    plan: RetainedCommitPlan
    admission: AnyAdmissionRecord


@dataclass(frozen=True)
class FailedScientificExecutionSource:
    """A closed failed delivery selected by a completed, fenced terminal unit."""

    plan: StepResultPlan
    unit: StepUnitResolution
    admission: AnyAdmissionRecord


@dataclass(frozen=True)
class CuratorExecutionSource:
    """An original curator execution and its frozen store/input admission."""

    plan: CuratorCommitPlan
    admission: CuratorAdmission
    execution_record: Mapping[str, object]
    """Verified physical metadata; optional diagnostic fields remain unparsed."""


type ExecutionSource = (
    ScientificExecutionSource | FailedScientificExecutionSource | CuratorExecutionSource
)


def read_execution_source(
    committer: RetainedCommitter, execution_run_id: str
) -> ExecutionSource | None:
    """Resolve a delivery or curator execution ID without hydration or recovery.

    Reused results retain their original source identifier. Step IDs, unclaimed
    seals and incomplete selections are not execution sources. Explicit source
    selection may inspect diagnostic results, but grants no replay or cache
    eligibility. Optional logs and replay configuration are read separately;
    absent diagnostics cannot invalidate this required metadata.
    """
    if not execution_run_id:
        message = "Execution source selection requires an explicit execution ID"
        raise ValueError(message)
    with committer._read_snapshot() as snapshot:
        return _read_execution_source(snapshot, execution_run_id)


def _read_execution_source(
    snapshot: RetainedReadSnapshot, execution_run_id: str
) -> ExecutionSource | None:
    """Resolve an exact source within one caller-owned metadata read."""
    committer = snapshot.committer
    candidates: list[
        RetainedCommitPlan
        | CuratorCommitPlan
        | tuple[StepResultPlan, StepUnitResolution]
    ] = []
    owners = snapshot.controls.filter(pl.col("state") == "complete")[
        "logical_commit_id"
    ]
    for owner in owners:
        plan = snapshot.completed_control_plan(owner)
        if isinstance(plan, RetainedCommitPlan):
            if plan.receipt.delivery.delivery_id == execution_run_id:
                candidates.append(plan)
        elif isinstance(plan, StepResultPlan) and isinstance(
            plan.selection, CurrentStepUnits
        ):
            candidates.extend(
                (plan, unit)
                for unit in plan.selection.units
                if unit.failed_delivery is not None
                and unit.failed_delivery.delivery_id == execution_run_id
            )
        elif isinstance(plan, CuratorCommitPlan):
            execution = plan.table(TablePath.EXECUTIONS.value)
            if execution is not None and (execution_run_id,) in execution.row_keys:
                candidates.append(plan)
    if not candidates:
        return None
    if len(candidates) != 1:
        message = "Execution ID identifies multiple completed source owners"
        raise StoreIntegrityError(message)
    selected = candidates[0]
    if isinstance(selected, tuple):
        plan, unit = selected
        terminal = read_terminal_metadata(snapshot, plan)
        if terminal.metadata.get(ADMISSION_DIGEST) != plan.admission.digest:
            msg = "Failed execution terminal changed its finite admission"
            raise StoreIntegrityError(msg)
        admitted = read_step_admission(
            plan.step_run_id,
            plan.admission.digest,
            delta_root=committer.delta_root,
            fs=committer.fs,
        )
        if admitted != plan.admission:
            msg = "Failed execution source changed its finite admission"
            raise StoreIntegrityError(msg)
        admission = read_admission_record(
            unit.admission, delta_root=committer.delta_root, fs=committer.fs
        )
        if admission.step_number != plan.step_number:
            msg = "Failed execution source changed its admitted step number"
            raise StoreIntegrityError(msg)
        return FailedScientificExecutionSource(plan, unit, admission)
    if isinstance(selected, RetainedCommitPlan):
        snapshot.projection_metadata(selected.receipt)
        admission = read_admission_record(
            selected.admission, delta_root=committer.delta_root, fs=committer.fs
        )
        return ScientificExecutionSource(selected, admission)
    terminal = read_terminal_metadata(snapshot, selected)
    _, execution_record = read_curator_source_metadata(snapshot, selected, terminal)
    return CuratorExecutionSource(
        selected,
        read_curator_admission(
            selected.admission, delta_root=committer.delta_root, fs=committer.fs
        ),
        MappingProxyType(execution_record),
    )
