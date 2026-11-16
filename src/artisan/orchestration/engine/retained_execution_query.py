"""Current outer execution membership without artifact or diagnostic hydration."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    ExecutionSource,
    FailedScientificExecutionSource,
    ScientificExecutionSource,
    _read_execution_source,
)
from artisan.storage.io.curator_plan import CuratorCommitPlan
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_metadata import (
    read_curator_source_metadata,
    read_terminal_metadata,
)
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.step_result_plan import (
    CurrentStepUnits,
    StepResultPlan,
    WholeStepReuse,
)


@dataclass(frozen=True)
class ExecutionMembership:
    """One current unit's original execution; repeated selections stay ordered."""

    state: StepState
    unit_index: int | None
    source: ExecutionSource
    cache_hit: bool


def read_retained_execution_membership(
    committer: RetainedCommitter, *, pipeline_run_id: str
) -> tuple[ExecutionMembership, ...]:
    """Select completed outer executions at the latest attempts of an exact run.

    Missing diagnostics and unavailable artifact bytes do not erase membership.
    A failed unit without a confirmed delivery has no execution to select; its
    failure remains in the step result. Inspection never recovers a plan, falls
    back to an older attempt, or manufactures executions for internal calls.
    """
    if not pipeline_run_id:
        msg = "Execution inspection requires an explicit pipeline run"
        raise ValueError(msg)
    with committer._read_snapshot() as snapshot:
        tracker = RetainedStepTracker(committer, pipeline_run_id)
        _, attempts = tracker._read_lifecycle(snapshot)
        states = tracker._select_current_states(attempts, pipeline_run_id)
        return _execution_membership(snapshot, states)


def _execution_membership(
    snapshot: RetainedReadSnapshot, states: list[StepState]
) -> tuple[ExecutionMembership, ...]:
    """Resolve a caller's fixed lifecycle selection with its existing read proof."""
    sources: dict[str, ExecutionSource] = {}

    def resolve(execution_run_id: str) -> ExecutionSource:
        if execution_run_id not in sources:
            source = _read_execution_source(snapshot, execution_run_id)
            if source is None:
                msg = "Selected execution has no completed original source"
                raise StoreIntegrityError(msg)
            sources[execution_run_id] = source
        return sources[execution_run_id]

    return tuple(
        member for state in states for member in _step_members(snapshot, state, resolve)
    )


def _step_members(
    snapshot: RetainedReadSnapshot,
    state: StepState,
    resolve: Callable[[str], ExecutionSource],
) -> Iterator[ExecutionMembership]:
    if state.status in {StepStatus.PENDING, StepStatus.RUNNING, StepStatus.SKIPPED}:
        return
    owner = f"step_result:{state.step_run_id}"
    if snapshot.controls.filter(
        (pl.col("logical_commit_id") == owner) & (pl.col("state") == "complete")
    ).is_empty():
        if state.status in {StepStatus.FAILED, StepStatus.CANCELLED}:
            return
        msg = "Successful step has no completed execution selection"
        raise StoreIntegrityError(msg)
    plan = snapshot.completed_control_plan(owner)
    if not isinstance(plan, (StepResultPlan, CuratorCommitPlan, CuratorReusePlan)):
        msg = "Current step has an incompatible execution selection"
        raise StoreIntegrityError(msg)
    terminal = read_terminal_metadata(snapshot, plan)
    if terminal != state.to_step_result():
        msg = "Current step differs from its completed execution selection"
        raise StoreIntegrityError(msg)
    if isinstance(plan, StepResultPlan):
        yield from _scientific_members(snapshot, state, plan, resolve)
        return
    if (
        isinstance(plan, CuratorCommitPlan)
        and plan.table(TablePath.EXECUTIONS.value) is None
        and terminal.status in {StepStatus.FAILED, StepStatus.CANCELLED}
    ):
        return
    selected, row = read_curator_source_metadata(snapshot, plan, terminal)
    source = resolve(row["execution_run_id"])
    if not isinstance(source, CuratorExecutionSource) or source.plan != selected:
        msg = "Selected curator execution differs from its original source"
        raise StoreIntegrityError(msg)
    yield ExecutionMembership(state, None, source, isinstance(plan, CuratorReusePlan))


def _scientific_members(
    snapshot: RetainedReadSnapshot,
    state: StepState,
    plan: StepResultPlan,
    resolve: Callable[[str], ExecutionSource],
) -> Iterator[ExecutionMembership]:
    selected = plan
    reused = isinstance(plan.selection, WholeStepReuse)
    if isinstance(plan.selection, WholeStepReuse):
        original = snapshot.completed_control_plan(
            f"step_result:{plan.selection.source_step_run_id}"
        )
        if not isinstance(original, StepResultPlan):
            msg = "Whole-step execution selection has no scientific source"
            raise StoreIntegrityError(msg)
        selected = original
        read_terminal_metadata(snapshot, selected)
    if not isinstance(selected.selection, CurrentStepUnits):
        msg = "Execution selection requires original ordered units"
        raise StoreIntegrityError(msg)
    for index, unit in enumerate(selected.selection.units):
        if unit.receipt is not None:
            source = resolve(unit.receipt.delivery.delivery_id)
            if (
                not isinstance(source, ScientificExecutionSource)
                or source.plan.receipt != unit.receipt
            ):
                msg = "Selected execution differs from its original receipt"
                raise StoreIntegrityError(msg)
        elif unit.failed_delivery is not None:
            source = resolve(unit.failed_delivery.delivery_id)
            if (
                not isinstance(source, FailedScientificExecutionSource)
                or source.plan != selected
                or source.unit != unit
            ):
                msg = "Selected failed execution differs from its original unit"
                raise StoreIntegrityError(msg)
        else:
            continue
        yield ExecutionMembership(state, index, source, reused or unit.reused)
