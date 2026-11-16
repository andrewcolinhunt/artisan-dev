"""Exact terminal and curator source metadata shared by inspection and repair."""

from __future__ import annotations

from typing import Any

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.curator_carrier_effects import sealed_curator_carriers
from artisan.storage.io.curator_plan import (
    CuratorCommitPlan,
    CuratorPlan,
    sealed_curator_cache_proof,
)
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan, curator_reuse_result
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.step_result_plan import StepResultPlan

type _StepPlan = StepResultPlan | CuratorPlan


def read_terminal_metadata(
    snapshot: RetainedReadSnapshot, plan: _StepPlan
) -> StepResult:
    """Bind the exact physical lifecycle result to its scientific selection plan."""
    row = _effect_row(snapshot, plan, TablePath.STEPS)
    result = StepTracker._validated_snapshot(row).to_step_result()
    if isinstance(plan, StepResultPlan) and result != plan.result:
        msg = "Scientific terminal metadata differs from its selection plan"
        raise StoreIntegrityError(msg)
    return result


def _effect_row(
    snapshot: RetainedReadSnapshot, plan: _StepPlan, path: TablePath
) -> dict[str, Any]:
    return snapshot.effect_row(plan, path.value)


def read_curator_source_metadata(
    snapshot: RetainedReadSnapshot, plan: CuratorPlan, terminal: StepResult
) -> tuple[CuratorCommitPlan, dict[str, Any]]:
    """Bind executed or reused curator metadata to its original sealed evidence."""
    if isinstance(plan, CuratorReusePlan):
        _effect_row(snapshot, plan, TablePath.CACHE_REUSE)
        source = snapshot.completed_control_plan(
            f"step_result:{plan.source.step_run_id}"
        )
        if not isinstance(source, CuratorCommitPlan):
            msg = "Curator selection requires its original executed source"
            raise StoreIntegrityError(msg)
        admission = read_curator_admission(
            plan.admission,
            delta_root=snapshot.committer.delta_root,
            fs=snapshot.committer.fs,
        )
        expected = curator_reuse_result(
            admission,
            read_terminal_metadata(snapshot, source),
            cancellation_status=terminal.cancellation_status,
            error=terminal.error,
        )
        if terminal != expected:
            msg = "Curator reuse changed its original terminal metadata"
            raise StoreIntegrityError(msg)
    else:
        source = plan
    row = _effect_row(snapshot, source, TablePath.EXECUTIONS)
    proof = sealed_curator_cache_proof(row["metadata"], success=row["success"])
    inventory = sealed_curator_carriers(row["metadata"], success=row["success"])
    if proof != source.cache_proof or inventory != source.carrier_inventory:
        msg = "Curator plan differs from sealed execution metadata"
        raise StoreIntegrityError(msg)
    return source, row
