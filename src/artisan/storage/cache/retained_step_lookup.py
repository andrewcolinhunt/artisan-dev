"""Whole-step cache identity includes every ordered unit, including failures."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import CachePolicy
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.step_admission import StepAdmission
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.cache.retained_identity import retained_cache_key
from artisan.storage.cache.retained_lookup import _admitted_key
from artisan.storage.io.admission import capture_admission, read_admission_record
from artisan.storage.io.logical_control import control_for
from artisan.storage.io.step_result_plan import (
    CurrentStepUnits,
    StepResultPlan,
    WholeStepReuse,
    read_step_result_plan,
)

if TYPE_CHECKING:
    from artisan.storage.core.retained_reader import RetainedReader
    from artisan.storage.io.retained_commit import RetainedCommitter
    from artisan.storage.io.retained_read import RetainedReadSnapshot


@dataclass(frozen=True)
class WholeStepCacheHit:
    """Verified original completion; publication remains controller-owned."""

    admission: StepAdmission
    cache_policy: CachePolicy
    source: StepResultPlan


def lookup_completed_step(
    admission: StepAdmission,
    cache_policy: CachePolicy,
    candidates: Sequence[StepResultPlan],
    *,
    reader: RetainedReader,
    verify_source: Callable[[StepResultPlan], None],
) -> WholeStepCacheHit | None:
    """Check newest-first complete candidates before independent unit reuse.

    The controller supplies authoritative terminal order and the existing exact
    completion verifier. No candidate acquires authority from metadata matching.
    """
    keys = tuple(_admitted_key(item, reader) for item in admission.assignments)
    if not keys or None in keys:
        return None
    for candidate in candidates:
        if (
            candidate.step_run_id == admission.step_run_id
            or candidate.step_number != admission.step_number
            or not eligible_step_status(candidate.result.status, cache_policy)
        ):
            continue
        if metadata_step_keys(candidate.decision_admissions, reader.committer) != keys:
            continue
        verify_source(candidate)
        source = canonical_step_source(candidate, reader.committer)
        verify_step_inputs(admission.assignments, source.decision_admissions, reader)
        return WholeStepCacheHit(admission, cache_policy, source)
    return None


def eligible_step_status(status: StepStatus, policy: CachePolicy) -> bool:
    return status == StepStatus.SUCCEEDED or (
        policy == CachePolicy.STEP_COMPLETED and status == StepStatus.PARTIAL
    )


def canonical_step_source(
    plan: StepResultPlan, committer: RetainedCommitter
) -> StepResultPlan:
    """Resolve at most one source edge; persisted chains are forbidden."""
    if isinstance(plan.selection, CurrentStepUnits):
        return plan
    source = read_step_result_plan(
        committer.delta_root, plan.selection.source_step_run_id, committer.fs
    )
    if (
        source is None
        or not isinstance(source.selection, CurrentStepUnits)
        or source.plan_digest != plan.selection.source_plan_digest
    ):
        msg = "Whole-step reuse must name its exact original completion"
        raise StoreIntegrityError(msg)
    return source


def verify_step_reuse_source(
    plan: StepResultPlan,
    controls: pl.DataFrame,
    plans: Mapping[str, StepResultPlan],
    committer: RetainedCommitter,
) -> StepResultPlan:
    """Validate source metadata within one fresh control proof without recursion."""
    selection = plan.selection
    assert isinstance(selection, WholeStepReuse)
    source = plans.get(selection.source_step_run_id)
    if (
        source is None
        or not isinstance(source.selection, CurrentStepUnits)
        or source.plan_digest != selection.source_plan_digest
        or source.step_run_id == plan.step_run_id
        or source.step_number != plan.step_number
        or not eligible_step_status(source.result.status, selection.cache_policy)
    ):
        msg = "Whole-step reuse has no exact eligible original source"
        raise StoreIntegrityError(msg)
    control = control_for(source, controls)
    if control is None or control["state"] != "complete":
        msg = "Whole-step reuse source is not complete"
        raise StoreIntegrityError(msg)
    current_keys = metadata_step_keys(plan.decision_admissions, committer)
    if (
        not current_keys
        or None in current_keys
        or current_keys != metadata_step_keys(source.decision_admissions, committer)
    ):
        msg = "Whole-step reuse changed its complete ordered scientific inputs"
        raise StoreIntegrityError(msg)
    if plan.result != reused_step_result(
        source,
        plan.admission,
        step_name=plan.result.step_name,
        duration_seconds=plan.result.duration_seconds,
    ):
        msg = "Whole-step reuse changed historical counts, outputs, error or metadata"
        raise StoreIntegrityError(msg)
    return source


def reused_step_result(
    source: StepResultPlan,
    admission: StepAdmission,
    *,
    step_name: str,
    duration_seconds: float | None,
) -> StepResult:
    """Copy only historical result facts, preserving the current attempt identity."""
    metadata = {
        key: value
        for key, value in source.result.metadata.items()
        if key not in {"timings", "assignment_admission_digest"}
    }
    metadata["assignment_admission_digest"] = admission.digest
    return StepResult.model_validate(
        {
            **source.result.model_dump(),
            "step_run_id": admission.step_run_id,
            "step_number": admission.step_number,
            "step_name": step_name,
            "disposition": StepDisposition.CACHE_HIT,
            "cancellation_status": None,
            "duration_seconds": duration_seconds,
            "error": source.result.error
            if source.result.status == StepStatus.PARTIAL
            else None,
            "metadata": metadata,
        }
    )


def metadata_step_keys(
    admissions: Sequence[AdmissionReference],
    committer: RetainedCommitter,
) -> tuple[str | None, ...]:
    """Read closed identity only; metadata gives no content or completion authority."""
    records = (
        read_admission_record(item, delta_root=committer.delta_root, fs=committer.fs)
        for item in admissions
    )
    return tuple(
        retained_cache_key(record.recipe, record.attempt, record.boundary)
        for record in records
    )


def verify_step_inputs(
    current: Sequence[AdmissionReference],
    source: Sequence[AdmissionReference],
    reader: RetainedReader,
    *,
    _read: RetainedReadSnapshot | None = None,
) -> None:
    """Recheck every current and historical input, including source failed slots."""
    if len(current) != len(source):
        msg = "Whole-step reuse changed finite assignment membership"
        raise StoreIntegrityError(msg)
    committer = reader.committer
    if _read is not None:
        _read.require_committer(committer)
    for expected, previous in zip(current, source, strict=True):
        keys = []
        for reference in (expected, previous):
            with capture_admission(
                reference,
                delta_root=committer.delta_root,
                files_root=committer.files_root,
                fs=committer.fs,
                verify_producer=partial(committer._verify_producer, _read=_read),
            ) as captured:
                keys.append(
                    retained_cache_key(
                        captured.record.recipe,
                        reference.attempt,
                        captured.record.boundary,
                    )
                )
        if keys[0] is None or keys[0] != keys[1]:
            msg = "Whole-step reuse changed complete frozen scientific inputs"
            raise StoreIntegrityError(msg)
