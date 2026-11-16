"""Select and recover current curator cache membership without worker execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.cache.curator_identity import curator_cache_key
from artisan.storage.cache.curator_lookup import (
    CuratorCacheHit,
    validate_curator_reuse_source,
)
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.curator_control import verify_curator_controls
from artisan.storage.io.curator_effects import (
    CapturedCuratorEffects,
    capture_curator_effects,
    prepare_curator_effects,
)
from artisan.storage.io.curator_plan import CuratorCommitPlan, publish_curator_plan
from artisan.storage.io.curator_reuse_plan import (
    CuratorReusePlan,
    CuratorReuseSource,
    curator_reuse_digest,
    curator_reuse_result,
)
from artisan.storage.io.logical_control import control_for

if TYPE_CHECKING:
    from artisan.orchestration.engine.retained_curator import CuratorResults


def reuse_curator_result(
    coordinator: CuratorResults, hit: CuratorCacheHit
) -> StepResult:
    """Revalidate a lookup result and select exactly one current-step terminal plan."""
    with coordinator.committer._lock, coordinator.tracker._lock:
        source = _source_identity(hit)
        admission = read_curator_admission(
            hit.admission,
            delta_root=coordinator.committer.delta_root,
            fs=coordinator.committer.fs,
        )
        if hit.cache_key != curator_cache_key(admission):
            msg = "Curator lookup key differs from current immutable admission"
            raise StoreIntegrityError(msg)
        existing = coordinator._plan(hit.admission.step_run_id)
        if existing is not None:
            if not isinstance(existing, CuratorReusePlan) or (
                existing.admission != hit.admission or existing.source != source
            ):
                msg = "Curator reuse retry changed its selected source"
                raise StoreIntegrityError(msg)
            return coordinator.recover(existing.step_run_id)
        accepted = validate_curator_reuse_source(
            hit.admission, source, reader=CuratorReader(coordinator.committer)
        )
        state = coordinator.tracker.current_state(hit.admission.step_run_id)
        _require_running(
            admission, state.status, state.step_spec_id, state.cancellation_status
        )
        result = curator_reuse_result(
            admission,
            accepted.result,
            cancellation_status=state.cancellation_status,
            error=state.error,
        )
        terminal = coordinator.tracker.prepare_terminal_candidate(
            hit.admission.step_run_id,
            StepStatus.RUNNING,
            StepStatus.SUCCEEDED,
            step_spec_id=admission.digest,
            result=result,
        )
        coordinator.staging.stage_cache_reuse(
            hit.admission.step_run_id,
            {source.execution_run_id},
            step_number=admission.record.step_number,
            operation_name=admission.operation_name,
        )
        coordinator.staging.stage_orchestrator_dataframe(
            terminal,
            TablePath.STEPS.value,
            commit_kind="step_result",
            step_run_id=hit.admission.step_run_id,
            step_number=admission.record.step_number,
            operation_name=admission.operation_name,
        )
        with prepare_curator_effects(
            staging_root=coordinator.staging.staging_dir,
            fs=coordinator.committer.fs,
            step_run_id=hit.admission.step_run_id,
            step_number=admission.record.step_number,
            operation_name=admission.operation_name,
            execution_run_ids=(),
        ) as staged:
            payload = {
                "domain": "artisan.curator-reuse",
                "admission": hit.admission.model_dump(mode="json"),
                "source": source.model_dump(mode="json"),
                "effects": staged.plan.model_dump(mode="json"),
            }
            plan = CuratorReusePlan.model_validate(
                {**payload, "plan_digest": curator_reuse_digest(payload)}
            )
            _validate_terminal(coordinator, staged, admission, result)
            controls = coordinator.committer._controls()
            verify_curator_controls(
                coordinator.committer,
                controls,
                [
                    *(
                        coordinator.committer._control_plan(row)
                        for row in controls.iter_rows(named=True)
                    ),
                    plan,
                ],
            )
            publish_curator_plan(
                plan,
                delta_root=coordinator.committer.delta_root,
                fs=coordinator.committer.fs,
            )
            coordinator._checkpoint("plan")
            coordinator._apply(plan, staged.tables)
        return coordinator.recover(plan.step_run_id)


def recover_curator_reuse(
    coordinator: CuratorResults, plan: CuratorReusePlan
) -> StepResult:
    """Recover the selected source, never substitute another equivalent cache hit."""
    accepted = validate_curator_reuse_source(
        plan.admission, plan.source, reader=CuratorReader(coordinator.committer)
    )
    controls = coordinator.committer._controls()
    control = control_for(plan, controls)
    if control is None or control["state"] == "planned":
        admission = read_curator_admission(
            plan.admission,
            delta_root=coordinator.committer.delta_root,
            fs=coordinator.committer.fs,
        )
        state = coordinator.tracker.current_state(plan.step_run_id)
        result = curator_reuse_result(
            admission,
            accepted.result,
            cancellation_status=state.cancellation_status,
            error=state.error,
        )
        with capture_curator_effects(
            plan.effects, coordinator.staging.staging_dir, coordinator.committer.fs
        ) as staged:
            _validate_terminal(coordinator, staged, admission, result)
            coordinator._apply(plan, staged.tables)
    elif control["state"] != "complete":
        msg = "Curator reuse is not eligible for recovery"
        raise StoreIntegrityError(msg)
    with coordinator.committer._read_snapshot() as read:
        coordinator._verify_complete(plan, read)
    result = (
        CuratorReader(coordinator.committer)
        .read_result(plan.step_run_id, plan.plan_digest)
        .result
    )
    if not coordinator.preserve_staging:
        coordinator.staging.cleanup_plan(
            [file for table in plan.tables for file in table.files]
        )
        coordinator._checkpoint("cleanup")
    return result


def _source_identity(hit: CuratorCacheHit) -> CuratorReuseSource:
    plan = hit.source.plan
    if not isinstance(plan, CuratorCommitPlan):
        msg = "Curator cache lookup must select an original executed source"
        raise StoreIntegrityError(msg)
    execution = plan.table(TablePath.EXECUTIONS.value)
    if execution is None or execution.row_count != 1:
        msg = "Curator cache source lacks one exact worker execution"
        raise StoreIntegrityError(msg)
    return CuratorReuseSource(
        step_run_id=plan.step_run_id,
        plan_digest=plan.plan_digest,
        execution_run_id=execution.row_keys[0][0],
    )


def _require_running(
    admission: CuratorAdmission,
    status: StepStatus,
    step_spec_id: str | None,
    cancellation: CancellationStatus | None,
) -> None:
    if status != StepStatus.RUNNING or step_spec_id != admission.digest:
        msg = "Curator reuse lacks its exact running admission"
        raise StoreIntegrityError(msg)
    if cancellation not in {None, CancellationStatus.REJECTED}:
        msg = "Curator reuse cannot cross a cancellation fence"
        raise StoreIntegrityError(msg)


def _validate_terminal(
    coordinator: CuratorResults,
    staged: CapturedCuratorEffects,
    admission: CuratorAdmission,
    expected: StepResult,
) -> None:
    state = coordinator.tracker.current_state(admission.record.step_run_id)
    _require_running(
        admission, state.status, state.step_spec_id, state.cancellation_status
    )
    table = staged.tables[TablePath.STEPS.value]
    terminal = table.get_row(table.keys[0])
    coordinator._validate_terminal_history(terminal)
    if StepTracker._validated_snapshot(terminal).to_step_result() != expected:
        msg = "Curator reuse terminal changed the source result or current admission"
        raise StoreIntegrityError(msg)
