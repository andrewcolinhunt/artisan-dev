"""Read current candidate output membership from exact completed terminal plans."""

from __future__ import annotations

from dataclasses import dataclass

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.retained_step_results import RetainedStepResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.retained_reader import AcceptedRole
from artisan.storage.io.curator_plan import read_curator_plan
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.step_result_plan import read_step_result_plan


@dataclass(frozen=True)
class CurrentStepOutputs:
    """One frozen current attempt and its ordered accepted output occurrences."""

    state: StepState
    outputs: tuple[AcceptedRole, ...]


def read_current_step_outputs(
    committer: RetainedCommitter, step_run_id: str
) -> tuple[AcceptedRole, ...]:
    """Read one terminal attempt without recovering or substituting another one."""
    tracker = RetainedStepTracker(committer, "")
    state = tracker.current_state(step_run_id)
    _require_terminal(state)
    scientific = read_step_result_plan(committer.delta_root, step_run_id, committer.fs)
    curator = read_curator_plan(committer.delta_root, step_run_id, committer.fs)
    if scientific is not None and curator is not None:
        msg = "Step has conflicting scientific and curator result plans"
        raise StoreIntegrityError(msg)
    if scientific is not None:
        return RetainedStepResults(tracker).outputs(step_run_id)
    if curator is not None:
        result = CuratorReader(committer).read_result(step_run_id, curator.plan_digest)
        return (
            result.outputs
            if result.result.status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
            else ()
        )
    if state.status in {StepStatus.FAILED, StepStatus.CANCELLED, StepStatus.SKIPPED}:
        return ()
    msg = "Successful step has no completed result membership"
    raise StoreIntegrityError(msg)


def read_source_run_outputs(
    committer: RetainedCommitter,
    *,
    pipeline_run_id: str,
    step_number: int,
    include_prior_steps: bool = False,
) -> tuple[CurrentStepOutputs, ...]:
    """Freeze latest attempts at an explicit run boundary, then read exact plans."""
    if not pipeline_run_id.strip() or step_number < 0:
        msg = "Source selection needs an explicit run and nonnegative step number"
        raise ValueError(msg)
    states = RetainedStepTracker(committer, pipeline_run_id).load_current_states(
        pipeline_run_id
    )
    if not any(state.step_number == step_number for state in states):
        msg = "Requested source run has no step at the selected boundary"
        raise ValueError(msg)
    selected = tuple(
        state
        for state in states
        if state.step_number == step_number
        or (include_prior_steps and state.step_number < step_number)
    )
    for state in selected:
        _require_terminal(state)
    return tuple(
        CurrentStepOutputs(
            state, read_current_step_outputs(committer, state.step_run_id)
        )
        for state in selected
    )


def _require_terminal(state: StepState) -> None:
    if state.status in {StepStatus.PENDING, StepStatus.RUNNING}:
        msg = "Current source attempt is not terminal"
        raise StoreIntegrityError(msg)
