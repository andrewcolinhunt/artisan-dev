"""Timing observations under verified current outer execution membership."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.retained_execution_query import (
    ExecutionMembership,
    _execution_membership,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    FailedScientificExecutionSource,
)
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.visualization.retained_inspect import (
    _execution_commands,
    _scientific_diagnostics,
)
from artisan.visualization.timing import PipelineTimings, _parse_timings, _timing_values


def read_retained_timings(
    committer: RetainedCommitter, *, pipeline_run_id: str | None = None
) -> PipelineTimings:
    """Read fresh successful outer observations from current usable steps.

    Reused executions contribute no historical worker time to the current run.
    Missing optional diagnostics leave the outer membership intact and its
    measurements unavailable. Internal invocations remain nested observations.
    """
    with committer._read_snapshot() as snapshot:
        tracker = RetainedStepTracker(committer, "")
        _, attempts = tracker._read_lifecycle(snapshot)
        states = [
            state
            for state in tracker._select_current_states(attempts, pipeline_run_id)
            if state.status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
        ]
        if not states:
            msg = "No usable terminal steps found"
            raise ValueError(msg)
        executions: dict[str, list[dict[str, Any]]] = {}
        for member in _execution_membership(snapshot, states):
            source = member.source
            if member.cache_hit or isinstance(source, FailedScientificExecutionSource):
                continue
            if (
                isinstance(source, CuratorExecutionSource)
                and not source.execution_record["success"]
            ):
                continue
            executions.setdefault(member.state.step_run_id, []).append(
                _observations(committer, member)
            )
        return PipelineTimings(
            {
                "pipeline_run_id": states[0].pipeline_run_id,
                "steps": [
                    {
                        "step_number": state.step_number,
                        "step_name": state.step_name,
                        "duration_seconds": state.duration_seconds,
                        "timings": {}
                        if state.disposition == StepDisposition.CACHE_HIT
                        else _timing_values(state.metadata.get("timings")),
                        "executions": executions.get(state.step_run_id, []),
                    }
                    for state in states
                ],
            }
        )


def _observations(
    committer: RetainedCommitter, member: ExecutionMembership
) -> dict[str, Any]:
    source = member.source
    assert not isinstance(source, FailedScientificExecutionSource)
    if isinstance(source, CuratorExecutionSource):
        row = source.execution_record
        execution_id = str(row["execution_run_id"])
        timings = _parse_timings(row["metadata"])
        start, end = row["timestamp_start"], row["timestamp_end"]
        duration = (
            (end - start).total_seconds()
            if isinstance(start, datetime) and isinstance(end, datetime)
            else None
        )
        if duration is not None and duration < 0:
            msg = "Curator timing observations have a negative elapsed duration"
            raise StoreIntegrityError(msg)
        return {
            "execution_run_id": execution_id,
            "operation_name": source.admission.operation_name,
            "unit_index": member.unit_index,
            "duration_seconds": duration,
            "diagnostics_status": "complete"
            if duration is not None or timings
            else "unavailable",
            "omitted_invocations": 0,
            "invocations": [],
            "timings": timings,
            "commands": _execution_commands(execution_id, source, None).model_dump(
                mode="json"
            ),
        }
    diagnostic = _scientific_diagnostics(committer, source)
    execution_id = source.plan.receipt.delivery.delivery_id
    return {
        "execution_run_id": execution_id,
        "operation_name": source.admission.recipe.definition.name,
        "unit_index": member.unit_index,
        "duration_seconds": diagnostic.duration_seconds
        if diagnostic is not None
        else None,
        "diagnostics_status": "unavailable"
        if diagnostic is None
        else "partial"
        if diagnostic.omitted_invocations
        else "complete",
        "omitted_invocations": diagnostic.omitted_invocations
        if diagnostic is not None
        else None,
        "invocations": [
            item.model_dump(
                mode="json",
                include={
                    "invocation_id",
                    "call_path",
                    "operation_name",
                    "operation_version",
                    "duration_seconds",
                    "timings",
                    "succeeded",
                    "cleanup",
                },
            )
            for item in diagnostic.invocations
        ]
        if diagnostic is not None
        else [],
        "timings": {},
        "commands": _execution_commands(execution_id, source, diagnostic).model_dump(
            mode="json"
        ),
    }
