"""Worker result aggregation and error handling.

Collect results from workers and aggregate success/failure counts
with respect to the configured failure policy.
"""

from __future__ import annotations

from artisan.schemas.enums import FailurePolicy
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.orchestration.step_lifecycle import StepStatus


def classify_step_status(
    succeeded: int,
    failed: int,
    failure_policy: FailurePolicy,
    *,
    infrastructure_error: bool = False,
) -> StepStatus:
    """Classify unit counts under one failure policy.

    Infrastructure failures are always failed. Under ``CONTINUE``, partial is
    reserved for a known mix whose successful subset is accepted.
    """
    if infrastructure_error or (failed > 0 and succeeded == 0):
        return StepStatus.FAILED
    if failed > 0:
        return (
            StepStatus.PARTIAL
            if failure_policy == FailurePolicy.CONTINUE
            else StepStatus.FAILED
        )
    return StepStatus.SUCCEEDED


def aggregate_results(
    results: list[UnitResult],
) -> tuple[int, int]:
    """Sum succeeded and failed item counts across worker results.

    Args:
        results: Unit results from workers.

    Returns:
        Tuple of (succeeded_count, failed_count).
    """
    succeeded = 0
    failed = 0
    for result in results:
        if result.success:
            succeeded += result.item_count
        else:
            failed += result.item_count
    return succeeded, failed


def extract_execution_run_ids(results: list[UnitResult]) -> list[str]:
    """Collect all execution run IDs from unit results.

    Args:
        results: Unit results with ``execution_run_ids``.

    Returns:
        Flat list of all non-None execution run IDs.
    """
    ids: list[str] = []
    for r in results:
        ids.extend(id for id in r.execution_run_ids if id)
    return ids
