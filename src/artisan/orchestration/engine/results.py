"""Worker result aggregation and error handling.

Collect results from workers and aggregate success/failure counts
with respect to the configured failure policy.
"""

from __future__ import annotations

from artisan.schemas.enums import FailurePolicy
from artisan.schemas.execution.unit_result import UnitResult


class FailFastAbort(RuntimeError):
    """Signals an intentional step abort under the fail_fast policy.

    Subclasses RuntimeError so callers can distinguish a deliberate
    fail-fast abort from an incidental RuntimeError raised by the dispatch
    machinery — the latter must be recorded as a dispatch_error rather than
    crashing the step.
    """


def aggregate_results(
    results: list[UnitResult],
    failure_policy: FailurePolicy,
) -> tuple[int, int]:
    """Sum succeeded and failed item counts across worker results.

    Args:
        results: Unit results from workers.
        failure_policy: How to handle failures.

    Returns:
        Tuple of (succeeded_count, failed_count).

    Raises:
        FailFastAbort: If fail_fast policy is active and any failure occurred.
    """
    succeeded = 0
    failed = 0

    for result in results:
        if result.success:
            succeeded += result.item_count
        else:
            failed += result.item_count

            if failure_policy == FailurePolicy.FAIL_FAST:
                error_msg = result.error or "Unknown error"
                msg = f"Step failed with fail_fast policy: {error_msg}"
                raise FailFastAbort(msg)

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
