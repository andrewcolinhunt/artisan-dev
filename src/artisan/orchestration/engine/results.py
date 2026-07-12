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

    Never raises: aggregation is pure counting. The fail_fast abort is
    raised separately by :func:`raise_if_fail_fast` *after* the step has
    committed its failure (and sibling success) records to Delta — so a
    fail_fast failure is always durably recorded before the pipeline aborts.

    Args:
        results: Unit results from workers.
        failure_policy: How to handle failures. Unused here (the abort moved
            to :func:`raise_if_fail_fast`); kept for a stable call signature.

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


def raise_if_fail_fast(
    failure_policy: FailurePolicy,
    failed: int,
    results: list[UnitResult],
    dispatch_error: str | None = None,
) -> None:
    """Abort the step under fail_fast once its records are committed.

    Call this only *after* the verify-staging → capture-logs → commit
    sequence has run, so the failure record and any sibling success records
    are already durable in Delta. No-op unless the policy is FAIL_FAST and at
    least one item failed.

    Args:
        failure_policy: The resolved step failure policy.
        failed: Number of failed items from ``aggregate_results``.
        results: Unit results, scanned for the first failure's error string.
        dispatch_error: Infrastructure error (pool break / dispatch
            exception) used when no per-unit error is available.

    Raises:
        FailFastAbort: When ``failure_policy`` is FAIL_FAST and ``failed`` > 0.
    """
    if failure_policy != FailurePolicy.FAIL_FAST or failed <= 0:
        return
    error_msg = (
        next((r.error for r in results if not r.success and r.error), None)
        or dispatch_error
        or "Unknown error"
    )
    msg = f"Step failed with fail_fast policy: {error_msg}"
    raise FailFastAbort(msg)


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
