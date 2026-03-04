"""Worker result aggregation and error handling.

Collect results from workers and aggregate success/failure counts
with respect to the configured failure policy.
"""

from __future__ import annotations

from artisan.schemas.enums import FailurePolicy


def aggregate_results(
    results: list[dict],
    failure_policy: FailurePolicy,
) -> tuple[int, int]:
    """Sum succeeded and failed item counts across worker results.

    Args:
        results: Result dicts from workers with ``success``,
            ``error``, ``item_count``, and ``execution_run_ids`` keys.
        failure_policy: How to handle failures.

    Returns:
        Tuple of (succeeded_count, failed_count).

    Raises:
        RuntimeError: If fail_fast policy is active and any failure occurred.
    """
    succeeded = 0
    failed = 0

    for result in results:
        if result.get("success"):
            succeeded += result.get("item_count", 1)
        else:
            failed += result.get("item_count", 1)

            if failure_policy == FailurePolicy.FAIL_FAST:
                error_msg = result.get("error", "Unknown error")
                msg = f"Step failed with fail_fast policy: {error_msg}"
                raise RuntimeError(msg)

    return succeeded, failed


def extract_execution_run_ids(results: list[dict]) -> list[str]:
    """Collect all execution run IDs from worker result dicts.

    Args:
        results: Result dicts with ``execution_run_ids`` key (list of str).

    Returns:
        Flat list of all non-None execution run IDs.

    Raises:
        KeyError: If a result dict uses the legacy singular
            ``execution_run_id`` key instead of the list form.
    """
    ids = []
    for r in results:
        # Fail fast if old format detected - helps catch migration issues
        if "execution_run_id" in r and "execution_run_ids" not in r:
            msg = "Result uses deprecated 'execution_run_id' format. "
            msg += f"Expected 'execution_run_ids' (list). Result: {r}"
            raise KeyError(msg)
        if "execution_run_ids" in r:
            ids.extend(id for id in r["execution_run_ids"] if id)
    return ids
