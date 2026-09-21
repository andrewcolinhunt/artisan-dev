"""Persist provider diagnostics without changing sealed execution evidence."""

from __future__ import annotations

import logging

from fsspec import AbstractFileSystem

from artisan.schemas.execution.unit_result import UnitResult
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.utils.log_paths import find_failure_log, worker_log_path

logger = logging.getLogger(__name__)


def persist_worker_logs(
    results: list[UnitResult],
    delta_root: str,
    failure_logs_root: str | None,
    *,
    fs: AbstractFileSystem,
) -> None:
    """Keep provider logs as separate immutable diagnostics and in failure logs.

    Args:
        results: Results that may contain provider-captured worker logs.
        delta_root: Store root containing execution-linked diagnostic objects.
        failure_logs_root: Directory containing human-readable failure logs.
        fs: Configured filesystem containing the store.
    """
    for result in results:
        if not result.worker_log:
            continue
        for run_id in result.execution_run_ids:
            try:
                publish_immutable_bytes(
                    fs,
                    worker_log_path(delta_root, run_id),
                    result.worker_log.encode("utf-8"),
                )
            except Exception:
                logger.warning(
                    "Failed to persist provider log for %s", run_id, exc_info=True
                )
            if not result.success and failure_logs_root:
                _append_worker_log(
                    failure_logs_root,
                    run_id,
                    result.worker_log,
                )


def _append_worker_log(
    failure_logs_root: str,
    execution_run_id: str,
    worker_log: str,
) -> None:
    """Append opaque provider output to an existing failure log, best-effort."""
    try:
        log_path = find_failure_log(failure_logs_root, execution_run_id)
        if log_path is not None:
            with open(log_path, "a") as file:
                file.write(f"\n\n=== Worker Log ===\n{worker_log}")
    except Exception:
        logger.debug(
            "Failed to append worker log to failure log for %s",
            execution_run_id,
            exc_info=True,
        )
