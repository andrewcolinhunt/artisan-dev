"""NFS-aware staging file verification.

On distributed filesystems (NFS) the orchestrator may not immediately see
files written by SLURM workers due to directory attribute caching. This
module forces cache invalidation and uses ``open()``-based close-to-open
consistency checks to confirm staging files are visible before commit.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

from artisan.utils.path import shard_path

logger = logging.getLogger(__name__)

# The file that is ALWAYS written by finalize_and_stage()
REQUIRED_STAGING_FILE = "executions.parquet"


def _invalidate_nfs_dir_cache(path: Path) -> None:
    """Force NFS to refresh directory entry caches along the path.

    Walk from root toward ``path``, calling ``os.listdir()`` on each
    ancestor to trigger READDIR RPCs and flush stale dcache entries.

    Args:
        path: Target whose ancestor directories need invalidation.
    """
    # Collect path components from root to target
    parts = list(path.parts)
    for i in range(1, len(parts) + 1):
        ancestor = Path(*parts[:i])
        try:
            os.listdir(ancestor)
        except (FileNotFoundError, PermissionError, OSError):
            # Ancestor doesn't exist yet or isn't accessible — stop here,
            # deeper paths won't be reachable anyway
            break


def verify_file_exists_nfs(path: Path) -> bool:
    """Check file existence using NFS close-to-open consistency.

    Invalidate parent directory caches, then ``open()`` and read one
    byte to confirm the file is visible and readable.

    Args:
        path: File to verify.
    """
    _invalidate_nfs_dir_cache(path)
    try:
        with open(path, "rb") as f:
            f.read(1)  # Force actual read to trigger consistency check
        return True
    except (FileNotFoundError, PermissionError, OSError):
        return False


def compute_expected_staging_paths(
    staging_root: Path,
    execution_run_ids: list[str],
    step_number: int | None = None,
    operation_name: str | None = None,
) -> list[Path]:
    """Compute the expected staging directory for each execution run ID.

    Args:
        staging_root: Root directory for staging files.
        execution_run_ids: Run IDs to resolve into shard paths.
        step_number: Pipeline step number used for directory
            partitioning.
        operation_name: Human-readable step directory suffix.

    Returns:
        One staging directory path per run ID, in the same order.
    """
    return [
        shard_path(
            staging_root,
            run_id,
            step_number=step_number,
            operation_name=operation_name,
        )
        for run_id in execution_run_ids
    ]


def verify_staging_directory(staging_dir: Path) -> tuple[bool, list[str]]:
    """Verify required staging files exist using NFS-safe checks.

    Args:
        staging_dir: Staging directory to inspect (e.g.
            ``staging_root/ab/cd/run_id/``).

    Returns:
        ``(True, [])`` on success, or ``(False, reasons)`` listing
        which files or directories are missing.
    """
    required_file = staging_dir / REQUIRED_STAGING_FILE

    if not verify_file_exists_nfs(required_file):
        if not staging_dir.exists():
            return False, [f"directory {staging_dir} not found"]
        return False, [REQUIRED_STAGING_FILE]

    return True, []


def await_staging_files(
    staging_root: Path,
    execution_run_ids: list[str],
    timeout_seconds: float = 60.0,
    poll_interval_seconds: float = 1.0,
    step_number: int | None = None,
    operation_name: str | None = None,
) -> None:
    """Poll until all expected staging files are visible.

    Use exponential backoff with NFS cache invalidation on each
    attempt. An empty ``execution_run_ids`` list returns immediately.

    Args:
        staging_root: Root directory for staging files.
        execution_run_ids: Run IDs whose staging files must appear.
        timeout_seconds: Maximum wait time. Defaults to 60.
        poll_interval_seconds: Initial polling interval (capped at 5 s
            via exponential backoff). Defaults to 1.
        step_number: Pipeline step number for directory partitioning.
        operation_name: Human-readable step directory suffix.

    Raises:
        TimeoutError: When one or more staging directories are still
            missing after ``timeout_seconds``. The message details
            which run IDs are affected.
    """
    if not execution_run_ids:
        logger.debug("No execution_run_ids to verify, skipping staging verification")
        return

    expected_paths = compute_expected_staging_paths(
        staging_root,
        execution_run_ids,
        step_number=step_number,
        operation_name=operation_name,
    )
    id_to_path = dict(zip(execution_run_ids, expected_paths, strict=False))

    start_time = time.monotonic()
    current_interval = poll_interval_seconds
    max_interval = 5.0  # Cap backoff at 5 seconds
    attempt = 0

    while True:
        attempt += 1
        elapsed = time.monotonic() - start_time

        # Check all paths using open() for close-to-open consistency
        missing: dict[str, str] = {}
        for run_id, path in id_to_path.items():
            success, issues = verify_staging_directory(path)
            if not success:
                missing[run_id] = issues[0] if issues else "unknown issue"

        if not missing:
            logger.debug(
                f"Staging verification complete: {len(execution_run_ids)} files "
                f"verified in {elapsed:.1f}s ({attempt} attempts)"
            )
            return

        # Check timeout
        if elapsed >= timeout_seconds:
            _raise_timeout_error(missing, len(execution_run_ids), timeout_seconds)

        # Log progress
        logger.debug(
            f"Staging verification attempt {attempt}: "
            f"{len(missing)}/{len(execution_run_ids)} still missing, "
            f"elapsed={elapsed:.1f}s"
        )

        # Wait with exponential backoff
        time.sleep(current_interval)
        current_interval = min(current_interval * 1.5, max_interval)


def _raise_timeout_error(
    missing: dict[str, str], total_count: int, timeout_seconds: float
) -> None:
    """Raise TimeoutError with details about missing staging files."""
    # Show first N missing IDs for debugging
    max_shown = 5
    missing_details = list(missing.items())[:max_shown]
    details_str = "\n".join(
        f"  - {run_id}: {reason}" for run_id, reason in missing_details
    )

    if len(missing) > max_shown:
        details_str += f"\n  ... and {len(missing) - max_shown} more"

    raise TimeoutError(
        f"Staging files not visible after {timeout_seconds}s.\n"
        f"Missing {len(missing)}/{total_count} execution_run_ids:\n"
        f"{details_str}\n"
        f"Check SLURM worker logs for these executions."
    )
