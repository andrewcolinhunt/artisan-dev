"""Prefect task and flow factories for worker dispatch.

Provides the ``execute_unit_task`` Prefect task, unit serialization
helpers, and result collection with SLURM log capture.
"""

from __future__ import annotations

import logging
import os
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed

from prefect import task

from artisan.execution.models.execution_composite import ExecutionComposite
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.utils.errors import format_error

logger = logging.getLogger(__name__)


def _save_units(
    units: list[ExecutionUnit | ExecutionComposite],
    staging_root: str,
    step_number: int,
) -> str:
    """Serialize execution units to a pickle file for Prefect dispatch.

    Pickle dispatch is always local NFS, so this uses os.path
    rather than fsspec.
    """
    path = os.path.join(staging_root, "_dispatch", f"step_{step_number}_units.pkl")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(units, f, protocol=pickle.HIGHEST_PROTOCOL)
    return path


def _load_units(path: str) -> list[ExecutionUnit | ExecutionComposite]:
    """Deserialize execution units from a pickle file."""
    with open(path, "rb") as f:
        return pickle.load(f)


@task
def execute_unit_task(
    unit: ExecutionUnit | ExecutionComposite,
    runtime_env: RuntimeEnvironment,
) -> UnitResult:
    """Execute a single unit or composite, routing to the appropriate executor.

    Args:
        unit: Batch of artifacts to process or a composite.
        runtime_env: Runtime paths and backend configuration.

    Returns:
        UnitResult with execution outcome.
    """
    try:
        import os

        # Get worker_id from backend-specific environment variable
        env_var = runtime_env.worker_id_env_var
        worker_id = int(os.environ.get(env_var, 0)) if env_var else 0

        # Route composite to composite executor
        if isinstance(unit, ExecutionComposite):
            from artisan.execution.executors.composite import run_composite

            result = run_composite(unit, runtime_env, worker_id=worker_id)
            return UnitResult(
                success=result.success,
                error=result.error,
                item_count=1,
                execution_run_ids=[result.execution_run_id],
            )

        from artisan.execution.executors.curator import (
            is_curator_operation,
            run_curator_flow,
        )

        # Route to appropriate executor based on operation type
        if is_curator_operation(unit.operation):
            result = run_curator_flow(unit, runtime_env, worker_id=worker_id)

            return UnitResult(
                success=result.success,
                error=result.error,
                item_count=len(result.artifact_ids) if result.success else 1,
                execution_run_ids=[result.execution_run_id],
            )
        # Creator ops return single StagingResult
        from artisan.execution.executors.creator import run_creator_flow

        result = run_creator_flow(unit, runtime_env, worker_id=worker_id)
        return UnitResult(
            success=result.success,
            error=result.error,
            item_count=unit.get_batch_size() or 1,
            execution_run_ids=[result.execution_run_id],
        )
    except KeyboardInterrupt:
        raise RuntimeError("Operation interrupted by SIGINT") from None
    except Exception as exc:
        return UnitResult(
            success=False,
            error=format_error(exc),
            item_count=1,
            execution_run_ids=[],
        )


def _get_one(future: object) -> UnitResult:
    """Retrieve result from a single future, converting exceptions to failures."""
    try:
        return future.result()
    except Exception as exc:
        logger.error(
            "Future raised during result collection: %s: %s",
            type(exc).__name__,
            exc,
        )
        return UnitResult(
            success=False,
            error=format_error(exc),
            item_count=1,
            execution_run_ids=[],
        )


def _collect_results(futures: list) -> list[UnitResult]:
    """Collect results from Prefect futures in parallel.

    Uses a thread pool so that multiple blocking ``f.result()`` calls
    run concurrently — critical for SLURM backends where each call
    blocks until scancel/squeue completes.
    """
    if not futures:
        return []

    results: list[UnitResult | None] = [None] * len(futures)
    max_workers = min(len(futures), 32)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        submitted = {pool.submit(_get_one, f): i for i, f in enumerate(futures)}
        for cf in as_completed(submitted):
            idx = submitted[cf]
            results[idx] = cf.result()

    logger.info("Collected results from %d futures", len(futures))

    # Best-effort SLURM log capture (may replace results with worker_log populated)
    results = [
        _capture_slurm_logs(future, result)
        for future, result in zip(futures, results, strict=False)
    ]

    return results


def _capture_slurm_logs(future: object, result: UnitResult) -> UnitResult:
    """Extract SLURM worker stdout/stderr from a future.

    Returns the original result unchanged for non-SLURM futures, or a new
    UnitResult with ``worker_log`` populated when logs are available.
    """
    try:
        # Get the loggable future: direct or via parent batch future
        log_future = None
        if hasattr(future, "logs"):
            log_future = future
        elif hasattr(future, "slurm_job_future"):
            log_future = future.slurm_job_future

        if log_future is None:
            return result

        stdout, stderr = log_future.logs()
        parts: list[str] = []
        if stdout:
            parts.append(stdout[-100_000:])
        if stderr:
            parts.append(f"--- stderr ---\n{stderr[-100_000:]}")
        if parts:
            worker_log = "\n".join(parts)
            logger.debug(
                "SLURM job log: stdout=%d bytes, stderr=%d bytes",
                len(stdout or ""),
                len(stderr or ""),
            )
            return UnitResult(
                success=result.success,
                error=result.error,
                item_count=result.item_count,
                execution_run_ids=result.execution_run_ids,
                worker_log=worker_log,
            )
        return result
    except Exception:
        return result  # Best-effort — log files may be cleaned up


def _patch_worker_logs(
    results: list[UnitResult],
    staging_root: str,
    failure_logs_root: str | None = None,
    operation_name: str | None = None,
) -> None:
    """Write SLURM worker logs into staged parquet files before commit.

    Also appends worker stderr to failure log files when present.

    Args:
        results: Unit results that may contain a ``worker_log``.
        staging_root: Root staging directory.
        failure_logs_root: Directory for failure log files.
        operation_name: Operation name for failure log directory layout.
    """
    import polars as pl

    for result in results:
        if not result.worker_log:
            continue
        for run_id in result.execution_run_ids:
            try:
                staging_dir = _find_staging_dir(staging_root, run_id)
                if staging_dir is None:
                    continue
                parquet_path = os.path.join(staging_dir, "executions.parquet")
                if not os.path.exists(parquet_path):
                    continue
                df = pl.read_parquet(parquet_path)
                df = df.with_columns(pl.lit(result.worker_log).alias("worker_log"))
                df.write_parquet(parquet_path, compression="zstd")
            except Exception:
                logger.debug("Failed to patch worker_log for %s", run_id, exc_info=True)

            # Append worker stderr to failure log if it exists
            if not result.success and failure_logs_root and operation_name:
                _append_worker_stderr_to_failure_log(
                    failure_logs_root, run_id, operation_name, result.worker_log
                )


def _append_worker_stderr_to_failure_log(
    failure_logs_root: str,
    execution_run_id: str,
    operation_name: str,
    worker_log: str,
) -> None:
    """Append worker stderr to an existing failure log file (best-effort)."""
    try:
        # Extract stderr portion if present
        stderr_marker = "--- stderr ---\n"
        idx = worker_log.find(stderr_marker)
        stderr_content = worker_log[idx + len(stderr_marker) :] if idx >= 0 else None
        if not stderr_content:
            return

        # Find the failure log file across step directories
        for entry in os.listdir(failure_logs_root):
            step_dir = os.path.join(failure_logs_root, entry)
            if not os.path.isdir(step_dir):
                continue
            log_path = os.path.join(step_dir, f"{execution_run_id}.log")
            if os.path.exists(log_path):
                with open(log_path, "a") as f:
                    f.write(f"\n\n=== Worker Stderr ===\n{stderr_content}")
                return
    except Exception:
        logger.debug(
            "Failed to append worker stderr to failure log for %s",
            execution_run_id,
            exc_info=True,
        )


def _find_staging_dir(staging_root: str, execution_run_id: str) -> str | None:
    """Locate the staging directory for an execution run ID.

    Searches shard subdirectories using the first two characters of the
    run ID as the prefix.
    """
    # The shard_uri uses the first 2 chars of the run_id as prefix
    prefix = execution_run_id[:2]
    # Search through step directories
    for entry in os.listdir(staging_root):
        step_dir = os.path.join(staging_root, entry)
        if not os.path.isdir(step_dir):
            continue
        shard_dir = os.path.join(step_dir, prefix)
        if not os.path.isdir(shard_dir):
            continue
        candidate = os.path.join(shard_dir, execution_run_id)
        if os.path.isdir(candidate):
            return candidate
    return None
