"""Prefect task and flow factories for worker dispatch.

Provides the ``execute_unit_task`` Prefect task, unit serialization
helpers, and result collection with SLURM log capture.
"""

from __future__ import annotations

import logging
import pickle
from pathlib import Path

from prefect import task

from artisan.execution.models.execution_chain import ExecutionChain
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.utils.errors import format_error

logger = logging.getLogger(__name__)


def _save_units(
    units: list[ExecutionUnit | ExecutionChain],
    staging_root: Path,
    step_number: int,
) -> Path:
    """Serialize execution units to a pickle file for Prefect dispatch."""
    path = staging_root / "_dispatch" / f"step_{step_number}_units.pkl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(units, f, protocol=pickle.HIGHEST_PROTOCOL)
    return path


def _load_units(path: Path) -> list[ExecutionUnit | ExecutionChain]:
    """Deserialize execution units from a pickle file."""
    with open(path, "rb") as f:
        return pickle.load(f)


@task
def execute_unit_task(
    unit: ExecutionUnit | ExecutionChain,
    runtime_env: RuntimeEnvironment,
) -> dict:
    """Execute a single unit or chain, routing to the appropriate executor.

    Args:
        unit: Batch of artifacts to process, or a chain of operations.
        runtime_env: Runtime paths and backend configuration.

    Returns:
        Dict with keys ``success``, ``error``, ``item_count``, and
        ``execution_run_ids``.
    """
    try:
        import os

        # Get worker_id from backend-specific environment variable
        env_var = runtime_env.worker_id_env_var
        worker_id = int(os.environ.get(env_var, 0)) if env_var else 0

        # Route chain to chain executor
        if isinstance(unit, ExecutionChain):
            from artisan.execution.executors.chain import run_creator_chain

            result = run_creator_chain(unit, runtime_env, worker_id=worker_id)
            first_unit = unit.operations[0]
            return {
                "success": result.success,
                "error": result.error,
                "item_count": first_unit.get_batch_size() or 1,
                "execution_run_ids": [result.execution_run_id],
            }

        from artisan.execution.executors.curator import (
            is_curator_operation,
            run_curator_flow,
        )

        # Route to appropriate executor based on operation type
        if is_curator_operation(unit.operation):
            result = run_curator_flow(unit, runtime_env, worker_id=worker_id)

            return {
                "success": result.success,
                "error": result.error,
                "item_count": len(result.artifact_ids) if result.success else 1,
                "execution_run_ids": [result.execution_run_id],
            }
        # Creator ops return single StagingResult
        from artisan.execution.executors.creator import run_creator_flow

        result = run_creator_flow(unit, runtime_env, worker_id=worker_id)
        return {
            "success": result.success,
            "error": result.error,
            "item_count": unit.get_batch_size() or 1,
            "execution_run_ids": [result.execution_run_id],
        }
    except Exception as exc:
        return {
            "success": False,
            "error": format_error(exc),
            "item_count": 1,
            "execution_run_ids": [],
        }


def _collect_results(futures: list) -> list[dict]:
    """Collect results from Prefect futures, converting exceptions to failure dicts."""
    results = []
    for f in futures:
        try:
            results.append(f.result())
        except Exception as exc:
            logger.error(
                "Future raised during result collection: %s: %s",
                type(exc).__name__,
                exc,
            )
            results.append(
                {
                    "success": False,
                    "error": format_error(exc),
                    "item_count": 1,
                    "execution_run_ids": [],
                }
            )

    logger.info("Collected results from %d futures", len(results))

    # Best-effort SLURM log capture
    for future, result in zip(futures, results, strict=False):
        _capture_slurm_logs(future, result)

    return results


def _capture_slurm_logs(future: object, result: dict) -> None:
    """Extract SLURM worker stdout/stderr from a future into the result dict.

    No-op for non-SLURM futures.
    """
    try:
        # Get the loggable future: direct or via parent batch future
        log_future = None
        if hasattr(future, "logs"):
            log_future = future
        elif hasattr(future, "slurm_job_future"):
            log_future = future.slurm_job_future

        if log_future is None:
            return

        stdout, stderr = log_future.logs()
        parts: list[str] = []
        if stdout:
            parts.append(stdout[-100_000:])
        if stderr:
            parts.append(f"--- stderr ---\n{stderr[-100_000:]}")
        if parts:
            result["worker_log"] = "\n".join(parts)
            logger.debug(
                "SLURM job log: stdout=%d bytes, stderr=%d bytes",
                len(stdout or ""),
                len(stderr or ""),
            )
        if hasattr(log_future, "slurm_job_id"):
            result["slurm_job_id"] = log_future.slurm_job_id
    except Exception:
        pass  # Best-effort — log files may be cleaned up


def _patch_worker_logs(
    results: list[dict],
    staging_root: Path,
    failure_logs_root: Path | None = None,
    operation_name: str | None = None,
) -> None:
    """Write SLURM worker logs into staged parquet files before commit.

    Also appends worker stderr to failure log files when present.

    Args:
        results: Result dicts that may contain a ``worker_log`` key.
        staging_root: Root staging directory.
        failure_logs_root: Directory for failure log files.
        operation_name: Operation name for failure log directory layout.
    """
    import polars as pl

    for result in results:
        worker_log = result.get("worker_log")
        if not worker_log:
            continue
        for run_id in result.get("execution_run_ids", []):
            try:
                staging_dir = _find_staging_dir(staging_root, run_id)
                if staging_dir is None:
                    continue
                parquet_path = staging_dir / "executions.parquet"
                if not parquet_path.exists():
                    continue
                df = pl.read_parquet(parquet_path)
                df = df.with_columns(pl.lit(worker_log).alias("worker_log"))
                df.write_parquet(parquet_path, compression="zstd")
            except Exception:
                logger.debug("Failed to patch worker_log for %s", run_id, exc_info=True)

            # Append worker stderr to failure log if it exists
            if not result.get("success", True) and failure_logs_root and operation_name:
                _append_worker_stderr_to_failure_log(
                    failure_logs_root, run_id, operation_name, worker_log
                )


def _append_worker_stderr_to_failure_log(
    failure_logs_root: Path,
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
        for step_dir in failure_logs_root.iterdir():
            if not step_dir.is_dir():
                continue
            log_path = step_dir / f"{execution_run_id}.log"
            if log_path.exists():
                with log_path.open("a") as f:
                    f.write(f"\n\n=== Worker Stderr ===\n{stderr_content}")
                return
    except Exception:
        logger.debug(
            "Failed to append worker stderr to failure log for %s",
            execution_run_id,
            exc_info=True,
        )


def _find_staging_dir(staging_root: Path, execution_run_id: str) -> Path | None:
    """Locate the staging directory for an execution run ID.

    Searches shard subdirectories using the first two characters of the
    run ID as the prefix.
    """
    # The shard_path uses the first 2 chars of the run_id as prefix
    prefix = execution_run_id[:2]
    # Search through step directories
    for step_dir in staging_root.iterdir():
        if not step_dir.is_dir():
            continue
        shard_dir = step_dir / prefix
        if not shard_dir.is_dir():
            continue
        candidate = shard_dir / execution_run_id
        if candidate.is_dir():
            return candidate
    return None
