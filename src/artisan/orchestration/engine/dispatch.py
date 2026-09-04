"""Transport-neutral execution helpers for lifecycle runners."""

from __future__ import annotations

import os

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.utils.traceback import format_error


def execute_unit(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
) -> UnitResult:
    """Execute one unit with the appropriate Artisan executor.

    Ordinary operation failures are returned as failed results so every
    submitted unit has one positionally aligned result.

    Args:
        unit: Unit of work to execute.
        runtime_env: Runtime paths and runner configuration.

    Returns:
        The execution outcome for ``unit``.

    Raises:
        RuntimeError: If the worker receives ``KeyboardInterrupt``.
    """
    try:
        env_var = runtime_env.worker_id_env_var
        worker_id = int(os.environ.get(env_var, "0")) if env_var else 0

        from artisan.execution.executors.curator import (
            is_curator_operation,
            run_curator_flow,
        )

        if is_curator_operation(unit.operation):
            result = run_curator_flow(unit, runtime_env, worker_id=worker_id)
            return UnitResult(
                success=result.success,
                error=result.error,
                item_count=len(result.artifact_ids) if result.success else 1,
                execution_run_ids=[result.execution_run_id],  # type: ignore[list-item]  # failure paths may omit a run id
            )

        from artisan.execution.executors.creator import run_creator_flow

        result = run_creator_flow(unit, runtime_env, worker_id=worker_id)
        return UnitResult(
            success=result.success,
            error=result.error,
            item_count=unit.get_batch_size() or 1,
            execution_run_ids=[result.execution_run_id],  # type: ignore[list-item]  # failure paths may omit a run id
        )
    except KeyboardInterrupt:
        msg = "Operation interrupted by SIGINT"
        raise RuntimeError(msg) from None
    except Exception as exc:
        return _failed_unit_result(exc, item_count=unit.get_batch_size() or 1)


def execute_unit_batch(
    units: list[ExecutionUnit],
    runtime_env: RuntimeEnvironment,
) -> list[UnitResult]:
    """Execute an ordered batch, returning one result per unit.

    Args:
        units: Units in submission order.
        runtime_env: Runtime paths and runner configuration.

    Returns:
        Positionally aligned unit results.
    """
    return [execute_unit(unit, runtime_env) for unit in units]


def failure_results_for_units(
    units: list[ExecutionUnit],
    error: BaseException | str,
) -> list[UnitResult]:
    """Convert a batch-level failure into one result per submitted unit.

    Args:
        units: Units affected by the failure.
        error: Exception or prepared error text describing the failure.

    Returns:
        Failed results positionally aligned with ``units``.
    """
    error_text = error if isinstance(error, str) else format_error(error)
    return [
        _failed_unit_result(error_text, item_count=unit.get_batch_size() or 1)
        for unit in units
    ]


def validate_batch_results(
    units: list[ExecutionUnit],
    results: object,
) -> list[UnitResult]:
    """Validate a worker batch result at the transport boundary.

    Malformed batches become failures for the entire submitted batch. This
    keeps positional alignment intact while allowing sibling batches to finish.

    Args:
        units: Units submitted in this batch.
        results: Value returned by the worker transport.

    Returns:
        Valid results, or one failed result per unit for a malformed response.
    """
    if not isinstance(results, list):
        type_error = TypeError(
            f"Worker batch returned {type(results).__name__}; expected list[UnitResult]"
        )
        return failure_results_for_units(units, type_error)
    if len(results) != len(units):
        cardinality_error = ValueError(
            "Worker batch returned "
            f"{len(results)} results for {len(units)} submitted units"
        )
        return failure_results_for_units(units, cardinality_error)
    for index, result in enumerate(results):
        if not isinstance(result, UnitResult):
            result_type_error = TypeError(
                f"Worker batch result {index} is {type(result).__name__}; "
                "expected UnitResult"
            )
            return failure_results_for_units(units, result_type_error)
    return results


def _failed_unit_result(
    error: BaseException | str,
    *,
    item_count: int = 1,
) -> UnitResult:
    """Build one failed unit result from an exception or error string."""
    error_text = error if isinstance(error, str) else format_error(error)
    return UnitResult(
        success=False,
        error=error_text,
        item_count=item_count,
        execution_run_ids=[],
    )
