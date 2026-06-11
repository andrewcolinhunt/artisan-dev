"""In-process dispatch handle for tool-endpoint (modal) steps.

Replaces the retired child-process batch handle: ``execute()`` is blocking
HTTP I/O against the deployed endpoint (the GIL is released), so units and
their per-artifact calls fan out on threads — Modal's container pool is the
parallelism. Each unit follows prep → per-artifact execute → post → record.
Cancellation threads into every poll loop via ``cancel_scope`` and posts
``/cancel`` server-side.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

from artisan.execution.executors.creator import (
    _ExecuteFailure,
    _PostprocessFailure,
)
from artisan.execution.executors.creator_phases import (
    _extract_inputs,
    post_unit,
    prep_unit,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.staging.recorder import (
    _read_tool_output,
    record_execution_failure,
    record_execution_success,
)
from artisan.execution.tool_endpoint.client import call_endpoint, cancel_scope
from artisan.orchestration.engine.lifecycle_router import LifecycleRouter, _RouterState
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.specs.input_models import ExecuteInput
from artisan.utils.errors import format_error
from artisan.utils.hashing import serialize_params
from artisan.utils.timing import phase_timer

logger = logging.getLogger(__name__)


class ToolEndpointDispatchHandle(LifecycleRouter):
    """Per-artifact endpoint dispatch, in-process and cancel-aware.

    Args:
        max_workers: Thread pool size for cross-unit parallelism. Each
            unit additionally fans out one thread per artifact.
    """

    def __init__(self, max_workers: int = 4) -> None:
        super().__init__()
        self._max_workers = max_workers
        self._cancel = threading.Event()

    def dispatch(  # type: ignore[override]  # narrower than base: endpoint handle only accepts ExecutionUnit, not composites
        self,
        units: list[ExecutionUnit],
        runtime_env: RuntimeEnvironment,
    ) -> None:
        """Run units on a thread pool in a background thread."""
        self._assert_idle()
        self._state = _RouterState.DISPATCHED

        def _run() -> list[UnitResult]:
            with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
                futures = [
                    pool.submit(self._process_unit, unit, runtime_env) for unit in units
                ]
                results: list[UnitResult] = []
                for future, unit in zip(futures, units, strict=True):
                    try:
                        results.append(future.result())
                    except Exception as exc:
                        logger.error("Unit dispatch raised: %s", format_error(exc))
                        results.append(
                            UnitResult(
                                success=False,
                                error=format_error(exc),
                                item_count=unit.get_batch_size() or 1,
                                execution_run_ids=[],
                            )
                        )
                return results

        self._start_background(_run)

    def cancel(self) -> None:
        """Signal every in-flight poll loop; they POST /cancel server-side."""
        self._cancel.set()

    # ------------------------------------------------------------------
    # Per-unit lifecycle (structure mirrors the retired batch handle)
    # ------------------------------------------------------------------

    def _process_unit(
        self, unit: ExecutionUnit, runtime_env: RuntimeEnvironment
    ) -> UnitResult:
        """Process one unit: prep → per-artifact execute → post → record."""
        timings: dict[str, Any] = {}
        total_start = time.perf_counter()
        original_inputs = _extract_inputs(unit)
        operation = unit.operation

        # --- prep ---
        try:
            prepped = prep_unit(unit, runtime_env)
        except Exception as exc:
            logger.error("Prep failed for unit: %s", format_error(exc))
            return UnitResult(
                success=False,
                error=format_error(exc),
                item_count=unit.get_batch_size() or 1,
                execution_run_ids=[],
            )

        def _record_failure(error: str, tool_output: str | None) -> UnitResult:
            record_execution_failure(
                execution_context=prepped.execution_context,
                error=error,
                inputs=original_inputs,
                timestamp_end=datetime.now(UTC),
                params=serialize_params(operation),
                user_overrides=unit.user_overrides,
                tool_output=tool_output,
                failure_logs_root=runtime_env.failure_logs_root,
            )
            return UnitResult(
                success=False,
                error=error,
                item_count=unit.get_batch_size() or 1,
                execution_run_ids=[prepped.execution_run_id],
            )

        # --- execute (per-artifact endpoint calls, threaded) ---
        try:
            with phase_timer("execute", prepped.timings):
                raw_results = self._execute_artifacts(
                    prepped.operation, prepped.artifact_execute_inputs
                )
        except Exception as exc:
            return _record_failure(
                format_error(exc), _read_tool_output(prepped.log_path)
            )

        # Per-artifact failures land as exception entries (the batch
        # contract); surface them here — downstream _reassemble_results
        # silently filters them, which masks the real error as an
        # empty-artifact validation failure.
        failures = [r for r in raw_results if isinstance(r, Exception)]
        if failures:
            msg = (
                f"{len(failures)}/{len(raw_results)} artifact executions "
                f"failed; first: {format_error(failures[0])}"
            )
            return _record_failure(msg, _read_tool_output(prepped.log_path))

        # --- post ---
        try:
            lifecycle_result = post_unit(prepped, raw_results, runtime_env)
            timings.update(prepped.timings)
        except (_PostprocessFailure, _ExecuteFailure) as exc:
            tool_output = getattr(exc, "tool_output", None)
            if tool_output is None:
                tool_output = _read_tool_output(prepped.log_path)
            return _record_failure(str(exc), tool_output)
        except Exception as exc:
            return _record_failure(
                format_error(exc), _read_tool_output(prepped.log_path)
            )

        # --- record success ---
        with phase_timer("record", timings):
            record_execution_success(
                execution_context=prepped.execution_context,
                artifacts=lifecycle_result.artifacts,
                lineage_edges=lifecycle_result.edges,
                inputs=original_inputs,
                timestamp_end=datetime.now(UTC),
                params=serialize_params(operation),
                result_metadata={"timings": timings},
                user_overrides=unit.user_overrides,
            )

        timings["total"] = round(time.perf_counter() - total_start, 4)
        return UnitResult(
            success=True,
            error=None,
            item_count=unit.get_batch_size() or 1,
            execution_run_ids=[prepped.execution_run_id],
        )

    def _execute_artifacts(
        self, operation: Any, execute_inputs: list[ExecuteInput]
    ) -> list[Any]:
        """Run per-artifact execute() concurrently; failures land per index."""
        if len(execute_inputs) <= 1:
            return [self._execute_one(operation, ei) for ei in execute_inputs]
        with ThreadPoolExecutor(max_workers=len(execute_inputs)) as pool:
            futures = [
                pool.submit(self._execute_one, operation, ei) for ei in execute_inputs
            ]
            return [f.result() for f in futures]

    def _execute_one(self, operation: Any, execute_input: ExecuteInput) -> Any:
        """One endpoint call under the handle's cancel scope.

        Returns the exception instance on failure (``post_unit`` surfaces
        per-artifact failures), mirroring the batch-execute contract.
        """
        try:
            with cancel_scope(self._cancel):
                call_endpoint(operation, execute_input)
                return None
        except Exception as exc:
            return exc
