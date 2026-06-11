"""Endpoint execute router — ship the execute phase over HTTP to Modal."""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any

from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.tool_endpoint.client import call_endpoint, cancel_scope
from artisan.schemas.specs.input_models import ExecuteInput

_CANCEL_POLL_SECONDS = 1.0
"""Watcher poll interval — soft-cancel detection latency is one poll."""


class EndpointExecuteRouter(ExecuteRouter):
    """Ship the execute phase to the op's deployed tool endpoint over HTTP.

    One thread per artifact, bounded by ``max_concurrent_calls`` —
    blocking HTTP I/O releases the GIL; Modal's container pool is the
    real parallelism. Per-artifact failures land as Exception entries
    (the batch contract). A watcher thread turns the orchestrator's
    cancel sentinel into the cancel event every poll loop observes.

    Args:
        cancel_check: Returns True once the orchestrator has requested
            cancellation (a staging-FS existence check built by the
            lifecycle). None disables soft cancel (composite-internal
            lifecycles, which carry no step_run_id).
        max_concurrent_calls: Client-side fan-out cap. Artifacts beyond
            the cap queue in the executor; results stay positionally
            aligned.
    """

    def __init__(
        self,
        cancel_check: Callable[[], bool] | None = None,
        max_concurrent_calls: int = 64,
    ) -> None:
        self._cancel_check = cancel_check
        self._max_concurrent_calls = max_concurrent_calls
        self._cancel = threading.Event()

    def route_execute(
        self,
        operation: Any,
        execute_inputs: list[ExecuteInput],
        sandbox_root: str,
    ) -> list[Any]:
        with self._watch_cancel():
            if len(execute_inputs) <= 1:
                return [self._call_one(operation, ei) for ei in execute_inputs]
            workers = min(self._max_concurrent_calls, len(execute_inputs))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [
                    pool.submit(self._call_one, operation, ei) for ei in execute_inputs
                ]
                return [f.result() for f in futures]

    @contextmanager
    def _watch_cancel(self) -> Iterator[None]:
        """Run a daemon watcher that sets the cancel event on the sentinel."""
        if self._cancel_check is None:
            yield
            return
        stop = threading.Event()

        def _watch() -> None:
            while not stop.wait(_CANCEL_POLL_SECONDS):
                if self._cancel_check():
                    self._cancel.set()
                    return

        threading.Thread(target=_watch, daemon=True).start()
        try:
            yield
        finally:
            stop.set()

    def _call_one(self, operation: Any, execute_input: ExecuteInput) -> Any:
        try:
            with cancel_scope(self._cancel):
                call_endpoint(operation, execute_input)
                return None
        except Exception as exc:
            return exc
