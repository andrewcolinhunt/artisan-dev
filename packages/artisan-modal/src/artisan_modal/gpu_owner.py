"""Retain a fixed GPU measurement input through SDK submission and collection."""

from __future__ import annotations

import contextlib
import math
import threading
import time
from concurrent.futures import Future

from artisan.orchestration.runner_api import (
    CommandOwnershipError,
    NativeGPUSoftware,
    OwnershipState,
)
from artisan_modal.gpu import GPUProbe, GPUProbeReply
from artisan_modal.pack_owner import ModalFunction, ModalFunctionCall

_UNKNOWN = "Modal GPU measurement ended without confirmed completion"


class GPUProbeOwner:
    """Own one non-authored measurement; SDK cancellation never proves closure."""

    def __init__(self, function: ModalFunction, request: GPUProbe) -> None:
        self._function, self._request = function, request
        self._lock = threading.RLock()
        self._future: Future[NativeGPUSoftware] = Future()
        self._finished = threading.Event()
        self._state = OwnershipState.PENDING
        self._deadline: float | None = None
        self._call: ModalFunctionCall | None = None
        self._cancel_thread: threading.Thread | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        try:
            self._thread.start()
        except BaseException:
            with self._lock:
                self._state = OwnershipState.UNKNOWN
                self._finished.set()
            raise

    def _run(self) -> None:
        submitted = False
        closed = False
        software = None
        error: BaseException | None = None
        try:
            encoded = self._request.model_dump_json().encode()
            with self._lock:
                if self._deadline is not None:
                    msg = "GPU measurement cancelled before submission"
                    raise RuntimeError(msg)
                submitted = True
            call = self._function.spawn(encoded)
            with self._lock:
                self._call = call
                cancelled = self._deadline is not None
            if cancelled:
                self._cancel_sdk()
            received = call.get()
            if not isinstance(received, bytes):
                msg = "GPU measurement returned a non-bytes exchange"
                raise TypeError(msg)
            reply = GPUProbeReply.model_validate_json(received)
            reply.verify(self._request)
            software = reply.software
            error = RuntimeError(reply.error) if reply.error is not None else None
            closed = True
        except BaseException:
            closed = not submitted
            error = RuntimeError("Modal GPU measurement failed")
        finally:
            with self._lock:
                closed &= self._state != OwnershipState.UNKNOWN
                closed &= self._deadline is None or time.monotonic() < self._deadline
                closed &= (
                    self._cancel_thread is None or not self._cancel_thread.is_alive()
                )
                self._state = (
                    OwnershipState.CLOSED if closed else OwnershipState.UNKNOWN
                )
                if not closed:
                    error = CommandOwnershipError(_UNKNOWN)
                if error is not None:
                    self._future.set_exception(error)
                else:
                    assert software is not None
                    self._future.set_result(software)
                self._finished.set()

    def result(self, timeout: float) -> NativeGPUSoftware:
        return self._future.result(timeout)

    def ownership_state(self) -> OwnershipState:
        with self._lock:
            return self._state

    def request_close(self, deadline: float) -> None:
        if not math.isfinite(deadline):
            msg = "GPU measurement close requires a finite local deadline"
            raise ValueError(msg)
        with self._lock:
            if self._state == OwnershipState.CLOSED:
                return
            self._deadline = (
                min(self._deadline, deadline)
                if self._deadline is not None
                else deadline
            )
            deadline = self._deadline
        self._finished.wait(max(0.0, deadline - time.monotonic()))
        with self._lock:
            if self.ownership_state() == OwnershipState.CLOSED:
                return
            self._state = OwnershipState.UNKNOWN
        self._cancel_sdk()

    def _cancel_sdk(self) -> None:
        with self._lock:
            if self._call is None or self._cancel_thread is not None:
                return
            self._cancel_thread = threading.Thread(
                target=self._cancel, args=(self._call,), daemon=True
            )
            thread = self._cancel_thread
        try:
            thread.start()
        except BaseException:
            with self._lock:
                self._state = OwnershipState.UNKNOWN

    @staticmethod
    def _cancel(call: ModalFunctionCall) -> None:
        with contextlib.suppress(BaseException):
            call.cancel(terminate_containers=False)
