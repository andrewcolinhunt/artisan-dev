"""Sequential SDK-owned science with sibling-safe control and retained uncertainty."""

from __future__ import annotations

import contextlib
import threading
import time
from uuid import uuid4

from pydantic import TypeAdapter

from artisan.orchestration.runner_api import (
    CommandOwnershipError,
    InertCallControl,
    InertCallResult,
    OwnershipState,
)
from artisan_modal.diagnostics import redacted_message
from artisan_modal.gpu import GPUProbe, probe_gpu
from artisan_modal.pack_control import (
    ModalPack,
    PackChannel,
    PackEntry,
    PackResult,
    UnitProgress,
)
from artisan_modal.transport import execute_call

_POLL_SECONDS = 0.5
_CLEANUP_SECONDS = 5.0
_REQUEST: TypeAdapter[ModalPack | GPUProbe] = TypeAdapter(ModalPack | GPUProbe)
_lease = threading.Lock()
_retained: list[_PackExecution] = []


class _PackExecution:
    def __init__(
        self, request: ModalPack, channel: PackChannel, entry: PackEntry
    ) -> None:
        self.request, self.channel, self.entry = request, channel, entry
        self.condition = threading.Condition()
        self.results: dict[int, UnitProgress] = {}
        self.publishing: set[int] = set()
        self.active: tuple[int, InertCallControl] | None = None
        self.cancelled: dict[int | None, float] = {}
        self.failed = False
        self.failure: str | None = None
        self.stop = threading.Event()
        self.monitor = threading.Thread(target=self._monitor, daemon=True)

    def _progress(self, index: int, outcome: InertCallResult | None) -> UnitProgress:
        return UnitProgress(
            **self.entry.model_dump(),
            index=index,
            payload_sha256=self.request.calls[index].header.payload_sha256,
            skipped=outcome is None,
            outcome=outcome,
        )

    def _publish_skip(self, index: int) -> None:
        with self.condition:
            if (
                index in self.results
                or index in self.publishing
                or (self.active is not None and self.active[0] == index)
            ):
                return
            self.publishing.add(index)
        result = self._progress(index, None)
        self.channel.publish(result)
        with self.condition:
            self.results[index] = result
            self.publishing.remove(index)
            self.condition.notify_all()

    def _poll(self) -> None:
        for index in (None, *range(len(self.request.calls))):
            if index not in self.cancelled:
                record = self.channel.cancellation(index)
                if record is not None:
                    self.cancelled[index] = time.monotonic() + record.cleanup_seconds
        with self.condition:
            active = self.active
            failed = self.failed
        if active is not None:
            deadlines = [
                self.cancelled[key]
                for key in (None, active[0])
                if key in self.cancelled
            ]
            if deadlines or failed:
                active[1].request_close(
                    min(deadlines) if deadlines else time.monotonic() + _CLEANUP_SECONDS
                )
        for index in range(len(self.request.calls)):
            if None in self.cancelled or index in self.cancelled:
                self._publish_skip(index)

    def _monitor(self) -> None:
        try:
            while not self.stop.is_set():
                self._poll()
                self.stop.wait(_POLL_SECONDS)
        except BaseException as error:
            self.record_failure(error)
            with self.condition:
                active = self.active
                self.condition.notify_all()
            if active is not None:
                with contextlib.suppress(BaseException):
                    active[1].request_close(time.monotonic() + _CLEANUP_SECONDS)

    def record_failure(self, error: BaseException) -> None:
        try:
            message = redacted_message(
                f"{type(error).__name__}: {error}", self.request.sensitive_values
            )
        except Exception:
            message = "Modal worker infrastructure diagnostic unavailable"
        with self.condition:
            self.failed = True
            if self.failure is None:
                self.failure = message

    def run(self) -> None:
        self._poll()
        self.monitor.start()
        for index, request in enumerate(self.request.calls):
            with self.condition:
                while index in self.publishing and not self.failed:
                    self.condition.wait()
                if self.failed:
                    return
                if index in self.results:
                    continue
                control = InertCallControl()
                self.active = (index, control)
                deadlines = [
                    self.cancelled[key]
                    for key in (None, index)
                    if key in self.cancelled
                ]
            if deadlines:
                control.request_close(min(deadlines))
            outcome = execute_call(request, control)
            result = self._progress(index, outcome)
            self.channel.publish(result)
            with self.condition:
                self.results[index] = result
                self.active = None
            if outcome.ownership != OwnershipState.CLOSED:
                return
            if outcome.reply is None and not outcome.cancelled:
                for remaining in range(index + 1, len(self.request.calls)):
                    self._publish_skip(remaining)
                return

    def finish(self) -> PackResult:
        self.stop.set()
        with self.condition:
            active = self.active
        if active is not None:
            try:
                active[1].request_close(time.monotonic() + _CLEANUP_SECONDS)
            except BaseException as error:
                self.record_failure(error)
        if self.monitor.ident is not None:
            deadline = min(
                time.monotonic() + _CLEANUP_SECONDS,
                self.cancelled.get(None, float("inf")),
            )
            self.monitor.join(max(0.0, deadline - time.monotonic()))
        with self.condition:
            results = tuple(self.results[index] for index in sorted(self.results))
            closed = (
                not self.failed
                and not self.monitor.is_alive()
                and self.active is None
                and len(results) == len(self.request.calls)
                and all(
                    result.outcome is None
                    or result.outcome.ownership == OwnershipState.CLOSED
                    for result in results
                )
            )
        return PackResult(
            **self.entry.model_dump(),
            results=results,
            failure=self.failure,
            ownership=OwnershipState.CLOSED if closed else OwnershipState.UNKNOWN,
        )


def invoke_pack(encoded: bytes) -> bytes:
    """Run an ordered pack; SDK cancellation propagates after bounded core closure.

    The shared store is a transport channel only. A poisoned input keeps its
    process lease, preventing a warm container from starting further science.
    """
    request = _REQUEST.validate_json(encoded)
    if not _lease.acquire(blocking=False):
        msg = "Modal worker already has an active or unresolved pack"
        raise CommandOwnershipError(msg)
    execution = None
    closed = False
    probe_started = False
    try:
        if isinstance(request, GPUProbe):
            probe_started = True
            reply = probe_gpu(request)
            closed = True
            return reply.model_dump_json().encode()
        channel = PackChannel(request)
        entry = channel.claim(uuid4())
        execution = _PackExecution(request, channel, entry)
        try:
            execution.run()
        except Exception as error:
            execution.record_failure(error)
        finally:
            result = execution.finish()
            closed = result.ownership == OwnershipState.CLOSED
        return result.model_dump_json().encode()
    finally:
        if closed or (execution is None and not probe_started):
            _lease.release()
        elif execution is not None:
            _retained.append(execution)
