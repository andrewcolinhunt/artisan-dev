"""Independent unit futures over one retained SDK input and shared control channel."""

from __future__ import annotations

import logging
import math
import re
import threading
import time
from collections.abc import Callable
from concurrent.futures import CancelledError, Future, TimeoutError
from typing import Protocol
from uuid import uuid4

from artisan.orchestration.runner_api import (
    CancellationAcknowledgement,
    CancellationStatus,
    CommandOwnershipError,
    InertCallResult,
    OwnershipState,
    WorkerOwner,
)
from artisan_modal.diagnostics import CallObservation, CallPhase, redacted_message
from artisan_modal.pack_control import (
    ModalPack,
    PackChannel,
    PackEntry,
    PackResult,
    UnitProgress,
)

_LOGGER = logging.getLogger("artisan_modal.call_owner")
_UNKNOWN = "Modal input ended without confirmed worker cleanup"
_POLL_SECONDS = 0.5
_CLEANUP_SECONDS = 5.0


class ModalFunctionCall(Protocol):
    def get(self) -> object: ...
    def cancel(self, *, terminate_containers: bool = False) -> None: ...


class ModalFunction(Protocol):
    def spawn(self, encoded: bytes) -> ModalFunctionCall: ...


class ModalPackOwner:
    """Retain every unit and the SDK input until all transport owners close.

    Unit cancellation requests only its core owner. SDK cancellation is reserved
    for a whole-worker close after the original cleanup deadline, never closure
    evidence. Earlier completed units survive later transport or sibling failures.
    """

    def __init__(
        self,
        function: ModalFunction,
        prepare: Callable[[], ModalPack],
        *,
        count: int,
        sensitive_values: tuple[str, ...] = (),
    ) -> None:
        if count < 1:
            msg = "Modal pack must contain at least one unit"
            raise ValueError(msg)
        self._function, self._prepare = function, prepare
        self._sensitive_values = sensitive_values
        self._owner_id = uuid4()
        self._started = time.monotonic()
        self._observations: list[CallObservation] = []
        self._call_id: str | None = None
        self._diagnostic_write_error: str | None = None
        self._lock = threading.RLock()
        self._futures: tuple[Future[bytes], ...] = tuple(Future() for _ in range(count))
        self._progress: dict[int, UnitProgress] = {}
        self._cancel_deadlines: dict[int, float] = {}
        self._cancel_unknown: set[int] = set()
        self._sent: set[int | None] = set()
        self._deadline: float | None = None
        self._unknown = False
        self._submitting = False
        self._state = OwnershipState.PENDING
        self._request: ModalPack | None = None
        self._channel: PackChannel | None = None
        self._entry: PackEntry | None = None
        self._call: ModalFunctionCall | None = None
        self._finished = threading.Event()
        self._collect_stop = threading.Event()
        self._wake = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._collector: threading.Thread | None = None
        self._cancel_thread: threading.Thread | None = None

    def start(self) -> None:
        self._observe("start")
        try:
            self._thread.start()
        except BaseException as error:
            self._mark_unknown()
            self._fail_remaining(CommandOwnershipError(_UNKNOWN))
            self._observe("failed", error=error)
            msg = "Modal pack thread startup is unresolved"
            raise RuntimeError(msg) from error

    def _run(self) -> None:
        closed = False
        try:
            self._observe("prepare")
            request = self._prepare()
            if len(request.calls) != len(self._futures):
                msg = "Prepared pack differs from its retained handle count"
                raise ValueError(msg)
            channel = PackChannel(request)
            encoded = request.model_dump_json().encode()
            with self._lock:
                self._request, self._channel = request, channel
                if self._deadline is not None:
                    raise CancelledError
            self._flush_cancellation()
            with self._lock:
                if self._deadline is not None:
                    raise CancelledError
                self._submitting = True
            self._observe("submit")
            call = self._function.spawn(encoded)
            with self._lock:
                self._call = call
                call_id = getattr(call, "object_id", None)
                if (
                    isinstance(call_id, str)
                    and len(call_id) <= 128
                    and re.fullmatch(r"fc-[A-Za-z0-9]+", call_id)
                ):
                    self._call_id = call_id
            self._collector = threading.Thread(target=self._collect, daemon=True)
            self._collector.start()
            if self._expired():
                self._flush_cancellation()
                self._request_sdk_cancel()
            self._observe("receive")
            received = call.get()
            self._observe("decode")
            if not isinstance(received, bytes):
                msg = "Modal worker returned a non-bytes exchange"
                raise TypeError(msg)
            result = PackResult.model_validate_json(received)
            result.verify(request)
            self._bind_entry(result)
            for progress in result.results:
                self._receive(progress)
            closed = result.ownership == OwnershipState.CLOSED
            if not closed:
                raise CommandOwnershipError(result.failure or _UNKNOWN)
            self._observe("complete")
        except BaseException as error:
            self._observe("failed", error=error)
            with self._lock:
                closed = not self._submitting
            if not closed:
                self._mark_unknown()
            failure = (
                CommandOwnershipError(_UNKNOWN)
                if not closed
                else CancelledError()
                if isinstance(error, CancelledError)
                else RuntimeError("Modal pack failed before submission")
            )
            self._fail_remaining(failure)
        finally:
            self._collect_stop.set()
            self._wake.set()
            deadline = self._cleanup_deadline()
            if self._collector is not None and self._collector.ident is not None:
                self._collector.join(max(0, deadline - time.monotonic()))
                closed &= not self._collector.is_alive()
            if (
                self._cancel_thread is not None
                and self._cancel_thread.ident is not None
            ):
                self._cancel_thread.join(max(0, deadline - time.monotonic()))
                closed &= not self._cancel_thread.is_alive()
            with self._lock:
                closed &= not self._unknown
            if closed and self._channel is not None:
                try:
                    self._channel.remove()
                except BaseException as error:
                    closed = False
                    self._observe("failed", error=error)
            with self._lock:
                if self._deadline is not None and time.monotonic() >= self._deadline:
                    self._unknown = True
                self._state = (
                    OwnershipState.CLOSED
                    if closed and not self._unknown
                    else OwnershipState.UNKNOWN
                )
                self._unknown |= self._state == OwnershipState.UNKNOWN
                if self._unknown:
                    self._fail_remaining(CommandOwnershipError(_UNKNOWN))
                self._finished.set()

    def _expired(self) -> bool:
        with self._lock:
            return self._deadline is not None and time.monotonic() >= self._deadline

    def _cleanup_deadline(self) -> float:
        with self._lock:
            ordinary = time.monotonic() + _CLEANUP_SECONDS
            return (
                min(ordinary, self._deadline)
                if self._deadline is not None
                else ordinary
            )

    def _bind_entry(self, entry: PackEntry) -> None:
        value = PackEntry(
            pack_id=entry.pack_id,
            request_sha256=entry.request_sha256,
            entry_id=entry.entry_id,
        )
        with self._lock:
            if self._entry is not None and self._entry != value:
                msg = "Modal SDK result and progress have different worker entries"
                raise ValueError(msg)
            self._entry = value

    def _receive(self, progress: UnitProgress) -> None:
        with self._lock:
            previous = self._progress.get(progress.index)
            if previous is not None:
                if previous != progress:
                    msg = "Modal unit progress changed after publication"
                    raise ValueError(msg)
                return
            if progress.outcome is not None and any(
                item.outcome is not None
                and item.outcome.entry_id == progress.outcome.entry_id
                for item in self._progress.values()
            ):
                msg = "Modal units reused an actual core entry"
                raise ValueError(msg)
            self._progress[progress.index] = progress
            future = self._futures[progress.index]
            if future.done():
                return
            outcome = progress.outcome
            deadline = self._cancel_deadlines.get(progress.index)
            if deadline is not None and time.monotonic() >= deadline:
                self._cancel_unknown.add(progress.index)
            if progress.index in self._cancel_unknown or (
                outcome is not None and outcome.ownership != OwnershipState.CLOSED
            ):
                self._unknown = True
                future.set_exception(CommandOwnershipError(_UNKNOWN))
            elif progress.skipped or (outcome is not None and outcome.cancelled):
                future.set_exception(CancelledError())
            elif outcome is not None and outcome.reply is not None:
                future.set_result(outcome.reply)
            else:
                future.set_exception(RuntimeError("Modal worker infrastructure failed"))
        if outcome is not None:
            self._observe("core_result", outcome=outcome)

    def _flush_cancellation(self) -> None:
        assert self._channel is not None
        with self._lock:
            deadlines = dict[int | None, float](self._cancel_deadlines.items())
            if self._deadline is not None or self._unknown:
                deadlines[None] = (
                    self._deadline if self._deadline is not None else time.monotonic()
                )
        for index, deadline in deadlines.items():
            if index not in self._sent:
                self._channel.cancel(index, deadline - time.monotonic())
                self._sent.add(index)

    def _collect(self) -> None:
        assert self._channel is not None
        try:
            while not self._collect_stop.is_set():
                self._flush_cancellation()
                entry = self._channel.entry()
                if entry is not None:
                    self._bind_entry(entry)
                    for index in range(len(self._futures)):
                        progress = self._channel.progress(entry, index)
                        if progress is not None:
                            self._receive(progress)
                self._wake.wait(_POLL_SECONDS)
                self._wake.clear()
        except BaseException as error:
            self._mark_unknown()
            self._observe("failed", error=error)
            self._fail_remaining(CommandOwnershipError(_UNKNOWN))

    def _mark_unknown(self) -> None:
        with self._lock:
            self._unknown = True
            self._state = OwnershipState.UNKNOWN
        self._wake.set()

    def _fail_remaining(self, error: BaseException) -> None:
        with self._lock:
            for future in self._futures:
                if not future.done():
                    future.set_exception(error)

    def result(self, index: int, timeout: float | None = None) -> bytes:
        return self._futures[index].result(timeout)

    def done(self, index: int) -> bool:
        return self._futures[index].done()

    def outcome(self, index: int) -> UnitProgress | None:
        with self._lock:
            return self._progress.get(index)

    def ownership_state(self) -> OwnershipState:
        with self._lock:
            if self._unknown:
                return OwnershipState.UNKNOWN
            return self._state if self._finished.is_set() else OwnershipState.PENDING

    def cancel(self, index: int) -> CancellationAcknowledgement:
        with self._lock:
            if self._futures[index].done():
                progress = self._progress.get(index)
                if (
                    index in self._cancel_unknown
                    or progress is None
                    or (
                        progress.outcome is not None
                        and progress.outcome.ownership != OwnershipState.CLOSED
                    )
                ):
                    return CancellationAcknowledgement(
                        CancellationStatus.UNKNOWN, _UNKNOWN
                    )
                return CancellationAcknowledgement(CancellationStatus.REJECTED)
            deadline = self._cancel_deadlines.setdefault(
                index, time.monotonic() + _CLEANUP_SECONDS
            )
        self._wake.set()
        try:
            self._futures[index].result(max(0, deadline - time.monotonic()))
        except TimeoutError:
            with self._lock:
                self._cancel_unknown.add(index)
            self._mark_unknown()
            return CancellationAcknowledgement(CancellationStatus.UNKNOWN, _UNKNOWN)
        except BaseException:
            pass
        with self._lock:
            progress = self._progress.get(index)
            if (
                progress is None
                or index in self._cancel_unknown
                or (
                    progress.outcome is not None
                    and progress.outcome.ownership != OwnershipState.CLOSED
                )
            ):
                return CancellationAcknowledgement(CancellationStatus.UNKNOWN, _UNKNOWN)
            cancelled = progress.skipped or (
                progress.outcome is not None and progress.outcome.cancelled
            )
        return CancellationAcknowledgement(
            CancellationStatus.CONFIRMED if cancelled else CancellationStatus.REJECTED
        )

    def request_close(self, deadline: float) -> None:
        if not math.isfinite(deadline):
            msg = "Modal close requires a finite local deadline"
            raise ValueError(msg)
        with self._lock:
            if self._finished.is_set() and self._state == OwnershipState.CLOSED:
                return
            self._deadline = (
                min(self._deadline, deadline)
                if self._deadline is not None
                else deadline
            )
            deadline = self._deadline
        self._wake.set()
        self._finished.wait(max(0, deadline - time.monotonic()))
        if self.ownership_state() != OwnershipState.CLOSED:
            self._mark_unknown()
            self._request_sdk_cancel()

    def _request_sdk_cancel(self) -> None:
        with self._lock:
            if self._call is None or self._cancel_thread is not None:
                return
            self._cancel_thread = threading.Thread(
                target=self._cancel_sdk, args=(self._call,), daemon=True
            )
            thread = self._cancel_thread
        try:
            thread.start()
        except BaseException as error:
            self._observe("cancel_failed", error=error)
            self._mark_unknown()

    def _cancel_sdk(self, call: ModalFunctionCall) -> None:
        self._observe("cancel_requested")
        try:
            if self._channel is not None:
                self._channel.cancel(None, 0.0)
            call.cancel(terminate_containers=False)
        except BaseException as error:
            self._observe("cancel_failed", error=error)
        else:
            self._observe("cancel_returned")

    def _observe(
        self,
        phase: CallPhase,
        *,
        error: BaseException | None = None,
        outcome: InertCallResult | None = None,
    ) -> None:
        try:
            self._record_observation(phase, error=error, outcome=outcome)
        except Exception:
            with self._lock:
                self._diagnostic_write_error = "Modal diagnostic observation failed"

    def _record_observation(
        self,
        phase: CallPhase,
        *,
        error: BaseException | None = None,
        outcome: InertCallResult | None = None,
    ) -> None:
        with self._lock:
            if len(self._observations) >= 64:
                return
            event = CallObservation(
                owner_id=self._owner_id,
                sequence=len(self._observations),
                phase=phase,
                elapsed_seconds=time.monotonic() - self._started,
                call_id=self._call_id,
                error_type=(
                    redacted_message(type(error).__name__, self._sensitive_values)[:128]
                    if error is not None
                    else None
                ),
                message=(
                    redacted_message(str(error), self._sensitive_values)
                    if error is not None
                    else redacted_message(outcome.error, self._sensitive_values)
                    if outcome is not None and outcome.error is not None
                    else None
                ),
                core_ownership=outcome.ownership if outcome is not None else None,
                core_entry_id=outcome.entry_id if outcome is not None else None,
                core_retained_working=outcome.retained_working
                if outcome is not None
                else None,
            )
            self._observations.append(event)
        _LOGGER.info("Modal call observation", extra={"modal_event": event})

    @property
    def diagnostic_write_error(self) -> str | None:
        """Report observer failure without changing scientific/cleanup authority."""
        with self._lock:
            return self._diagnostic_write_error

    @property
    def observations(self) -> tuple[CallObservation, ...]:
        """Return redacted phase evidence independently of the result future."""
        with self._lock:
            return tuple(self._observations)


def modal_owner_observations(owner: WorkerOwner) -> tuple[CallObservation, ...]:
    """Read a Modal owner's bounded diagnostic snapshot without polling or waiting.

    Observations do not establish cleanup. Check the owner's ownership_state()
    separately; only CLOSED confirms that its required cleanup has finished.

    Args:
        owner: The worker owner returned by the Modal runner.

    Returns:
        Up to 64 frozen, redacted observations in their recorded order. Later
        observations do not change a previously returned tuple.

    Raises:
        TypeError: If the owner is not a Modal worker owner.
    """
    if not isinstance(owner, ModalPackOwner):
        msg = "Expected a Modal worker owner"
        raise TypeError(msg)
    return owner.observations
