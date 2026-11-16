"""Main-thread signal ownership with cancellation outside Python signal handlers."""

from __future__ import annotations

import signal
import threading
import time
import weakref
from collections.abc import Callable
from queue import SimpleQueue
from types import FrameType
from typing import Literal, Protocol

from artisan.utils._command_supervisor import CLEANUP_SECONDS
from artisan.utils.command_supervisor import CommandOwnershipError


class _CancellationTarget(Protocol):
    def cancel(self) -> object: ...


class PipelineSignalRegistration:
    """Own one weak cancellation callback and its prestarted queue consumer."""

    def __init__(self, owner: _CancellationTarget) -> None:
        self.released = False
        self.requested = False
        self.unknown = False
        self.error: BaseException | None = None
        self._deadline: float | None = None
        self._deadline_lock = threading.Lock()
        self._queue: SimpleQueue[Literal["request", "stop"]] = SimpleQueue()
        self._processed = threading.Event()
        self._owner = weakref.ref(owner, self._owner_collected)
        self._thread = threading.Thread(
            target=self._consume, name="artisan-signal-cancellation", daemon=True
        )
        self._thread.start()

    def _owner_collected(
        self, reference: weakref.ReferenceType[_CancellationTarget]
    ) -> None:
        # An unused helper can stop; this makes no statement about worker cleanup.
        self.released = True
        self._queue.put("stop")
        reconcile_pipeline_signals()

    def _consume(self) -> None:
        if self._queue.get() == "stop" or self.released:
            return
        self.deadline()
        try:
            owner = self._owner()
            if owner is not None:
                owner.cancel()
        except BaseException as error:
            self.error = error
        finally:
            owner = None
            if time.monotonic() >= self.deadline():
                self.unknown = True
            self._processed.set()

    def deadline(self) -> float:
        """Keep one absolute cleanup budget across dispatch, close and retries."""
        with self._deadline_lock:
            if self._deadline is None:
                self._deadline = time.monotonic() + CLEANUP_SECONDS
            return self._deadline

    def check_pending(self, *, waiting: bool = False) -> None:
        """Reject unresolved cancellation at its deadline, preserving the helper."""
        if self.released and not self._thread.is_alive() and not self.unknown:
            return
        if self.unknown or (
            self.requested
            and (waiting or not self._processed.is_set())
            and time.monotonic() >= self.deadline()
        ):
            self.unknown = True
            msg = (
                "Pipeline signal cancellation is unresolved; ownership remains retained"
            )
            raise CommandOwnershipError(msg)

    def drain_request(self) -> None:
        """Wait for an earlier cancellation callback without granting a new budget."""
        self.check_pending()
        if self.requested:
            self._processed.wait(max(0, self.deadline() - time.monotonic()))
            self.check_pending()

    def release(self) -> None:
        """Release only after the caller proved scientific ownership is closed."""
        self.check_pending()
        self.released = True
        self._queue.put("stop")
        reconcile_pipeline_signals()
        self._thread.join(max(0, self.deadline() - time.monotonic()))
        if self._thread.is_alive():
            self.unknown = True
        self.check_pending()
        if self.error is not None:
            raise self.error


class _Handlers:
    def __init__(self) -> None:
        self.previous = {
            signum: signal.getsignal(signum)
            for signum in (signal.SIGINT, signal.SIGTERM)
        }
        self.registrations: tuple[
            weakref.ReferenceType[PipelineSignalRegistration], ...
        ] = ()
        self.enabled = True
        self.handler: Callable[[int, FrameType | None], None] = self._handle

    def install(self) -> None:
        try:
            for signum in self.previous:
                signal.signal(signum, self.handler)
            if not self.enabled:
                self.restore()
        except BaseException:
            self.restore()
            raise

    def restore(self) -> None:
        self.enabled = False
        for signum, previous in self.previous.items():
            if previous is not None and signal.getsignal(signum) is self.handler:
                signal.signal(signum, previous)

    def _handle(self, signum: int, frame: FrameType | None) -> None:
        selected = (
            next(
                (
                    token
                    for reference in reversed(self.registrations)
                    if (token := reference()) is not None and not token.released
                ),
                None,
            )
            if self.enabled
            else None
        )
        if selected is None:
            self.restore()
            signal.raise_signal(signum)
        elif selected.requested:
            # Relinquishment does not suppress the first queued cancellation.
            self.restore()
        else:
            selected.requested = True
            selected._queue.put("request")


_handlers: _Handlers | None = None


def reconcile_pipeline_signals() -> None:
    """Restore only Artisan-owned handlers, on a main-thread boundary."""
    if threading.current_thread() is not threading.main_thread() or _handlers is None:
        return
    _handlers.registrations = tuple(
        reference
        for reference in _handlers.registrations
        if (token := reference()) is not None and not token.released
    )
    if not _handlers.registrations:
        _handlers.restore()


def register_pipeline_signals(
    owner: _CancellationTarget,
) -> PipelineSignalRegistration | None:
    """Register main-thread submissions without replacing an opaque native handler."""
    global _handlers  # noqa: PLW0603 -- One process signal owner, changed on the main thread.
    if threading.current_thread() is not threading.main_thread():
        return None
    reconcile_pipeline_signals()
    handlers = _handlers if _handlers is not None and _handlers.enabled else _Handlers()
    if any(previous is None for previous in handlers.previous.values()):
        return None
    token = PipelineSignalRegistration(owner)
    handlers.registrations += (weakref.ref(token),)
    try:
        if handlers is not _handlers:
            handlers.install()
            _handlers = handlers
    except BaseException:
        token.release()
        raise
    return token
