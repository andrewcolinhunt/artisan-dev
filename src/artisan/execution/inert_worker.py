"""Own one inert call inside an already-qualified provider worker interpreter."""

from __future__ import annotations

import math
import shutil
import tempfile
import threading
import time
from pathlib import Path
from typing import Literal, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.execution.assignment_reply import AssignmentReply, AssignmentReplyContext
from artisan.execution.curator_reply import CuratorReply
from artisan.execution.local_call import InertWorkerCall
from artisan.execution.worker_directory import worker_directory_scope
from artisan.execution.worker_reply import execute_call_directory, parse_reply_context
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.utils._command_supervisor import CLEANUP_SECONDS
from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    CommandSupervisor,
    reject_nested_native_scope,
)

_lease = threading.Lock()
_retained: list[tuple[CommandSupervisor | None, Path | None]] = []
_UNKNOWN = "Worker cleanup could not be confirmed; its lease and scratch are retained"
_INFRASTRUCTURE = "Worker infrastructure failed before producing a typed reply"
_CANCELLED = "Worker call cancelled after confirmed cleanup"


class InertCallControl:
    """Request one call's closure without exposing its supervisor or stopping siblings.

    Deadlines are local monotonic times. The executing thread alone can establish
    closure: an empty supervisor does not prove a native Python body returned.
    A control is single-use, and an elapsed active deadline remains UNKNOWN.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._owner: CommandSupervisor | None = None
        self._used = False
        self._active = False
        self._deadline: float | None = None
        self._state = OwnershipState.PENDING

    def request_close(self, deadline: float) -> None:
        """Request only this unit's closure, preserving the shortest deadline."""
        if not math.isfinite(deadline):
            message = "Inert cancellation requires a finite local deadline"
            raise ValueError(message)
        with self._lock:
            if self._state == OwnershipState.CLOSED:
                return
            self._deadline = (
                min(self._deadline, deadline)
                if self._deadline is not None
                else deadline
            )
            self._refresh()
            if self._owner is not None:
                self._request_owner()

    def ownership_state(self) -> OwnershipState:
        """Observe retained closure independently of command or result readiness."""
        with self._lock:
            self._refresh()
            return self._state

    def _refresh(self) -> None:
        if (
            self._active
            and self._state == OwnershipState.PENDING
            and self._deadline is not None
            and time.monotonic() >= self._deadline
        ):
            self._state = OwnershipState.UNKNOWN

    def _claim(self) -> None:
        with self._lock:
            if self._used:
                message = "Inert call control has already been used"
                raise ValueError(message)
            self._used = True

    def _start(self) -> bool:
        with self._lock:
            if self._deadline is not None:
                return False
            self._active = True
            return True

    def _bind(self, owner: CommandSupervisor) -> None:
        with self._lock:
            self._owner = owner
            self._refresh()
            if self._deadline is not None:
                self._request_owner()

    def _request_owner(self) -> None:
        assert self._owner is not None
        assert self._deadline is not None
        try:
            self._owner.request_close(self._deadline)
        except BaseException:
            self._state = OwnershipState.UNKNOWN
            raise

    def _cancelled(self) -> bool:
        with self._lock:
            return self._deadline is not None

    def _finish_deadline(self) -> float:
        with self._lock:
            ordinary = time.monotonic() + CLEANUP_SECONDS
            return (
                min(ordinary, self._deadline)
                if self._deadline is not None
                else ordinary
            )


class InertCallResult(BaseModel):
    """Closed transport evidence; unknown ownership can never expose a success reply."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    entry_id: UUID
    ownership: Literal[OwnershipState.CLOSED, OwnershipState.UNKNOWN]
    reply: bytes | None = Field(default=None, min_length=1)
    error: str | None = Field(default=None, min_length=1, max_length=512)
    retained_working: bool = Field(strict=True)
    cancelled: bool = Field(default=False, strict=True)

    @model_validator(mode="after")
    def _outcome(self) -> Self:
        if self.ownership == OwnershipState.UNKNOWN:
            if self.reply is not None or not self.error or self.cancelled:
                message = "Unknown ownership requires an error and no usable reply"
                raise ValueError(message)
        elif self.cancelled and (self.reply is not None or self.error != _CANCELLED):
            message = (
                "Confirmed cancellation requires no reply and its fixed diagnostic"
            )
            raise ValueError(message)
        elif (self.reply is None) == (self.error is None):
            message = "Closed ownership requires exactly one typed reply or infrastructure error"
            raise ValueError(message)
        return self


def execute_inert_call(
    call: InertWorkerCall,
    *,
    working_root: Path,
    preserve_working: bool = False,
    control: InertCallControl | None = None,
) -> InertCallResult:
    """Run one call with fresh scratch, exact bootstrap and confirmed command closure.

    The provider must verify its actual interpreter/image before entry. This
    function owns command descendants inside that interpreter, not the provider
    container. A failed cleanup permanently poisons this process's one-call lease.
    Provider cancellation BaseExceptions propagate after bounded cleanup.
    """
    reject_nested_native_scope()
    InertWorkerCall(call.header, call.payload, call.reply_context)
    if not _lease.acquire(blocking=False):
        message = "Worker already has an active or unresolved inert call"
        raise CommandOwnershipError(message)
    control = InertCallControl() if control is None else control
    try:
        control._claim()
    except BaseException:
        _lease.release()
        raise
    entry_id = uuid4()
    directory: Path | None = None
    owner: CommandSupervisor | None = None
    reply: bytes | None = None
    error: BaseException | None = None
    unknown = False
    start_attempted = False
    try:
        if control._start():
            working_root.mkdir(parents=True, exist_ok=True)
            directory = Path(
                tempfile.mkdtemp(prefix=f"call-{entry_id}-", dir=working_root)
            )
            transport, worker = directory / "transport", directory / "worker"
            transport.mkdir()
            worker.mkdir()
            start_attempted = True
            owner = CommandSupervisor.start()
            control._bind(owner)
            if not control._cancelled():
                call.write(transport, sensitive_values=tuple(owner.attachment.values()))
                with worker_directory_scope(worker, preserve_working=preserve_working):
                    if not control._cancelled():
                        reply = execute_call_directory(
                            transport, attachment=owner.attachment
                        )
            if reply is not None:
                unknown = _reply_unknown(reply, call.reply_context)
    except BaseException as failure:
        error = failure
        unknown = isinstance(failure, CommandOwnershipError)
    finally:
        closed, cleanup_error = _finish(
            owner,
            failed=error is not None or unknown or control._cancelled(),
            deadline=control._finish_deadline(),
        )
        unknown |= not closed or (start_attempted and owner is None)
        if (
            cleanup_error is not None
            and not isinstance(cleanup_error, Exception)
            and (error is None or isinstance(error, Exception))
        ):
            error = cleanup_error
        unknown |= control.ownership_state() == OwnershipState.UNKNOWN
        if not unknown and directory is not None:
            try:
                _remove_transport(directory, preserve_working=preserve_working)
            except BaseException as cleanup_error:
                unknown = True
                if not isinstance(cleanup_error, Exception) and (
                    error is None or isinstance(error, Exception)
                ):
                    error = cleanup_error
        # Filesystem cleanup cannot block control requests or observations. A
        # deadline crossed during deletion still loses closure; retain anything
        # left, without claiming that already removed files can be recovered.
        with control._lock:
            control._refresh()
            unknown |= control._state == OwnershipState.UNKNOWN
            control._state = (
                OwnershipState.UNKNOWN if unknown else OwnershipState.CLOSED
            )
            cancelled = control._deadline is not None and not unknown
            if unknown:
                _retained.append((owner, directory))
            else:
                _lease.release()
    if error is not None and not isinstance(error, Exception):
        raise error
    retained = directory is not None and (directory / "worker").exists()
    if unknown:
        return InertCallResult(
            entry_id=entry_id,
            ownership=OwnershipState.UNKNOWN,
            error=_UNKNOWN,
            retained_working=retained,
        )
    return InertCallResult(
        entry_id=entry_id,
        ownership=OwnershipState.CLOSED,
        reply=reply if error is None and not cancelled else None,
        error=_CANCELLED
        if cancelled
        else _INFRASTRUCTURE
        if error is not None
        else None,
        retained_working=retained,
        cancelled=cancelled,
    )


def _finish(
    owner: CommandSupervisor | None, *, failed: bool, deadline: float
) -> tuple[bool, BaseException | None]:
    if owner is None:
        return True, None
    cleanup_error: BaseException | None = None
    if failed:
        try:
            owner.request_close(deadline)
        except BaseException as error:
            cleanup_error = error
    try:
        closed = owner.finish(deadline).state == "closed"
    except BaseException as error:
        closed = False
        if cleanup_error is None or not isinstance(error, Exception):
            cleanup_error = error
    return closed and cleanup_error is None, cleanup_error


def _remove_transport(directory: Path, *, preserve_working: bool) -> None:
    if preserve_working:
        shutil.rmtree(directory / "transport")
    else:
        shutil.rmtree(directory)


def _reply_unknown(reply: bytes, encoded_context: bytes) -> bool:
    context = parse_reply_context(encoded_context)
    if isinstance(context, AssignmentReplyContext):
        assignment = AssignmentReply.model_validate_json(reply)
        if assignment.attempt != context.attempt:
            message = "Worker reply belongs to a different admitted assignment"
            raise ValueError(message)
        return assignment.kind == "ownership"
    curator = CuratorReply.model_validate_json(reply)
    if curator.admission != context.admission:
        message = "Worker reply belongs to a different admitted curator"
        raise ValueError(message)
    return curator.kind == "ownership"
