"""Private clients and fixed invocation lifetimes for managed native commands.

Attachments are transient capabilities. They never belong in public operation,
runner or storage models. Independent standalone commands do not mutate the
process-wide invocation used by native workers and endpoint requests.
"""

from __future__ import annotations

import codecs
import contextlib
import io
import locale
import os
import select
import selectors
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from artisan.errors import ArtisanError, ErrorCode
from artisan.utils._command_supervisor import (
    CLEANUP_SECONDS,
    POLL_SECONDS,
    Channel,
    receive,
    send,
)
from artisan.utils.configuration_secrets import COMMAND_ATTACHMENT_ENV as ATTACHMENT_ENV

_UNKNOWN = "Managed command cleanup could not be confirmed; workspace retained"
_INVALID = "Managed command attachment is invalid or unavailable"


class CommandOwnershipError(ArtisanError):
    """Managed work lacks sufficient ownership evidence for safe publication."""

    def __init__(self, message: str = _UNKNOWN) -> None:
        super().__init__(
            code=ErrorCode.OP_EXECUTE_FAILED,
            message=message,
            error_type="compute",
            recovery_hint="REPORT_TO_USER",
        )
        self.cleanup_outcome = OwnershipOutcome("unknown", reason=message)
        self.cleanup_deadline: float | None = None
        self.scope_reported = False


@dataclass(frozen=True)
class OwnershipOutcome:
    """Transient host-cleanup outcome, independent of runner acknowledgements."""

    state: Literal["pending", "closed", "unknown"]
    external: bool = False
    reason: str | None = None


Attachment = dict[str, str]
_current: _Invocation | None = None
_bootstrap: _Invocation | None = None
_invocation_lock = threading.RLock()
_request_lock = threading.Lock()
_diagnostic_invocation: ContextVar[int | None] = ContextVar(
    "command_invocation", default=None
)
_diagnostic_tool: ContextVar[Any] = ContextVar("command_tool", default=None)


def strip_internal_attachment(environment: Mapping[str, str]) -> dict[str, str]:
    """Copy an environment without ephemeral runtime attachment fields."""
    return {
        key: value for key, value in environment.items() if key not in ATTACHMENT_ENV
    }


def _validate_attachment(value: object) -> Attachment:
    if (
        not isinstance(value, dict)
        or set(value) != {"socket", "secret", "scope", "parent"}
        or any(not isinstance(item, str) for item in value.values())
        or any(not value[key] for key in ("socket", "secret", "scope"))
    ):
        raise CommandOwnershipError(_INVALID)
    return dict(value)


def _inherited_attachment() -> Attachment | None:
    present = [key in os.environ for key in ATTACHMENT_ENV]
    if not any(present):
        return None
    if not all(present):
        raise CommandOwnershipError(_INVALID)
    return _validate_attachment(
        dict(
            zip(
                ("socket", "secret", "scope", "parent"),
                (os.environ[key] for key in ATTACHMENT_ENV),
                strict=True,
            )
        )
    )


def _ambient_attachment() -> Attachment | None:
    if _current is not None:
        return dict(_current.attachment)
    if _bootstrap is not None:
        return dict(_bootstrap.attachment)
    return _inherited_attachment()


def attachment_sensitive_values() -> tuple[str, ...]:
    """Return known scope secrets even when the remaining attachment is invalid."""
    values = {os.environ.get(ATTACHMENT_ENV[1], "")}
    for invocation in (_current, _bootstrap):
        if invocation is not None:
            values.add(invocation.attachment.get("secret", ""))
    return tuple(value for value in values if value)


def reject_nested_native_scope() -> None:
    """Reject ambient, partial or expected attachments before native startup."""
    if (
        _current is not None
        or _bootstrap is not None
        or any(key in os.environ for key in ATTACHMENT_ENV)
    ):
        msg = "Nested native execution inside managed work is unsupported"
        raise CommandOwnershipError(msg)


class _Connection:
    """One authenticated request stream with fixed, payload-free errors."""

    def __init__(
        self, attachment: Attachment, *, expires_at: float | None = None
    ) -> None:
        self.attachment = _validate_attachment(attachment)
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.settimeout(
            1.0
            if expires_at is None
            else max(0, min(1.0, expires_at - time.monotonic()))
        )
        self.lock = threading.Lock()
        try:
            self.socket.connect(self.attachment["socket"])
        except OSError:
            self.socket.close()
            raise CommandOwnershipError(_INVALID) from None
        except BaseException:
            self.socket.close()
            raise

    def request(
        self,
        kind: str,
        *,
        fds: tuple[int, ...] = (),
        expires_at: float | None = None,
        **values: Any,
    ) -> dict[str, Any]:
        message = {
            "type": kind,
            "capability": self.attachment["secret"],
            "scope": self.attachment["scope"],
            **values,
        }
        expires_at = min(
            float("inf") if expires_at is None else expires_at,
            time.monotonic() + 1.0,
        )
        if not self.lock.acquire(timeout=max(0, expires_at - time.monotonic())):
            raise CommandOwnershipError(_INVALID)
        try:
            send(self.socket, message, fds, expires_at=expires_at)
            return receive(self.socket, expires_at=expires_at)
        except (OSError, EOFError, ValueError, TypeError):
            self.socket.close()
            raise CommandOwnershipError(_INVALID) from None
        finally:
            self.lock.release()

    def notify(
        self, kind: str, *, expires_at: float | None = None, **values: Any
    ) -> None:
        """Send a final assertion without introducing an ambiguous reply."""
        expires_at = min(expires_at or float("inf"), time.monotonic() + 1.0)
        if not self.lock.acquire(timeout=max(0, expires_at - time.monotonic())):
            raise CommandOwnershipError(_INVALID)
        try:
            send(
                self.socket,
                {
                    "type": kind,
                    "capability": self.attachment["secret"],
                    "scope": self.attachment["scope"],
                    **values,
                },
                expires_at=expires_at,
            )
        except (OSError, ValueError, TypeError):
            self.socket.close()
            raise CommandOwnershipError(_INVALID) from None
        finally:
            self.lock.release()

    def close(self) -> None:
        self.socket.close()


def _record_uncertainty(
    attachment: Attachment,
    error: CommandOwnershipError,
    deadline: float | None = None,
) -> None:
    """Latch client evidence locally and report it without extending cleanup."""
    deadlines = [d for d in (deadline, error.cleanup_deadline) if d is not None]
    error.cleanup_deadline = min(deadlines, default=None)
    for invocation in (_current, _bootstrap):
        if invocation is not None and invocation.attachment == attachment:
            invocation.uncertain = True
            if error.cleanup_deadline is not None:
                invocation.deadline = min(
                    invocation.deadline or error.cleanup_deadline,
                    error.cleanup_deadline,
                )
    if error.scope_reported:
        return
    error.scope_reported = True
    expires_at = min(error.cleanup_deadline or float("inf"), time.monotonic() + 1.0)
    if expires_at <= time.monotonic():
        return
    connection = None
    try:
        connection = _Connection(attachment, expires_at=expires_at)
        connection.request(
            "uncertain", parent=attachment["parent"] or None, expires_at=expires_at
        )
    except CommandOwnershipError:
        pass
    finally:
        if connection is not None:
            connection.close()


def _reap_owner(process: subprocess.Popen[bytes], root: str) -> None:
    """Retain the exact owner handle after a bounded caller deadline expires."""
    process.wait()
    shutil.rmtree(root, ignore_errors=True)


def _retain_owner(process: subprocess.Popen[bytes], root: str) -> None:
    threading.Thread(
        target=_reap_owner,
        args=(process, root),
        name="artisan-command-owner-reaper",
        daemon=True,
    ).start()


class CommandSupervisor:
    """Retain the exact dedicated owner and its separate parent control channel."""

    def __init__(
        self,
        process: subprocess.Popen[bytes],
        connection: socket.socket,
        root: str,
        secret: str,
    ) -> None:
        self.process = process
        self.connection = connection
        self.root = root
        self.attachment = {
            "socket": str(Path(root) / "owner.sock"),
            "secret": secret,
            "scope": "root",
            "parent": "",
        }
        self.channel = Channel(connection, parent=True)
        self.connection.setblocking(False)
        self.deadline: float | None = None
        self._snapshot: dict[str, Any] = {"state": "pending", "external": False}
        self._failed = False
        self._terminal_unknown = False
        self._closed = False
        self._cancelled = False
        self._finishing = False
        self._lock = threading.RLock()

    @classmethod
    def start(cls) -> CommandSupervisor:
        """Start the owner before any worker or command can be submitted."""
        root = tempfile.mkdtemp(prefix="artisan-owner-", dir="/tmp")
        parent, child = socket.socketpair()
        secret = uuid.uuid4().hex + uuid.uuid4().hex
        process: subprocess.Popen[bytes] | None = None
        try:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "artisan.utils._command_supervisor",
                    str(child.fileno()),
                    root,
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                pass_fds=(child.fileno(),),
                start_new_session=True,
                env=strip_internal_attachment(os.environ),
            )
            child.close()
            assert process.stdin is not None
            process.stdin.write((secret + "\n").encode())
            process.stdin.close()
            parent.settimeout(CLEANUP_SECONDS)
            if receive(parent, expires_at=time.monotonic() + CLEANUP_SECONDS) != {
                "ready": True
            }:
                raise ValueError
            return cls(process, parent, root, secret)
        except BaseException as error:
            parent.close()
            child.close()
            if process is not None:
                if process.stdin is not None:
                    with contextlib.suppress(OSError):
                        process.stdin.close()
                with contextlib.suppress(subprocess.TimeoutExpired):
                    process.wait(timeout=0.1)
            if process is not None and process.returncode is None:
                _retain_owner(process, root)
            else:
                shutil.rmtree(root, ignore_errors=True)
            if isinstance(error, (OSError, EOFError, ValueError)):
                msg = "Managed command owner could not start"
                raise CommandOwnershipError(msg) from None
            raise

    def _send(self, kind: str, **values: Any) -> None:
        if self._failed or self._closed:
            return
        try:
            send(self.connection, {"type": kind, **values})
        except (OSError, ValueError):
            self._failed |= self._snapshot.get("state") not in {"closed", "unknown"}
            self.connection.close()

    def request_close(self, deadline: float, *, immediate: bool = False) -> None:
        """Accept cancellation without waiting; repeated calls shorten its budget."""
        with self._lock:
            if (
                self._cancelled
                and self.deadline is not None
                and deadline >= self.deadline
            ):
                return
            self.deadline = min(self.deadline or deadline, deadline)
            self._cancelled = True
            self._send("cancel", deadline=self.deadline, immediate=immediate)

    def worker_lost(self, worker_pid: int, deadline: float) -> None:
        """Conservatively close the whole owner when independent worker evidence is lost."""
        del worker_pid
        self.request_close(deadline)

    def poll(self) -> OwnershipOutcome:
        """Read bounded current evidence without waiting for process termination."""
        with self._lock:
            if (
                self.deadline is not None
                and time.monotonic() >= self.deadline
                and self._snapshot.get("state") != "closed"
            ):
                self._terminal_unknown = True
            if not self._finishing:
                self._send("status")
            try:
                for _ in range(64):
                    if not select.select([self.connection], [], [], 0)[0]:
                        break
                    response = self.channel.read()
                    if response is not None:
                        if (
                            self.deadline is not None
                            and time.monotonic() >= self.deadline
                            and self._snapshot.get("state") != "closed"
                        ):
                            self._terminal_unknown = True
                        self._snapshot = response
            except (OSError, EOFError, ValueError):
                if self._snapshot.get("state") not in {"closed", "unknown"}:
                    self._failed = True
            state = self._snapshot.get("state", "unknown")
            if self._failed or (
                self.deadline is not None
                and time.monotonic() >= self.deadline
                and state != "closed"
            ):
                state = "unknown"
            if state == "closed" and self._cancelled and self._snapshot.get("external"):
                state = "unknown"
            if state not in {"pending", "closed", "unknown"}:
                state = "unknown"
            self._terminal_unknown |= state == "unknown"
            if self._terminal_unknown:
                state = "unknown"
            return OwnershipOutcome(
                state,
                bool(self._snapshot.get("external")),
                _UNKNOWN if state == "unknown" else None,
            )

    def request_finish(self, deadline: float) -> None:
        """Close ordinary admission promptly while worker evidence is collected."""
        with self._lock:
            self.deadline = min(self.deadline or deadline, deadline)
            self._finishing = True
            self._send("shutdown", deadline=self.deadline, cancelled=self._cancelled)

    def finish(self, deadline: float) -> OwnershipOutcome:
        """Close ordinary admission and collect evidence within one absolute deadline."""
        with self._lock:
            if self._closed:
                return self.poll()
            if not self._finishing:
                self.request_finish(deadline)
        assert self.deadline is not None
        outcome = self.poll()
        while outcome.state == "pending" and time.monotonic() < self.deadline:
            time.sleep(POLL_SECONDS)
            outcome = self.poll()
        with self._lock:
            self._closed = True
            self.connection.close()
        with contextlib.suppress(subprocess.TimeoutExpired):
            self.process.wait(timeout=max(0, self.deadline - time.monotonic()))
        if self.process.returncode is not None:
            shutil.rmtree(self.root, ignore_errors=True)
        else:
            _retain_owner(self.process, self.root)
        return outcome


class _Invocation:
    def __init__(
        self,
        attachment: Attachment,
        *,
        owner: CommandSupervisor | None = None,
        bootstrap: bool = False,
    ) -> None:
        self.connection = _Connection(attachment)
        self.owner = owner
        self.observer: Any = None
        self.diagnostics: tuple[int | None, Any] = (None, None)
        self.uncertain = False
        self.reported = False
        self.closed = False
        self.deadline: float | None = None
        identity = uuid.uuid4().hex
        try:
            response = self.connection.request(
                "open",
                id=identity,
                parent=attachment["parent"] or None,
                bootstrap=bootstrap,
            )
            if response.get("state") != "open":
                raise CommandOwnershipError(_INVALID)
        except BaseException:
            self.connection.close()
            raise
        self.attachment = {**attachment, "scope": identity}
        self.connection.attachment = self.attachment

    def check(self) -> None:
        if self.closed:
            raise CommandOwnershipError(
                _UNKNOWN if self.uncertain else "Managed invocation is closed"
            )
        try:
            response = self.connection.request("check")
        except CommandOwnershipError as error:
            self.uncertain = True
            _record_uncertainty(self.attachment, error, self.deadline)
            raise
        if (
            self.uncertain
            or response.get("uncertain")
            or response.get("state") == "unknown"
            or not response.get("admission")
        ):
            self.uncertain = response.get("state") != "closed" or self.uncertain
            raise CommandOwnershipError(
                _UNKNOWN if self.uncertain else "Managed invocation is closed"
            )

    def close(self, error: BaseException | None = None) -> bool:
        if self.closed and not self.uncertain:
            return True
        if isinstance(error, CommandOwnershipError):
            self.uncertain = True
            _record_uncertainty(self.attachment, error, self.deadline)
        if not self.closed:
            self.deadline = self.deadline or time.monotonic() + CLEANUP_SECONDS
            try:
                response = self.connection.request(
                    "close",
                    deadline=self.deadline,
                    expires_at=self.deadline,
                    cancelled=error is not None,
                    uncertain=self.uncertain,
                )
                while (
                    response.get("state") == "pending"
                    and time.monotonic() < self.deadline
                ):
                    time.sleep(
                        min(POLL_SECONDS, max(0, self.deadline - time.monotonic()))
                    )
                    response = self.connection.request(
                        "check", expires_at=self.deadline
                    )
                self.uncertain |= response.get("state") != "closed"
                if not self.uncertain:
                    self.closed = True
                    self.connection.notify("completed", expires_at=self.deadline)
            except CommandOwnershipError:
                self.uncertain = True
            self.closed = True
        if self.uncertain:
            self.reported = True
            if error is not None:
                error.add_note(_UNKNOWN)
                return False
            raise CommandOwnershipError(_UNKNOWN)
        return True

    def dispose(self) -> None:
        self.connection.close()
        if self.owner is not None:
            outcome = self.owner.finish(
                self.deadline or time.monotonic() + CLEANUP_SECONDS
            )
            self.uncertain |= outcome.state != "closed"


def initialize_native_worker(attachment: Attachment) -> None:
    """Install process-wide bootstrap ownership before deserialization/imports."""
    global _bootstrap  # noqa: PLW0603 — worker attachment must cover joined threads
    if _bootstrap is not None or _current is not None:
        msg = "Native worker already has managed ownership"
        raise CommandOwnershipError(msg)
    _bootstrap = _Invocation(_validate_attachment(attachment), bootstrap=True)
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def validate_worker_attachment() -> None:
    """Validate expected worker ownership before trusted cloudpickle executes."""
    if _bootstrap is not None:
        if _bootstrap.uncertain:
            raise CommandOwnershipError(_UNKNOWN)
        response = _bootstrap.connection.request("check")
        if response.get("uncertain") or response.get("state") in {"unknown", "closed"}:
            raise CommandOwnershipError(_INVALID)
    elif any(key in os.environ for key in ATTACHMENT_ENV):
        attachment = _inherited_attachment()
        assert attachment is not None
        connection = _Connection(attachment)
        try:
            result = connection.request("check")
            if not result.get("admission") or result.get("state") == "unknown":
                raise CommandOwnershipError(_INVALID)
        finally:
            connection.close()


@contextlib.contextmanager
def managed_invocation() -> Iterator[None]:
    """Attach one unit before bootstrap; share it with joined operation threads."""
    global _current  # noqa: PLW0603 — invocation ownership is process-wide
    with _invocation_lock:
        previous = _current
        attachment = _ambient_attachment()
        owner = None
        if attachment is None:
            owner = CommandSupervisor.start()
            attachment = owner.attachment
        if _bootstrap is not None and previous is None:
            if _bootstrap.uncertain:
                raise CommandOwnershipError(_UNKNOWN)
            deadline = _bootstrap.deadline or time.monotonic() + CLEANUP_SECONDS
            response = _bootstrap.connection.request("bootstrap", expires_at=deadline)
            while (
                response.get("collecting")
                and not response.get("uncertain")
                and time.monotonic() < deadline
            ):
                time.sleep(min(POLL_SECONDS, max(0, deadline - time.monotonic())))
                response = _bootstrap.connection.request("check", expires_at=deadline)
            if (
                response.get("uncertain")
                or response.get("state") == "unknown"
                or not response.get("active")
            ):
                raise CommandOwnershipError(_UNKNOWN)
        try:
            invocation = _Invocation(attachment, owner=owner)
        except BaseException as error:
            if owner is not None:
                deadline = time.monotonic() + CLEANUP_SECONDS
                owner.request_close(deadline, immediate=True)
                if owner.finish(deadline).state != "closed":
                    error.add_note(_UNKNOWN)
            raise
        _current = invocation
        from artisan.utils.external_tools import command_observer

        invocation.observer = command_observer.get()
        invocation.diagnostics = (
            _diagnostic_invocation.get(),
            _diagnostic_tool.get(),
        )
    try:
        yield
    except BaseException as error:
        invocation.close(error)
        raise
    finally:
        try:
            if not invocation.reported:
                invocation.close()
        finally:
            with _invocation_lock:
                _current = previous
            invocation.dispose()


def bind_scope_observer(observer: Any) -> Any:
    """Bind command evidence to this invocation, including its joined threads."""
    if _current is None:
        return None
    previous = _current.observer
    _current.observer = observer
    return previous


def scope_command_observer() -> Any:
    """Return the current invocation's observer without changing thread contexts."""
    return None if _current is None else _current.observer


def bind_scope_diagnostics(invocation: int | None, tool: Any) -> tuple[int | None, Any]:
    """Bind dispatch attribution for joined threads inside this managed unit."""
    if _current is None:
        return None, None
    previous = _current.diagnostics
    _current.diagnostics = invocation, tool
    return previous


def scope_command_diagnostics() -> tuple[int | None, Any]:
    """Return dispatch attribution without changing thread-local contexts."""
    return (None, None) if _current is None else _current.diagnostics


def check_command_scope() -> None:
    """Check current and bootstrap uncertainty without ending the invocation."""
    if _current is not None:
        _current.check()


def close_command_scope(error: BaseException | None = None) -> bool:
    """Close before any output collection, publication or workspace deletion."""
    return True if _current is None else _current.close(error)


@contextlib.contextmanager
def request_lease() -> Iterator[None]:
    """Reject overlapping/nested endpoint requests before touching active state."""
    if not _request_lock.acquire(blocking=False):
        msg = "Overlapping managed endpoint requests are unsupported"
        raise CommandOwnershipError(msg)
    try:
        with managed_invocation():
            yield
    finally:
        _request_lock.release()


@contextlib.contextmanager
def command_attachment() -> Iterator[Attachment]:
    """Use expected ownership, or create an invocation-local standalone owner."""
    attachment = _ambient_attachment()
    owner = None
    if attachment is None:
        owner = CommandSupervisor.start()
        attachment = owner.attachment
    else:
        connection = None
        try:
            connection = _Connection(attachment)
            response = connection.request("attach", parent=attachment["parent"] or None)
        except CommandOwnershipError as error:
            _record_uncertainty(attachment, error)
            raise
        finally:
            if connection is not None:
                connection.close()
        if (
            response.get("state") != "attached"
            or not response.get("admission")
            or response.get("uncertain")
        ):
            raise CommandOwnershipError(_INVALID)
    try:
        yield attachment
    except BaseException as error:
        if isinstance(error, CommandOwnershipError):
            _record_uncertainty(attachment, error)
        if owner is not None:
            deadline = (
                error.cleanup_deadline
                if isinstance(error, CommandOwnershipError)
                and error.cleanup_deadline is not None
                else time.monotonic() + CLEANUP_SECONDS
            )
            owner.request_close(deadline)
            outcome = owner.finish(deadline)
            if outcome.state != "closed":
                error.add_note(_UNKNOWN)
        raise
    finally:
        if owner is not None and not owner._closed:
            outcome = owner.finish(time.monotonic() + CLEANUP_SECONDS)
            if outcome.state != "closed":
                raise CommandOwnershipError(_UNKNOWN)


class ManagedCommand:
    """Worker-side output pipes; process identity stays exclusively with the owner."""

    def __init__(
        self,
        attachment: Attachment,
        argv: list[str],
        env: dict[str, str],
        cwd: str | None,
        *,
        merged: bool,
        external: bool,
    ) -> None:
        self.connection = _Connection(attachment)
        self.identity = uuid.uuid4().hex
        self.readers: list[int] = []
        self.launch_seconds: float | None = None
        self.returncode: int | None = None
        self.deadline: float | None = None
        self.state = "starting"
        self.collection_complete = False
        writers: list[int] = []
        try:
            stdout_read, stdout_write = os.pipe()
            self.readers.append(stdout_read)
            writers.append(stdout_write)
            if merged:
                stderr_write = stdout_write
            else:
                stderr_read, stderr_write = os.pipe()
                self.readers.append(stderr_read)
                writers.append(stderr_write)
            # Inherit the requesting process's real stdin, including a redirected fd 0.
            response = self.connection.request(
                "launch",
                id=self.identity,
                argv=argv,
                env=env,
                cwd=cwd,
                parent=attachment["parent"] or None,
                external=external,
                fds=(0, stdout_write, stderr_write),
            )
            self._update(response)
            if self.state == "launch_failed":
                launch_error = response.get("launch_error")
                error_type = {"missing": FileNotFoundError, "invalid": ValueError}.get(
                    launch_error if isinstance(launch_error, str) else "os", OSError
                )
                msg = "Managed command could not be launched"
                raise error_type(msg)
            if self.state == "rejected":
                msg = "Managed command admission is closed"
                raise CommandOwnershipError(msg)
        except BaseException as error:
            if isinstance(error, CommandOwnershipError):
                _record_uncertainty(attachment, error, self.deadline)
            self.dispose()
            raise
        finally:
            for descriptor in writers:
                os.close(descriptor)

    def _update(self, response: dict[str, Any]) -> None:
        self.state = response.get("state", "unknown")
        self.returncode = response.get("returncode")
        self.launch_seconds = response.get("launch_seconds", self.launch_seconds)
        deadline = response.get("deadline")
        if deadline is not None:
            self.deadline = min(self.deadline or deadline, deadline)

    def poll(self) -> None:
        try:
            if (
                self.deadline is not None
                and time.monotonic() >= self.deadline
                and self.state != "closed"
            ):
                raise CommandOwnershipError(_UNKNOWN)
            self._update(
                self.connection.request(
                    "command", id=self.identity, expires_at=self.deadline
                )
            )
            if self.state == "unknown" or (
                self.deadline is not None
                and time.monotonic() >= self.deadline
                and self.state != "closed"
            ):
                raise CommandOwnershipError(_UNKNOWN)
        except CommandOwnershipError as error:
            _record_uncertainty(self.connection.attachment, error, self.deadline)
            raise

    def communicate(
        self, on_line: Callable[[str], None] | None = None
    ) -> tuple[str, str]:
        selector = selectors.DefaultSelector()
        encoding = locale.getpreferredencoding(False)
        decoders = [
            io.IncrementalNewlineDecoder(
                codecs.getincrementaldecoder(encoding)(errors="strict"), translate=True
            )
            for _ in self.readers
        ]
        chunks: list[list[str]] = [[] for _ in self.readers]
        pending_line = ""
        for index, descriptor in enumerate(self.readers):
            os.set_blocking(descriptor, False)
            selector.register(descriptor, selectors.EVENT_READ, index)
        try:
            while selector.get_map() or self.state != "closed":
                if self.deadline is not None and time.monotonic() >= self.deadline:
                    raise CommandOwnershipError(_UNKNOWN)
                self.poll()
                for key, _mask in selector.select(0.05):
                    data = os.read(key.fd, 65536)
                    value = decoders[key.data].decode(data, final=not data)
                    chunks[key.data].append(value)
                    if on_line is not None:
                        pending_line += value
                        while "\n" in pending_line:
                            line, pending_line = pending_line.split("\n", 1)
                            on_line(line + "\n")
                    if not data:
                        selector.unregister(key.fd)
                if (
                    self.state == "closed"
                    and selector.get_map()
                    and not selector.select(0)
                ):
                    # After proven group closure, buffered output remains readable
                    # through EOF. EAGAIN instead proves an out-of-scope writer.
                    raise CommandOwnershipError(_UNKNOWN)
            if on_line is not None and pending_line:
                on_line(pending_line)
            if self.returncode is None:
                raise CommandOwnershipError(_UNKNOWN)
            result = "".join(chunks[0]), "".join(chunks[1]) if len(chunks) > 1 else ""
            self.collection_complete = True
            self.connection.notify(
                "collected", id=self.identity, expires_at=self.deadline
            )
            return result
        except CommandOwnershipError as error:
            _record_uncertainty(self.connection.attachment, error, self.deadline)
            raise
        finally:
            selector.close()

    def interrupt(self, error: BaseException) -> None:
        if self.collection_complete:
            return
        deadline = min(
            self.deadline or float("inf"), time.monotonic() + CLEANUP_SECONDS
        )
        self.deadline = deadline
        if isinstance(error, CommandOwnershipError):
            _record_uncertainty(self.connection.attachment, error, deadline)
        try:
            self._update(
                self.connection.request(
                    "stop", id=self.identity, deadline=deadline, expires_at=deadline
                )
            )
            while (
                self.state not in {"closed", "unknown", "launch_failed"}
                and time.monotonic() < deadline
            ):
                time.sleep(min(POLL_SECONDS, max(0, deadline - time.monotonic())))
                self.poll()
            if self.state != "closed":
                raise CommandOwnershipError(_UNKNOWN)
        except CommandOwnershipError as cleanup_error:
            _record_uncertainty(self.connection.attachment, cleanup_error, deadline)
            error.add_note(_UNKNOWN)

    def dispose(self) -> None:
        self.connection.close()
        for descriptor in self.readers:
            with contextlib.suppress(OSError):
                os.close(descriptor)
        self.readers.clear()
