"""Private native command owner and bounded descriptor protocol.

This process alone launches, signals and reaps managed command groups. No request
payload is persisted or included in diagnostic errors.
"""

from __future__ import annotations

import array
import ctypes
import json
import math
import os
import select
import selectors
import signal
import socket
import struct
import subprocess
import sys
import time
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from artisan.utils.configuration_secrets import COMMAND_ATTACHMENT_ENV as ATTACHMENT_ENV

FRAME_LIMIT = 65536
POLL_SECONDS = 0.01
CLEANUP_SECONDS = 10.0
TERM_SECONDS = 3.0


def enable_subreaper() -> None:
    """Require Linux adoption only in the dedicated owner subprocess."""
    if sys.platform != "linux":
        return
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def encode(message: dict[str, Any]) -> bytes:
    """Encode a bounded private JSON frame."""
    payload = json.dumps(message).encode()
    if len(payload) > FRAME_LIMIT:
        error = "Command control frame is too large"
        raise ValueError(error)
    return struct.pack("!I", len(payload)) + payload


def send(
    connection: socket.socket,
    message: dict[str, Any],
    fds: tuple[int, ...] = (),
    *,
    expires_at: float | None = None,
) -> None:
    """Send one frame, transferring descriptors only with its first bytes."""
    payload = encode(message)
    _set_remaining_timeout(connection, expires_at)
    count = (
        socket.send_fds(connection, [payload], list(fds))
        if fds
        else connection.send(payload)
    )
    if count < len(payload):
        _set_remaining_timeout(connection, expires_at)
        connection.sendall(payload[count:])


def receive(
    connection: socket.socket, *, expires_at: float | None = None
) -> dict[str, Any]:
    """Read a response with the caller's socket timeout."""
    header = _read_exact(connection, 4, expires_at)
    length = struct.unpack("!I", header)[0]
    if not 0 < length <= FRAME_LIMIT:
        message = "Invalid command response length"
        raise ValueError(message)
    message = json.loads(_read_exact(connection, length, expires_at))
    if not isinstance(message, dict):
        error = "Invalid command response"
        raise ValueError(error)
    return message


def _set_remaining_timeout(connection: socket.socket, expires_at: float | None) -> None:
    """A partial frame must not acquire a fresh socket timeout."""
    if expires_at is not None:
        remaining = expires_at - time.monotonic()
        if remaining <= 0:
            raise TimeoutError
        connection.settimeout(remaining)


def _read_exact(
    connection: socket.socket, length: int, expires_at: float | None
) -> bytes:
    data = bytearray()
    while len(data) < length:
        _set_remaining_timeout(connection, expires_at)
        chunk = connection.recv(length - len(data))
        if not chunk:
            message = "Command owner disconnected"
            raise EOFError(message)
        data.extend(chunk)
    return bytes(data)


class ExitWatch:
    """Observe the exact child's exit without relinquishing its identity."""

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.queue: Any = None
        self.fast_exit = False
        if sys.platform == "darwin":
            self.queue = select.kqueue()
            event = select.kevent(
                pid,
                filter=select.KQ_FILTER_PROC,
                flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
                fflags=select.KQ_NOTE_EXIT,
            )
            try:
                self.queue.control([event], 0, 0)
            except ProcessLookupError:
                # ESRCH initiates cleanup; only later waitpid proves exit/status.
                self.fast_exit = True
            except OSError:
                self.close()
                raise
        elif sys.platform != "linux":
            message = "Command ownership requires Linux or macOS"
            raise RuntimeError(message)

    def exited(self) -> bool:
        if self.queue is not None:
            return self.fast_exit or bool(self.queue.control(None, 1, 0))
        return (
            os.waitid(os.P_PID, self.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            is not None
        )

    def close(self) -> None:
        if self.queue is not None:
            self.queue.close()
            self.queue = None


def group_absent(pgid: int) -> bool:
    """Accept ESRCH alone as absence; EPERM remains unproved."""
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


@dataclass
class Command:
    """One reserved leader and its foreground group, owned by one event loop."""

    request_id: str
    client: int
    process: subprocess.Popen[bytes] | None = None
    watch: ExitWatch | None = None
    state: str = "starting"
    deadline: float | None = None
    kill_at: float | None = None
    final_signal: bool = False
    reaped: bool = False
    observation_error: bool = False
    scope: str = "root"
    parent: str | None = None
    launch_seconds: float | None = None
    launch_error: str | None = None
    external: bool = False
    external_uncertain: bool = False
    collection: str = "pending"
    client_channel: Channel | None = None

    def settle_collection(self, *, waive: bool = False) -> str:
        if self.collection == "pending" and (
            self.state == "unknown"
            or (
                self.state == "closed"
                and self.deadline is not None
                and time.monotonic() >= self.deadline
            )
        ):
            self.collection = "unknown"
        if waive and self.collection == "pending":
            self.collection = "waived"
        return self.collection

    @property
    def pid(self) -> int:
        assert self.process is not None
        return self.process.pid

    def owned_commands(self) -> tuple[Command, ...]:
        """Return exact native children whose unreaped identities stay reserved."""
        return (self,)

    def launch(
        self, argv: list[str], env: dict[str, str], cwd: str | None, fds: list[int]
    ) -> None:
        """Launch one native child without retaining its sensitive arguments."""
        started = time.perf_counter()
        try:
            self.process = subprocess.Popen(
                argv,
                cwd=cwd,
                env=env,
                stdin=fds[0],
                stdout=fds[1],
                stderr=fds[2],
                process_group=0,
                close_fds=True,
            )
        except (OSError, ValueError) as exc:
            self.state = "launch_failed"
            self.launch_error = (
                "missing"
                if isinstance(exc, FileNotFoundError)
                else "invalid"
                if isinstance(exc, ValueError)
                else "os"
            )
        else:
            self.process.args = ()
            self.launch_seconds = time.perf_counter() - started
            self.state = "running"
            self.register_watch()

    def register_watch(self) -> None:
        try:
            self.watch = ExitWatch(self.pid)
        except OSError:
            self.observation_error = True
            self.stop(time.monotonic() + CLEANUP_SECONDS, immediate=True)

    def stop(
        self,
        deadline: float,
        *,
        immediate: bool = False,
        accepted_at: float | None = None,
    ) -> None:
        if self.state in {"closed", "unknown", "launch_failed"}:
            return
        self.deadline = min(self.deadline or deadline, deadline)
        accepted_at = time.monotonic() if accepted_at is None else accepted_at
        self.kill_at = min(
            self.kill_at or deadline,
            accepted_at + (0 if immediate else TERM_SECONDS),
            deadline,
        )
        if self.state != "stopping":
            self.state = "stopping"
            if not immediate:
                self._signal(signal.SIGTERM)

    def _signal(self, sig: int) -> None:
        if self.reaped or self.final_signal:
            message = "Destructive signal after final signal or identity release"
            raise RuntimeError(message)
        if self.process is None:
            return
        # A failed signal proves neither liveness nor closure. Exact reaping
        # and subsequent positive group absence still determine the outcome.
        with suppress(ProcessLookupError, PermissionError):
            os.killpg(self.pid, sig)
        if sig == signal.SIGKILL:
            self.final_signal = True

    def tick(self, reserved: dict[int, str]) -> None:
        if self.state in {"closed", "unknown", "launch_failed", "starting"}:
            return
        now = time.monotonic()
        if self.state == "running":
            try:
                assert self.watch is not None
                exited = self.watch.exited()
            except (OSError, ValueError):
                self.observation_error = True
                exited = True
            if exited:
                self.stop(now + CLEANUP_SECONDS, immediate=True)
        if self.kill_at is not None and now >= self.kill_at and not self.final_signal:
            self._signal(signal.SIGKILL)
        if self.final_signal:
            self._observe_closure(reserved)
        if (
            self.state == "stopping"
            and self.deadline is not None
            and now >= self.deadline
        ):
            self.state = "unknown"
            self._close_watch()

    def _observe_closure(self, reserved: dict[int, str]) -> None:
        assert self.process is not None
        if not self.reaped:
            try:
                waited, status = os.waitpid(self.pid, os.WNOHANG)
            except ChildProcessError:
                self.observation_error = True
                return
            if not waited:
                return
            self.process.returncode = os.waitstatus_to_exitcode(status)
            self.reaped = True
            self._close_watch()
        if reserved.get(self.pid, self.request_id) != self.request_id:
            # A different owned leader with this PID proves the old lifetime ended.
            self.state = "unknown" if self.observation_error else "closed"
            return
        if sys.platform == "linux":
            # Only this closed group's adopted children; never a blanket -1 wait.
            while True:
                try:
                    exited = os.waitid(
                        os.P_PGID, self.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT
                    )
                except ChildProcessError:
                    break
                if exited is None or exited.si_pid in reserved:
                    break
                os.waitpid(exited.si_pid, os.WNOHANG)
        if group_absent(self.pid):
            self.state = "unknown" if self.observation_error else "closed"

    def _close_watch(self) -> None:
        if self.watch is not None:
            self.watch.close()
            self.watch = None

    def snapshot(self) -> dict[str, Any]:
        return {
            "id": self.request_id,
            "pid": self.process.pid if self.process else None,
            "state": self.state,
            "returncode": self.process.returncode if self.process else None,
            "deadline": self.deadline,
            "kill_at": self.kill_at,
            "launch_seconds": self.launch_seconds,
            "launch_error": self.launch_error,
        }


@dataclass
class Channel:
    connection: socket.socket
    parent: bool = False
    buffer: bytearray = field(default_factory=bytearray)
    fds: list[int] = field(default_factory=list)
    outgoing: bytearray = field(default_factory=bytearray)
    expected: int = 4
    body: bool = False

    def read(self) -> dict[str, Any] | None:
        data, ancillary, flags, _address = self.connection.recvmsg(
            self.expected - len(self.buffer),
            socket.CMSG_SPACE(4 * array.array("i").itemsize),
        )
        for level, kind, payload in ancillary:
            if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                descriptors = array.array("i")
                descriptors.frombytes(
                    payload[: len(payload) - len(payload) % descriptors.itemsize]
                )
                for descriptor in descriptors:
                    os.set_inheritable(descriptor, False)
                    self.fds.append(descriptor)
        if not data or flags & socket.MSG_CTRUNC or len(self.fds) > 3:
            message = "Incomplete command control frame"
            raise EOFError(message)
        self.buffer.extend(data)
        if len(self.buffer) != self.expected:
            return None
        if not self.body:
            self.expected = struct.unpack("!I", self.buffer)[0]
            if not 0 < self.expected <= FRAME_LIMIT:
                message = "Invalid command frame length"
                raise ValueError(message)
            self.buffer.clear()
            self.body = True
            return None
        message = json.loads(self.buffer)
        if not isinstance(message, dict):
            error = "Invalid command message"
            raise ValueError(error)
        self.buffer.clear()
        self.body = False
        self.expected = 4
        return message

    def close_fds(self) -> None:
        for descriptor in self.fds:
            os.close(descriptor)
        self.fds.clear()


TERMINAL = {"closed", "unknown", "launch_failed"}


@dataclass
class Scope:
    """One fixed worker/bootstrap or unit lifetime."""

    identity: str
    parent: str | None
    client: int
    parent_command: str | None = None
    admission: bool = True
    closed: bool = False
    uncertain: bool = False
    bootstrap: bool = False
    completion: str = "pending"
    deadline: float | None = None
    client_channel: Channel | None = None

    def settle_completion(self, *, waive: bool = False) -> str:
        if self.completion == "pending" and (
            self.uncertain
            or (
                self.closed
                and self.deadline is not None
                and time.monotonic() >= self.deadline
            )
        ):
            self.completion = "unknown"
        if waive and self.completion == "pending":
            self.completion = "waived"
        return self.completion


class Supervisor:
    """Serialize launch admission, cancellation and ownership evidence."""

    def __init__(self, parent_fd: int, root: Path, capability: str) -> None:
        self.root = root
        self.capability = capability
        self.selector = selectors.DefaultSelector()
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(str(root / "owner.sock"))
        self.listener.listen()
        self.listener.setblocking(False)
        self.selector.register(self.listener, selectors.EVENT_READ, None)
        self.channels: dict[int, Channel] = {}
        self.commands: dict[str, Command] = {}
        self.scopes = {"root": Scope("root", None, parent_fd, completion="waived")}
        self.admission = True
        self.deadline: float | None = None
        self.shutting_down = False
        self.cancelled = False
        self.final_reported = False
        self.external = False
        parent = socket.socket(fileno=parent_fd)
        self.add_channel(parent, parent=True)
        self.reply(self.channels[parent_fd], {"ready": True})

    def add_channel(self, connection: socket.socket, *, parent: bool = False) -> None:
        connection.setblocking(False)
        channel = Channel(connection, parent=parent)
        self.channels[connection.fileno()] = channel
        self.selector.register(connection, selectors.EVENT_READ, channel)

    def reply(self, channel: Channel, message: dict[str, Any]) -> None:
        payload = encode(message)
        if len(channel.outgoing) + len(payload) > FRAME_LIMIT:
            self.disconnect(channel)
            return
        channel.outgoing.extend(payload)
        self.selector.modify(
            channel.connection, selectors.EVENT_READ | selectors.EVENT_WRITE, channel
        )

    def _descends(self, scope: str, ancestor: str) -> bool:
        while scope in self.scopes:
            if scope == ancestor:
                return True
            parent = self.scopes[scope].parent
            if parent is None:
                break
            scope = parent
        return False

    def _nested(self, command: Command, ancestor: str) -> bool:
        parent = command.parent
        while parent in self.commands:
            if parent == ancestor:
                return True
            parent = self.commands[parent].parent
        return False

    def disconnect(self, channel: Channel) -> None:
        descriptor = channel.connection.fileno()
        self.selector.unregister(channel.connection)
        self.channels.pop(descriptor)
        channel.close_fds()
        channel.connection.close()
        deadline = min(
            self.deadline or float("inf"), time.monotonic() + CLEANUP_SECONDS
        )
        if channel.parent:
            self.close_scope("root", deadline, cancelled=True)
            self.shutting_down = True
            return
        for scope in list(self.scopes.values()):
            if scope.client == descriptor and not scope.closed:
                self.close_scope(scope.identity, deadline, cancelled=True)
        accepted_at = time.monotonic()
        for command in self.commands.values():
            if command.client == descriptor and command.state not in TERMINAL:
                self.stop_command(command, deadline, accepted_at=accepted_at)

    def stop_command(
        self,
        command: Command,
        deadline: float,
        *,
        immediate: bool = False,
        accepted_at: float | None = None,
        interrupted: bool = True,
    ) -> None:
        accepted_at = time.monotonic() if accepted_at is None else accepted_at
        if interrupted and command.external and command.state == "running":
            command.external_uncertain = True
        command.stop(deadline, immediate=immediate, accepted_at=accepted_at)
        for child in self.commands.values():
            if self._nested(child, command.request_id):
                if child.external and child.state == "running":
                    child.external_uncertain = True
                child.stop(deadline, immediate=immediate, accepted_at=accepted_at)
        for scope in self.scopes.values():
            if scope.parent_command == command.request_id:
                scope.admission = False
                scope.closed = True
                scope.deadline = min(scope.deadline or deadline, deadline)

    def close_scope(
        self,
        identity: str,
        deadline: float,
        *,
        cancelled: bool,
        immediate: bool = False,
    ) -> None:
        accepted_at = time.monotonic()
        if identity == "root":
            self.admission = False
            self.deadline = min(self.deadline or deadline, deadline)
            self.cancelled |= cancelled
            deadline = self.deadline
        for scope in self.scopes.values():
            if self._descends(scope.identity, identity):
                scope.admission = False
                scope.closed = True
                scope.deadline = min(scope.deadline or deadline, deadline)
        for command in self.commands.values():
            if self._descends(command.scope, identity):
                if command.state == "closed" and command.collection == "pending":
                    command.deadline = min(command.deadline or deadline, deadline)
                self.stop_command(
                    command, deadline, immediate=immediate, accepted_at=accepted_at
                )
                if identity == "root" and cancelled and command.external:
                    self.scopes[identity].uncertain = True

    def scope_snapshot(
        self, identity: str, *, ancestors: bool = True
    ) -> dict[str, Any]:
        scope = self.scopes[identity]
        members = [
            command
            for command in self.commands.values()
            if self._descends(command.scope, identity)
        ]
        children = [
            child
            for child in self.scopes.values()
            if self._descends(child.identity, identity)
        ]
        uncertain = any(
            child.uncertain or child.settle_completion() == "unknown"
            for child in children
        ) or any(
            c.state == "unknown"
            or c.external_uncertain
            or c.settle_collection() == "unknown"
            for c in members
        )
        physical_pending = any(c.state not in TERMINAL for c in members)
        pending = any(
            c.state not in TERMINAL
            or (c.state != "launch_failed" and c.settle_collection() == "pending")
            for c in members
        )
        pending |= any(
            child.identity != identity and child.settle_completion() == "pending"
            for child in children
        )
        if ancestors:
            parent = scope.parent
            while parent is not None:
                ancestor = self.scopes[parent]
                direct = [
                    c
                    for c in self.commands.values()
                    if c.scope == parent and ancestor.bootstrap
                ]
                uncertain |= (
                    ancestor.uncertain
                    or ancestor.settle_completion() == "unknown"
                    or any(
                        c.state == "unknown"
                        or c.external_uncertain
                        or c.settle_collection() == "unknown"
                        for c in direct
                    )
                )
                physical_pending |= any(c.state not in TERMINAL for c in direct)
                pending |= any(
                    c.state not in TERMINAL
                    or (
                        c.state != "launch_failed"
                        and c.settle_collection() == "pending"
                    )
                    for c in direct
                )
                parent = ancestor.parent
        state = (
            "pending"
            if pending or not scope.closed
            else "unknown"
            if uncertain
            else "closed"
        )
        return {
            "state": state,
            "admission": scope.admission,
            "active": not pending,
            "collecting": pending and not physical_pending,
            "uncertain": uncertain,
            "external": self.external,
            "cancelled": self.cancelled,
            "deadline": self.deadline,
        }

    def command_snapshot(self, command: Command) -> dict[str, Any]:
        snapshot = command.snapshot()
        descendants = [
            c for c in self.commands.values() if self._nested(c, command.request_id)
        ]
        attached = [
            scope
            for scope in self.scopes.values()
            if scope.parent_command == command.request_id
            or (
                scope.parent_command in self.commands
                and self._nested(
                    self.commands[scope.parent_command], command.request_id
                )
            )
        ]
        if self.scope_snapshot(command.scope)["uncertain"] or any(
            c.state == "unknown"
            or c.external_uncertain
            or c.settle_collection() == "unknown"
            for c in descendants
        ):
            snapshot["state"] = "unknown"
        elif snapshot["state"] in TERMINAL and (
            any(
                c.state not in TERMINAL
                or (c.state != "launch_failed" and c.settle_collection() == "pending")
                for c in descendants
            )
            or any(scope.settle_completion() == "pending" for scope in attached)
        ):
            snapshot["state"] = "stopping"
        snapshot["deadline"] = min(
            [d for d in (command.deadline, self.deadline) if d is not None],
            default=None,
        )
        return snapshot

    def _scope(self, request: dict[str, Any]) -> Scope:
        identity = request.get("scope")
        if not isinstance(identity, str) or identity not in self.scopes:
            msg = "Invalid command scope"
            raise ValueError(msg)
        return self.scopes[identity]

    def _admitted(self, scope: Scope, parent: str | None) -> bool:
        if (
            not self.admission
            or not scope.admission
            or self.scope_snapshot(scope.identity)["uncertain"]
        ):
            return False
        if parent is not None:
            command = self.commands.get(parent)
            return (
                command is not None
                and command.state == "running"
                and self._descends(scope.identity, command.scope)
            )
        return scope.parent_command is None

    def message(self, channel: Channel, message: Any) -> None:
        if not isinstance(message, dict):
            msg = "Invalid command control message"
            raise ValueError(msg)
        kind = message.get("type")
        if channel.parent and not channel.fds:
            self.parent_message(channel, message)
            return
        if message.get("capability") != self.capability:
            msg = "Invalid command attachment"
            raise ValueError(msg)
        scope = self._scope(message)
        if kind == "launch":
            self.launch(channel, message, scope)
            return
        if channel.fds:
            msg = "Unexpected command descriptors"
            raise ValueError(msg)
        if kind == "open":
            self.open_scope(channel, message, scope)
        elif kind == "attach":
            snapshot = self.scope_snapshot(scope.identity)
            allowed = self._admitted(scope, message.get("parent"))
            allowed &= not snapshot["uncertain"]
            self.reply(
                channel,
                {**snapshot, "state": "attached" if allowed else "rejected"},
            )
        elif kind == "completed":
            if (
                scope.client_channel is not channel
                or self.scope_snapshot(scope.identity)["state"] != "closed"
            ):
                msg = "Invalid invocation completion assertion"
                raise ValueError(msg)
            if scope.settle_completion() == "pending":
                scope.completion = "completed"
        elif kind == "uncertain":
            parent = message.get("parent")
            target = self.commands.get(parent) if isinstance(parent, str) else None
            if parent is not None and (
                target is None or not self._descends(scope.identity, target.scope)
            ):
                msg = "Invalid uncertainty parent identity"
                raise ValueError(msg)
            scope.uncertain = True
            scope.admission = False
            self.reply(channel, self.scope_snapshot(scope.identity))
        elif kind in {"check", "close", "bootstrap"}:
            if kind == "close":
                scope.uncertain |= bool(message.get("uncertain"))
                self.close_scope(
                    scope.identity,
                    self._deadline(message),
                    cancelled=bool(message.get("cancelled")),
                    immediate=not bool(message.get("cancelled")),
                )
            elif kind == "bootstrap":
                scope.admission = False
                accepted_at = time.monotonic()
                for command in self.commands.values():
                    if command.scope == scope.identity:
                        self.stop_command(
                            command,
                            accepted_at + CLEANUP_SECONDS,
                            immediate=True,
                            accepted_at=accepted_at,
                        )
            self.reply(channel, self.scope_snapshot(scope.identity))
        elif kind in {"command", "stop", "collected"}:
            identity = message.get("id")
            target = self.commands.get(identity) if isinstance(identity, str) else None
            if target is None or target.scope != scope.identity:
                msg = "Invalid command identity"
                raise ValueError(msg)
            if kind == "collected":
                if target.client_channel is not channel or target.state != "closed":
                    msg = "Invalid command collection assertion"
                    raise ValueError(msg)
                if target.settle_collection() == "pending":
                    target.collection = "collected"
                return
            if kind == "stop":
                self.stop_command(target, self._deadline(message))
            self.reply(channel, self.command_snapshot(target))
        else:
            msg = "Invalid command control request"
            raise ValueError(msg)

    def _deadline(self, message: dict[str, Any]) -> float:
        deadline = float(message["deadline"])
        if not math.isfinite(deadline):
            msg = "Invalid cancellation deadline"
            raise ValueError(msg)
        return min(self.deadline or deadline, deadline)

    def parent_message(self, channel: Channel, message: dict[str, Any]) -> None:
        kind = message.get("type")
        if kind in {"cancel", "shutdown", "worker_lost"}:
            if kind != "shutdown" or message.get("cancelled"):
                for command in self.commands.values():
                    command.settle_collection(waive=True)
                for scope in self.scopes.values():
                    scope.settle_completion(waive=True)
            self.close_scope(
                "root",
                self._deadline(message),
                cancelled=kind != "shutdown" or bool(message.get("cancelled")),
                immediate=bool(message.get("immediate")),
            )
            self.shutting_down |= kind == "shutdown"
        elif kind != "status":
            msg = "Invalid owner control request"
            raise ValueError(msg)
        self.reply(channel, self.scope_snapshot("root", ancestors=False))

    def open_scope(
        self, channel: Channel, request: dict[str, Any], parent: Scope
    ) -> None:
        identity = request.get("id")
        parent_command = request.get("parent")
        if not isinstance(identity, str) or not identity or len(identity) > 100:
            msg = "Invalid invocation identity"
            raise ValueError(msg)
        # A worker bootstrap remains an ancestor after direct launch admission ends.
        allowed = (
            self.admission
            and not parent.closed
            and not self.scope_snapshot(parent.identity)["uncertain"]
        )
        if parent_command is not None:
            allowed &= self._admitted(parent, parent_command)
        if not allowed or identity in self.scopes:
            self.reply(channel, {"state": "rejected"})
            return
        self.scopes[identity] = Scope(
            identity,
            parent.identity,
            channel.connection.fileno(),
            parent_command,
            bootstrap=bool(request.get("bootstrap")),
            completion="waived" if request.get("bootstrap") else "pending",
            client_channel=channel,
        )
        self.reply(channel, {"state": "open", "scope": identity})

    def launch(self, channel: Channel, request: dict[str, Any], scope: Scope) -> None:
        request_id = request.get("id")
        if (
            not isinstance(request_id, str)
            or not request_id
            or len(request_id) > 100
            or len(channel.fds) != 3
        ):
            msg = "Invalid command identity or descriptors"
            raise ValueError(msg)
        if request_id in self.commands:
            command = self.commands[request_id]
            if command.scope != scope.identity:
                msg = "Invalid repeated command identity"
                raise ValueError(msg)
            self.reply(channel, self.command_snapshot(command))
            return
        if not self._admitted(scope, request.get("parent")):
            self.reply(channel, {"state": "rejected"})
            return
        argv, env, cwd = request.get("argv"), request.get("env"), request.get("cwd")
        if (
            not isinstance(argv, list)
            or not argv
            or not all(isinstance(arg, str) for arg in argv)
            or not isinstance(env, dict)
            or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in env.items()
            )
            or not (cwd is None or isinstance(cwd, str))
        ):
            msg = "Invalid command launch payload"
            raise ValueError(msg)
        docker_create = request.get("docker_create")
        if docker_create is not None and (
            not isinstance(docker_create, list)
            or docker_create[:2] != ["docker", "create"]
            or not all(isinstance(arg, str) for arg in docker_create)
        ):
            msg = "Invalid managed Docker command"
            raise ValueError(msg)
        command = Command(
            request_id,
            channel.connection.fileno(),
            scope=scope.identity,
            parent=request.get("parent"),
            external=bool(request.get("external", True)),
            client_channel=channel,
        )
        for key in ATTACHMENT_ENV:
            env.pop(key, None)
        if docker_create is not None:
            from artisan.utils._docker_supervisor import DockerCommand

            command = DockerCommand(command, argv, docker_create, env, cwd, channel.fds)
        elif not command.external:
            env.update(
                dict(
                    zip(
                        ATTACHMENT_ENV,
                        (
                            str(self.root / "owner.sock"),
                            self.capability,
                            scope.identity,
                            request_id,
                        ),
                        strict=True,
                    )
                )
            )
        self.commands[request_id] = command
        self.external |= command.external
        if docker_create is not None:
            assert isinstance(command, DockerCommand)
            command.begin()
        else:
            command.launch(argv, env, cwd, channel.fds)
        self.reply(channel, self.command_snapshot(command))

    def run(self) -> None:
        while True:
            for key, mask in self.selector.select(POLL_SECONDS):
                if key.data is None:
                    self.add_channel(self.listener.accept()[0])
                else:
                    self.service(key.data, mask)
            reserved = {
                child.pid: child.request_id
                for c in self.commands.values()
                for child in c.owned_commands()
                if child.process is not None and not child.reaped
            }
            for command in list(self.commands.values()):
                previous = command.state
                command.tick(reserved)
                self.external |= command.external
                command.settle_collection()
                if previous == "running" and command.state != "running":
                    self.stop_command(
                        command,
                        command.deadline or time.monotonic() + CLEANUP_SECONDS,
                        immediate=True,
                        interrupted=False,
                    )
            settled = all(
                c.state in TERMINAL
                and (c.state == "launch_failed" or c.settle_collection() != "pending")
                for c in self.commands.values()
            )
            settled &= all(
                scope.settle_completion() != "pending" for scope in self.scopes.values()
            )
            flushed = not any(channel.outgoing for channel in self.channels.values())
            expired = self.deadline is not None and time.monotonic() >= self.deadline
            if self.shutting_down and settled:
                if not self.final_reported:
                    for channel in list(self.channels.values()):
                        if channel.parent:
                            self.reply(
                                channel, self.scope_snapshot("root", ancestors=False)
                            )
                    self.final_reported = True
                elif flushed or expired:
                    break
        for channel in list(self.channels.values()):
            self.disconnect(channel)
        self.listener.close()
        self.selector.close()

    def service(self, channel: Channel, mask: int) -> None:
        try:
            if mask & selectors.EVENT_WRITE and channel.outgoing:
                count = channel.connection.send(channel.outgoing)
                del channel.outgoing[:count]
                if not channel.outgoing:
                    self.selector.modify(
                        channel.connection, selectors.EVENT_READ, channel
                    )
            if mask & selectors.EVENT_READ:
                message = channel.read()
                if message is not None:
                    try:
                        self.message(channel, message)
                    finally:
                        channel.close_fds()
        except BlockingIOError:
            pass
        except (OSError, EOFError, ValueError, KeyError, TypeError):
            if channel.connection.fileno() in self.channels:
                self.disconnect(channel)


def main() -> None:
    """Enter the dedicated owner without importing application code."""
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    enable_subreaper()
    supervisor = Supervisor(
        int(sys.argv[1]), Path(sys.argv[2]), sys.stdin.readline().strip()
    )
    supervisor.run()


if __name__ == "__main__":
    main()
