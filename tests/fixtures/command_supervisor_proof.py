"""Test-only owner prototype; never imported by Artisan production code.

This isolates native process and descriptor semantics before production wiring.
The small protocol deliberately omits production invocation scopes, secrets,
recording, nested commands and external environments. Trace files contain only
test identities and state transitions, never command/environment payloads.
"""

from __future__ import annotations

import array
import ctypes
import json
import os
import select
import selectors
import signal
import socket
import struct
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

FRAME_LIMIT = 65536
POLL_SECONDS = 0.01
CLEANUP_SECONDS = 10.0
TERM_SECONDS = 3.0


def enable_subreaper() -> None:
    """Require Linux adoption only in the dedicated proof subprocess."""
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
        message = "Proof control frame is too large"
        raise ValueError(message)
    return struct.pack("!I", len(payload)) + payload


def send(
    connection: socket.socket, message: dict[str, Any], fds: tuple[int, ...] = ()
) -> None:
    """Send one frame, transferring descriptors only with its first bytes."""
    payload = encode(message)
    count = (
        socket.send_fds(connection, [payload], list(fds))
        if fds
        else connection.send(payload)
    )
    connection.sendall(payload[count:])


def receive(connection: socket.socket) -> dict[str, Any]:
    """Read a response with the caller's socket timeout."""
    header = _read_exact(connection, 4)
    length = struct.unpack("!I", header)[0]
    if not 0 < length <= FRAME_LIMIT:
        message = "Invalid proof response length"
        raise ValueError(message)
    return json.loads(_read_exact(connection, length))


def _read_exact(connection: socket.socket, length: int) -> bytes:
    data = bytearray()
    while len(data) < length:
        chunk = connection.recv(length - len(data))
        if not chunk:
            message = "Proof owner disconnected"
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
            message = "The proof requires Linux or macOS"
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
    events: list[str] = field(default_factory=lambda: ["accepted"])

    @property
    def pid(self) -> int:
        assert self.process is not None
        return self.process.pid

    def register_watch(self) -> None:
        try:
            self.watch = ExitWatch(self.pid)
        except OSError:
            self.observation_error = True
            self.events.append("observation_failed")
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
        self.events.append(f"signal:{sig.name}")
        try:
            os.killpg(self.pid, sig)
        except (ProcessLookupError, PermissionError):
            self.events.append("signal_unproved")
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
                self.events.append("observation_failed")
            if exited:
                self.events.append("exit_observed")
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
            self.events.append("unknown")
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
            self.events.append("reaped")
            self._close_watch()
        if reserved.get(self.pid, self.request_id) != self.request_id:
            # A different owned leader with this PID proves the old lifetime ended.
            self.state = "unknown" if self.observation_error else "closed"
            self.events.append(self.state)
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
                self.events.append("descendant_reaped")
        if group_absent(self.pid):
            self.state = "unknown" if self.observation_error else "closed"
            self.events.append(self.state)

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
            "events": self.events,
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
            message = "Incomplete proof control frame"
            raise EOFError(message)
        self.buffer.extend(data)
        if len(self.buffer) != self.expected:
            return None
        if not self.body:
            self.expected = struct.unpack("!I", self.buffer)[0]
            if not 0 < self.expected <= FRAME_LIMIT:
                message = "Invalid proof frame length"
                raise ValueError(message)
            self.buffer.clear()
            self.body = True
            return None
        message = json.loads(self.buffer)
        self.buffer.clear()
        self.body = False
        self.expected = 4
        return message

    def close_fds(self) -> None:
        for descriptor in self.fds:
            os.close(descriptor)
        self.fds.clear()


class Supervisor:
    """Minimal process owner for platform tests, without production integration."""

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
        self.admission = True
        self.deadline: float | None = None
        self.shutting_down = False
        parent = socket.socket(fileno=parent_fd)
        self.add_channel(parent, parent=True)
        self.reply(
            self.channels[parent_fd],
            {"ready": True, "pid": os.getpid(), "subreaper": sys.platform == "linux"},
        )

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

    def disconnect(self, channel: Channel) -> None:
        descriptor = channel.connection.fileno()
        self.selector.unregister(channel.connection)
        self.channels.pop(descriptor)
        channel.close_fds()
        channel.connection.close()
        if channel.parent:
            self.close_scope(time.monotonic() + CLEANUP_SECONDS)
            self.shutting_down = True
        else:
            accepted_at = time.monotonic()
            for command in self.commands.values():
                if command.client == descriptor:
                    command.stop(
                        min(
                            self.deadline or float("inf"),
                            accepted_at + CLEANUP_SECONDS,
                        ),
                        accepted_at=accepted_at,
                    )

    def close_scope(self, deadline: float) -> None:
        self.admission = False
        self.deadline = min(self.deadline or deadline, deadline)
        accepted_at = time.monotonic()
        for command in self.commands.values():
            command.stop(self.deadline, accepted_at=accepted_at)

    def message(self, channel: Channel, message: Any) -> None:
        if not isinstance(message, dict):
            message = "Invalid proof message"
            raise ValueError(message)
        kind = message.get("type")
        if channel.parent and not channel.fds:
            if kind in {"cancel", "shutdown"}:
                self.close_scope(float(message["deadline"]))
                self.shutting_down |= kind == "shutdown"
            elif kind == "worker_lost":
                # Deliberately closes the whole test scope when mapping is uncertain.
                self.close_scope(float(message["deadline"]))
            elif kind != "status":
                message = "Invalid proof control request"
                raise ValueError(message)
            self.reply(channel, self.snapshot())
        elif kind == "launch" and message.get("capability") == self.capability:
            self.launch(channel, message)
        else:
            message = "Invalid proof attachment"
            raise ValueError(message)

    def launch(self, channel: Channel, request: dict[str, Any]) -> None:
        request_id = request.get("id")
        if (
            not isinstance(request_id, str)
            or not request_id
            or len(request_id) > 100
            or len(channel.fds) != 3
        ):
            message = "Invalid proof launch identity or descriptors"
            raise ValueError(message)
        if request_id in self.commands:
            self.reply(channel, self.commands[request_id].snapshot())
            return
        if not self.admission:
            self.reply(channel, {"state": "rejected"})
            return
        argv, env, cwd = request.get("argv"), request.get("env"), request.get("cwd")
        if (
            not isinstance(argv, list)
            or not argv
            or not all(isinstance(arg, str) for arg in argv)
            or not isinstance(env, dict)
            or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in env.items()
            )
            or not (cwd is None or isinstance(cwd, str))
        ):
            message = "Invalid proof launch payload"
            raise ValueError(message)
        command = Command(request_id, channel.connection.fileno())
        self.commands[request_id] = command
        self.write_trace()  # Evidence that acceptance precedes process creation.
        # Test-only deterministic stand-in for Popen blocked inside an OS call.
        while (self.root / "hold-launch").exists():
            time.sleep(POLL_SECONDS)
        try:
            command.process = subprocess.Popen(
                argv,
                cwd=cwd,
                env=env,
                stdin=channel.fds[0],
                stdout=channel.fds[1],
                stderr=channel.fds[2],
                process_group=0,
                close_fds=True,
            )
        except (OSError, ValueError):
            command.state = "launch_failed"
            command.events.append("launch_failed")
        else:
            command.state = "running"
            command.events.append("launched")
            command.register_watch()
        self.reply(channel, command.snapshot())

    def snapshot(self) -> dict[str, Any]:
        return {
            "admission": self.admission,
            "commands": [command.snapshot() for command in self.commands.values()],
        }

    def write_trace(self) -> None:
        temporary = self.root / "trace.tmp"
        temporary.write_text(json.dumps(self.snapshot()))
        temporary.replace(self.root / "trace.json")

    def run(self) -> None:
        while True:
            for key, mask in self.selector.select(POLL_SECONDS):
                if key.data is None:
                    self.add_channel(self.listener.accept()[0])
                else:
                    self.service(key.data, mask)
            reserved = {
                command.pid: command.request_id
                for command in self.commands.values()
                if command.process is not None and not command.reaped
            }
            for command in self.commands.values():
                command.tick(reserved)
            self.write_trace()
            settled = all(
                command.state in {"closed", "unknown", "launch_failed"}
                for command in self.commands.values()
            )
            flushed = not any(channel.outgoing for channel in self.channels.values())
            expired = self.deadline is not None and time.monotonic() >= self.deadline
            if self.shutting_down and settled and (flushed or expired):
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


if __name__ == "__main__":
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    enable_subreaper()
    # The capability travels over inherited stdin, never supervisor argv.
    supervisor = Supervisor(
        int(sys.argv[1]), Path(sys.argv[2]), sys.stdin.readline().strip()
    )
    supervisor.run()
