"""Linux per-command descendant reaping under the shared supervisor's deadline."""

from __future__ import annotations

import math
import os
import select
import signal
import socket
import subprocess
import sys
import time
from contextlib import suppress
from typing import Any

from artisan.utils._command_supervisor import (
    CLEANUP_SECONDS,
    FRAME_LIMIT,
    POLL_SECONDS,
    Channel,
    enable_subreaper,
    encode,
    receive,
    send,
)

# Include children created with a non-SIGCHLD clone termination signal.
_WAIT_ALL = 0x40000000


def _finite_number(value: object) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        message = "Invalid subtree time"
        raise ValueError(message)
    return float(value)


def _validate_outcome(message: dict[str, Any]) -> None:
    kind = message.get("type")
    if kind == "launched" and set(message) == {"type", "launch_seconds"}:
        if _finite_number(message["launch_seconds"]) >= 0:
            return
    elif kind == "exited" and set(message) == {"type", "returncode", "finished_at"}:
        if (
            type(message["returncode"]) is int
            and 0 < _finite_number(message["finished_at"]) <= time.monotonic()
        ):
            return
    elif kind == "launch_failed" and set(message) == {"type", "error"}:
        if message["error"] in {"missing", "invalid", "os"}:
            return
    elif kind in {"closed", "unknown"} and set(message) == {"type"}:
        return
    error = "Invalid subtree outcome"
    raise ValueError(error)


class CommandTree:
    """Owner-side private channel; argv and environment never enter a file."""

    def __init__(self, fds: list[int]) -> None:
        self.socket, child = socket.socketpair()
        try:
            self.process = subprocess.Popen(
                [sys.executable, "-m", __name__, str(child.fileno())],
                pass_fds=(child.fileno(),),
                stdin=fds[0],
                stdout=fds[1],
                stderr=fds[2],
                process_group=0,
                close_fds=True,
            )
        except BaseException:
            self.socket.close()
            raise
        finally:
            child.close()
        self.channel = Channel(self.socket)
        self.last_stop: tuple[float, float] | None = None
        self.write_failed = False

    def launch(self, argv: list[str], env: dict[str, str], cwd: str | None) -> None:
        send(self.socket, {"type": "launch", "argv": argv, "env": env, "cwd": cwd})
        self.socket.setblocking(False)

    def stop(self, deadline: float, kill_at: float) -> None:
        if self.last_stop is not None:
            previous_deadline, previous_kill = self.last_stop
            if deadline >= previous_deadline and kill_at >= previous_kill:
                return
        self.last_stop = deadline, kill_at
        self.channel.outgoing.extend(
            encode({"type": "stop", "deadline": deadline, "kill_at": kill_at})
        )
        if len(self.channel.outgoing) > FRAME_LIMIT:
            message = "Subtree control queue exceeded its bound"
            raise ValueError(message)

    def _flush(self) -> None:
        if self.write_failed or not self.channel.outgoing:
            return
        try:
            count = self.socket.send(self.channel.outgoing)
        except BlockingIOError:
            return
        except (BrokenPipeError, ConnectionResetError):
            self.write_failed = True
            return
        del self.channel.outgoing[:count]

    def messages(self) -> list[dict[str, Any]]:
        messages = []
        for _ in range(16):
            if not select.select([self.socket], [], [], 0)[0]:
                break
            message = self.channel.read()
            if message is not None:
                _validate_outcome(message)
                messages.append(message)
                if message.get("type") in {"closed", "launch_failed", "unknown"}:
                    return messages
        self._flush()
        return messages

    def close(self) -> None:
        self.channel.close_fds()
        self.socket.close()


class _Tree:
    """Reap only this command's descendants, never another command's children."""

    def __init__(self, connection: socket.socket) -> None:
        self.connection = connection
        self.channel = Channel(connection)
        self.process: subprocess.Popen[bytes] | None = None
        self.returncode: int | None = None
        self.deadline: float | None = None
        self.kill_at: float | None = None
        self.uncertain = False
        self.connected = True
        self.reported_unknown = False
        self.term_sent: set[int] = set()

    def report(self, kind: str, **values: Any) -> None:
        if not self.connected:
            return
        try:
            send(
                self.connection,
                {"type": kind, **values},
                expires_at=time.monotonic() + 0.1,
            )
            self.connection.setblocking(False)
        except (OSError, ValueError):
            self.connected = False
            self.fail()

    def launch(self, request: dict[str, Any]) -> bool:
        argv, env, cwd = request.get("argv"), request.get("env"), request.get("cwd")
        if (
            set(request) != {"type", "argv", "env", "cwd"}
            or request["type"] != "launch"
            or not isinstance(argv, list)
            or not argv
            or not all(isinstance(value, str) for value in argv)
            or not isinstance(env, dict)
            or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in env.items()
            )
            or not (cwd is None or isinstance(cwd, str))
        ):
            message = "Invalid subtree launch"
            raise ValueError(message)
        started = time.perf_counter()
        try:
            self.process = subprocess.Popen(
                argv, env=env, cwd=cwd, process_group=0, close_fds=True
            )
        except (OSError, ValueError) as error:
            kind = (
                "missing"
                if isinstance(error, FileNotFoundError)
                else "invalid"
                if isinstance(error, ValueError)
                else "os"
            )
            self.report("launch_failed", error=kind)
            return False
        self.process.args = ()
        self.report("launched", launch_seconds=time.perf_counter() - started)
        for descriptor in (0, 1, 2):
            with suppress(OSError):
                os.close(descriptor)
        return True

    def stop(self, deadline: float, kill_at: float) -> None:
        self.deadline = min(self.deadline or deadline, deadline)
        self.kill_at = min(self.kill_at or kill_at, kill_at, self.deadline)

    def fail(self) -> None:
        self.uncertain = True
        now = time.monotonic()
        self.stop(now + CLEANUP_SECONDS, now)

    def read_requests(self) -> None:
        if not self.connected:
            return
        try:
            while select.select([self.connection], [], [], 0)[0]:
                request = self.channel.read()
                if request is None:
                    continue
                deadline, kill_at = request.get("deadline"), request.get("kill_at")
                if (
                    set(request) != {"type", "deadline", "kill_at"}
                    or request["type"] != "stop"
                ):
                    message = "Invalid subtree control"
                    raise ValueError(message)
                self.stop(_finite_number(deadline), _finite_number(kill_at))
        except (OSError, EOFError, ValueError):
            self.connected = False
            self.fail()

    def observe_leader(self) -> None:
        if self.process is None or self.returncode is not None:
            return
        waited, status = os.waitpid(self.process.pid, os.WNOHANG | _WAIT_ALL)
        if waited:
            self.term_sent.discard(waited)
            self.returncode = os.waitstatus_to_exitcode(status)
            self.process.returncode = self.returncode
            self.report(
                "exited", returncode=self.returncode, finished_at=time.monotonic()
            )

    def signal_children(self) -> None:
        sig = (
            signal.SIGKILL
            if time.monotonic() >= (self.kill_at or 0)
            else signal.SIGTERM
        )
        # This listing can omit racing children. ECHILD below is the only absence proof.
        with open(f"/proc/self/task/{os.getpid()}/children") as handle:
            children = handle.read().split()
        for value in children:
            pid = int(value)
            try:
                os.waitid(
                    os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT | _WAIT_ALL
                )
            except ChildProcessError:
                continue
            if sig == signal.SIGTERM and pid in self.term_sent:
                continue
            # This single-threaded helper cannot reap between this check and kill;
            # its direct child's identity is still reserved even after child exit.
            with suppress(ProcessLookupError, PermissionError):
                os.kill(pid, sig)
                if sig == signal.SIGTERM:
                    self.term_sent.add(pid)

    def reap_children(self) -> bool:
        while True:
            try:
                waited, status = os.waitpid(-1, os.WNOHANG | _WAIT_ALL)
            except ChildProcessError:
                return True
            if not waited:
                return False
            self.term_sent.discard(waited)
            if (
                self.process is not None
                and self.returncode is None
                and waited == self.process.pid
            ):
                self.returncode = os.waitstatus_to_exitcode(status)
                self.process.returncode = self.returncode
                self.report(
                    "exited", returncode=self.returncode, finished_at=time.monotonic()
                )

    def run(self) -> None:
        while True:
            self.read_requests()
            try:
                self.observe_leader()
                if self.deadline is not None:
                    self.uncertain |= time.monotonic() >= self.deadline
                    self.signal_children()
            except (OSError, ValueError):
                self.fail()
            if self.deadline is not None:
                try:
                    if self.reap_children():
                        self.report("unknown" if self.uncertain else "closed")
                        return
                except OSError:
                    self.fail()
            if self.deadline is not None and time.monotonic() >= self.deadline:
                self.uncertain = True
                if not self.reported_unknown:
                    self.reported_unknown = True
                    self.report("unknown")
            time.sleep(POLL_SECONDS)


def main() -> None:
    """Accept one bounded transient launch, without importing authored code."""
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    enable_subreaper()
    with socket.socket(fileno=int(sys.argv[1])) as connection:
        tree = _Tree(connection)
        try:
            request = receive(connection, expires_at=time.monotonic() + CLEANUP_SECONDS)
            if tree.launch(request):
                tree.run()
        except BaseException:
            tree.fail()
            tree.run()


if __name__ == "__main__":
    main()
