"""Native platform proof, deliberately disconnected from production launching."""

from __future__ import annotations

import json
import os
import secrets
import select
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fixtures import command_supervisor_proof
from fixtures.command_supervisor_proof import (
    FRAME_LIMIT,
    Command,
    ExitWatch,
    encode,
    group_absent,
    receive,
    send,
)

pytestmark = pytest.mark.skipif(
    sys.platform not in {"linux", "darwin"}, reason="Native Linux/macOS process proof"
)
PROTOTYPE = Path(__file__).parents[2] / "fixtures" / "command_supervisor_proof.py"

# Readiness is emitted only after the descendant installed its TERM disposition.
# The alarm is an independent escape hatch if an assertion or the owner fails.
TREE = """
import json, os, signal, sys
signal.alarm(30)
ready_read, ready_write = os.pipe()
child = os.fork()
if child == 0:
    os.close(ready_read)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.alarm(25)
    os.write(ready_write, b'R')
    os.close(ready_write)
    while True:
        signal.pause()
os.close(ready_write)
assert os.read(ready_read, 1) == b'R'
os.close(ready_read)
print(json.dumps({'leader': os.getpid(), 'child': child}), flush=True)
sys.stdin.buffer.read()
"""


def await_condition(predicate: Callable[[], bool], *, timeout: float = 12) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "Native proof condition timed out"
        time.sleep(0.01)


def read_until(descriptor: int, *, newline: bool = False, timeout: float = 12) -> bytes:
    deadline = time.monotonic() + timeout
    result = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        assert remaining > 0, "Output did not settle"
        assert select.select([descriptor], [], [], remaining)[0], (
            "Output did not settle"
        )
        chunk = os.read(descriptor, 65536)
        if not chunk:
            return bytes(result)
        result.extend(chunk)
        if newline and b"\n" in result:
            return bytes(result)


@dataclass
class Launched:
    channel: socket.socket
    stdin: int
    stdout: int
    stderr: int
    response: dict[str, Any]

    def close_stdin(self) -> None:
        if self.stdin >= 0:
            os.close(self.stdin)
            self.stdin = -1

    def close(self) -> None:
        self.close_stdin()
        self.channel.close()
        os.close(self.stdout)
        os.close(self.stderr)


class Proof:
    """Test controller retaining an exact owner handle and independent cleanup."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.capability = secrets.token_hex(24)
        self.parent, child = socket.socketpair()
        self.parent.settimeout(12)
        self.stderr = (root / "owner-stderr.txt").open("wb")
        self.process = subprocess.Popen(
            [sys.executable, str(PROTOTYPE), str(child.fileno()), str(root)],
            pass_fds=(child.fileno(),),
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=self.stderr,
            start_new_session=True,
        )
        child.close()
        assert self.process.stdin is not None
        self.process.stdin.write((self.capability + "\n").encode())
        self.process.stdin.close()
        self.launched: list[Launched] = []
        try:
            self.ready = receive(self.parent)
        except (EOFError, OSError):
            self.close()
            message = (root / "owner-stderr.txt").read_text()
            raise RuntimeError(message) from None

    def connect(self) -> socket.socket:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(12)
        client.connect(str(self.root / "owner.sock"))
        return client

    def launch(
        self,
        code: str,
        request_id: str = "one",
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
    ) -> Launched:
        client = self.connect()
        stdin_read, stdin_write = os.pipe()
        stdout_read, stdout_write = os.pipe()
        stderr_read, stderr_write = os.pipe()
        try:
            send(
                client,
                {
                    "type": "launch",
                    "capability": self.capability,
                    "id": request_id,
                    "argv": [sys.executable, "-c", code],
                    "env": dict(os.environ) if env is None else env,
                    "cwd": cwd,
                },
                (stdin_read, stdout_write, stderr_write),
            )
        finally:
            for descriptor in (stdin_read, stdout_write, stderr_write):
                os.close(descriptor)
        launched = Launched(client, stdin_write, stdout_read, stderr_read, {})
        self.launched.append(launched)
        launched.response = receive(client)
        return launched

    def control(self, kind: str = "status", **kwargs: Any) -> dict[str, Any]:
        send(self.parent, {"type": kind, **kwargs})
        return receive(self.parent)

    def outcome(self, request_id: str = "one") -> dict[str, Any]:
        found: dict[str, Any] = {}

        def settled() -> bool:
            nonlocal found
            found = next(
                record
                for record in self.control()["commands"]
                if record["id"] == request_id
            )
            return found["state"] in {"closed", "unknown", "launch_failed"}

        await_condition(settled)
        return found

    def close(self) -> None:
        # Closing the independent lifetime channel requests cleanup even if an
        # assertion interrupted a worker connection or stopped reading its output.
        self.parent.close()
        for command in self.launched:
            command.close()
        try:
            self.process.wait(timeout=12)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=3)
            raise
        finally:
            self.stderr.close()


@pytest.fixture
def proof(tmp_path: Path) -> Iterator[Proof]:
    # macOS Unix socket paths are short; evidence is copied into pytest's retained
    # tmp_path, while the mode-0700 live socket directory remains under /tmp.
    with tempfile.TemporaryDirectory(prefix="artisan-proof-", dir="/tmp") as temporary:
        try:
            instance = Proof(Path(temporary))
            try:
                yield instance
            finally:
                instance.close()
        finally:
            for name in ("trace.json", "owner-stderr.txt"):
                path = Path(temporary) / name
                if path.exists():
                    (tmp_path / name).write_bytes(path.read_bytes())


def assert_closed(record: dict[str, Any]) -> None:
    assert record["state"] == "closed", record
    events = record["events"]
    assert events.index("accepted") < events.index("launched")
    assert (
        events.index("signal:SIGKILL") < events.index("reaped") < events.index("closed")
    )
    assert not any(
        event.startswith("signal:") for event in events[events.index("reaped") + 1 :]
    )
    assert group_absent(record["pid"])


def test_owner_waits_without_reaping_and_preserves_actual_exit(proof: Proof) -> None:
    launched = proof.launch("raise SystemExit(23)")
    record = proof.outcome()
    assert_closed(record)
    assert record["returncode"] == 23
    assert "exit_observed" in record["events"]
    assert read_until(launched.stdout) == b""
    assert proof.ready["subreaper"] == (sys.platform == "linux")


def test_owner_descriptor_transfer_preserves_cwd_env_and_stdin(
    proof: Proof, tmp_path: Path
) -> None:
    code = "import os, sys; print(os.getcwd()); print(os.environ['PROOF_VALUE']); print(sys.stdin.read()); print('stderr', file=sys.stderr)"
    launched = proof.launch(
        code, env={"PROOF_VALUE": "worker-snapshot"}, cwd=str(tmp_path)
    )
    os.write(launched.stdin, b"input-from-worker")
    launched.close_stdin()
    assert_closed(proof.outcome())
    assert read_until(launched.stdout).decode().splitlines() == [
        str(tmp_path),
        "worker-snapshot",
        "input-from-worker",
    ]
    assert read_until(launched.stderr) == b"stderr\n"


def test_owner_closes_control_descriptors_before_exec(proof: Proof) -> None:
    code = """
import json, os
inherited = []
for descriptor in range(3, 256):
    try:
        os.fstat(descriptor)
    except OSError:
        continue
    inherited.append(descriptor)
print(json.dumps(inherited))
"""
    launched = proof.launch(code)
    assert_closed(proof.outcome())
    assert json.loads(read_until(launched.stdout)) == []


def test_owner_stdout_and_stderr_can_drain_independently(proof: Proof) -> None:
    launched = proof.launch(
        "import os; os.write(1, b'o' * 1048576); os.write(2, b'e' * 1048576)"
    )
    with ThreadPoolExecutor(max_workers=2) as threads:
        stdout = threads.submit(read_until, launched.stdout)
        stderr = threads.submit(read_until, launched.stderr)
        assert_closed(proof.outcome())
        assert stdout.result(timeout=1) == b"o" * 1048576
        assert stderr.result(timeout=1) == b"e" * 1048576


@pytest.mark.parametrize("cwd", ["/missing-artisan-proof-directory", "invalid\x00cwd"])
def test_owner_launch_failure_settles_accepted_request(proof: Proof, cwd: str) -> None:
    launched = proof.launch("pass", cwd=cwd)
    assert launched.response["state"] == "launch_failed"
    record = proof.outcome()
    assert record["pid"] is None
    assert record["events"] == ["accepted", "launch_failed"]
    assert read_until(launched.stdout) == b""


def test_owner_kills_term_resistant_descendant_after_leader_exits(proof: Proof) -> None:
    launched = proof.launch(TREE)
    identities = json.loads(read_until(launched.stdout, newline=True))
    started = time.monotonic()
    proof.control("cancel", deadline=started + 8)
    # A real non-reaping check in the owner will retain the leader until KILL.
    assert_closed(record := proof.outcome())
    assert record["returncode"] == -signal.SIGTERM
    assert time.monotonic() - started < 8
    with pytest.raises(ProcessLookupError):
        os.kill(identities["child"], 0)
    assert read_until(launched.stdout) == b""


def test_owner_natural_exit_closes_child_holding_stdout_without_grace(
    proof: Proof,
) -> None:
    launched = proof.launch(TREE)
    read_until(launched.stdout, newline=True)
    started = time.monotonic()
    launched.close_stdin()
    assert_closed(record := proof.outcome())
    assert record["returncode"] == 0
    assert "signal:SIGTERM" not in record["events"]
    assert time.monotonic() - started < 3
    assert read_until(launched.stdout) == b""


def test_owner_cancel_is_independent_of_blocked_output_reader(proof: Proof) -> None:
    launched = proof.launch(
        "import os, signal; signal.alarm(30); os.write(2, b'ready\\n');\nwhile True: os.write(1, b'x' * 65536)"
    )
    assert read_until(launched.stderr, newline=True) == b"ready\n"
    proof.control("cancel", deadline=time.monotonic() + 5)
    assert_closed(proof.outcome())
    assert read_until(launched.stdout)


def test_owner_closes_multiple_groups_with_one_deadline(proof: Proof) -> None:
    for request_id in ("first", "second", "third"):
        launched = proof.launch(TREE, request_id)
        read_until(launched.stdout, newline=True)
    deadline = time.monotonic() + 5
    proof.control("cancel", deadline=deadline)
    for request_id in ("first", "second", "third"):
        assert_closed(record := proof.outcome(request_id))
        assert record["deadline"] == deadline
    assert time.monotonic() < deadline


def test_owner_repeated_cancel_never_extends_budget_and_rejects_new_launch(
    proof: Proof,
) -> None:
    launched = proof.launch(TREE)
    read_until(launched.stdout, newline=True)
    deadline = time.monotonic() + 5
    proof.control("cancel", deadline=deadline)
    proof.control("cancel", deadline=deadline + 20)
    assert (
        proof.launch("raise RuntimeError('must not launch')", "late").response["state"]
        == "rejected"
    )
    record = proof.outcome()
    assert record["deadline"] == deadline
    assert record["events"].count("signal:SIGTERM") == 1
    assert record["events"].count("signal:SIGKILL") == 1
    assert_closed(record)


def test_owner_duplicate_request_never_launches_twice(proof: Proof) -> None:
    first = proof.launch("import sys; sys.stdin.read()")
    duplicate = proof.launch("raise RuntimeError('must not launch')")
    assert duplicate.response["pid"] == first.response["pid"]
    assert len(proof.control()["commands"]) == 1
    first.close_stdin()
    assert_closed(proof.outcome())


def test_owner_partial_or_invalid_frame_cannot_launch(proof: Proof) -> None:
    for payload in (
        struct.pack("!I", FRAME_LIMIT + 1),
        b"\x00\x00",
        encode({"type": "launch", "capability": "invalid"}),
    ):
        with proof.connect() as client:
            client.sendall(payload)
    assert proof.control()["commands"] == []
    assert proof.control("cancel", deadline=time.monotonic() + 2)["commands"] == []


def test_owner_parent_loss_closes_real_groups(proof: Proof) -> None:
    launched = proof.launch(TREE)
    read_until(launched.stdout, newline=True)
    proof.parent.close()
    assert proof.process.wait(timeout=12) == 0
    trace = json.loads((proof.root / "trace.json").read_text())
    assert not trace["admission"]
    assert_closed(trace["commands"][0])


def test_owner_client_loss_closes_only_its_groups(proof: Proof) -> None:
    lost = proof.launch(TREE, "lost")
    read_until(lost.stdout, newline=True)
    retained = proof.launch(
        "import sys; print('ready', flush=True); sys.stdin.read()", "retained"
    )
    read_until(retained.stdout, newline=True)
    lost.channel.close()
    assert_closed(proof.outcome("lost"))
    still_running = next(
        item for item in proof.control()["commands"] if item["id"] == "retained"
    )
    assert still_running["state"] == "running"
    retained.close_stdin()
    assert_closed(proof.outcome("retained"))


@pytest.mark.parametrize("before_launch", [True, False])
def test_owner_survives_killed_worker_before_and_after_acceptance(
    proof: Proof, before_launch: bool
) -> None:
    code = """
import json, os, signal, socket, sys
payload = json.loads(sys.stdin.readline())
sys.path.insert(0, payload['helpers'])
from command_supervisor_proof import receive, send
client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
client.connect(payload['socket'])
if payload['before_launch']:
    client.sendall(b'\\x00\\x00')
else:
    stdin_read, stdin_write = os.pipe()
    stdout_read, stdout_write = os.pipe()
    stderr_read, stderr_write = os.pipe()
    send(client, {'type': 'launch', 'capability': payload['capability'],
         'id': 'worker', 'argv': [sys.executable, '-c', payload['code']],
         'env': dict(os.environ), 'cwd': None},
         (stdin_read, stdout_write, stderr_write))
    for descriptor in (stdin_read, stdout_write, stderr_write):
        os.close(descriptor)
    receive(client)
    assert os.read(stdout_read, 65536)
print('ready', flush=True)
signal.alarm(25)
signal.pause()
"""
    with direct_child(code) as worker:
        assert worker.stdin is not None
        assert worker.stdout is not None
        payload = {
            "helpers": str(PROTOTYPE.parent),
            "socket": str(proof.root / "owner.sock"),
            "capability": proof.capability,
            "before_launch": before_launch,
            "code": TREE,
        }
        worker.stdin.write((json.dumps(payload) + "\n").encode())
        worker.stdin.flush()
        assert read_until(worker.stdout.fileno(), newline=True) == b"ready\n"
        worker.kill()
        if before_launch:
            assert proof.control()["commands"] == []
        else:
            assert_closed(proof.outcome("worker"))


def test_owner_uses_worker_exit_evidence_when_fork_retains_socket(proof: Proof) -> None:
    code = """
import json, os, signal, socket, sys
payload = json.loads(sys.stdin.readline())
sys.path.insert(0, payload['helpers'])
from command_supervisor_proof import receive, send
client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
client.connect(payload['socket'])
stdin_read, stdin_write = os.pipe()
stdout_read, stdout_write = os.pipe()
stderr_read, stderr_write = os.pipe()
send(client, {'type': 'launch', 'capability': payload['capability'],
     'id': 'worker', 'argv': [sys.executable, '-c', payload['code']],
     'env': dict(os.environ), 'cwd': None},
     (stdin_read, stdout_write, stderr_write))
receive(client)
assert os.read(stdout_read, 65536)
holder = os.fork()
if holder == 0:
    signal.alarm(20)
    signal.pause()
print('ready', flush=True)
signal.alarm(25)
signal.pause()
"""
    with direct_child(code) as worker:
        assert worker.stdin is not None
        assert worker.stdout is not None
        worker.stdin.write(
            (
                json.dumps(
                    {
                        "helpers": str(PROTOTYPE.parent),
                        "socket": str(proof.root / "owner.sock"),
                        "capability": proof.capability,
                        "code": TREE,
                    }
                )
                + "\n"
            ).encode()
        )
        worker.stdin.flush()
        assert read_until(worker.stdout.fileno(), newline=True) == b"ready\n"
        worker.kill()
        watch = ExitWatch(worker.pid)
        try:
            await_condition(watch.exited)
        finally:
            watch.close()
        assert proof.control()["commands"][0]["state"] == "running"
        proof.control("worker_lost", deadline=time.monotonic() + 6)
        assert_closed(proof.outcome("worker"))


def test_owner_lifetime_channel_closes_when_parent_process_dies(proof: Proof) -> None:
    launched = proof.launch(TREE)
    read_until(launched.stdout, newline=True)
    holder = subprocess.Popen(
        [sys.executable, "-c", "import signal; signal.alarm(25); signal.pause()"],
        pass_fds=(proof.parent.fileno(),),
    )
    proof.parent.close()
    try:
        holder.kill()
        holder.wait(timeout=3)
        assert proof.process.wait(timeout=12) == 0
        trace = json.loads((proof.root / "trace.json").read_text())
        assert_closed(trace["commands"][0])
    finally:
        if holder.returncode is None:
            holder.kill()
            holder.wait(timeout=3)


def test_owner_destruction_is_missing_evidence_not_empty_success(proof: Proof) -> None:
    launched = proof.launch(
        "import signal, sys; signal.alarm(25); print('ready', flush=True); sys.stdin.read()"
    )
    read_until(launched.stdout, newline=True)
    try:
        proof.process.kill()
        proof.process.wait(timeout=3)
        with pytest.raises((BrokenPipeError, EOFError, ConnectionResetError)):
            proof.control("cancel", deadline=time.monotonic() + 1)
        assert not group_absent(launched.response["pid"])
    finally:
        # Independent, cooperative cleanup. Never signal an ID whose owner died.
        launched.close_stdin()
        assert read_until(launched.stdout) == b""
        await_condition(lambda: group_absent(launched.response["pid"]))


def test_owner_accepted_creation_remains_unresolved_during_cancel(proof: Proof) -> None:
    barrier = proof.root / "hold-launch"
    barrier.touch()
    with ThreadPoolExecutor(max_workers=1) as thread:
        future = thread.submit(proof.launch, "import sys; sys.stdin.read()")
        try:
            await_condition(
                lambda: (proof.root / "trace.json").exists()
                and bool(
                    json.loads((proof.root / "trace.json").read_text())["commands"]
                )
            )
            trace = json.loads((proof.root / "trace.json").read_text())
            assert trace["commands"][0]["state"] == "starting"
            assert trace["commands"][0]["pid"] is None
            send(proof.parent, {"type": "cancel", "deadline": time.monotonic() + 7})
            assert not select.select([proof.parent], [], [], 0.1)[0]
            assert not future.done()
        finally:
            barrier.unlink(missing_ok=True)
        future.result(timeout=3)
        receive(proof.parent)
    assert_closed(proof.outcome())


@pytest.mark.parametrize(
    "payload",
    [
        b"\x00\x00",
        struct.pack("!I", FRAME_LIMIT + 1),
        encode({"type": "launch", "capability": "invalid"}),
    ],
)
def test_owner_rejection_closes_transferred_descriptors(
    proof: Proof, payload: bytes
) -> None:
    read_end, write_end = os.pipe()
    null = os.open(os.devnull, os.O_RDONLY)
    try:
        with proof.connect() as client:
            socket.send_fds(client, [payload], [null, write_end, write_end])
        os.close(write_end)
        write_end = -1
        assert read_until(read_end) == b""
        assert proof.control()["commands"] == []
    finally:
        os.close(read_end)
        os.close(null)
        if write_end >= 0:
            os.close(write_end)


@contextmanager
def direct_child(code: str) -> Iterator[subprocess.Popen[bytes]]:
    """Keep a direct-child identity pinned for independent failure cleanup."""
    process = subprocess.Popen(
        [sys.executable, "-c", code],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        process_group=0,
    )
    try:
        yield process
    finally:
        if process.returncode is None:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=3)
        await_condition(lambda: group_absent(process.pid), timeout=3)
        for stream in (process.stdin, process.stdout, process.stderr):
            assert stream is not None
            stream.close()


def test_owner_does_not_signal_unrelated_group(proof: Proof) -> None:
    with direct_child(
        "import sys; print('ready', flush=True); sys.stdin.read()"
    ) as unrelated:
        assert unrelated.stdout is not None
        read_until(unrelated.stdout.fileno(), newline=True)
        launched = proof.launch(TREE)
        read_until(launched.stdout, newline=True)
        proof.control("cancel", deadline=time.monotonic() + 6)
        assert_closed(proof.outcome())
        # Observe without reaping: the negative control must still be running.
        watcher = ExitWatch(unrelated.pid)
        try:
            assert not watcher.exited()
        finally:
            watcher.close()


def test_unknown_signal_failure_never_confirms_a_live_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with direct_child(
        "import sys; print('ready', flush=True); sys.stdin.read()"
    ) as process:
        assert process.stdout is not None
        read_until(process.stdout.fileno(), newline=True)
        command = Command(
            "negative-control",
            0,
            process=process,
            watch=ExitWatch(process.pid),
            state="running",
        )
        original = os.killpg

        def denied(pgid: int, sig: int) -> None:
            if sig:
                message = "Injected permission failure, with a real surviving child"
                raise PermissionError(message)
            original(pgid, sig)

        with monkeypatch.context() as patch:
            patch.setattr(os, "killpg", denied)
            command.stop(time.monotonic() + 0.05, immediate=True)
            await_condition(
                lambda: (
                    command.tick({process.pid: command.request_id})
                    or command.state == "unknown"
                ),
                timeout=1,
            )
        assert command.state == "unknown"
        assert not command.reaped
        assert not group_absent(process.pid)
        # The independent direct-child finalizer, not the relinquished owner,
        # closes this intentionally surviving negative-control process.


@pytest.mark.parametrize("during_registration", [True, False])
def test_unknown_watch_failure_survives_positive_group_cleanup(
    monkeypatch: pytest.MonkeyPatch, during_registration: bool
) -> None:
    with direct_child(
        "import sys; print('ready', flush=True); sys.stdin.read()"
    ) as process:
        assert process.stdout is not None
        read_until(process.stdout.fileno(), newline=True)
        command = Command("watch-error", 0, process=process, state="running")

        def unavailable(*args: Any) -> None:
            message = "Injected non-ESRCH watch failure"
            raise PermissionError(message)

        with monkeypatch.context() as patch:
            if during_registration:
                patch.setattr(command_supervisor_proof, "ExitWatch", unavailable)
            command.register_watch()
            if not during_registration:
                assert command.watch is not None
                patch.setattr(command.watch, "exited", unavailable)
            await_condition(
                lambda: (
                    command.tick({process.pid: command.request_id})
                    or command.state == "unknown"
                )
            )
        assert command.reaped
        assert group_absent(process.pid)
        assert command.state == "unknown"
        assert "observation_failed" in command.events


def test_released_identity_cannot_signal_or_reap_a_reused_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with direct_child("pass") as process:
        command = Command(
            "old", 0, process=process, state="stopping", final_signal=True
        )
        process.wait(timeout=3)
        command.reaped = True

        def forbidden(*args: Any) -> None:
            pytest.fail("Released identity attempted destructive signaling/reaping")

        with monkeypatch.context() as patch:
            patch.setattr(os, "killpg", forbidden)
            patch.setattr(os, "waitpid", forbidden)
            with pytest.raises(RuntimeError, match="identity release"):
                command._signal(signal.SIGKILL)
            command._observe_closure({process.pid: "new-owner"})
        assert command.state == "closed"


@pytest.mark.skipif(
    sys.platform != "darwin", reason="Actual macOS kqueue fast-exit qualification"
)
def test_macos_exit_before_watch_registration_preserves_status() -> None:
    with direct_child("raise SystemExit(17)") as process:
        first_watch = ExitWatch(process.pid)
        try:
            await_condition(first_watch.exited)
        finally:
            first_watch.close()
        # The initial watcher has proved exit without reaping. Registering again
        # deterministically covers an already exited, still reserved child.
        command = Command(
            "fast-exit",
            0,
            process=process,
            watch=ExitWatch(process.pid),
            state="running",
        )
        assert command.watch.fast_exit
        await_condition(
            lambda: (
                command.tick({process.pid: "fast-exit"}) or command.state == "closed"
            )
        )
        assert process.returncode == 17
        assert command.events.index("signal:SIGKILL") < command.events.index("reaped")
