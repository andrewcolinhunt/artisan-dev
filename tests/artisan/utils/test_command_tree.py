"""Linux descendant ownership survives process-group and session changes."""

from __future__ import annotations

import os
import signal
import socket
import sys
import time
from pathlib import Path

import pytest
from fixtures.commands import IdentityCommandEnvironment

from artisan.utils import _command_supervisor as native
from artisan.utils._command_tree import CommandTree
from artisan.utils.command_supervisor import CommandSupervisor, ManagedCommand
from artisan.utils.external_tools import run_command

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux subreaper proof")

_DETACH = r"""
import os, pathlib, signal, sys, time
marker = pathlib.Path(sys.argv[1])
child = os.fork()
if not child:
    os.setsid()
    if sys.argv[2] == 'double':
        if os.fork():
            os._exit(0)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.alarm(15)
    with open(os.devnull, 'r+b') as null:
        for fd in (0, 1, 2):
            os.dup2(null.fileno(), fd)
    marker.write_text(str(os.getpid()))
    while True:
        time.sleep(.01)
while not marker.exists():
    time.sleep(.005)
if len(sys.argv) > 3:
    signal.alarm(15)
    time.sleep(12)
print('finished', flush=True)
os._exit(0)
"""


@pytest.mark.parametrize("fork_mode", ["single", "double"])
@pytest.mark.parametrize("streaming", [False, True])
def test_natural_completion_reaps_detached_descendants(
    tmp_path: Path, fork_mode: str, streaming: bool
) -> None:
    marker = tmp_path / "child.pid"
    result = run_command(
        IdentityCommandEnvironment(),
        [sys.executable, "-c", _DETACH, str(marker), fork_mode],
        stream_output=streaming,
    )
    assert result.stdout == "finished\n"
    with pytest.raises(ProcessLookupError):
        os.kill(int(marker.read_text()), 0)


def _wait_path(path: Path) -> None:
    deadline = time.monotonic() + 5
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert path.exists()


@pytest.mark.parametrize("fork_mode", ["single", "double"])
def test_cancelling_one_command_reaps_detached_children_and_preserves_sibling(
    tmp_path: Path, fork_mode: str
) -> None:
    owner = CommandSupervisor.start()
    commands: list[ManagedCommand] = []
    try:
        for index in range(2):
            marker = tmp_path / f"{index}.pid"
            commands.append(
                ManagedCommand(
                    owner.attachment,
                    [sys.executable, "-c", _DETACH, str(marker), fork_mode, "hold"],
                    dict(os.environ),
                    None,
                    merged=False,
                    external=False,
                )
            )
            _wait_path(marker)
        commands[0].interrupt(RuntimeError("requested cancellation"))
        assert commands[0].state == "closed"
        commands[0].communicate()
        with pytest.raises(ProcessLookupError):
            os.kill(int((tmp_path / "0.pid").read_text()), 0)
        os.kill(int((tmp_path / "1.pid").read_text()), 0)
        commands[1].poll()
        assert commands[1].state == "running"
        commands[1].interrupt(RuntimeError("requested cancellation"))
        commands[1].communicate()
        with pytest.raises(ProcessLookupError):
            os.kill(int((tmp_path / "1.pid").read_text()), 0)
        assert owner.finish(time.monotonic() + 3).state == "closed"
    finally:
        owner.request_close(time.monotonic() + 3)
        for command in commands:
            command.dispose()
        owner.finish(time.monotonic() + 3)


def _settle(command: native.Command) -> None:
    deadline = time.monotonic() + 5
    while command.state not in native.TERMINAL and time.monotonic() < deadline:
        command.tick({command.pid: command.request_id})
        time.sleep(0.01)
    assert command.state in native.TERMINAL


def test_killed_subreaper_cannot_claim_closed_group() -> None:
    command = native.Command("helper-death", 0)
    with open(os.devnull, "r+b") as null:
        command.launch(
            [sys.executable, "-c", "pass"], dict(os.environ), None, [null.fileno()] * 3
        )
    os.kill(command.pid, signal.SIGKILL)
    _settle(command)
    assert command.state == "unknown"
    assert command.reaped
    assert native.group_absent(command.pid)


def test_missing_subtree_proof_cannot_claim_closed_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = native.Command("missing-proof", 0)
    with open(os.devnull, "r+b") as null:
        command.launch(
            [sys.executable, "-c", "pass"], dict(os.environ), None, [null.fileno()] * 3
        )
    assert command.tree is not None
    messages = command.tree.messages

    def omit_closure() -> list[dict[str, object]]:
        return [message for message in messages() if message.get("type") != "closed"]

    monkeypatch.setattr(command.tree, "messages", omit_closure)
    _settle(command)
    assert command.state == "unknown"
    assert command.reaped
    assert native.group_absent(command.pid)


def test_short_deadline_keeps_reaper_owned_and_unknown_sticky(tmp_path: Path) -> None:
    marker = tmp_path / "child.pid"
    command = native.Command("short-deadline", 0)
    with open(os.devnull, "r+b") as null:
        command.launch(
            [sys.executable, "-c", _DETACH, str(marker), "double", "hold"],
            dict(os.environ),
            None,
            [null.fileno()] * 3,
        )
    _wait_path(marker)
    command.stop(time.monotonic() - 0.001, immediate=True)
    command.tick({command.pid: command.request_id})
    assert command.state == "unknown"
    assert not command.reaped
    assert command.tree is not None
    assert command.tree.socket.fileno() >= 0
    deadline = time.monotonic() + 5
    while not command.reaped and time.monotonic() < deadline:
        command.tick({command.pid: command.request_id})
        time.sleep(0.01)
    assert command.reaped
    assert command.state == "unknown"
    with pytest.raises(ProcessLookupError):
        os.kill(int(marker.read_text()), 0)


def test_buffered_subtree_proof_survives_late_stop_after_helper_exit() -> None:
    local, remote = socket.socketpair()
    tree = object.__new__(CommandTree)
    tree.socket, tree.channel = local, native.Channel(local)
    tree.last_stop, tree.write_failed = None, False
    try:
        native.send(
            remote,
            {"type": "exited", "returncode": -15, "finished_at": time.monotonic()},
        )
        native.send(remote, {"type": "closed"})
        remote.close()
        local.setblocking(False)
        tree.stop(time.monotonic() + 3, time.monotonic())
        assert [message["type"] for message in tree.messages()] == ["exited", "closed"]
    finally:
        remote.close()
        tree.close()
