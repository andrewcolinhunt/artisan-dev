"""Bound control failures and release exact resources during partial startup."""

from __future__ import annotations

import contextlib
import os
import select
import socket
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.utils import command_supervisor as client
from artisan.utils._command_supervisor import encode, receive, send
from artisan.utils.external_tools import run_command


def _stop_peer(
    peer: socket.socket, stop: threading.Event, responder: threading.Thread
) -> None:
    stop.set()
    with contextlib.suppress(OSError):
        peer.shutdown(socket.SHUT_RDWR)
    responder.join(timeout=2)
    assert not responder.is_alive()


@pytest.fixture
def connection_pair() -> Iterator[tuple[client._Connection, socket.socket]]:
    local, remote = socket.socketpair()
    connection = object.__new__(client._Connection)
    connection.attachment = {
        "socket": "unused",
        "secret": "test-capability",
        "scope": "root",
        "parent": "",
    }
    connection.socket = local
    connection.lock = threading.Lock()
    try:
        yield connection, remote
    finally:
        connection.close()
        remote.close()


@pytest.mark.parametrize("prefix_size", [2, 6])
def test_partial_response_obeys_one_absolute_deadline(
    connection_pair: tuple[client._Connection, socket.socket], prefix_size: int
) -> None:
    connection, peer = connection_pair
    stop = threading.Event()

    def respond() -> None:
        try:
            receive(peer)
            frame = encode({"state": "closed"})
            peer.sendall(frame[:prefix_size])
            # Each byte arrives within the ordinary socket timeout. The whole
            # frame must still use the original caller's shorter deadline.
            for byte in frame[prefix_size:]:
                if stop.wait(0.04):
                    break
                peer.sendall(bytes([byte]))
        except (OSError, EOFError):
            pass

    responder = threading.Thread(target=respond)
    responder.start()
    started = time.monotonic()
    try:
        with pytest.raises(client.CommandOwnershipError):
            connection.request("check", expires_at=started + 0.12)
        assert time.monotonic() - started < 0.5
        assert connection.socket.fileno() == -1
    finally:
        _stop_peer(peer, stop, responder)


def test_scope_close_uses_remaining_deadline_and_preserves_primary_error(
    connection_pair: tuple[client._Connection, socket.socket],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection, peer = connection_pair
    stop = threading.Event()

    def respond() -> None:
        with contextlib.suppress(OSError, EOFError):
            receive(peer)
            send(peer, {"state": "open"})
            receive(peer)
            peer.sendall(b"\x00\x00")
            stop.wait(2)

    responder = threading.Thread(target=respond)
    responder.start()
    monkeypatch.setattr(client, "_Connection", lambda _: connection)
    try:
        invocation = client._Invocation(connection.attachment)
        started = time.monotonic()
        invocation.deadline = started + 0.12
        primary = ValueError("primary failure")
        assert invocation.close(primary) is False
        assert time.monotonic() - started < 0.5
        assert str(primary) == "primary failure"
        assert "cleanup could not be confirmed" in primary.__notes__[0]
        with pytest.raises(client.CommandOwnershipError):
            invocation.close()
    finally:
        _stop_peer(peer, stop, responder)


def test_command_interrupt_uses_remaining_deadline(
    connection_pair: tuple[client._Connection, socket.socket],
) -> None:
    connection, peer = connection_pair
    stop = threading.Event()

    def respond() -> None:
        with contextlib.suppress(OSError, EOFError):
            receive(peer)
            peer.sendall(b"\x00\x00")
            stop.wait(2)

    responder = threading.Thread(target=respond)
    responder.start()
    command = object.__new__(client.ManagedCommand)
    command.connection = connection
    command.identity = "accepted-command"
    command.collection_complete = False
    started = time.monotonic()
    command.deadline = started + 0.12
    primary = KeyboardInterrupt()
    try:
        command.interrupt(primary)
        assert time.monotonic() - started < 0.5
        assert "cleanup could not be confirmed" in primary.__notes__[0]
    finally:
        _stop_peer(peer, stop, responder)


@pytest.mark.parametrize("failure", [client.CommandOwnershipError, KeyboardInterrupt])
def test_failed_invocation_open_closes_its_connection(
    monkeypatch: pytest.MonkeyPatch, failure: type[BaseException]
) -> None:
    connection = Mock()
    connection.request.side_effect = failure
    monkeypatch.setattr(client, "_Connection", Mock(return_value=connection))
    with pytest.raises(failure):
        client._Invocation({"parent": ""})
    connection.close.assert_called_once()


def test_second_pipe_failure_closes_first_pipe_and_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = Mock()
    monkeypatch.setattr(client, "_Connection", Mock(return_value=connection))
    descriptors = os.pipe()
    monkeypatch.setattr(client.os, "pipe", Mock(side_effect=[descriptors, OSError]))
    try:
        with pytest.raises(OSError):
            client.ManagedCommand({}, ["tool"], {}, None, merged=False, external=False)
        connection.request.assert_not_called()
        connection.close.assert_called_once()
        for descriptor in descriptors:
            with pytest.raises(OSError):
                os.fstat(descriptor)
    finally:
        for descriptor in descriptors:
            with contextlib.suppress(OSError):
                os.close(descriptor)


def test_invocation_entry_failure_closes_and_reaps_new_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owners = []
    start = client.CommandSupervisor.start

    def start_owner() -> Any:
        owner = start()
        owners.append(owner)
        return owner

    monkeypatch.setattr(client.CommandSupervisor, "start", start_owner)
    monkeypatch.setattr(
        client, "_Invocation", Mock(side_effect=client.CommandOwnershipError)
    )
    with pytest.raises(client.CommandOwnershipError), client.managed_invocation():
        pytest.fail("user code ran after failed attachment")
    assert len(owners) == 1
    assert owners[0].process.returncode is not None
    assert not Path(owners[0].root).exists()
    assert client._current is None


def test_interrupted_owner_start_retains_exact_process_until_reaped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processes = []
    launch = client.subprocess.Popen

    def start_process(*args: Any, **kwargs: Any) -> Any:
        process = launch(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(client.subprocess, "Popen", start_process)
    monkeypatch.setattr(client, "receive", Mock(side_effect=KeyboardInterrupt))
    with pytest.raises(KeyboardInterrupt):
        client.CommandSupervisor.start()
    assert len(processes) == 1
    processes[0].wait(timeout=5)


def test_partial_startup_handshake_has_one_total_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processes = []
    launch = client.subprocess.Popen
    program = """
import socket, struct, sys, time
channel = socket.socket(fileno=int(sys.argv[1]))
channel.sendall(struct.pack('!I', 4096))
deadline = time.monotonic() + 1
try:
    while time.monotonic() < deadline:
        channel.sendall(b' ')
        time.sleep(.04)
except BrokenPipeError:
    pass
"""

    def start_process(*args: Any, **kwargs: Any) -> Any:
        process = launch(
            [sys.executable, "-c", program, str(kwargs["pass_fds"][0])], **kwargs
        )
        processes.append(process)
        return process

    monkeypatch.setattr(client.subprocess, "Popen", start_process)
    monkeypatch.setattr(client, "CLEANUP_SECONDS", 0.12)
    started = time.monotonic()
    try:
        with pytest.raises(client.CommandOwnershipError):
            client.CommandSupervisor.start()
        assert time.monotonic() - started < 0.7
    finally:
        for process in processes:
            process.wait(timeout=3)


@pytest.mark.parametrize("poll_before_report", [False, True])
def test_expired_owner_outcome_never_upgrades_from_late_closed_report(
    poll_before_report: bool,
) -> None:
    owner = client.CommandSupervisor.start()
    deadline = time.monotonic() - 0.01
    try:
        owner.request_close(deadline)
        if poll_before_report:
            assert owner.poll().state == "unknown"
        if owner._snapshot.get("state") != "closed":
            assert select.select([owner.connection], [], [], 2)[0]
        limit = time.monotonic() + 2
        while owner._snapshot.get("state") != "closed" and time.monotonic() < limit:
            assert owner.poll().state == "unknown"
        assert owner._snapshot["state"] == "closed"
        assert owner.poll().state == "unknown"
        assert owner.finish(deadline).state == "unknown"
    finally:
        owner.finish(deadline)


def test_completed_external_command_survives_ordinary_worker_eof() -> None:
    owner = client.CommandSupervisor.start()
    invocation = None
    command = None
    try:
        invocation = client._Invocation(owner.attachment, bootstrap=True)
        command = client.ManagedCommand(
            invocation.attachment,
            [sys.executable, "-c", "pass"],
            dict(os.environ),
            None,
            merged=False,
            external=True,
        )
        command.communicate()
        command.dispose()
        invocation.connection.close()
        assert owner.finish(time.monotonic() + 5).state == "closed"
    finally:
        if command is not None:
            command.dispose()
        if invocation is not None:
            invocation.connection.close()
        owner.finish(time.monotonic() + 5)


def test_active_external_client_loss_is_sticky_in_scope_and_owner(
    tmp_path: Path,
) -> None:
    owner = client.CommandSupervisor.start()
    invocation = None
    command = None
    marker = tmp_path / "external-ready"
    code = """
import os, pathlib, signal, sys, time
signal.alarm(15)
pathlib.Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(12)
"""
    try:
        invocation = client._Invocation(owner.attachment)
        command = client.ManagedCommand(
            invocation.attachment,
            [sys.executable, "-c", code, str(marker)],
            dict(os.environ),
            None,
            merged=False,
            external=True,
        )
        limit = time.monotonic() + 5
        while not marker.exists() and time.monotonic() < limit:
            time.sleep(0.01)
        assert marker.exists()
        command.dispose()
        limit = time.monotonic() + 2
        snapshot = invocation.connection.request("check")
        while not snapshot["uncertain"] and time.monotonic() < limit:
            time.sleep(0.01)
            snapshot = invocation.connection.request("check")
        assert snapshot["uncertain"]
        assert owner.finish(time.monotonic() + 5).state == "unknown"
        with pytest.raises(ProcessLookupError):
            os.kill(int(marker.read_text()), 0)
    finally:
        if command is not None:
            command.dispose()
        if invocation is not None:
            invocation.connection.close()
        owner.finish(time.monotonic() + 5)


def test_external_nested_interruption_prevents_parent_success(tmp_path: Path) -> None:
    marker = tmp_path / "nested-ready"
    code = """
import os, pathlib, sys, time
from artisan.schemas.operation_config.environment_spec import EnvironmentSpec
from artisan.utils.external_tools import run_command
nested = '''import os, pathlib, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
signal.alarm(15)
pathlib.Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(12)
'''
child = os.fork()
if not child:
    run_command(EnvironmentSpec(), [sys.executable, '-c', nested, sys.argv[1]])
    os._exit(0)
while not pathlib.Path(sys.argv[1]).exists(): time.sleep(.01)
os._exit(0)
"""
    with pytest.raises(client.CommandOwnershipError):
        run_command(LocalEnvironmentSpec(), [sys.executable, "-c", code, str(marker)])
    pid = int(marker.read_text())
    limit = time.monotonic() + 3
    while time.monotonic() < limit:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.01)
    else:
        pytest.fail("Interrupted nested external host group remains alive")


def test_caught_command_status_failure_poisoned_unit_cannot_seal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = client._Connection.request
    failed = False

    def lose_one_response(
        connection: client._Connection, kind: str, **kwargs: Any
    ) -> Any:
        nonlocal failed
        response = request(connection, kind, **kwargs)
        if kind == "command" and not failed:
            failed = True
            connection.close()
            raise client.CommandOwnershipError()
        return response

    monkeypatch.setattr(client._Connection, "request", lose_one_response)
    with pytest.raises(client.CommandOwnershipError), client.managed_invocation():
        invocation = client._current
        assert invocation is not None
        with pytest.raises(client.CommandOwnershipError):
            run_command(LocalEnvironmentSpec(), [sys.executable, "-c", "pass"])
        assert invocation.uncertain
        assert invocation.connection.request("check")["uncertain"]
        client.close_command_scope()


def test_caught_bootstrap_failure_blocks_first_unit_when_report_is_lost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = client.CommandSupervisor.start()
    bootstrap = client._Invocation(owner.attachment, bootstrap=True)
    monkeypatch.setattr(client, "_bootstrap", bootstrap)
    request = client._Connection.request
    failed = False

    def lose_command_evidence(
        connection: client._Connection, kind: str, **kwargs: Any
    ) -> Any:
        nonlocal failed
        if kind == "uncertain":
            raise client.CommandOwnershipError()
        response = request(connection, kind, **kwargs)
        if kind == "command" and not failed:
            failed = True
            connection.close()
            raise client.CommandOwnershipError()
        return response

    monkeypatch.setattr(client._Connection, "request", lose_command_evidence)
    try:
        with pytest.raises(client.CommandOwnershipError):
            run_command(LocalEnvironmentSpec(), [sys.executable, "-c", "pass"])
        assert bootstrap.uncertain
        assert not bootstrap.connection.request("check")["uncertain"]
        with pytest.raises(client.CommandOwnershipError):
            client.validate_worker_attachment()
        with pytest.raises(client.CommandOwnershipError), client.managed_invocation():
            pytest.fail("First unit entered after caught bootstrap uncertainty")
    finally:
        bootstrap.connection.close()
        owner.finish(time.monotonic() + 5)


def test_caught_raw_nested_status_failure_reaches_parent_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    marker = tmp_path / "caught-nested-failure"
    code = """
import pathlib, sys
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.utils import command_supervisor as client
from artisan.utils.external_tools import run_command
request = client._Connection.request
failed = False
def lose_one_response(connection, kind, **kwargs):
    global failed
    response = request(connection, kind, **kwargs)
    if kind == "command" and not failed:
        failed = True
        connection.close()
        raise client.CommandOwnershipError()
    return response
client._Connection.request = lose_one_response
try:
    run_command(LocalEnvironmentSpec(), [sys.executable, "-c", "pass"])
except client.CommandOwnershipError:
    pathlib.Path(sys.argv[1]).write_text("caught")
else:
    raise AssertionError("Expected injected ownership failure")
"""
    request = client._Connection.request

    def observe_after_nested_catch(
        connection: client._Connection, kind: str, **kwargs: Any
    ) -> Any:
        if kind == "command":
            limit = time.monotonic() + 5
            while not marker.exists() and time.monotonic() < limit:
                time.sleep(0.01)
            assert marker.exists()
        return request(connection, kind, **kwargs)

    monkeypatch.setattr(client._Connection, "request", observe_after_nested_catch)
    with pytest.raises(client.CommandOwnershipError):
        run_command(LocalEnvironmentSpec(), [sys.executable, "-c", code, str(marker)])
    assert marker.read_text() == "caught"


def test_owned_scope_close_poisoned_by_primary_ownership_error() -> None:
    owner = client.CommandSupervisor.start()
    invocation = client._Invocation(owner.attachment)
    try:
        assert not invocation.close(client.CommandOwnershipError())
        assert invocation.connection.request("check")["uncertain"]
        assert owner.finish(time.monotonic() + 5).state == "unknown"
    finally:
        invocation.connection.close()
        owner.finish(time.monotonic() + 5)


def test_caught_attachment_validation_failure_poisoned_unit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = client._Connection.request

    def fail_attachment(
        connection: client._Connection, kind: str, **kwargs: Any
    ) -> Any:
        if kind == "attach":
            raise client.CommandOwnershipError()
        return request(connection, kind, **kwargs)

    monkeypatch.setattr(client._Connection, "request", fail_attachment)
    with pytest.raises(client.CommandOwnershipError), client.managed_invocation():
        invocation = client._current
        assert invocation is not None
        with pytest.raises(client.CommandOwnershipError), client.command_attachment():
            pytest.fail("Command wrapper entered after failed attachment validation")
        assert invocation.uncertain
        assert invocation.connection.request("check")["uncertain"]


def test_reported_uncertainty_rejects_scope_open_and_direct_launch() -> None:
    owner = client.CommandSupervisor.start()
    invocation = client._Invocation(owner.attachment)
    command = None
    try:
        invocation.connection.request("uncertain", parent=None)
        with pytest.raises(client.CommandOwnershipError):
            client._Invocation(invocation.attachment)
        with pytest.raises(client.CommandOwnershipError):
            command = client.ManagedCommand(
                invocation.attachment,
                [sys.executable, "-c", "pass"],
                dict(os.environ),
                None,
                merged=False,
                external=False,
            )
    finally:
        if command is not None:
            command.dispose()
        invocation.connection.close()
        owner.finish(time.monotonic() + 5)


def test_retained_pipe_reports_uncertainty_without_new_drain_deadline() -> None:
    owner = client.CommandSupervisor.start()
    invocation = client._Invocation(owner.attachment)
    command = None
    retained_read, retained_write = os.pipe()
    try:
        command = client.ManagedCommand(
            invocation.attachment,
            [sys.executable, "-c", "pass"],
            dict(os.environ),
            None,
            merged=False,
            external=False,
        )
        # Model a writer retained outside the proven group. Its lifetime is owned
        # by this test, so no escaped descendant or orphan survives the assertion.
        command.readers.append(retained_read)
        started = time.monotonic()
        with pytest.raises(client.CommandOwnershipError):
            command.communicate()
        assert time.monotonic() - started < 2
        assert invocation.connection.request("check")["uncertain"]
    finally:
        os.close(retained_write)
        if command is not None:
            command.dispose()
        else:
            os.close(retained_read)
        invocation.connection.close()
        owner.finish(time.monotonic() + 5)


def _closed_uncollected_command(
    owner: client.CommandSupervisor,
) -> client.ManagedCommand:
    command = client.ManagedCommand(
        owner.attachment,
        [sys.executable, "-c", "print('buffered')"],
        dict(os.environ),
        None,
        merged=False,
        external=False,
    )
    limit = time.monotonic() + 3
    while command.state != "closed" and time.monotonic() < limit:
        command.poll()
        time.sleep(0.01)
    assert command.state == "closed"
    return command


@pytest.mark.parametrize("controller_event", ["cancel", "worker_lost"])
def test_authoritative_cancellation_waives_uncollected_dead_client(
    controller_event: str,
) -> None:
    owner = client.CommandSupervisor.start()
    command = None
    try:
        command = _closed_uncollected_command(owner)
        command.dispose()
        deadline = time.monotonic() + 2
        if controller_event == "worker_lost":
            owner.worker_lost(12345, deadline)
        else:
            owner.request_close(deadline)
        assert owner.finish(deadline).state == "closed"
    finally:
        if command is not None:
            command.dispose()
        owner.finish(time.monotonic() + 2)


def test_missing_collection_on_generic_eof_is_unknown_at_existing_deadline() -> None:
    owner = client.CommandSupervisor.start()
    command = None
    try:
        command = _closed_uncollected_command(owner)
        command.dispose()
        deadline = time.monotonic() + 0.15
        assert owner.finish(deadline).state == "unknown"
        owner.request_close(time.monotonic() + 2)
        assert owner.poll().state == "unknown"
    finally:
        if command is not None:
            command.dispose()
        owner.finish(time.monotonic() + 2)


def test_collection_requires_original_launch_channel() -> None:
    owner = client.CommandSupervisor.start()
    command = None
    forged = None
    try:
        command = _closed_uncollected_command(owner)
        forged = client._Connection(owner.attachment)
        forged.notify("collected", id=command.identity)
        with pytest.raises(client.CommandOwnershipError):
            forged.request("check")
        deadline = time.monotonic() + 0.15
        assert owner.finish(deadline).state == "unknown"
    finally:
        if forged is not None:
            forged.close()
        if command is not None:
            command.dispose()
        owner.finish(time.monotonic() + 2)


def test_final_collection_assertion_does_not_wait_for_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = client.CommandSupervisor.start()
    command = None
    try:
        command = _closed_uncollected_command(owner)
        notify = command.connection.notify

        def notify_without_followup(kind: str, **kwargs: Any) -> None:
            notify(kind, **kwargs)
            monkeypatch.setattr(
                command.connection,
                "request",
                Mock(side_effect=AssertionError("Reply read after final assertion")),
            )

        monkeypatch.setattr(command.connection, "notify", notify_without_followup)
        assert command.communicate() == ("buffered\n", "")
        command.dispose()
        assert owner.finish(time.monotonic() + 2).state == "closed"
    finally:
        if command is not None:
            command.dispose()
        owner.finish(time.monotonic() + 2)


def test_bootstrap_gate_waits_for_final_collection_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = client.CommandSupervisor.start()
    bootstrap = client._Invocation(owner.attachment, bootstrap=True)
    monkeypatch.setattr(client, "_bootstrap", bootstrap)
    command = None
    try:
        command = client.ManagedCommand(
            bootstrap.attachment,
            [sys.executable, "-c", "pass"],
            dict(os.environ),
            None,
            merged=False,
            external=False,
        )
        notify = command.connection.notify
        delayed = Mock()
        monkeypatch.setattr(command.connection, "notify", delayed)
        assert command.communicate() == ("", "")
        request = bootstrap.connection.request
        waited = False

        def deliver_after_gate(kind: str, **kwargs: Any) -> Any:
            nonlocal waited
            if kind == "check" and not waited:
                waited = True
                notify(*delayed.call_args.args, **delayed.call_args.kwargs)
            return request(kind, **kwargs)

        monkeypatch.setattr(bootstrap.connection, "request", deliver_after_gate)
        with client.managed_invocation():
            assert waited
    finally:
        if command is not None:
            command.dispose()
        bootstrap.connection.close()
        owner.finish(time.monotonic() + 2)


def test_post_collection_error_never_reopens_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = client.CommandSupervisor.start()
    command = None
    try:
        command = _closed_uncollected_command(owner)
        assert command.communicate() == ("buffered\n", "")
        request = Mock(side_effect=AssertionError("Cleanup after final collection"))
        monkeypatch.setattr(command.connection, "request", request)
        primary = OSError("diagnostic write failed")
        command.interrupt(primary)
        request.assert_not_called()
        assert not hasattr(primary, "__notes__")
        command.dispose()
        assert owner.finish(time.monotonic() + 2).state == "closed"
    finally:
        if command is not None:
            command.dispose()
        owner.finish(time.monotonic() + 2)


def test_partial_collection_frame_cannot_complete_result() -> None:
    owner = client.CommandSupervisor.start()
    command = None
    try:
        command = _closed_uncollected_command(owner)
        frame = encode(
            {
                "type": "collected",
                "capability": owner.attachment["secret"],
                "scope": owner.attachment["scope"],
                "id": command.identity,
            }
        )
        command.connection.socket.sendall(frame[:7])
        command.dispose()
        assert owner.finish(time.monotonic() + 0.15).state == "unknown"
    finally:
        if command is not None:
            command.dispose()
        owner.finish(time.monotonic() + 2)


def test_caught_raw_nested_collection_failure_survives_lost_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = tmp_path / "caught-missing-collection"
    code = """
import pathlib, sys
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.utils import command_supervisor as client
from artisan.utils.external_tools import run_command
request = client._Connection.request
def lose_report(connection, kind, **kwargs):
    if kind == "uncertain":
        raise client.CommandOwnershipError()
    return request(connection, kind, **kwargs)
def lose_collection(connection, kind, **kwargs):
    connection.close()
    raise client.CommandOwnershipError()
client._Connection.request = lose_report
client._Connection.notify = lose_collection
try:
    run_command(LocalEnvironmentSpec(), [sys.executable, "-c", "pass"])
except client.CommandOwnershipError:
    pathlib.Path(sys.argv[1]).write_text("caught")
else:
    raise AssertionError("Expected failed collection assertion")
"""
    request = client._Connection.request

    def wait_for_nested_catch(
        connection: client._Connection, kind: str, **kwargs: Any
    ) -> Any:
        if kind == "command":
            limit = time.monotonic() + 5
            while not marker.exists() and time.monotonic() < limit:
                time.sleep(0.01)
            assert marker.exists()
        return request(connection, kind, **kwargs)

    monkeypatch.setattr(client._Connection, "request", wait_for_nested_catch)
    with pytest.raises(client.CommandOwnershipError):
        run_command(LocalEnvironmentSpec(), [sys.executable, "-c", code, str(marker)])
    assert marker.read_text() == "caught"


def test_caught_nested_scope_close_failure_survives_lost_report(tmp_path: Path) -> None:
    marker = tmp_path / "caught-nested-close"
    code = """
import pathlib, sys
from artisan.utils import command_supervisor as client
request = client._Connection.request
def lose_close(connection, kind, **kwargs):
    response = request(connection, kind, **kwargs)
    if kind == "close":
        connection.close()
        raise client.CommandOwnershipError()
    return response
client._Connection.request = lose_close
client._record_uncertainty = lambda *args, **kwargs: None
try:
    with client.managed_invocation():
        pass
except client.CommandOwnershipError:
    pathlib.Path(sys.argv[1]).write_text("caught")
"""
    with pytest.raises(client.CommandOwnershipError):
        run_command(LocalEnvironmentSpec(), [sys.executable, "-c", code, str(marker)])
    assert marker.read_text() == "caught"


def test_completed_invocation_close_is_local_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = client.CommandSupervisor.start()
    invocation = client._Invocation(owner.attachment)
    try:
        assert invocation.close()
        request = Mock(side_effect=AssertionError("Read after completion assertion"))
        monkeypatch.setattr(invocation.connection, "request", request)
        assert invocation.close()
        with pytest.raises(client.CommandOwnershipError, match="closed"):
            invocation.check()
        request.assert_not_called()
        invocation.dispose()
        assert owner.finish(time.monotonic() + 2).state == "closed"
    finally:
        invocation.connection.close()
        owner.finish(time.monotonic() + 2)


def test_worker_loss_waives_unfinished_invocation_completion() -> None:
    owner = client.CommandSupervisor.start()
    invocation = client._Invocation(owner.attachment)
    try:
        invocation.connection.close()
        deadline = time.monotonic() + 2
        owner.worker_lost(12345, deadline)
        assert owner.finish(deadline).state == "closed"
    finally:
        invocation.connection.close()
        owner.finish(time.monotonic() + 2)


def test_interrupt_after_scope_completion_does_not_reopen_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(KeyboardInterrupt), client.managed_invocation():
        invocation = client._current
        assert invocation is not None
        notify = invocation.connection.notify
        request = Mock(side_effect=AssertionError("Read after completion assertion"))

        def interrupt_after_notify(kind: str, **kwargs: Any) -> None:
            notify(kind, **kwargs)
            monkeypatch.setattr(invocation.connection, "request", request)
            raise KeyboardInterrupt

        monkeypatch.setattr(invocation.connection, "notify", interrupt_after_notify)
        client.close_command_scope()
    request.assert_not_called()
    assert invocation.owner is not None
    assert invocation.owner.process.returncode is not None
