"""Warm provider calls retain exact command ownership and private scratch lifetime."""

from __future__ import annotations

import hashlib
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fixtures.commands import IdentityCommandEnvironment
from pydantic import ValidationError

from artisan.execution import inert_worker
from artisan.execution.admitted_worker import execute_admitted_assignment
from artisan.execution.curator_reply import CuratorReply, CuratorReplyContext
from artisan.execution.curator_worker import execute_admitted_curator
from artisan.execution.inert_worker import (
    InertCallControl,
    InertCallResult,
    execute_inert_call,
)
from artisan.execution.local_call import InertWorkerCall
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.execution.worker_directory import (
    resolve_worker_directory,
    worker_directory_scope,
)
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.execution.worker_transport import (
    TRANSPORT_PACKAGES,
    RuntimeCompatibility,
    WorkerTransportHeader,
)
from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    CommandSupervisor,
    ManagedCommand,
    OwnershipOutcome,
    attached_worker_unit,
    managed_invocation,
)
from artisan.utils.external_tools import run_command


@pytest.fixture(autouse=True)
def isolate_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(inert_worker, "_lease", threading.Lock())
    monkeypatch.setattr(inert_worker, "_retained", [])


@pytest.fixture
def inert() -> InertWorkerCall:
    payload = b"opaque trusted live call"
    compatibility = RuntimeCompatibility.model_validate(
        {
            "python_implementation": "CPython",
            "python_version": "3.12.12",
            "cache_tag": "cpython-312",
            "packages": [
                {
                    "name": name,
                    "version": "1",
                }
                for name in sorted(TRANSPORT_PACKAGES)
            ],
        }
    )
    header = WorkerTransportHeader(
        compatibility=compatibility, payload_sha256=hashlib.sha256(payload).hexdigest()
    )
    context = CuratorReplyContext(
        admission=CuratorAdmissionReference(
            step_run_id="a" * 32, admission_digest="b" * 32
        ),
        payload_sha256=header.payload_sha256,
        sensitive_values=("synthetic-secret",),
    )
    return InertWorkerCall(header, payload, context.model_dump_json().encode())


def reply(call: InertWorkerCall, *, unknown: bool = False) -> bytes:
    context = CuratorReplyContext.model_validate_json(call.reply_context)
    value = (
        CuratorReply.failed(context, CommandOwnershipError())
        if unknown
        else CuratorReply.staged(
            context, StagingResult(success=False, error="authored failure")
        )
    )
    return value.model_dump_json().encode()


@pytest.fixture
def owner(monkeypatch: pytest.MonkeyPatch) -> Mock:
    value = Mock(spec=CommandSupervisor)
    value.attachment = {
        "socket": "owned",
        "secret": "synthetic-owner",
        "scope": "root",
        "parent": "",
    }
    value.finish.return_value = OwnershipOutcome("closed")
    monkeypatch.setattr(CommandSupervisor, "start", lambda: value)
    return value


def science(
    monkeypatch: pytest.MonkeyPatch,
    call: InertWorkerCall,
    *,
    unknown: bool = False,
    preserve: bool = False,
) -> list[Path]:
    seen = []

    def execute(directory: Path, **_kwargs: object) -> bytes:
        context = CuratorReplyContext.model_validate_json(
            (directory / "reply-context.json").read_bytes()
        )
        assert "synthetic-owner" in context.sensitive_values
        worker = resolve_worker_directory(None, preserve_working=preserve)
        (worker / "result.txt").write_text("worker result")
        seen.append(worker)
        return reply(call, unknown=unknown)

    monkeypatch.setattr(inert_worker, "execute_call_directory", execute)
    return seen


@pytest.mark.parametrize("preserve", [False, True])
def test_warm_calls_get_fresh_scratch_and_remove_transport_credentials(
    inert: InertWorkerCall,
    owner: Mock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    preserve: bool,
) -> None:
    seen = science(monkeypatch, inert, preserve=preserve)
    first = execute_inert_call(inert, working_root=tmp_path, preserve_working=preserve)
    second = execute_inert_call(inert, working_root=tmp_path, preserve_working=preserve)
    assert first.entry_id != second.entry_id
    assert len(set(seen)) == 2
    assert first.ownership == second.ownership == OwnershipState.CLOSED
    assert first.retained_working is preserve
    assert first.reply == reply(inert)
    assert owner.finish.call_count == 2
    for worker in seen:
        assert worker.exists() is preserve
        assert not (worker.parent / "transport").exists()
        if preserve:
            assert {p.name for p in worker.parent.iterdir()} == {"worker"}
    with pytest.raises(RuntimeError, match="active worker-owned"):
        resolve_worker_directory(None)


@pytest.mark.parametrize("failure", ["outcome", "finish", "request", "reply", "remove"])
def test_unknown_cleanup_hides_reply_retains_owner_and_poisons_warm_entry(
    inert: InertWorkerCall,
    owner: Mock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    seen = science(monkeypatch, inert, unknown=failure in {"reply", "request"})
    if failure == "outcome":
        owner.finish.return_value = OwnershipOutcome("unknown")
    elif failure == "finish":
        owner.finish.side_effect = RuntimeError("synthetic-secret")
    elif failure == "request":
        owner.request_close.side_effect = RuntimeError("synthetic-secret")
    elif failure == "remove":
        monkeypatch.setattr(
            inert_worker,
            "_remove_transport",
            Mock(side_effect=OSError("synthetic-secret")),
        )
    result = execute_inert_call(inert, working_root=tmp_path)
    assert result.ownership == OwnershipState.UNKNOWN
    assert result.reply is None
    assert "synthetic-secret" not in result.model_dump_json()
    assert result.retained_working
    assert seen[0].exists()
    assert inert_worker._retained[0][0] is owner
    assert owner.finish.call_count == 1
    with pytest.raises(CommandOwnershipError, match="active or unresolved"):
        execute_inert_call(inert, working_root=tmp_path)
    assert len(seen) == 1


class ProviderCancellation(BaseException):
    pass


@pytest.mark.parametrize("failed_cleanup", [False, True])
def test_provider_cancellation_propagates_after_cleanup_and_resets_directory(
    inert: InertWorkerCall,
    owner: Mock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed_cleanup: bool,
) -> None:
    cancellation = ProviderCancellation()

    def execute(*_args: object, **_kwargs: object) -> bytes:
        assert resolve_worker_directory(None).is_dir()
        raise cancellation

    monkeypatch.setattr(inert_worker, "execute_call_directory", execute)
    if failed_cleanup:
        owner.finish.side_effect = RuntimeError("synthetic-secret")
    with pytest.raises(ProviderCancellation) as caught:
        execute_inert_call(inert, working_root=tmp_path)
    assert caught.value is cancellation
    owner.request_close.assert_called_once()
    owner.finish.assert_called_once()
    assert bool(list(tmp_path.glob("call-*"))) is failed_cleanup
    with pytest.raises(RuntimeError, match="active worker-owned"):
        resolve_worker_directory(None)
    if failed_cleanup:
        with pytest.raises(CommandOwnershipError, match="active or unresolved"):
            execute_inert_call(inert, working_root=tmp_path)
    else:
        science(monkeypatch, inert)
        assert (
            execute_inert_call(inert, working_root=tmp_path).ownership
            == OwnershipState.CLOSED
        )


def test_interrupted_owner_start_retains_lease(
    inert: InertWorkerCall, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def start() -> None:
        raise ProviderCancellation

    monkeypatch.setattr(CommandSupervisor, "start", start)
    with pytest.raises(ProviderCancellation):
        execute_inert_call(inert, working_root=tmp_path)
    assert list(tmp_path.glob("call-*"))
    with pytest.raises(CommandOwnershipError, match="active or unresolved"):
        execute_inert_call(inert, working_root=tmp_path)


def test_overlapping_warm_entry_rejects_before_science(
    inert: InertWorkerCall, owner: Mock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()

    def execute(*_args: object, **_kwargs: object) -> bytes:
        entered.set()
        assert release.wait(5)
        return reply(inert)

    monkeypatch.setattr(inert_worker, "execute_call_directory", execute)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(execute_inert_call, inert, working_root=tmp_path)
        assert entered.wait(5)
        try:
            with pytest.raises(CommandOwnershipError, match="active or unresolved"):
                execute_inert_call(inert, working_root=tmp_path)
        finally:
            release.set()
        assert future.result(5).ownership == OwnershipState.CLOSED
    assert owner.finish.call_count == 1


def test_worker_directory_requires_scope_for_both_admitted_entries(
    tmp_path: Path,
) -> None:
    for execute in (execute_admitted_assignment, execute_admitted_curator):
        with pytest.raises(RuntimeError, match="active worker-owned"):
            execute(Mock(), Mock(), Mock())
    with worker_directory_scope(tmp_path):
        assert resolve_worker_directory(None) == tmp_path
        assert resolve_worker_directory(tmp_path / "explicit") == tmp_path / "explicit"
        with (
            pytest.raises(RuntimeError, match="already active"),
            worker_directory_scope(tmp_path / "nested"),
        ):
            pytest.fail("nested scope entered")
    with pytest.raises(RuntimeError, match="active worker-owned"):
        resolve_worker_directory(None)


def test_unknown_result_cannot_encode_an_accept_ready_reply() -> None:
    with pytest.raises(ValidationError, match="no usable reply"):
        InertCallResult(
            entry_id=uuid4(),
            ownership=OwnershipState.UNKNOWN,
            reply=b"{}",
            error="unknown",
            retained_working=True,
        )


def test_real_managed_child_closes_before_wrapper_returns(
    inert: InertWorkerCall, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owners = []
    start = CommandSupervisor.start

    def capture() -> CommandSupervisor:
        value = start()
        owners.append(value)
        return value

    monkeypatch.setattr(CommandSupervisor, "start", capture)

    def execute(_directory: Path, *, attachment: dict[str, str]) -> bytes:
        with attached_worker_unit(attachment), managed_invocation():
            result = run_command(
                IdentityCommandEnvironment(),
                [
                    sys.executable,
                    "-c",
                    "import signal; signal.alarm(5); print('closed')",
                ],
                cwd=str(resolve_worker_directory(None)),
            )
            assert result.stdout.strip() == "closed"
        return reply(inert)

    monkeypatch.setattr(inert_worker, "execute_call_directory", execute)
    result = execute_inert_call(inert, working_root=tmp_path)
    assert result.ownership == OwnershipState.CLOSED
    assert result.reply == reply(inert)
    assert not list(tmp_path.glob("call-*"))
    assert owners[0].process.poll() is not None


def test_bootstrap_failure_is_bounded_infrastructure_after_closed_cleanup(
    inert: InertWorkerCall,
    owner: Mock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = ValueError("synthetic-secret")

    def execute(*_args: object, **_kwargs: object) -> bytes:
        raise failure

    monkeypatch.setattr(inert_worker, "execute_call_directory", execute)
    result = execute_inert_call(inert, working_root=tmp_path)
    assert result.ownership == OwnershipState.CLOSED
    assert result.reply is None
    assert result.error
    assert "synthetic-secret" not in result.model_dump_json()
    assert not list(tmp_path.glob("call-*"))
    owner.request_close.assert_called_once()
    science(monkeypatch, inert)
    assert execute_inert_call(inert, working_root=tmp_path).reply == reply(inert)


@pytest.mark.parametrize("kind", ["assignment", "curator"])
@pytest.mark.parametrize(
    ("outer", "inner"), [(False, True), (True, False), (True, True)]
)
def test_admitted_preservation_must_match_worker_scope_before_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    outer: bool,
    inner: bool,
) -> None:
    from artisan.execution.assignment_transport import AssignmentDispatch
    from artisan.schemas.execution.storage_config import StorageConfig

    dispatch = AssignmentDispatch(
        str(tmp_path / "delta"),
        str(tmp_path / "files"),
        str(tmp_path / "staging"),
        StorageConfig(),
        tmp_path,
        preserve_working=inner,
    )
    reached = []

    class EntryReached(Exception):
        pass

    def enter(*_args: object) -> None:
        reached.append(resolve_worker_directory(None, preserve_working=inner))
        raise EntryReached

    if kind == "assignment":
        monkeypatch.setattr(
            "artisan.execution.admitted_worker.verify_assignment", enter
        )
        execute = execute_admitted_assignment
    else:
        monkeypatch.setattr("artisan.execution.curator_worker._capture_entry", enter)
        execute = execute_admitted_curator
    with worker_directory_scope(tmp_path / "worker", preserve_working=outer):
        if outer != inner:
            with pytest.raises(ValueError, match="preservation policy differs"):
                execute(Mock(), Mock(), dispatch)
            assert not reached
        else:
            with pytest.raises(EntryReached):
                execute(Mock(), Mock(), dispatch)
            assert reached == [tmp_path / "worker"]


def test_preentry_cancellation_skips_owner_and_decode_and_control_is_single_use(
    inert: InertWorkerCall, owner: Mock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control = InertCallControl()
    control.request_close(time.monotonic() - 1)
    existing = set(tmp_path.iterdir())
    entered = Mock(side_effect=AssertionError("cancelled call entered"))
    monkeypatch.setattr(inert_worker, "execute_call_directory", entered)
    result = execute_inert_call(inert, working_root=tmp_path, control=control)
    assert result.cancelled
    assert result.ownership == OwnershipState.CLOSED
    assert result.reply is None
    assert not result.retained_working
    assert control.ownership_state() == OwnershipState.CLOSED
    owner.finish.assert_not_called()
    entered.assert_not_called()
    assert set(tmp_path.iterdir()) == existing
    with pytest.raises(ValueError, match="already been used"):
        execute_inert_call(inert, working_root=tmp_path, control=control)
    science(monkeypatch, inert)
    assert execute_inert_call(inert, working_root=tmp_path).reply == reply(inert)


def test_cancellation_during_owner_start_is_applied_before_live_entry(
    inert: InertWorkerCall, owner: Mock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control = InertCallControl()
    starting, release = threading.Event(), threading.Event()

    def start() -> Mock:
        starting.set()
        assert release.wait(5)
        return owner

    monkeypatch.setattr(CommandSupervisor, "start", start)
    entered = Mock(side_effect=AssertionError("cancelled bootstrap entered"))
    monkeypatch.setattr(inert_worker, "execute_call_directory", entered)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            execute_inert_call, inert, working_root=tmp_path, control=control
        )
        assert starting.wait(5)
        deadline = time.monotonic() + 5
        try:
            control.request_close(deadline)
        finally:
            release.set()
        result = future.result(5)
    assert result.cancelled
    assert result.ownership == OwnershipState.CLOSED
    entered.assert_not_called()
    assert all(call.args[0] <= deadline for call in owner.request_close.call_args_list)
    assert owner.finish.call_args.args[0] <= deadline
    assert not list(tmp_path.glob("call-*"))


@pytest.mark.parametrize(
    ("expired", "poll"), [(False, False), (True, False), (True, True)]
)
def test_native_return_honors_shortest_deadline_and_sticky_unknown(
    inert: InertWorkerCall,
    owner: Mock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    expired: bool,
    poll: bool,
) -> None:
    clock = [100.0]
    monkeypatch.setattr(inert_worker.time, "monotonic", lambda: clock[0])
    entered, release = threading.Event(), threading.Event()
    control = InertCallControl()
    seen = []

    def execute(*_args: object, **_kwargs: object) -> bytes:
        seen.append(resolve_worker_directory(None))
        entered.set()
        assert release.wait(5)
        return reply(inert)

    monkeypatch.setattr(inert_worker, "execute_call_directory", execute)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            execute_inert_call, inert, working_root=tmp_path, control=control
        )
        assert entered.wait(5)
        try:
            control.request_close(110.0)
            control.request_close(103.0)
            control.request_close(120.0)
            assert control.ownership_state() == OwnershipState.PENDING
            assert not future.done()
            if expired:
                clock[0] = 104.0
                if poll:
                    assert control.ownership_state() == OwnershipState.UNKNOWN
        finally:
            release.set()
        result = future.result(5)
    assert owner.finish.call_args.args == (103.0,)
    assert owner.request_close.call_args.args == (103.0,)
    assert result.reply is None
    if expired:
        assert result.ownership == control.ownership_state() == OwnershipState.UNKNOWN
        assert not result.cancelled
        assert seen[0].is_dir()
        with pytest.raises(CommandOwnershipError, match="unresolved"):
            execute_inert_call(inert, working_root=tmp_path)
    else:
        assert result.cancelled
        assert result.ownership == control.ownership_state() == OwnershipState.CLOSED
        assert not seen[0].exists()


def test_completed_result_wins_late_cancellation(
    inert: InertWorkerCall, owner: Mock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control = InertCallControl()
    science(monkeypatch, inert)
    result = execute_inert_call(inert, working_root=tmp_path, control=control)
    control.request_close(time.monotonic() - 1)
    assert not result.cancelled
    assert result.reply == reply(inert)
    assert control.ownership_state() == OwnershipState.CLOSED
    owner.request_close.assert_not_called()


def test_cancel_during_final_collection_keeps_shortened_deadline_and_scratch(
    inert: InertWorkerCall, owner: Mock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [100.0]
    monkeypatch.setattr(inert_worker.time, "monotonic", lambda: clock[0])
    collecting, release = threading.Event(), threading.Event()
    control = InertCallControl()
    seen = science(monkeypatch, inert)

    def finish(_deadline: float) -> OwnershipOutcome:
        collecting.set()
        assert release.wait(5)
        return OwnershipOutcome("closed")

    owner.finish.side_effect = finish
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            execute_inert_call, inert, working_root=tmp_path, control=control
        )
        assert collecting.wait(5)
        try:
            control.request_close(102.0)
            control.request_close(110.0)
            assert owner.request_close.call_args.args == (102.0,)
            clock[0] = 103.0
        finally:
            release.set()
        result = future.result(5)
    assert result.ownership == control.ownership_state() == OwnershipState.UNKNOWN
    assert result.reply is None
    assert seen[0].exists()


@pytest.mark.parametrize("remove_error", [False, True])
def test_slow_cleanup_keeps_control_prompt_and_deadline_unknown(
    inert: InertWorkerCall,
    owner: Mock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    remove_error: bool,
) -> None:
    clock = [100.0]
    monkeypatch.setattr(inert_worker.time, "monotonic", lambda: clock[0])
    cleaning, release = threading.Event(), threading.Event()
    control = InertCallControl()
    seen = science(monkeypatch, inert, preserve=True)
    remove = inert_worker._remove_transport

    def cleanup(directory: Path, *, preserve_working: bool) -> None:
        cleaning.set()
        assert release.wait(5)
        if remove_error:
            message = "synthetic-secret"
            raise OSError(message)
        remove(directory, preserve_working=preserve_working)

    monkeypatch.setattr(inert_worker, "_remove_transport", cleanup)
    with ThreadPoolExecutor(max_workers=2) as pool:
        future = pool.submit(
            execute_inert_call,
            inert,
            working_root=tmp_path,
            preserve_working=True,
            control=control,
        )
        assert cleaning.wait(5)
        try:
            pool.submit(control.request_close, 102.0).result(timeout=1)
            assert (
                pool.submit(control.ownership_state).result(timeout=1)
                == OwnershipState.PENDING
            )
            clock[0] = 103.0
            assert (
                pool.submit(control.ownership_state).result(timeout=1)
                == OwnershipState.UNKNOWN
            )
            assert not future.done()
        finally:
            release.set()
        result = future.result(5)
    assert result.ownership == control.ownership_state() == OwnershipState.UNKNOWN
    assert result.reply is None
    assert not result.cancelled
    assert result.retained_working
    assert seen[0].exists()
    assert (seen[0].parent / "transport").exists() is remove_error
    assert "synthetic-secret" not in result.model_dump_json()
    assert inert_worker._retained[0][0] is owner
    with pytest.raises(CommandOwnershipError, match="unresolved"):
        execute_inert_call(inert, working_root=tmp_path)


@pytest.mark.parametrize("failure", ["request", "finish"])
def test_cancel_cleanup_failure_remains_unknown_without_reply(
    inert: InertWorkerCall,
    owner: Mock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    control = InertCallControl()

    def execute(*_args: object, **_kwargs: object) -> bytes:
        if failure == "request":
            owner.request_close.side_effect = RuntimeError("synthetic-secret")
            with pytest.raises(RuntimeError):
                control.request_close(time.monotonic() + 5)
        else:
            control.request_close(time.monotonic() + 5)
            owner.finish.side_effect = RuntimeError("synthetic-secret")
        return reply(inert)

    monkeypatch.setattr(inert_worker, "execute_call_directory", execute)
    result = execute_inert_call(inert, working_root=tmp_path, control=control)
    assert result.ownership == control.ownership_state() == OwnershipState.UNKNOWN
    assert not result.cancelled
    assert result.reply is None
    assert result.retained_working
    assert "synthetic-secret" not in result.model_dump_json()


@pytest.mark.parametrize("ownership", [OwnershipState.UNKNOWN, OwnershipState.CLOSED])
def test_cancelled_result_rejects_usable_or_unconfirmed_reply(
    ownership: OwnershipState,
) -> None:
    with pytest.raises(ValidationError):
        InertCallResult(
            entry_id=uuid4(),
            ownership=ownership,
            reply=b"receipt",
            error="failure",
            retained_working=True,
            cancelled=True,
        )


def test_real_cancelled_detached_command_closes_before_next_call(
    inert: InertWorkerCall, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control = InertCallControl()
    ready = threading.Event()
    children: list[int] = []
    calls = 0

    def execute(_directory: Path, *, attachment: dict[str, str]) -> bytes:
        nonlocal calls
        calls += 1
        program = (
            "import subprocess,sys; subprocess.run([sys.executable,'-c',"
            "'import os,time; os.setsid(); print(os.getpid(),flush=True); time.sleep(20)'])"
            if calls == 1
            else "print('sibling completed')"
        )
        with attached_worker_unit(attachment), managed_invocation():
            command = ManagedCommand(
                attachment,
                [sys.executable, "-c", program],
                dict(os.environ),
                str(resolve_worker_directory(None)),
                merged=False,
                external=False,
            )
            try:

                def output(line: str) -> None:
                    if line.strip().isdigit():
                        children.append(int(line.strip()))
                        ready.set()

                command.communicate(on_line=output)
            finally:
                command.dispose()
        return reply(inert)

    monkeypatch.setattr(inert_worker, "execute_call_directory", execute)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            execute_inert_call, inert, working_root=tmp_path, control=control
        )
        assert ready.wait(5)
        control.request_close(time.monotonic() + 5)
        result = future.result(8)
    assert result.ownership == OwnershipState.CLOSED
    assert result.cancelled
    assert result.reply is None
    with pytest.raises(ProcessLookupError):
        os.kill(children[0], 0)
    sibling = execute_inert_call(inert, working_root=tmp_path)
    assert sibling.ownership == OwnershipState.CLOSED
    assert not sibling.cancelled
    assert sibling.reply == reply(inert)
    assert sibling.entry_id != result.entry_id
    assert not list(tmp_path.glob("call-*"))
