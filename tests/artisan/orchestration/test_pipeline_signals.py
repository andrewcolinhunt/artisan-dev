"""Signal handoff owns only a cancellation helper, never scientific closure."""

from __future__ import annotations

import gc
import json
import signal
import subprocess
import sys
import threading
import time
import weakref
from collections.abc import Iterator
from pathlib import Path
from types import FrameType
from typing import Any

import pytest
from fixtures.composition_operations import Transform, structure
from fixtures.retained_pipeline import NativePipelineRunner

from artisan.orchestration.engine import pipeline_signals as signals
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.utils.command_supervisor import CommandOwnershipError


class _Target:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.resume = threading.Event()
        self.resume.set()
        self.calls = 0
        self.thread: threading.Thread | None = None

    def cancel(self) -> None:
        self.thread = threading.current_thread()
        self.calls += 1
        self.entered.set()
        assert self.resume.wait(5)


class _FakeSignals:
    def __init__(self) -> None:
        self.previous = {
            signal.SIGINT: signal.default_int_handler,
            signal.SIGTERM: signal.SIG_IGN,
        }
        self.handlers = dict(self.previous)
        self.redelivered: list[int] = []
        self.tokens: list[signals.PipelineSignalRegistration] = []

    def install(self, signum: int, handler: Any) -> Any:
        previous = self.handlers[signum]
        self.handlers[signum] = handler
        return previous

    def register(self, target: _Target) -> signals.PipelineSignalRegistration:
        token = signals.register_pipeline_signals(target)
        assert token is not None
        self.tokens.append(token)
        return token

    def emit(self, signum: int = signal.SIGINT) -> None:
        handler = self.handlers[signum]
        assert callable(handler)
        handler(signum, None)


@pytest.fixture
def fake_signals(monkeypatch: pytest.MonkeyPatch) -> Iterator[_FakeSignals]:
    fake = _FakeSignals()
    monkeypatch.setattr(signals, "_handlers", None)
    monkeypatch.setattr(signals.signal, "getsignal", fake.handlers.__getitem__)
    monkeypatch.setattr(signals.signal, "signal", fake.install)
    monkeypatch.setattr(signals.signal, "raise_signal", fake.redelivered.append)
    yield fake
    current = signals._handlers
    if current is not None:
        fake.tokens.extend(
            token for ref in current.registrations if (token := ref()) is not None
        )
    for token in fake.tokens:
        token.released = True
        token._queue.put("stop")
        token._thread.join(5)
        assert not token._thread.is_alive()


def test_handler_never_waits_for_cancel_and_requests_coalesce(
    fake_signals: _FakeSignals,
) -> None:
    target = _Target()
    target.resume.clear()
    token = fake_signals.register(target)
    fake_signals.emit()
    assert target.entered.wait(1)
    assert target.thread is not threading.main_thread()
    token._queue.put("request")
    assert target.calls == 1
    target.resume.set()
    token.drain_request()
    token.release()
    assert target.calls == 1
    assert fake_signals.handlers == fake_signals.previous


def test_two_managers_and_out_of_order_close_never_restore_closed_manager(
    fake_signals: _FakeSignals,
) -> None:
    first, second = _Target(), _Target()
    one = fake_signals.register(first)
    two = fake_signals.register(second)
    handler = fake_signals.handlers[signal.SIGINT]
    one.release()
    assert fake_signals.handlers[signal.SIGINT] is handler
    fake_signals.emit()
    assert second.entered.wait(1)
    two.drain_request()
    two.release()
    assert first.calls == 0
    assert second.calls == 1
    assert fake_signals.handlers == fake_signals.previous


def test_blocked_manager_does_not_delay_an_independent_registration(
    fake_signals: _FakeSignals,
) -> None:
    first, second = _Target(), _Target()
    first.resume.clear()
    one = fake_signals.register(first)
    fake_signals.emit()
    assert first.entered.wait(1)
    two = fake_signals.register(second)
    fake_signals.emit()
    assert second.entered.wait(1)
    two.drain_request()
    two.release()
    first.resume.set()
    one.drain_request()
    one.release()


def test_off_main_release_defers_restoration_without_swallowing_next_signal(
    fake_signals: _FakeSignals,
) -> None:
    target = _Target()
    token = fake_signals.register(target)
    worker = threading.Thread(target=token.release)
    worker.start()
    worker.join(2)
    assert not worker.is_alive()
    assert fake_signals.handlers != fake_signals.previous
    fake_signals.emit(signal.SIGTERM)
    assert fake_signals.handlers == fake_signals.previous
    assert fake_signals.redelivered == [signal.SIGTERM]
    assert target.calls == 0


def test_main_reconciliation_preserves_external_handler(
    fake_signals: _FakeSignals,
) -> None:
    target = _Target()
    token = fake_signals.register(target)

    def external(signum: int, frame: FrameType | None) -> None:
        pass

    fake_signals.handlers[signal.SIGINT] = external
    token.release()
    assert fake_signals.handlers[signal.SIGINT] is external
    assert fake_signals.handlers[signal.SIGTERM] == signal.SIG_IGN


def test_reentrant_second_signal_relinquishes_without_dropping_first_request(
    fake_signals: _FakeSignals, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _Target()
    token = fake_signals.register(target)
    original = token._queue

    class ReentrantQueue:
        def put(self, item: str) -> None:
            if item == "request":
                fake_signals.emit(signal.SIGTERM)
            original.put(item)

        def get(self) -> str:
            return original.get()

    monkeypatch.setattr(token, "_queue", ReentrantQueue())
    fake_signals.emit()
    assert target.entered.wait(1)
    token.drain_request()
    token.release()
    assert target.calls == 1
    assert fake_signals.handlers == fake_signals.previous


def test_late_request_after_closed_release_does_not_restart_worker(
    fake_signals: _FakeSignals,
) -> None:
    target = _Target()
    token = fake_signals.register(target)
    token.release()
    token._queue.put("request")
    assert not token._thread.is_alive()
    assert target.calls == 0


def test_callback_failure_is_retained_and_observed_after_helper_closes(
    fake_signals: _FakeSignals,
) -> None:
    class FailingTarget(_Target):
        def cancel(self) -> None:
            msg = "cancellation failed"
            raise RuntimeError(msg)

    target = FailingTarget()
    token = fake_signals.register(target)
    fake_signals.emit()
    token.drain_request()
    with pytest.raises(RuntimeError, match="cancellation failed"):
        token.release()
    assert not token._thread.is_alive()
    assert isinstance(token.error, RuntimeError)


def test_expiry_retains_same_budget_and_late_completion_never_becomes_closed(
    fake_signals: _FakeSignals, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(signals, "CLEANUP_SECONDS", 0.03)
    target = _Target()
    target.resume.clear()
    token = fake_signals.register(target)
    fake_signals.emit()
    assert target.entered.wait(1)
    deadline = token.deadline()
    with pytest.raises(CommandOwnershipError):
        token.drain_request()
    assert token.unknown
    assert not token.released
    assert token._thread.is_alive()
    target.resume.set()
    assert token._processed.wait(1)
    with pytest.raises(CommandOwnershipError):
        token.release()
    assert token.deadline() == deadline


def test_collected_idle_owner_stops_only_unused_helper(
    fake_signals: _FakeSignals,
) -> None:
    target = _Target()
    token = fake_signals.register(target)
    reference = weakref.ref(target)
    del target
    gc.collect()
    token._thread.join(2)
    assert reference() is None
    assert not token._thread.is_alive()
    assert token.released
    assert fake_signals.handlers == fake_signals.previous


def test_timely_dispatch_can_be_released_after_its_original_deadline(
    fake_signals: _FakeSignals, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(signals, "CLEANUP_SECONDS", 0.04)
    target = _Target()
    token = fake_signals.register(target)
    fake_signals.emit()
    token.drain_request()
    token._thread.join(1)
    assert not token._thread.is_alive()
    time.sleep(0.05)
    token.release()
    token.release()
    assert not token.unknown


def test_failed_helper_start_installs_no_handlers(
    fake_signals: _FakeSignals, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _Target()

    def fail_start(thread: threading.Thread) -> None:
        msg = "thread startup failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(threading.Thread, "start", fail_start)
    with pytest.raises(RuntimeError, match="thread startup failed"):
        signals.register_pipeline_signals(target)
    assert fake_signals.handlers == fake_signals.previous


def test_partial_handler_install_restores_originals_and_stops_helper(
    fake_signals: _FakeSignals, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _Target()
    original = fake_signals.install

    def install(signum: int, handler: Any) -> Any:
        if signum == signal.SIGTERM and handler != signal.SIG_IGN:
            msg = "handler installation failed"
            raise RuntimeError(msg)
        return original(signum, handler)

    monkeypatch.setattr(signals.signal, "signal", install)
    previous = set(threading.enumerate())
    with pytest.raises(RuntimeError, match="handler installation failed"):
        signals.register_pipeline_signals(target)
    assert fake_signals.handlers == fake_signals.previous
    assert set(threading.enumerate()) == previous


def test_second_signal_during_install_does_not_reinstall_remaining_handler(
    fake_signals: _FakeSignals, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _Target()
    original = fake_signals.install

    def install(signum: int, handler: Any) -> Any:
        previous = original(signum, handler)
        if signum == signal.SIGINT and handler is not signal.default_int_handler:
            handler(signum, None)
            handler(signum, None)
        return previous

    monkeypatch.setattr(signals.signal, "signal", install)
    token = fake_signals.register(target)
    try:
        token.drain_request()
        assert target.calls == 1
        assert fake_signals.handlers == fake_signals.previous
    finally:
        token.release()


def test_non_main_registration_and_opaque_original_do_not_install(
    fake_signals: _FakeSignals,
) -> None:
    target = _Target()
    results = []
    thread = threading.Thread(
        target=lambda: results.append(signals.register_pipeline_signals(target))
    )
    thread.start()
    thread.join(2)
    assert results == [None]
    fake_signals.handlers[signal.SIGTERM] = None
    assert signals.register_pipeline_signals(target) is None
    assert fake_signals.handlers[signal.SIGINT] == signal.default_int_handler


def _pipeline(tmp_path: Path) -> PipelineManager:
    pipeline = PipelineManager.create(
        "signals",
        str(tmp_path / "signals-store"),
        str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    pipeline.runner = NativePipelineRunner(pipeline.committer)
    return pipeline


def test_idle_and_invalid_submission_create_no_signal_helper(
    tmp_path: Path, fake_signals: _FakeSignals
) -> None:
    pipeline = _pipeline(tmp_path)
    with pytest.raises(TypeError):
        pipeline.submit(object)
    assert pipeline._signals is None
    assert fake_signals.handlers == fake_signals.previous
    pipeline.finalize()


def test_signal_expiry_does_not_synthesize_a_terminal_or_lose_owners(
    tmp_path: Path, fake_signals: _FakeSignals, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(signals, "CLEANUP_SECONDS", 0.05)
    pipeline = _pipeline(tmp_path)
    runner = pipeline.runner
    assert isinstance(runner, NativePipelineRunner)
    runner.release.clear()
    future = pipeline.submit(Transform, inputs={"structure": [structure(0)]})
    assert runner.entered.wait(2)
    fake_signals.emit()
    with pytest.raises(CommandOwnershipError):
        pipeline.finalize()
    assert pipeline.ownership_state() == OwnershipState.UNKNOWN
    assert pipeline._summary is None
    assert (
        pipeline.tracker.current_state(pipeline.executions[0].record.step_run_id).status
        == StepStatus.PENDING
    )
    runner.release.set()
    future.result(5)
    with pytest.raises(CommandOwnershipError):
        pipeline.close()
    assert len(pipeline.executions) == 1


def test_normal_finalize_keeps_unlimited_science_wait_and_signal_eligibility(
    tmp_path: Path, fake_signals: _FakeSignals, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(signals, "CLEANUP_SECONDS", 0.05)
    pipeline = _pipeline(tmp_path)
    runner = pipeline.runner
    assert isinstance(runner, NativePipelineRunner)
    runner.release.clear()
    future = pipeline.submit(Transform, inputs={"structure": [structure(0)]})
    assert runner.entered.wait(2)
    finished = threading.Event()
    errors: list[BaseException] = []

    def close() -> None:
        try:
            pipeline.finalize()
        except BaseException as error:
            errors.append(error)
        finally:
            finished.set()

    closer = threading.Thread(target=close)
    closer.start()
    assert not finished.wait(0.1)
    assert pipeline._signals is not None
    assert not pipeline._signals.released
    runner.release.set()
    future.result(5)
    closer.join(5)
    assert finished.is_set()
    assert errors == []
    signals.reconcile_pipeline_signals()
    assert fake_signals.handlers == fake_signals.previous


def test_signal_still_cancels_while_finalize_waits(
    tmp_path: Path, fake_signals: _FakeSignals
) -> None:
    pipeline = _pipeline(tmp_path)
    runner = pipeline.runner
    assert isinstance(runner, NativePipelineRunner)
    runner.release.clear()
    future = pipeline.submit(Transform, inputs={"structure": [structure(0)]})
    assert runner.entered.wait(2)
    original_cancel = pipeline.cancel

    def cancel() -> object:
        result = original_cancel()
        runner.release.set()
        return result

    pipeline.cancel = cancel
    summaries: list[object] = []
    closer = threading.Thread(target=lambda: summaries.append(pipeline.finalize()))
    closer.start()
    fake_signals.emit()
    closer.join(5)
    assert not closer.is_alive()
    assert len(summaries) == 1
    assert future.result().status == StepStatus.CANCELLED
    assert pipeline.ownership_state() == OwnershipState.CLOSED
    signals.reconcile_pipeline_signals()
    assert fake_signals.handlers == fake_signals.previous


def test_collected_failed_pipeline_does_not_keep_an_idle_registration_alive(
    tmp_path: Path, fake_signals: _FakeSignals
) -> None:
    pipeline = _pipeline(tmp_path)

    def fail(*args: object, **kwargs: object) -> None:
        msg = "closed resolver failure"
        raise RuntimeError(msg)

    pipeline.runner.resolve = fail
    assert (
        pipeline.run(Transform, inputs={"structure": [structure(0)]}).status
        == StepStatus.FAILED
    )
    token = pipeline._signals
    assert token is not None
    reference = weakref.ref(pipeline)
    del pipeline
    gc.collect()
    token._thread.join(2)
    assert reference() is None
    assert not token._thread.is_alive()
    assert not token.requested
    assert fake_signals.handlers == fake_signals.previous


_CHILD = r'''
import json, os, pathlib, signal, sys, threading, time
from artisan.orchestration.engine.pipeline_signals import register_pipeline_signals
from artisan.utils.command_supervisor import CommandSupervisor, ManagedCommand
root = pathlib.Path(sys.argv[1])
signal.alarm(25)
program = """
import os,pathlib,signal,sys,time
if not os.fork():
    os.setsid()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.alarm(20)
    with open(os.devnull,'r+b') as sink:
        for fd in (0,1,2): os.dup2(sink.fileno(),fd)
    pathlib.Path(sys.argv[1]).write_text(str(os.getpid()))
    time.sleep(30)
while not pathlib.Path(sys.argv[1]).exists(): time.sleep(.01)
signal.alarm(20)
time.sleep(30)
"""
owners = [CommandSupervisor.start(), CommandSupervisor.start()]
commands = []
settled = set()
try:
    for i, owner in enumerate(owners):
        commands.append(ManagedCommand(owner.attachment, [sys.executable,'-c',program,str(root/f'{i}.pid')],dict(os.environ),None,merged=False,external=False))
    deadline = time.monotonic()+5
    while not all((root/f'{i}.pid').exists() for i in range(2)):
        assert time.monotonic()<deadline
        time.sleep(.01)
    class Target:
        def __init__(self): self.done=threading.Event(); self.off_main=False
        def cancel(self):
            self.off_main=threading.current_thread() is not threading.main_thread()
            commands[0].interrupt(RuntimeError('owned signal cancellation'))
            commands[0].communicate()
            settled.add(0)
            self.done.set()
    target=Target()
    token=register_pipeline_signals(target)
    (root/'ready').write_text(str(os.getpid()))
    assert target.done.wait(12)
    token.drain_request()
    assert owners[0].finish(time.monotonic()+2).state=='closed'
    assert not pathlib.Path('/proc', (root/'0.pid').read_text()).exists()
    assert pathlib.Path('/proc', (root/'1.pid').read_text()).exists()
    assert commands[1].poll() is None
    token.release()
    (root/'result.json').write_text(json.dumps({'off_main':target.off_main,'closed':commands[0].state,'sibling':commands[1].state,'helper_closed':not token._thread.is_alive()}))
finally:
    for owner in owners: owner.request_close(time.monotonic()+3)
    for i, command in enumerate(commands):
        if i not in settled: command.communicate()
        command.dispose()
    for owner in owners: assert owner.finish(time.monotonic()+3).state=='closed'
'''

_ESCALATION = r"""
import pathlib, signal, sys, threading, time
from artisan.orchestration.engine.pipeline_signals import register_pipeline_signals
root=pathlib.Path(sys.argv[1]); chosen=int(sys.argv[2]); signal.alarm(12)
resume=threading.Event()
def original(signum, frame): resume.set()
signal.signal(chosen, original)
class Target:
    def __init__(self): self.calls=0
    def cancel(self):
        assert threading.current_thread() is not threading.main_thread()
        self.calls+=1
        (root/'requested').write_text('yes')
        assert resume.wait(8)
target=Target(); token=register_pipeline_signals(target)
(root/'ready').write_text('yes')
deadline=time.monotonic()+8
while signal.getsignal(chosen) is not original:
    assert time.monotonic()<deadline
    time.sleep(.01)
(root/'restored').write_text('yes')
assert resume.wait(8)
token.drain_request(); token.release()
assert target.calls==1 and not token._thread.is_alive()
assert signal.getsignal(chosen) is original
"""


@pytest.mark.skipif(sys.platform != "linux", reason="Linux descendant closure proof")
@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_real_signal_cancels_exact_command_descendants_and_preserves_sibling(
    tmp_path: Path, signum: int
) -> None:
    with subprocess.Popen(
        [sys.executable, "-c", _CHILD, str(tmp_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as child:
        try:
            deadline = time.monotonic() + 8
            while not (tmp_path / "ready").exists() and child.poll() is None:
                assert time.monotonic() < deadline
                time.sleep(0.01)
            assert (tmp_path / "ready").read_text() == str(child.pid)
            child.send_signal(signum)
            stdout, stderr = child.communicate(timeout=20)
            assert child.returncode == 0, (stdout, stderr)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=3)
    result = json.loads((tmp_path / "result.json").read_text())
    assert result == {
        "off_main": True,
        "closed": "closed",
        "sibling": "running",
        "helper_closed": True,
    }
    for i in range(2):
        assert not Path("/proc", (tmp_path / f"{i}.pid").read_text()).exists()


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_actual_second_signal_restores_original_without_losing_first_request(
    tmp_path: Path, signum: int
) -> None:
    with subprocess.Popen(
        [sys.executable, "-c", _ESCALATION, str(tmp_path), str(signum)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as child:
        try:
            for marker in ("ready", "requested", "restored"):
                deadline = time.monotonic() + 5
                while not (tmp_path / marker).exists() and child.poll() is None:
                    assert time.monotonic() < deadline
                    time.sleep(0.01)
                assert (tmp_path / marker).exists()
                child.send_signal(signum)
            stdout, stderr = child.communicate(timeout=10)
            assert child.returncode == 0, (stdout, stderr)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=3)
