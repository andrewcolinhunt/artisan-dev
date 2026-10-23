"""Curator cancellation proves process exit without waiting for ordinary work."""

from __future__ import annotations

import os
import signal
import sys
import threading
import time
from pathlib import Path

import pytest

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.operations.examples.data_generator import DataGenerator
from artisan.orchestration.engine import step_executor
from artisan.orchestration.engine.native_pool import _NativePoolOwnership
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus
from artisan.utils.external_tools import run_command


def _blocking_worker(unit, runtime):
    root = Path(runtime.working_root)
    (root / "ready").write_text(str(os.getpid()))
    while not (root / "release").exists():
        time.sleep(0.02)
    (root / "completed").touch()
    return StagingResult(success=True, execution_run_id="unexpected-completion")


def _command_worker(unit, runtime):
    root = Path(runtime.working_root)
    (root / "ready").write_text(str(os.getpid()))
    code = """
import os, pathlib, signal, sys, time
root = pathlib.Path(sys.argv[1])
signal.signal(signal.SIGTERM, signal.SIG_IGN)
signal.alarm(15)
child = os.fork()
if child == 0:
    (root / 'grandchild').write_text(str(os.getpid()))
else:
    (root / 'command').write_text(str(os.getpid()))
while True: time.sleep(.02)
"""
    run_command(LocalEnvironmentSpec(), [sys.executable, "-c", code, str(root)])
    return StagingResult(success=True, execution_run_id="unexpected-completion")


@pytest.mark.parametrize("mode", ["plain", "command", "worker_loss", "owner_loss"])
def test_curator_cancellation_terminates_real_child_before_return(
    tmp_path, monkeypatch, mode
):
    event = threading.Event()
    cancelled_at = []
    ready = tmp_path / "ready"
    release = tmp_path / "release"
    with_commands = mode in {"command", "worker_loss"}
    monkeypatch.setattr(
        step_executor,
        "run_curator_flow",
        _command_worker if with_commands else _blocking_worker,
    )
    ownership = _NativePoolOwnership()
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path),
    )
    unit = ExecutionUnit(operation=DataGenerator(), execution_spec_id="a" * 32)

    def cancel_after_start():
        deadline = time.monotonic() + 5
        markers = (
            [ready, tmp_path / "command", tmp_path / "grandchild"]
            if with_commands
            else [ready]
        )
        while (
            not all(path.exists() for path in markers) and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        if mode == "owner_loss":
            ownership.supervisor.process.kill()
        elif mode == "worker_loss" and all(path.exists() for path in markers):
            os.kill(int(ready.read_text()), signal.SIGKILL)
        cancelled_at.append(time.monotonic())
        event.set()

    canceller = threading.Thread(target=cancel_after_start)
    # Release a regressed child so even a blocking pool shutdown ends the test.
    watchdog = threading.Timer(8, release.touch)
    canceller.start()
    watchdog.start()
    try:
        result = step_executor._run_curator_in_subprocess(
            unit, runtime, event, ownership=ownership
        )
        assert ready.exists()
        assert cancelled_at, result.error
        assert time.monotonic() - cancelled_at[0] < 5
        assert not result.success
        expected = (
            CancellationStatus.UNKNOWN
            if mode == "owner_loss"
            else CancellationStatus.CONFIRMED
        )
        assert result.cancellation_acknowledgement.status is expected
        assert result.execution_run_id is None
        assert not (tmp_path / "completed").exists()
        with pytest.raises(ProcessLookupError):
            os.kill(int(ready.read_text()), 0)
        if with_commands:
            for marker in ("command", "grandchild"):
                with pytest.raises(ProcessLookupError):
                    os.kill(int((tmp_path / marker).read_text()), 0)
    finally:
        release.touch()
        canceller.join(timeout=6)
        watchdog.cancel()
        watchdog.join()
        ownership.finish(failed=True)


@pytest.mark.parametrize("blocked_at", ["serialization", "pool_start", "worker_start"])
def test_curator_cancellation_bounds_unresolved_dispatch_and_rejects_late_work(
    tmp_path, monkeypatch, blocked_at
):
    entered = threading.Event()
    release = threading.Event()
    settled = threading.Event()
    cancellation = threading.Event()
    ownership = _NativePoolOwnership()
    original_end = ownership.end_dispatch

    def end_dispatch():
        try:
            original_end()
        finally:
            settled.set()

    monkeypatch.setattr(ownership, "end_dispatch", end_dispatch)
    if blocked_at == "worker_start":
        target_object = step_executor.ProcessPoolExecutor
        target = "_adjust_process_count"
    else:
        target_object = step_executor
        target = (
            "serialize_process_call"
            if blocked_at == "serialization"
            else "ProcessPoolExecutor"
        )
    original = getattr(target_object, target)

    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(target_object, target, blocked)
    monkeypatch.setattr(step_executor, "run_curator_flow", _blocking_worker)
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path),
    )
    unit = ExecutionUnit(operation=DataGenerator(), execution_spec_id="a" * 32)
    cancelled_at = []

    def cancel_pending_dispatch():
        assert entered.wait(5)
        cancelled_at.append(time.monotonic())
        ownership.request_cancel(cancelled_at[0] + 0.15)
        cancellation.set()

    canceller = threading.Thread(target=cancel_pending_dispatch)
    canceller.start()
    try:
        result = step_executor._run_curator_in_subprocess(
            unit, runtime, cancellation, ownership=ownership
        )
        assert time.monotonic() - cancelled_at[0] < 0.8
        assert result.cancellation_acknowledgement.status is CancellationStatus.UNKNOWN
        assert not (tmp_path / "ready").exists()
    finally:
        release.set()
        canceller.join(timeout=5)
        assert settled.wait(5)
        ownership.finish(failed=True)
    assert not (tmp_path / "ready").exists()
    assert ownership.acknowledgement().status is CancellationStatus.UNKNOWN
