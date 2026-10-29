"""Curator cancellation proves process exit without waiting for ordinary work."""

from __future__ import annotations

import os
import signal
import sys
import threading
import time
from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import pytest
from pydantic import BaseModel, Field

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.operations.base import OperationDefinition
from artisan.operations.examples.data_generator import DataGenerator
from artisan.orchestration.engine import step_executor
from artisan.orchestration.engine.native_pool import _NativePoolOwnership
from artisan.schemas import (
    ArtifactEmission,
    ArtifactStreamResult,
    CSVArtifact,
    OutputSpec,
)
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


class _StreamingCancellationOp(OperationDefinition):
    """Pause after enough emissions to complete an unpublished staging chunk."""

    name = "streaming_cancellation_test"

    class OutputRole(StrEnum):
        csv = "csv"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "csv": OutputSpec(artifact_type="csv", derives_from={"inputs": []})
    }

    class Params(BaseModel):
        directory: str = Field(
            description="Directory containing test coordination files."
        )

    params: Params

    def execute_curator(self, inputs, step_number, artifact_store):
        root = Path(self.params.directory)

        def items():
            payload = b"value\n" + (b"x" * 8192 + b"\n") * 128
            for index in range(20):
                yield ArtifactEmission(
                    role="csv",
                    artifact=CSVArtifact.draft(
                        payload, f"item_{index}.csv", step_number
                    ),
                    sources={},
                )
            (root / "stream-ready").write_text(str(os.getpid()))
            while not (root / "stream-release").exists():
                time.sleep(0.02)

        return ArtifactStreamResult(items=items())


def test_forced_stream_cancellation_preserves_completed_temps_without_a_seal(tmp_path):
    from artisan.storage.io.publication import is_publication_temporary

    cancellation = threading.Event()
    ownership = _NativePoolOwnership()
    ready = tmp_path / "stream-ready"
    release = tmp_path / "stream-release"
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path),
    )
    unit = ExecutionUnit(
        operation=_StreamingCancellationOp(params={"directory": str(tmp_path)}),
        inputs={},
        step_number=1,
    )
    evidence = {}

    def cancel_after_emissions():
        deadline = time.monotonic() + 30
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        for path in Path(runtime.staging_root).rglob("*.parquet.tmp-*"):
            evidence[path] = path.read_bytes()
        cancellation.set()

    canceller = threading.Thread(target=cancel_after_emissions)
    watchdog = threading.Timer(40, release.touch)
    canceller.start()
    watchdog.start()
    try:
        result = step_executor._run_curator_in_subprocess(
            unit, runtime, cancellation, ownership=ownership
        )
        assert ready.exists(), result.error
        assert evidence
        assert not result.success
        assert result.execution_run_id is None
        assert (
            result.cancellation_acknowledgement.status is CancellationStatus.CONFIRMED
        )
        assert all(is_publication_temporary(path.name) for path in evidence)
        assert all(path.read_bytes() == content for path, content in evidence.items())
        assert not list(Path(runtime.staging_root).rglob("executions.parquet"))
        assert not list(Path(runtime.staging_root).rglob("csvs.parquet"))
        with pytest.raises(ProcessLookupError):
            os.kill(int(ready.read_text()), 0)
    finally:
        release.touch()
        canceller.join(timeout=5)
        watchdog.cancel()
        watchdog.join()
        ownership.finish(failed=True)
