"""Curator cancellation proves process exit without waiting for ordinary work."""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.operations.examples.data_generator import DataGenerator
from artisan.orchestration.engine import step_executor
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus


def _blocking_worker(unit, runtime):
    root = Path(runtime.working_root)
    (root / "ready").write_text(str(os.getpid()))
    while not (root / "release").exists():
        time.sleep(0.02)
    (root / "completed").touch()
    return StagingResult(success=True, execution_run_id="unexpected-completion")


def test_curator_cancellation_terminates_real_child_before_return(
    tmp_path, monkeypatch
):
    event = threading.Event()
    cancelled_at = []
    ready = tmp_path / "ready"
    release = tmp_path / "release"
    monkeypatch.setattr(step_executor, "run_curator_flow", _blocking_worker)
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path),
    )
    unit = ExecutionUnit(operation=DataGenerator(), execution_spec_id="a" * 32)

    def cancel_after_start():
        deadline = time.monotonic() + 5
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        cancelled_at.append(time.monotonic())
        event.set()

    canceller = threading.Thread(target=cancel_after_start)
    # Release a regressed child so even a blocking pool shutdown ends the test.
    watchdog = threading.Timer(8, release.touch)
    canceller.start()
    watchdog.start()
    try:
        result = step_executor._run_curator_in_subprocess(unit, runtime, event)
        assert ready.exists()
        assert time.monotonic() - cancelled_at[0] < 3
        assert not result.success
        assert (
            result.cancellation_acknowledgement.status is CancellationStatus.CONFIRMED
        )
        assert result.execution_run_id is None
        assert not (tmp_path / "completed").exists()
        with pytest.raises(ProcessLookupError):
            os.kill(int(ready.read_text()), 0)
    finally:
        release.touch()
        canceller.join(timeout=6)
        watchdog.cancel()
        watchdog.join()


@pytest.mark.parametrize("exit_proved", [False, OSError("cannot signal worker")])
def test_unknown_curator_exit_never_joins_uncertain_worker(
    tmp_path, monkeypatch, exit_proved
):
    event = threading.Event()
    event.set()
    pool = MagicMock()
    pool.submit.return_value.done.return_value = False
    monkeypatch.setattr(step_executor, "ProcessPoolExecutor", lambda **_kwargs: pool)

    def terminate(_pool):
        if isinstance(exit_proved, Exception):
            raise exit_proved
        return exit_proved

    monkeypatch.setattr(step_executor, "_terminate_process_pool", terminate)
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"), staging_root=str(tmp_path / "staging")
    )
    unit = ExecutionUnit(operation=DataGenerator(), execution_spec_id="a" * 32)
    result = step_executor._run_curator_in_subprocess(unit, runtime, event)
    assert result.cancellation_acknowledgement.status is CancellationStatus.UNKNOWN
    assert result.execution_run_id is None
    pool.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
