"""Real-process regressions for local cancellation exit observation."""

from __future__ import annotations

import multiprocessing
import os
import signal
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import _ExecutorManagerThread
from contextlib import contextmanager, suppress
from multiprocessing.process import BaseProcess
from pathlib import Path

import pytest

from artisan.orchestration.runners.local import _terminate_process_pool
from artisan.utils.process_call import execute_process_call, serialize_process_call
from artisan.utils.spawn import suppress_main_reimport


def _block(root: str, ignore_term: bool) -> None:
    if ignore_term:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    Path(root, f"ready-{os.getpid()}").touch()
    signal.pause()


@contextmanager
def _running_pool(
    tmp_path: Path, *, count: int = 1, ignore_term: bool = False
) -> Iterator[
    tuple[ProcessPoolExecutor, tuple[BaseProcess, ...], _ExecutorManagerThread]
]:
    with suppress_main_reimport():
        pool = ProcessPoolExecutor(
            max_workers=count, mp_context=multiprocessing.get_context("spawn")
        )
        futures = [
            pool.submit(
                execute_process_call,
                serialize_process_call(_block, str(tmp_path), ignore_term),
            )
            for _ in range(count)
        ]
        processes = tuple(pool._processes.values())
        manager = pool._executor_manager_thread
        try:
            deadline = time.monotonic() + 10
            while len(list(tmp_path.glob("ready-*"))) != count:
                for future in futures:
                    if future.done():
                        future.result()
                assert time.monotonic() < deadline, list(tmp_path.iterdir())
                threading.Event().wait(0.01)
            yield pool, processes, manager
        finally:
            for process in processes:
                with suppress(ProcessLookupError):
                    os.kill(process.pid, signal.SIGKILL)
            manager.join(timeout=10)
            assert not manager.is_alive(), "Pool manager did not settle during cleanup"
            for process in processes:
                process.join(timeout=1)
                assert not process.is_alive()
            pool.shutdown(wait=True, cancel_futures=True)


@pytest.mark.parametrize("count", [1, 2])
def test_exit_is_proved_before_manager_publishes_waitpid_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, count: int
) -> None:
    """A real reap racing with observation cannot look like a live worker."""
    reaped = threading.Event()
    publish = threading.Event()
    shutdown_returned = threading.Event()
    original_waitpid = os.waitpid
    original_broken = _ExecutorManagerThread.terminate_broken
    observed: list[tuple[int, int]] = []

    with _running_pool(tmp_path, count=count) as (pool, processes, manager):
        original_shutdown = pool.shutdown

        def delayed_waitpid(pid: int, options: int) -> tuple[int, int]:
            result = original_waitpid(pid, options)
            if result[0] > 0 and threading.current_thread() is manager:
                observed.append(result)
                reaped.set()
                assert publish.wait(10), "Exit publication barrier timed out"
            return result

        def ordered_broken(self: _ExecutorManagerThread, cause: str | None) -> None:
            assert shutdown_returned.wait(10), "Shutdown ordering barrier timed out"
            original_broken(self, cause)

        def ordered_shutdown(
            wait: bool = True, *, cancel_futures: bool = False
        ) -> None:
            original_shutdown(wait=wait, cancel_futures=cancel_futures)
            if not wait:
                shutdown_returned.set()
                assert reaped.wait(10), "Manager did not reap an owned worker"

        with monkeypatch.context() as instrumentation:
            instrumentation.setattr(os, "waitpid", delayed_waitpid)
            instrumentation.setattr(
                _ExecutorManagerThread, "terminate_broken", ordered_broken
            )
            instrumentation.setattr(pool, "shutdown", ordered_shutdown)
            try:
                assert _terminate_process_pool(pool)
                pid, status = observed[0]
                assert os.waitstatus_to_exitcode(status) == -signal.SIGTERM
                reaped_process = next(
                    process for process in processes if process.pid == pid
                )
                assert reaped_process._popen.returncode is None
                with pytest.raises(ProcessLookupError):
                    os.kill(pid, 0)
            finally:
                publish.set()
                shutdown_returned.set()
                manager.join(timeout=10)


def test_exit_without_ownership_handles_is_not_confirmed(tmp_path: Path) -> None:
    """Nonblocking shutdown drops handles while real work can remain alive."""
    with _running_pool(tmp_path) as (pool, processes, _manager):
        pool.shutdown(wait=False, cancel_futures=True)
        assert pool._processes is None
        assert not _terminate_process_pool(pool)
        assert processes[0].is_alive()


def test_worker_ignoring_termination_remains_unconfirmed(tmp_path: Path) -> None:
    with _running_pool(tmp_path, ignore_term=True) as (pool, processes, _manager):
        assert not _terminate_process_pool(pool)
        assert processes[0].is_alive()


def test_unavailable_exit_sentinel_is_not_confirmed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An actual child with unavailable exit evidence stays unconfirmed."""
    with _running_pool(tmp_path) as (pool, processes, _manager):

        def unavailable(_process: BaseProcess) -> int:
            message = "Exit observation was closed"
            raise ValueError(message)

        with monkeypatch.context() as instrumentation:
            instrumentation.setattr(
                type(processes[0]), "sentinel", property(unavailable)
            )
            assert not _terminate_process_pool(pool)
