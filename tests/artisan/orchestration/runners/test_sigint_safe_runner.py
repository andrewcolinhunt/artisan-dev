"""Tests for SIGINT-safe native process-pool construction."""

from __future__ import annotations

import sys
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from artisan.orchestration.runners.local import LocalLifecycleRouter, LocalRunner
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.utils.spawn import ignore_sigint


class TestNativeProcessPool:
    @patch("artisan.orchestration.runners.local.ProcessPoolExecutor")
    def test_uses_spawn_context_and_sigint_initializer(
        self,
        mock_executor_class: MagicMock,
    ) -> None:
        future: Future[list[UnitResult]] = Future()
        future.set_result([UnitResult(True, None, 1, [])])
        mock_executor_class.return_value.submit.return_value = future
        handle = LocalLifecycleRouter(max_workers=2, units_per_worker=1)

        handle.run([MagicMock()], MagicMock())

        kwargs = mock_executor_class.call_args.kwargs
        assert kwargs["max_workers"] == 2
        assert kwargs["mp_context"].get_start_method() == "spawn"
        assert kwargs["initializer"] is ignore_sigint

    @patch("artisan.orchestration.runners.local.ProcessPoolExecutor")
    def test_suppresses_main_reimport_for_pool_lifetime(
        self,
        mock_executor_class: MagicMock,
    ) -> None:
        main_module = sys.modules["__main__"]
        original_file = main_module.__file__
        observed_files: list[str | None] = []
        future: Future[list[UnitResult]] = Future()
        future.set_result([UnitResult(True, None, 1, [])])

        def _record_submit(*args: object) -> Future[list[UnitResult]]:
            observed_files.append(main_module.__file__)
            return future

        mock_executor_class.return_value.submit.side_effect = _record_submit
        handle = LocalLifecycleRouter(max_workers=1, units_per_worker=1)

        handle.run([MagicMock()], MagicMock())

        assert observed_files == [None]
        assert main_module.__file__ == original_file

    def test_local_runner_builds_native_lifecycle_router(self) -> None:
        runner = LocalRunner(default_max_workers=2)

        router = runner.create_lifecycle_router(
            RunnerResources(),
            BatchStrategy(),
            step_number=0,
            job_name="test",
        )

        assert isinstance(router, LocalLifecycleRouter)
        assert router._max_workers == 2
