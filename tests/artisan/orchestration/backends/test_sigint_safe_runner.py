"""Tests for SIGINTSafeProcessPoolTaskRunner."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

from artisan.orchestration.backends.local import SIGINTSafeProcessPoolTaskRunner


class TestSIGINTSafeProcessPoolTaskRunner:
    """Fix 2e: process pool workers ignore SIGINT."""

    @patch("artisan.orchestration.backends.local.ProcessPoolExecutor")
    def test_enter_creates_pool_with_initializer(self, mock_ppe_cls):
        """__enter__ re-creates executor with _ignore_sigint initializer."""
        from artisan.orchestration.backends.local import _ignore_sigint

        # Mock the parent's __enter__ to set _executor
        mock_original_executor = MagicMock()
        mock_new_executor = MagicMock()
        mock_ppe_cls.return_value = mock_new_executor

        runner = SIGINTSafeProcessPoolTaskRunner(max_workers=2)

        with patch.object(
            SIGINTSafeProcessPoolTaskRunner.__bases__[0],
            "__enter__",
            return_value=runner,
        ):
            runner._executor = mock_original_executor
            runner._max_workers = 2
            runner.__enter__()

        # Original executor should be shut down
        mock_original_executor.shutdown.assert_called_once_with(wait=False)

        # New executor should be created with initializer
        call_kwargs = mock_ppe_cls.call_args[1]
        assert call_kwargs["max_workers"] == 2
        assert call_kwargs["initializer"] is _ignore_sigint

        # Clean up the spawn guard
        runner.__exit__(None, None, None)

    @patch("artisan.orchestration.backends.local.ProcessPoolExecutor")
    def test_enter_neuters_main_file(self, mock_ppe_cls):
        """__enter__ neuters __main__.__file__, __exit__ restores it."""
        main_mod = sys.modules["__main__"]
        original = main_mod.__file__

        runner = SIGINTSafeProcessPoolTaskRunner(max_workers=1)

        with (
            patch.object(
                SIGINTSafeProcessPoolTaskRunner.__bases__[0],
                "__enter__",
                return_value=runner,
            ),
            patch.object(
                SIGINTSafeProcessPoolTaskRunner.__bases__[0],
                "__exit__",
                return_value=None,
            ),
        ):
            runner._executor = MagicMock()
            runner._max_workers = 1
            runner.__enter__()
            assert main_mod.__file__ is None

            runner.__exit__(None, None, None)
            assert main_mod.__file__ == original

    def test_is_subclass_of_process_pool_task_runner(self):
        """SIGINTSafeProcessPoolTaskRunner is a ProcessPoolTaskRunner."""
        from prefect.task_runners import ProcessPoolTaskRunner

        assert issubclass(SIGINTSafeProcessPoolTaskRunner, ProcessPoolTaskRunner)

    def test_create_dispatch_handle_uses_sigint_safe_runner(self):
        """LocalBackend.create_dispatch_handle uses SIGINTSafeProcessPoolTaskRunner."""
        from artisan.orchestration.backends.local import LocalBackend
        from artisan.schemas.execution.execution_config import ExecutionConfig
        from artisan.schemas.operation_config.resource_config import ResourceConfig

        backend = LocalBackend(default_max_workers=2)
        handle = backend.create_dispatch_handle(
            ResourceConfig(), ExecutionConfig(), step_number=0, job_name="test"
        )

        assert isinstance(handle._task_runner, SIGINTSafeProcessPoolTaskRunner)
