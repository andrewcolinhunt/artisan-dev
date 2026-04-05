"""Tests for cancel-through-run flow on concrete dispatch handles.

Verifies the integration between ``run(cancel_event=...)``, the
background thread, and each handle's ``cancel()`` method.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from artisan.orchestration.backends.local import LocalBackend
from artisan.orchestration.backends.slurm import SlurmDispatchHandle
from artisan.orchestration.engine.dispatch_handle import _HandleState
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.resource_config import ResourceConfig


def _result(**overrides: object) -> UnitResult:
    defaults = dict(success=True, error=None, item_count=1, execution_run_ids=[])
    return UnitResult(**{**defaults, **overrides})


def _fake_flow(**flow_kwargs):
    """Mock ``@flow`` decorator: returns a wrapper that sleeps then returns results."""

    def decorator(fn):
        def wrapper(*args, **kwargs):
            time.sleep(0.3)
            return [_result()]

        return wrapper

    return decorator


class TestLocalDispatchHandleCancelFlow:
    """Cancel-through-run on a real LocalDispatchHandle."""

    @patch("prefect.unmapped", MagicMock())
    @patch("prefect.flow", side_effect=_fake_flow)
    def test_run_with_pre_set_cancel_event(self, _mock_flow) -> None:
        """run() with an already-set cancel_event completes without hanging."""
        handle = LocalBackend(default_max_workers=1).create_dispatch_handle(
            ResourceConfig(), ExecutionConfig(), step_number=0, job_name="test"
        )

        cancel_event = threading.Event()
        cancel_event.set()

        results = handle.run([], MagicMock(), cancel_event=cancel_event)
        assert isinstance(results, list)

    @patch("prefect.unmapped", MagicMock())
    @patch("prefect.flow", side_effect=_fake_flow)
    def test_run_completes_after_delayed_cancel(self, _mock_flow) -> None:
        """run() returns after cancel_event is set mid-execution."""
        handle = LocalBackend(default_max_workers=1).create_dispatch_handle(
            ResourceConfig(), ExecutionConfig(), step_number=0, job_name="test"
        )

        cancel_event = threading.Event()

        def _set_after_delay():
            time.sleep(0.1)
            cancel_event.set()

        threading.Thread(target=_set_after_delay, daemon=True).start()

        start = time.monotonic()
        results = handle.run([], MagicMock(), cancel_event=cancel_event)
        elapsed = time.monotonic() - start

        assert isinstance(results, list)
        assert elapsed < 3.0


class TestSlurmDispatchHandleCancelFlow:
    """Cancel-through-run on a real SlurmDispatchHandle."""

    @patch("artisan.orchestration.backends.slurm.subprocess")
    def test_run_with_pre_set_cancel_calls_scancel(
        self, mock_subprocess: MagicMock
    ) -> None:
        """run() with pre-set cancel_event calls scancel before completing."""
        with (
            patch("prefect.flow", side_effect=_fake_flow),
            patch("prefect.unmapped", MagicMock()),
            patch(
                "artisan.orchestration.engine.dispatch._save_units",
                return_value=Path("/fake/units.pkl"),
            ),
            patch(
                "artisan.orchestration.engine.dispatch._load_units",
                return_value=[],
            ),
        ):
            handle = SlurmDispatchHandle(
                task_runner=MagicMock(),
                job_name="s0_test_op",
                staging_root=Path("/staging"),
                step_number=0,
            )

            cancel_event = threading.Event()
            cancel_event.set()

            results = handle.run([], MagicMock(), cancel_event=cancel_event)

        mock_subprocess.run.assert_called_with(
            ["scancel", "--name", "s0_test_op"],
            check=False,
            capture_output=True,
        )
        assert isinstance(results, list)

    @patch("artisan.orchestration.backends.slurm.subprocess")
    def test_run_with_delayed_cancel_calls_scancel(
        self, mock_subprocess: MagicMock
    ) -> None:
        """Delayed cancel_event triggers scancel during run() poll loop."""
        with (
            patch("prefect.flow", side_effect=_fake_flow),
            patch("prefect.unmapped", MagicMock()),
            patch(
                "artisan.orchestration.engine.dispatch._save_units",
                return_value=Path("/fake/units.pkl"),
            ),
            patch(
                "artisan.orchestration.engine.dispatch._load_units",
                return_value=[],
            ),
        ):
            handle = SlurmDispatchHandle(
                task_runner=MagicMock(),
                job_name="s1_my_op",
                staging_root=Path("/staging"),
                step_number=1,
            )

            cancel_event = threading.Event()

            def _set_after_delay():
                time.sleep(0.1)
                cancel_event.set()

            threading.Thread(target=_set_after_delay, daemon=True).start()

            results = handle.run([], MagicMock(), cancel_event=cancel_event)

        mock_subprocess.run.assert_called_with(
            ["scancel", "--name", "s1_my_op"],
            check=False,
            capture_output=True,
        )
        assert isinstance(results, list)


class TestSlurmDispatchHandleCancelBeforeDispatch:
    """Cancel on a SlurmDispatchHandle that hasn't dispatched yet."""

    @patch("artisan.orchestration.backends.slurm.subprocess")
    def test_cancel_before_dispatch_calls_scancel(
        self, mock_subprocess: MagicMock
    ) -> None:
        handle = SlurmDispatchHandle(
            task_runner=MagicMock(),
            job_name="s2_early",
            staging_root=Path("/staging"),
            step_number=2,
        )

        assert handle._state is _HandleState.IDLE
        handle.cancel()
        mock_subprocess.run.assert_called_once()
