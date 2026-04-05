"""Tests for signal-related fixes in step_executor (Fixes 3, 4, 5)."""

from __future__ import annotations

import threading
from concurrent.futures.process import BrokenProcessPool
from unittest.mock import MagicMock, patch

import pytest


class TestCreatorBrokenProcessPool:
    """Fix 3: BrokenProcessPool in creator dispatch produces failed StepResult."""

    @patch("artisan.orchestration.engine.step_executor._create_runtime_environment")
    @patch("artisan.orchestration.engine.step_executor.ExecutionUnit")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.utils.hashing.compute_execution_spec_id")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    @patch("artisan.orchestration.engine.step_executor.get_batch_config")
    @patch(
        "artisan.orchestration.engine.step_executor.generate_execution_unit_batches",
    )
    def test_broken_pool_produces_failed_result(
        self,
        mock_batches,
        mock_batch_config,
        mock_resolve,
        mock_spec_id,
        mock_cache,
        mock_eu_cls,
        mock_create_rt,
    ):
        from artisan.orchestration.engine.step_executor import _execute_creator_step

        mock_op = MagicMock()
        mock_op.name = "test_op"
        mock_op.outputs = {}
        mock_op.group_by = None
        mock_resolve.return_value = {"data": ["id1", "id2"]}

        # Each batch yields (inputs_dict, group_ids)
        mock_batches.return_value = [
            ({"data": ["id1"]}, None),
            ({"data": ["id2"]}, None),
        ]
        mock_spec_id.return_value = "spec-123"
        mock_cache.return_value = None  # No cache hit

        mock_unit = MagicMock()
        mock_eu_cls.return_value = mock_unit

        mock_backend = MagicMock()
        mock_handle = MagicMock()
        mock_handle.run.side_effect = BrokenProcessPool("pool broken")
        mock_backend.create_dispatch_handle.return_value = mock_handle

        config = MagicMock()
        config.delta_root = MagicMock()
        config.staging_root = MagicMock()

        result = _execute_creator_step(
            operation=mock_op,
            inputs={"data": ["id1", "id2"]},
            backend=mock_backend,
            step_number=1,
            config=config,
        )

        assert result.success is False
        assert result.failed_count == 2


class TestCuratorCancelAwareMessage:
    """Fix 4: BrokenProcessPool during cancellation uses cancel-specific message."""

    @patch("artisan.orchestration.engine.step_executor.record_execution_failure")
    @patch("artisan.orchestration.engine.step_executor.build_curator_execution_context")
    @patch("artisan.orchestration.engine.step_executor._create_runtime_environment")
    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.ExecutionUnit")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_cancel_message_logged_when_event_set(
        self,
        mock_resolve,
        mock_eu_cls,
        mock_run_sub,
        mock_create_rt,
        mock_build_ctx,
        mock_record_failure,
        caplog,
    ):
        """BrokenProcessPool with cancel_event set logs cancellation message."""
        from artisan.orchestration.engine.step_executor import _execute_curator_step

        mock_op = MagicMock()
        mock_op.name = "filter"
        mock_op.outputs = {}
        mock_op.group_by = None
        mock_resolve.return_value = {"passthrough": ["id1"]}

        mock_unit = MagicMock()
        mock_unit.execution_spec_id = "spec-123-456789012345678901"
        mock_unit.inputs = {"passthrough": ["id1"]}
        mock_unit.operation = mock_op
        mock_unit.step_number = 1
        mock_eu_cls.return_value = mock_unit

        mock_record_failure.return_value = MagicMock(success=False, error="killed")

        # Set cancel_event during the subprocess call
        event = threading.Event()

        def raise_broken_pool(*args, **kwargs):
            event.set()
            raise BrokenProcessPool("killed")

        mock_run_sub.side_effect = raise_broken_pool

        config = MagicMock()
        config.delta_root = MagicMock()

        _execute_curator_step(
            operation=mock_op,
            inputs={"passthrough": ["id1"]},
            step_number=1,
            config=config,
            cancel_event=event,
            step_spec_id="test-spec-id",
        )

        # Verify the cancellation message was logged (not OOM)
        assert any(
            "cancellation" in record.message for record in caplog.records
        ), f"Expected 'cancellation' in log, got: {[r.message for r in caplog.records]}"

    @patch("artisan.orchestration.engine.step_executor.record_execution_failure")
    @patch("artisan.orchestration.engine.step_executor.build_curator_execution_context")
    @patch("artisan.orchestration.engine.step_executor._create_runtime_environment")
    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.ExecutionUnit")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_oom_message_logged_when_no_cancel(
        self,
        mock_resolve,
        mock_eu_cls,
        mock_run_sub,
        mock_create_rt,
        mock_build_ctx,
        mock_record_failure,
        caplog,
    ):
        """BrokenProcessPool without cancel logs OOM diagnostic message."""
        from artisan.orchestration.engine.step_executor import _execute_curator_step

        mock_op = MagicMock()
        mock_op.name = "filter"
        mock_op.outputs = {}
        mock_op.group_by = None
        mock_resolve.return_value = {"passthrough": ["id1"]}

        mock_unit = MagicMock()
        mock_unit.execution_spec_id = "spec-123-456789012345678901"
        mock_unit.inputs = {"passthrough": ["id1"]}
        mock_unit.operation = mock_op
        mock_unit.step_number = 1
        mock_eu_cls.return_value = mock_unit

        mock_run_sub.side_effect = BrokenProcessPool("killed")
        mock_record_failure.return_value = MagicMock(success=False, error="killed")

        config = MagicMock()
        config.delta_root = MagicMock()

        _execute_curator_step(
            operation=mock_op,
            inputs={"passthrough": ["id1"]},
            step_number=1,
            config=config,
            cancel_event=None,
            step_spec_id="test-spec-id",
        )

        assert any(
            "OOM" in record.message for record in caplog.records
        ), f"Expected 'OOM' in log, got: {[r.message for r in caplog.records]}"


class TestCuratorCancelAwareWait:
    """Fix 5: _run_curator_in_subprocess raises on cancel within ~1s."""

    @patch("artisan.orchestration.engine.step_executor.ProcessPoolExecutor")
    def test_cancel_event_interrupts_wait(self, mock_ppe_cls):
        from artisan.orchestration.engine.step_executor import (
            _run_curator_in_subprocess,
        )

        mock_future = MagicMock()
        mock_future.result.side_effect = TimeoutError

        mock_pool = MagicMock()
        mock_pool.__enter__ = MagicMock(return_value=mock_pool)
        mock_pool.__exit__ = MagicMock(return_value=False)
        mock_pool.submit.return_value = mock_future
        mock_ppe_cls.return_value = mock_pool

        event = threading.Event()
        event.set()

        with pytest.raises(RuntimeError, match="Curator interrupted by cancellation"):
            _run_curator_in_subprocess(
                unit=MagicMock(),
                runtime_env=MagicMock(),
                cancel_event=event,
            )

    @patch("artisan.orchestration.engine.step_executor.ProcessPoolExecutor")
    def test_returns_result_when_no_cancel(self, mock_ppe_cls):
        from artisan.orchestration.engine.step_executor import (
            _run_curator_in_subprocess,
        )

        mock_result = MagicMock()
        mock_future = MagicMock()
        mock_future.result.side_effect = [TimeoutError, mock_result]

        mock_pool = MagicMock()
        mock_pool.__enter__ = MagicMock(return_value=mock_pool)
        mock_pool.__exit__ = MagicMock(return_value=False)
        mock_pool.submit.return_value = mock_future
        mock_ppe_cls.return_value = mock_pool

        result = _run_curator_in_subprocess(
            unit=MagicMock(),
            runtime_env=MagicMock(),
            cancel_event=None,
        )

        assert result is mock_result
