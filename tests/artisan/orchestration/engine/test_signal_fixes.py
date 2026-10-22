"""Tests for process failure and cancellation in step execution."""

from __future__ import annotations

import threading
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool
from unittest.mock import MagicMock, patch

import pytest

from artisan.orchestration.engine.inputs import PreparedInputs
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.utils.hashing import CacheInputIdentity


def _prepared(inputs: dict[str, list[str]]) -> PreparedInputs:
    """Build a typed prepared-input snapshot for signal-path tests."""
    types = {
        artifact_id: "metric"
        for artifact_ids in inputs.values()
        for artifact_id in artifact_ids
    }
    cache_inputs = {
        role: [
            CacheInputIdentity(role, None, position, types[artifact_id], artifact_id)
            for position, artifact_id in enumerate(artifact_ids)
        ]
        for role, artifact_ids in inputs.items()
    }
    return PreparedInputs(inputs, None, cache_inputs)


class TestCreatorBrokenProcessPool:
    """BrokenProcessPool in creator dispatch produces failed StepResult."""

    @patch("artisan.orchestration.engine.step_executor._create_runtime_environment")
    @patch(
        "artisan.orchestration.engine.step_executor.build_replay_snapshot",
        new=lambda *args, **kwargs: ReplaySnapshot.unavailable("mock_unit"),
    )
    @patch("artisan.orchestration.engine.step_executor.ExecutionUnit")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.utils.hashing.compute_execution_spec_id")
    @patch("artisan.orchestration.engine.step_executor.get_batch_config")
    @patch(
        "artisan.orchestration.engine.step_executor.generate_execution_unit_batches",
    )
    def test_broken_pool_produces_failed_result(
        self,
        mock_batches,
        mock_batch_config,
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
        # Each batch yields inputs, group IDs, and the sliced cache identities.
        mock_batches.return_value = [
            ({"data": ["id1"]}, None, _prepared({"data": ["id1"]}).cache_inputs),
            ({"data": ["id2"]}, None, _prepared({"data": ["id2"]}).cache_inputs),
        ]
        mock_spec_id.return_value = "spec-123"
        mock_cache.return_value = None  # No cache hit

        mock_unit = MagicMock()
        mock_unit.get_batch_size.return_value = 1
        mock_eu_cls.return_value = mock_unit

        mock_backend = MagicMock()
        mock_handle = MagicMock()
        mock_handle.run.side_effect = BrokenProcessPool("pool broken")
        mock_backend.create_lifecycle_router.return_value = mock_handle

        config = MagicMock()
        config.delta_root = MagicMock()
        config.staging_root = MagicMock()

        result = _execute_creator_step(
            operation=mock_op,
            inputs=_prepared({"data": ["id1", "id2"]}),
            step_runner=mock_backend,
            step_number=1,
            config=config,
        )

        assert result.status == StepStatus.FAILED
        assert result.failed_count == 2


class TestCuratorBrokenProcessPool:
    """Real subprocess error handling preserves failure and cancellation evidence."""

    @pytest.mark.parametrize("failure_boundary", ["dispatch", "worker"])
    @pytest.mark.parametrize("cancelled", [False, True])
    def test_failed_future_returns_failure_and_closes_ownership(
        self,
        failure_boundary: str,
        cancelled: bool,
        mock_native_ownership: MagicMock,
    ) -> None:
        from artisan.execution.recording.parquet_writer import StagingResult
        from artisan.operations.curator.merge import Merge
        from artisan.orchestration.engine.step_executor import (
            _run_curator_in_subprocess,
        )

        error = BrokenProcessPool("worker terminated")
        dispatched: Future[Future[StagingResult]] = Future()
        if failure_boundary == "dispatch":
            dispatched.set_exception(error)
        else:
            failed: Future[StagingResult] = Future()
            failed.set_exception(error)
            dispatched.set_result(failed)
        event = threading.Event()
        if cancelled:
            event.set()
            mock_native_ownership.cancelled = True
            mock_native_ownership.acknowledgement.return_value = (
                CancellationAcknowledgement(CancellationStatus.CONFIRMED)
            )
        unit = MagicMock(operation=Merge(), inputs={"data": ["a" * 32]})
        with patch(
            "artisan.orchestration.engine.step_executor._dispatch_curator",
            return_value=dispatched,
        ):
            result = _run_curator_in_subprocess(
                unit, MagicMock(), event, ownership=mock_native_ownership
            )

        assert not result.success
        assert result.error is not None
        assert "Curator subprocess killed (likely OOM)" in result.error
        assert not result.execution_run_id
        mock_native_ownership.finish.assert_called_once_with(
            failed=True, cancel_event=event
        )
        if cancelled:
            assert result.cancellation_acknowledgement is not None
            assert (
                result.cancellation_acknowledgement.status
                is CancellationStatus.CONFIRMED
            )
            mock_native_ownership.request_cancel.assert_called_once()
        else:
            assert result.cancellation_acknowledgement is None
            mock_native_ownership.request_cancel.assert_not_called()


class TestCuratorCancelAwareWait:
    """Curator cancellation returns typed process-exit evidence."""

    @patch("artisan.orchestration.engine.step_executor.ProcessPoolExecutor")
    def test_cancel_event_interrupts_wait(self, mock_ppe_cls, mock_native_ownership):
        from artisan.orchestration.engine.step_executor import (
            _run_curator_in_subprocess,
        )

        mock_future = MagicMock()
        # An unfinished child forces the cancellation path.
        mock_future.done.return_value = False

        mock_pool = MagicMock()
        mock_pool.submit.return_value = mock_future
        mock_ppe_cls.return_value = mock_pool

        event = threading.Event()
        event.set()

        mock_native_ownership.cancelled = True
        mock_native_ownership.advance.return_value = True
        mock_native_ownership.acknowledgement.return_value = (
            CancellationAcknowledgement(CancellationStatus.CONFIRMED)
        )
        result = _run_curator_in_subprocess(
            unit=MagicMock(),
            runtime_env=MagicMock(),
            cancel_event=event,
            ownership=mock_native_ownership,
        )
        assert (
            result.cancellation_acknowledgement.status is CancellationStatus.CONFIRMED
        )
        mock_native_ownership.request_cancel.assert_called_once()
        mock_native_ownership.finish.assert_called_once_with(
            failed=True, cancel_event=event
        )
        mock_future.result.assert_not_called()

    @patch("artisan.orchestration.engine.step_executor.ProcessPoolExecutor")
    def test_returns_result_when_no_cancel(self, mock_ppe_cls, mock_native_ownership):
        from artisan.orchestration.engine.step_executor import (
            _run_curator_in_subprocess,
        )

        mock_result = MagicMock()
        mock_future = MagicMock()
        # Done immediately: result() is called exactly once, with no timeout.
        mock_future.done.return_value = True
        mock_future.result.return_value = mock_result

        mock_pool = MagicMock()
        mock_pool.submit.return_value = mock_future
        mock_ppe_cls.return_value = mock_pool

        result = _run_curator_in_subprocess(
            unit=MagicMock(),
            runtime_env=MagicMock(),
            cancel_event=None,
            ownership=mock_native_ownership,
        )

        assert result is mock_result
