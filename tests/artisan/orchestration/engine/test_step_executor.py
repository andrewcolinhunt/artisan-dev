"""Tests for cancel_event handling in step_executor."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

from artisan.orchestration.engine.step_executor import (
    _cancelled_result,
    execute_step,
)
from artisan.schemas.enums import FailurePolicy


class TestExecuteStepPassesCancelEvent:
    """execute_step should forward cancel_event to creator/curator paths."""

    @patch("artisan.orchestration.engine.step_executor._execute_creator_step")
    @patch(
        "artisan.orchestration.engine.step_executor.is_curator_operation",
        return_value=False,
    )
    @patch("artisan.orchestration.engine.step_executor.instantiate_operation")
    def test_passes_cancel_event_to_creator(
        self, mock_instantiate, mock_is_curator, mock_creator
    ):
        mock_op = MagicMock()
        mock_op.name = "test"
        mock_instantiate.return_value = mock_op
        mock_creator.return_value = MagicMock()

        event = threading.Event()
        execute_step(
            operation_class=MagicMock(),
            inputs=None,
            params=None,
            backend=MagicMock(),
            cancel_event=event,
        )

        _, kwargs = mock_creator.call_args
        assert kwargs["cancel_event"] is event

    @patch("artisan.orchestration.engine.step_executor._execute_curator_step")
    @patch(
        "artisan.orchestration.engine.step_executor.is_curator_operation",
        return_value=True,
    )
    @patch("artisan.orchestration.engine.step_executor.instantiate_operation")
    def test_passes_cancel_event_to_curator(
        self, mock_instantiate, mock_is_curator, mock_curator
    ):
        mock_op = MagicMock()
        mock_op.name = "test"
        mock_instantiate.return_value = mock_op
        mock_curator.return_value = MagicMock()

        event = threading.Event()
        execute_step(
            operation_class=MagicMock(),
            inputs=None,
            params=None,
            backend=MagicMock(),
            cancel_event=event,
        )

        _, kwargs = mock_curator.call_args
        assert kwargs["cancel_event"] is event


class TestCreatorCancelChecks:
    """_execute_creator_step returns cancelled result when event is set."""

    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    @patch("artisan.orchestration.engine.step_executor.get_batch_config")
    @patch(
        "artisan.orchestration.engine.step_executor.generate_execution_unit_batches",
        return_value=[],
    )
    def test_cancel_before_execute_phase(
        self, mock_batches, mock_batch_config, mock_resolve
    ):
        """Cancel event set before PHASE 2 should return cancelled result."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step

        mock_op = MagicMock()
        mock_op.name = "test_op"
        mock_op.outputs = {}
        mock_op.group_by = None
        mock_resolve.return_value = {"data": ["id1"]}

        event = threading.Event()
        event.set()  # Pre-set = cancelled

        config = MagicMock()
        config.delta_root = MagicMock()
        config.staging_root = MagicMock()

        result = _execute_creator_step(
            operation=mock_op,
            inputs={"data": ["id1"]},
            backend=MagicMock(),
            step_number=1,
            config=config,
            cancel_event=event,
        )

        assert result.metadata.get("cancelled") is True

    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_cancel_before_execute_phase_curator(self, mock_resolve):
        """Cancel event set before execute should return cancelled result for curator."""
        from artisan.orchestration.engine.step_executor import _execute_curator_step

        mock_op = MagicMock()
        mock_op.name = "filter"
        mock_op.outputs = {}
        mock_op.group_by = None
        mock_resolve.return_value = {"data": ["id1"]}

        event = threading.Event()
        event.set()

        config = MagicMock()
        config.delta_root = MagicMock()

        result = _execute_curator_step(
            operation=mock_op,
            inputs={"data": ["id1"]},
            step_number=1,
            config=config,
            cancel_event=event,
            step_spec_id="test-spec-id",
        )

        assert result.metadata.get("cancelled") is True


class TestCancelledResult:
    """Tests for the _cancelled_result helper."""

    def test_cancelled_result_has_metadata(self):
        mock_op = MagicMock()
        mock_op.name = "test"
        mock_op.outputs = {}

        result = _cancelled_result(mock_op, 1, FailurePolicy.CONTINUE)
        assert result.metadata["cancelled"] is True
        assert result.succeeded_count == 0
        assert result.failed_count == 0
