"""Tests for escalating signal handler behavior."""

from __future__ import annotations

import signal
from unittest.mock import MagicMock, patch

from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.schemas.orchestration.pipeline_config import PipelineConfig


def _make_pipeline(tmp_path) -> PipelineManager:
    config = PipelineConfig(
        name="test",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "working"),
    )
    return PipelineManager(config)


class TestEscalatingSignalHandler:
    """Fix 1: second Ctrl+C restores default handlers."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_first_signal_cancels(self, mock_tracker_cls, tmp_path):
        """First signal sets cancel event."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        pipeline._handle_signal(signal.SIGINT, None)

        assert pipeline._cancel_event.is_set()

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_second_signal_restores_handlers(self, mock_tracker_cls, tmp_path):
        """Second signal restores default handlers (prev handlers become None)."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        # Install handlers so _prev_sigint/sigterm are set
        pipeline._prev_sigint = signal.SIG_DFL
        pipeline._prev_sigterm = signal.SIG_DFL

        # First signal: cancel
        pipeline._handle_signal(signal.SIGINT, None)
        assert pipeline._cancel_event.is_set()

        # Second signal: restore (prev handlers cleared)
        pipeline._handle_signal(signal.SIGINT, None)
        assert pipeline._prev_sigint is None
        assert pipeline._prev_sigterm is None

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_second_signal_does_not_cancel_again(self, mock_tracker_cls, tmp_path):
        """Second signal should not call cancel() again."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        pipeline._handle_signal(signal.SIGINT, None)

        with patch.object(pipeline, "cancel") as mock_cancel:
            pipeline._handle_signal(signal.SIGINT, None)
            mock_cancel.assert_not_called()
