"""Tests for parallel result collection in dispatch.py."""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from artisan.orchestration.engine.dispatch import _collect_results


class TestCollectResults:
    """Tests for _collect_results with parallel collection."""

    def test_results_preserve_order_when_futures_complete_out_of_order(self):
        """Results must match the order of the input futures list."""
        barriers = [threading.Event() for _ in range(3)]
        # Release in reverse order to force out-of-order completion
        release_order = [2, 0, 1]

        def _make_future(idx: int, value: dict) -> MagicMock:
            mock = MagicMock()

            def _result():
                barriers[idx].wait(timeout=5)
                return value

            mock.result = _result
            return mock

        expected = [
            {"success": True, "item_count": 1, "execution_run_ids": ["a"]},
            {"success": True, "item_count": 2, "execution_run_ids": ["b"]},
            {"success": True, "item_count": 3, "execution_run_ids": ["c"]},
        ]
        futures = [_make_future(i, expected[i]) for i in range(3)]

        # Release futures out of order in a background thread
        def _release():
            for idx in release_order:
                barriers[idx].set()
                time.sleep(0.01)

        t = threading.Thread(target=_release)
        t.start()

        results = _collect_results(futures)
        t.join()

        assert results == expected

    def test_exception_produces_failure_dict(self):
        """A future that raises should become a failure dict at the correct index."""
        good = MagicMock()
        good.result.return_value = {
            "success": True,
            "item_count": 1,
            "execution_run_ids": ["ok"],
        }

        bad = MagicMock()
        bad.result.side_effect = RuntimeError("boom")

        results = _collect_results([good, bad])

        assert results[0]["success"] is True
        assert results[1]["success"] is False
        assert "boom" in results[1]["error"]
        assert results[1]["item_count"] == 1

    def test_empty_futures_list(self):
        """An empty futures list should return an empty results list."""
        assert _collect_results([]) == []


class TestExecuteUnitTaskKeyboardInterrupt:
    """KeyboardInterrupt in subprocess is converted to RuntimeError."""

    def test_keyboard_interrupt_raises_runtime_error(self):
        """execute_unit_task converts KeyboardInterrupt to RuntimeError."""
        from artisan.orchestration.engine.dispatch import execute_unit_task

        unit = MagicMock()
        unit.operation = MagicMock()
        runtime_env = MagicMock()
        runtime_env.worker_id_env_var = None

        with (
            patch(
                "artisan.execution.executors.curator.is_curator_operation",
                return_value=False,
            ),
            patch(
                "artisan.execution.executors.creator.run_creator_flow",
                side_effect=KeyboardInterrupt,
            ),
        ):
            with pytest.raises(RuntimeError, match="SIGINT"):
                execute_unit_task(unit, runtime_env)
