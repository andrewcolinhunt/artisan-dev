"""Tests for orchestration/engine/dispatch.py — resilient dispatch."""

from __future__ import annotations

import os
from unittest.mock import MagicMock

from artisan.orchestration.engine.dispatch import (
    _collect_results,
    _load_units,
    _save_units,
)
from artisan.schemas.execution.unit_result import UnitResult


def _result(**overrides: object) -> UnitResult:
    """Build a UnitResult with sensible defaults."""
    defaults = dict(success=True, error=None, item_count=1, execution_run_ids=[])
    return UnitResult(**{**defaults, **overrides})


class TestCollectResults:
    """Tests for _collect_results() resilient futures collection."""

    def test_all_succeed(self):
        """All futures succeed — returns all results."""
        futures = [MagicMock(), MagicMock(), MagicMock()]
        for f in futures:
            f.result.return_value = _result()

        results = _collect_results(futures)

        assert len(results) == 3
        assert all(r.success for r in results)

    def test_one_fails_others_still_collected(self):
        """One future raises — other results still collected."""
        futures = [MagicMock(), MagicMock(), MagicMock()]
        futures[0].result.return_value = _result()
        futures[1].result.side_effect = RuntimeError("task crashed")
        futures[2].result.return_value = _result()

        results = _collect_results(futures)

        assert len(results) == 3
        assert results[0].success is True
        assert results[1].success is False
        assert "RuntimeError" in results[1].error
        assert results[2].success is True

    def test_all_fail(self):
        """All futures raise — all converted to failure results."""
        futures = [MagicMock(), MagicMock()]
        futures[0].result.side_effect = ValueError("bad")
        futures[1].result.side_effect = OSError("disk")

        results = _collect_results(futures)

        assert len(results) == 2
        assert all(not r.success for r in results)
        assert "ValueError" in results[0].error
        assert "OSError" in results[1].error

    def test_empty_futures(self):
        """Empty futures list returns empty results."""
        assert _collect_results([]) == []


class TestSaveLoadUnits:
    """Tests for _save_units() and _load_units() round-trip serialization."""

    def _make_units(self, count: int = 2) -> list:
        """Create minimal ExecutionUnit instances for testing."""
        from artisan.execution.models.execution_unit import ExecutionUnit
        from artisan.operations.examples.data_generator import DataGenerator

        return [
            ExecutionUnit(
                operation=DataGenerator(),
                inputs={},
                execution_spec_id=f"{i:032x}",
                step_number=0,
            )
            for i in range(count)
        ]

    def test_round_trip(self, tmp_path):
        """Save then load returns identical units."""
        units = self._make_units(3)
        path = _save_units(units, str(tmp_path), step_number=1)
        loaded = _load_units(path)

        assert len(loaded) == len(units)
        for original, restored in zip(units, loaded, strict=True):
            assert original.execution_spec_id == restored.execution_spec_id
            assert original.inputs == restored.inputs

    def test_creates_dispatch_directory(self, tmp_path):
        """_dispatch/ directory is created if absent."""
        units = self._make_units(1)
        dispatch_dir = tmp_path / "_dispatch"
        assert not dispatch_dir.exists()

        _save_units(units, str(tmp_path), step_number=0)

        assert dispatch_dir.is_dir()

    def test_file_naming(self, tmp_path):
        """Path matches staging_root/_dispatch/step_{n}_units.pkl."""
        units = self._make_units(1)
        path = _save_units(units, str(tmp_path), step_number=5)

        assert path == os.path.join(str(tmp_path), "_dispatch", "step_5_units.pkl")
        assert os.path.exists(path)
