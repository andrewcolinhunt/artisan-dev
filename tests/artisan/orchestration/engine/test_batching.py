"""Tests for orchestration batch helpers."""

from __future__ import annotations

import pytest

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.examples.data_generator import DataGenerator
from artisan.orchestration.engine.batching import pack_units


def _units(count: int) -> list[ExecutionUnit]:
    """Build distinct generative units."""
    return [
        ExecutionUnit(
            operation=DataGenerator(),
            execution_spec_id=f"{index:032x}",
        )
        for index in range(count)
    ]


class TestPackUnits:
    def test_preserves_order_and_remainder(self) -> None:
        units = _units(5)

        batches = pack_units(units, units_per_worker=2)

        assert batches == [units[:2], units[2:4], units[4:]]

    def test_one_unit_per_worker(self) -> None:
        units = _units(3)

        assert pack_units(units, units_per_worker=1) == [
            [units[0]],
            [units[1]],
            [units[2]],
        ]

    def test_batch_larger_than_input(self) -> None:
        units = _units(2)

        assert pack_units(units, units_per_worker=10) == [units]

    def test_empty_input(self) -> None:
        assert pack_units([], units_per_worker=3) == []

    @pytest.mark.parametrize("units_per_worker", [0, -1])
    def test_rejects_non_positive_batch_size(self, units_per_worker: int) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            pack_units(_units(1), units_per_worker)
