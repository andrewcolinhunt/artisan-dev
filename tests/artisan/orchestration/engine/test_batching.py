"""Tests for orchestration batch helpers."""

from __future__ import annotations

import pytest

from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.examples.data_generator import DataGenerator
from artisan.orchestration.engine.batching import (
    generate_execution_unit_batches,
    get_batch_config,
    pack_units,
)
from artisan.schemas.orchestration.batch_config import BatchConfig
from artisan.utils.hashing import CacheInputIdentity


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


def _cache_inputs(inputs: dict[str, list[str]]) -> dict[str, list[CacheInputIdentity]]:
    return {
        role: [
            CacheInputIdentity(role, None, position, "data", artifact_id)
            for position, artifact_id in enumerate(artifact_ids)
        ]
        for role, artifact_ids in inputs.items()
    }


class TestGetBatchConfig:
    """Tests for get_batch_config function."""

    def test_defaults_from_operation_instance(self):
        """Test defaults when reading from operation instance."""
        from typing import ClassVar

        from artisan.operations.base.operation_definition import OperationDefinition
        from artisan.schemas.execution.batch_strategy import BatchStrategy
        from artisan.schemas.specs.input_spec import InputSpec
        from artisan.schemas.specs.output_spec import OutputSpec

        class MockOp(OperationDefinition):
            name: ClassVar[str] = "mock_batch_test"
            inputs: ClassVar[dict[str, InputSpec]] = {}
            outputs: ClassVar[dict[str, OutputSpec]] = {}
            batch_strategy: BatchStrategy = BatchStrategy(artifacts_per_unit=5)

            def execute_function(self, inputs, output_dir):
                pass

        config = get_batch_config(MockOp())
        assert config.artifacts_per_unit == 5
        assert config.units_per_worker == 1

    def test_units_per_worker_from_instance(self):
        """Test units_per_worker read from operation instance."""
        from typing import ClassVar

        from artisan.operations.base.operation_definition import OperationDefinition
        from artisan.schemas.execution.batch_strategy import BatchStrategy
        from artisan.schemas.specs.input_spec import InputSpec
        from artisan.schemas.specs.output_spec import OutputSpec

        class MockOp(OperationDefinition):
            name: ClassVar[str] = "mock_upw_test"
            inputs: ClassVar[dict[str, InputSpec]] = {}
            outputs: ClassVar[dict[str, OutputSpec]] = {}
            batch_strategy: BatchStrategy = BatchStrategy(
                artifacts_per_unit=10, units_per_worker=4
            )

            def execute_function(self, inputs, output_dir):
                pass

        config = get_batch_config(MockOp())
        assert config.artifacts_per_unit == 10
        assert config.units_per_worker == 4

    def test_default_operation_gets_default_config(self):
        """Test operation with default BatchStrategy gets defaults."""
        from typing import ClassVar

        from artisan.operations.base.operation_definition import OperationDefinition
        from artisan.schemas.specs.input_spec import InputSpec
        from artisan.schemas.specs.output_spec import OutputSpec

        class MockOp(OperationDefinition):
            name: ClassVar[str] = "mock_default_test"
            inputs: ClassVar[dict[str, InputSpec]] = {}
            outputs: ClassVar[dict[str, OutputSpec]] = {}

            def execute_function(self, inputs, output_dir):
                pass

        config = get_batch_config(MockOp())
        assert config.artifacts_per_unit == 1
        assert config.units_per_worker == 1

    def test_max_artifacts_per_unit_cap(self):
        """max_artifacts_per_unit caps artifacts_per_unit."""
        from typing import ClassVar

        from artisan.operations.base.operation_definition import OperationDefinition
        from artisan.schemas.execution.batch_strategy import BatchStrategy
        from artisan.schemas.specs.input_spec import InputSpec
        from artisan.schemas.specs.output_spec import OutputSpec

        class MockOp(OperationDefinition):
            name: ClassVar[str] = "mock_cap_test"
            inputs: ClassVar[dict[str, InputSpec]] = {}
            outputs: ClassVar[dict[str, OutputSpec]] = {}
            batch_strategy: BatchStrategy = BatchStrategy(
                artifacts_per_unit=100, max_artifacts_per_unit=5
            )

            def execute_function(self, inputs, output_dir):
                pass

        config = get_batch_config(MockOp())
        assert config.artifacts_per_unit == 5


class TestGenerateExecutionUnitBatches:
    """Tests for generate_execution_unit_batches function."""

    def test_empty_inputs_returns_single_empty_batch(self):
        """Test generative operation case."""
        config = BatchConfig(artifacts_per_unit=5)
        batches = generate_execution_unit_batches({}, config, cache_inputs={})
        assert len(batches) == 1
        batch_inputs, batch_gids, batch_cache_inputs = batches[0]
        assert batch_inputs == {}
        assert batch_gids is None
        assert batch_cache_inputs == {}

    def test_exact_artifacts_per_unit_split(self):
        """Test inputs that divide evenly into batches."""
        inputs = {"data": ["a", "b", "c", "d"]}
        config = BatchConfig(artifacts_per_unit=2)
        batches = generate_execution_unit_batches(
            inputs, config, cache_inputs=_cache_inputs(inputs)
        )
        assert len(batches) == 2
        assert batches[0][0] == {"data": ["a", "b"]}
        assert batches[1][0] == {"data": ["c", "d"]}
        # No group_ids when not provided
        assert batches[0][1] is None
        assert batches[1][1] is None

    def test_remainder_batch(self):
        """Test inputs with remainder."""
        inputs = {"data": ["a", "b", "c", "d", "e"]}
        config = BatchConfig(artifacts_per_unit=2)
        batches = generate_execution_unit_batches(
            inputs, config, cache_inputs=_cache_inputs(inputs)
        )
        assert len(batches) == 3
        assert batches[0][0] == {"data": ["a", "b"]}
        assert batches[1][0] == {"data": ["c", "d"]}
        assert batches[2][0] == {"data": ["e"]}

    def test_multi_role_inputs(self):
        """Test inputs with multiple roles."""
        inputs = {
            "data": ["s1", "s2", "s3", "s4"],
            "config": ["c1", "c2", "c3", "c4"],
        }
        config = BatchConfig(artifacts_per_unit=2)
        batches = generate_execution_unit_batches(
            inputs, config, cache_inputs=_cache_inputs(inputs)
        )
        assert len(batches) == 2
        assert batches[0][0] == {"data": ["s1", "s2"], "config": ["c1", "c2"]}
        assert batches[1][0] == {"data": ["s3", "s4"], "config": ["c3", "c4"]}

    def test_single_item(self):
        """Test single item input."""
        inputs = {"data": ["a"]}
        config = BatchConfig(artifacts_per_unit=10)
        batches = generate_execution_unit_batches(
            inputs, config, cache_inputs=_cache_inputs(inputs)
        )
        assert len(batches) == 1
        assert batches[0][0] == {"data": ["a"]}

    def test_artifacts_per_unit_larger_than_inputs(self):
        """Test artifacts_per_unit larger than input count."""
        inputs = {"data": ["a", "b"]}
        config = BatchConfig(artifacts_per_unit=100)
        batches = generate_execution_unit_batches(
            inputs, config, cache_inputs=_cache_inputs(inputs)
        )
        assert len(batches) == 1
        assert batches[0][0] == {"data": ["a", "b"]}

    def test_group_ids_sliced_with_inputs(self):
        """Test that group_ids are sliced in sync with input lists."""
        inputs = {"data": ["s1", "s2", "s3", "s4"]}
        group_ids = ["g1", "g2", "g3", "g4"]
        config = BatchConfig(artifacts_per_unit=2)
        batches = generate_execution_unit_batches(
            inputs,
            config,
            group_ids=group_ids,
            cache_inputs=_cache_inputs(inputs),
        )
        assert len(batches) == 2
        assert batches[0][0] == {"data": ["s1", "s2"]}
        assert batches[0][1] == ["g1", "g2"]
        assert batches[1][0] == {"data": ["s3", "s4"]}
        assert batches[1][1] == ["g3", "g4"]

    def test_group_ids_sliced_with_remainder(self):
        """Test that group_ids remainder batch aligns correctly."""
        inputs = {"data": ["s1", "s2", "s3"]}
        group_ids = ["g1", "g2", "g3"]
        config = BatchConfig(artifacts_per_unit=2)
        batches = generate_execution_unit_batches(
            inputs,
            config,
            group_ids=group_ids,
            cache_inputs=_cache_inputs(inputs),
        )
        assert len(batches) == 2
        assert batches[0][1] == ["g1", "g2"]
        assert batches[1][1] == ["g3"]

    def test_group_ids_sliced_multi_role(self):
        """Test group_ids slicing preserves pair alignment across roles."""
        inputs = {
            "data": ["s1", "s2", "s3", "s4"],
            "config": ["c1", "c2", "c3", "c4"],
        }
        group_ids = ["g1", "g2", "g3", "g4"]
        config = BatchConfig(artifacts_per_unit=2)
        batches = generate_execution_unit_batches(
            inputs,
            config,
            group_ids=group_ids,
            cache_inputs=_cache_inputs(inputs),
        )
        assert len(batches) == 2

        # Batch 0: data[0:2], config[0:2], group_ids[0:2]
        batch_inputs_0, batch_gids_0, batch_cache_inputs_0 = batches[0]
        assert batch_inputs_0["data"] == ["s1", "s2"]
        assert batch_inputs_0["config"] == ["c1", "c2"]
        assert batch_gids_0 == ["g1", "g2"]
        assert [entry.artifact_id for entry in batch_cache_inputs_0["data"]] == [
            "s1",
            "s2",
        ]
        assert [entry.position for entry in batch_cache_inputs_0["data"]] == [0, 1]

        # Batch 1: data[2:4], config[2:4], group_ids[2:4]
        batch_inputs_1, batch_gids_1, batch_cache_inputs_1 = batches[1]
        assert batch_inputs_1["data"] == ["s3", "s4"]
        assert batch_inputs_1["config"] == ["c3", "c4"]
        assert batch_gids_1 == ["g3", "g4"]
        assert [entry.artifact_id for entry in batch_cache_inputs_1["data"]] == [
            "s3",
            "s4",
        ]
        assert [entry.position for entry in batch_cache_inputs_1["data"]] == [0, 1]

    def test_group_ids_none_when_not_provided(self):
        """Test that group_ids defaults to None in each batch when not provided."""
        inputs = {"data": ["s1", "s2"]}
        config = BatchConfig(artifacts_per_unit=1)
        batches = generate_execution_unit_batches(
            inputs, config, cache_inputs=_cache_inputs(inputs)
        )
        for _, batch_gids, _ in batches:
            assert batch_gids is None

    def test_group_ids_single_batch(self):
        """Test group_ids with a single batch (all items fit)."""
        inputs = {"data": ["s1", "s2"]}
        group_ids = ["g1", "g2"]
        config = BatchConfig(artifacts_per_unit=10)
        batches = generate_execution_unit_batches(
            inputs,
            config,
            group_ids=group_ids,
            cache_inputs=_cache_inputs(inputs),
        )
        assert len(batches) == 1
        assert batches[0][1] == ["g1", "g2"]
