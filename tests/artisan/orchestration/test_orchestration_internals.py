"""Tests for orchestration engine helper modules.

Tests for:
- BatchConfig and batch generation functions
- Reference resolution functions
- Worker results aggregation
"""

from __future__ import annotations

import pytest

from artisan.orchestration.engine.batching import (
    generate_execution_unit_batches,
    get_batch_config,
)
from artisan.orchestration.engine.results import aggregate_results
from artisan.schemas.enums import FailurePolicy
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.orchestration.batch_config import BatchConfig


class TestBatchConfig:
    """Tests for BatchConfig dataclass."""

    def test_defaults(self):
        """Test default values."""
        config = BatchConfig()
        assert config.artifacts_per_unit == 1
        assert config.units_per_worker == 1

    def test_custom_values(self):
        """Test custom values."""
        config = BatchConfig(artifacts_per_unit=10, units_per_worker=5)
        assert config.artifacts_per_unit == 10
        assert config.units_per_worker == 5


class TestGetBatchConfig:
    """Tests for get_batch_config function."""

    def test_defaults_from_operation_instance(self):
        """Test defaults when reading from operation instance."""
        from typing import ClassVar

        from artisan.operations.base.operation_definition import OperationDefinition
        from artisan.schemas.execution.execution_config import ExecutionConfig
        from artisan.schemas.specs.input_spec import InputSpec
        from artisan.schemas.specs.output_spec import OutputSpec

        class MockOp(OperationDefinition):
            name: ClassVar[str] = "mock_batch_test"
            inputs: ClassVar[dict[str, InputSpec]] = {}
            outputs: ClassVar[dict[str, OutputSpec]] = {}
            execution: ExecutionConfig = ExecutionConfig(artifacts_per_unit=5)

            def execute(self, inputs, output_dir):
                pass

        config = get_batch_config(MockOp())
        assert config.artifacts_per_unit == 5
        assert config.units_per_worker == 1

    def test_units_per_worker_from_instance(self):
        """Test units_per_worker read from operation instance."""
        from typing import ClassVar

        from artisan.operations.base.operation_definition import OperationDefinition
        from artisan.schemas.execution.execution_config import ExecutionConfig
        from artisan.schemas.specs.input_spec import InputSpec
        from artisan.schemas.specs.output_spec import OutputSpec

        class MockOp(OperationDefinition):
            name: ClassVar[str] = "mock_upw_test"
            inputs: ClassVar[dict[str, InputSpec]] = {}
            outputs: ClassVar[dict[str, OutputSpec]] = {}
            execution: ExecutionConfig = ExecutionConfig(
                artifacts_per_unit=10, units_per_worker=4
            )

            def execute(self, inputs, output_dir):
                pass

        config = get_batch_config(MockOp())
        assert config.artifacts_per_unit == 10
        assert config.units_per_worker == 4

    def test_default_operation_gets_default_config(self):
        """Test operation with default ExecutionConfig gets defaults."""
        from typing import ClassVar

        from artisan.operations.base.operation_definition import OperationDefinition
        from artisan.schemas.specs.input_spec import InputSpec
        from artisan.schemas.specs.output_spec import OutputSpec

        class MockOp(OperationDefinition):
            name: ClassVar[str] = "mock_default_test"
            inputs: ClassVar[dict[str, InputSpec]] = {}
            outputs: ClassVar[dict[str, OutputSpec]] = {}

            def execute(self, inputs, output_dir):
                pass

        config = get_batch_config(MockOp())
        assert config.artifacts_per_unit == 1
        assert config.units_per_worker == 1

    def test_max_artifacts_per_unit_cap(self):
        """max_artifacts_per_unit caps artifacts_per_unit."""
        from typing import ClassVar

        from artisan.operations.base.operation_definition import OperationDefinition
        from artisan.schemas.execution.execution_config import ExecutionConfig
        from artisan.schemas.specs.input_spec import InputSpec
        from artisan.schemas.specs.output_spec import OutputSpec

        class MockOp(OperationDefinition):
            name: ClassVar[str] = "mock_cap_test"
            inputs: ClassVar[dict[str, InputSpec]] = {}
            outputs: ClassVar[dict[str, OutputSpec]] = {}
            execution: ExecutionConfig = ExecutionConfig(
                artifacts_per_unit=100, max_artifacts_per_unit=5
            )

            def execute(self, inputs, output_dir):
                pass

        config = get_batch_config(MockOp())
        assert config.artifacts_per_unit == 5


class TestGenerateExecutionUnitBatches:
    """Tests for generate_execution_unit_batches function."""

    def test_empty_inputs_returns_single_empty_batch(self):
        """Test generative operation case."""
        config = BatchConfig(artifacts_per_unit=5)
        batches = generate_execution_unit_batches({}, config)
        assert len(batches) == 1
        batch_inputs, batch_gids = batches[0]
        assert batch_inputs == {}
        assert batch_gids is None

    def test_exact_artifacts_per_unit_split(self):
        """Test inputs that divide evenly into batches."""
        inputs = {"data": ["a", "b", "c", "d"]}
        config = BatchConfig(artifacts_per_unit=2)
        batches = generate_execution_unit_batches(inputs, config)
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
        batches = generate_execution_unit_batches(inputs, config)
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
        batches = generate_execution_unit_batches(inputs, config)
        assert len(batches) == 2
        assert batches[0][0] == {"data": ["s1", "s2"], "config": ["c1", "c2"]}
        assert batches[1][0] == {"data": ["s3", "s4"], "config": ["c3", "c4"]}

    def test_single_item(self):
        """Test single item input."""
        inputs = {"data": ["a"]}
        config = BatchConfig(artifacts_per_unit=10)
        batches = generate_execution_unit_batches(inputs, config)
        assert len(batches) == 1
        assert batches[0][0] == {"data": ["a"]}

    def test_artifacts_per_unit_larger_than_inputs(self):
        """Test artifacts_per_unit larger than input count."""
        inputs = {"data": ["a", "b"]}
        config = BatchConfig(artifacts_per_unit=100)
        batches = generate_execution_unit_batches(inputs, config)
        assert len(batches) == 1
        assert batches[0][0] == {"data": ["a", "b"]}

    def test_group_ids_sliced_with_inputs(self):
        """Test that group_ids are sliced in sync with input lists."""
        inputs = {"data": ["s1", "s2", "s3", "s4"]}
        group_ids = ["g1", "g2", "g3", "g4"]
        config = BatchConfig(artifacts_per_unit=2)
        batches = generate_execution_unit_batches(inputs, config, group_ids=group_ids)
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
        batches = generate_execution_unit_batches(inputs, config, group_ids=group_ids)
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
        batches = generate_execution_unit_batches(inputs, config, group_ids=group_ids)
        assert len(batches) == 2

        # Batch 0: data[0:2], config[0:2], group_ids[0:2]
        batch_inputs_0, batch_gids_0 = batches[0]
        assert batch_inputs_0["data"] == ["s1", "s2"]
        assert batch_inputs_0["config"] == ["c1", "c2"]
        assert batch_gids_0 == ["g1", "g2"]

        # Batch 1: data[2:4], config[2:4], group_ids[2:4]
        batch_inputs_1, batch_gids_1 = batches[1]
        assert batch_inputs_1["data"] == ["s3", "s4"]
        assert batch_inputs_1["config"] == ["c3", "c4"]
        assert batch_gids_1 == ["g3", "g4"]

    def test_group_ids_none_when_not_provided(self):
        """Test that group_ids defaults to None in each batch when not provided."""
        inputs = {"data": ["s1", "s2"]}
        config = BatchConfig(artifacts_per_unit=1)
        batches = generate_execution_unit_batches(inputs, config)
        for _, batch_gids in batches:
            assert batch_gids is None

    def test_group_ids_single_batch(self):
        """Test group_ids with a single batch (all items fit)."""
        inputs = {"data": ["s1", "s2"]}
        group_ids = ["g1", "g2"]
        config = BatchConfig(artifacts_per_unit=10)
        batches = generate_execution_unit_batches(inputs, config, group_ids=group_ids)
        assert len(batches) == 1
        assert batches[0][1] == ["g1", "g2"]


class TestAggregateResults:
    """Tests for aggregate_results function."""

    def test_all_success(self):
        """Test with all successful results."""
        results = [
            UnitResult(success=True, error=None, item_count=5, execution_run_ids=[]),
            UnitResult(success=True, error=None, item_count=3, execution_run_ids=[]),
        ]
        succeeded, failed = aggregate_results(results, FailurePolicy.CONTINUE)
        assert succeeded == 8
        assert failed == 0

    def test_all_failure(self):
        """Test with all failed results."""
        results = [
            UnitResult(success=False, error="err1", item_count=2, execution_run_ids=[]),
            UnitResult(success=False, error="err2", item_count=3, execution_run_ids=[]),
        ]
        succeeded, failed = aggregate_results(results, FailurePolicy.CONTINUE)
        assert succeeded == 0
        assert failed == 5

    def test_mixed_results_continue(self):
        """Test mixed results with continue policy."""
        results = [
            UnitResult(success=True, error=None, item_count=4, execution_run_ids=[]),
            UnitResult(
                success=False, error="failed", item_count=1, execution_run_ids=[]
            ),
            UnitResult(success=True, error=None, item_count=2, execution_run_ids=[]),
        ]
        succeeded, failed = aggregate_results(results, FailurePolicy.CONTINUE)
        assert succeeded == 6
        assert failed == 1

    def test_fail_fast_raises_on_failure(self):
        """Test fail_fast policy raises on failure."""
        results = [
            UnitResult(success=True, error=None, item_count=1, execution_run_ids=[]),
            UnitResult(
                success=False,
                error="something broke",
                item_count=1,
                execution_run_ids=[],
            ),
        ]
        with pytest.raises(RuntimeError, match="fail_fast policy"):
            aggregate_results(results, FailurePolicy.FAIL_FAST)

    def test_fail_fast_no_failures(self):
        """Test fail_fast policy with all successes."""
        results = [
            UnitResult(success=True, error=None, item_count=5, execution_run_ids=[]),
            UnitResult(success=True, error=None, item_count=5, execution_run_ids=[]),
        ]
        succeeded, failed = aggregate_results(results, FailurePolicy.FAIL_FAST)
        assert succeeded == 10
        assert failed == 0

    def test_empty_results(self):
        """Test with empty results list."""
        succeeded, failed = aggregate_results([], FailurePolicy.CONTINUE)
        assert succeeded == 0
        assert failed == 0
