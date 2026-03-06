"""Tests for ChainBuilder: add creator, reject curator, role compatibility."""

from __future__ import annotations

import pytest

from artisan.orchestration.chain_builder import (
    ChainBuilder,
    _validate_role_compatibility,
)
from artisan.operations.examples.data_transformer import DataTransformer
from artisan.operations.examples.metric_calculator import MetricCalculator
from artisan.operations.examples.data_generator import DataGenerator
from artisan.operations.curator.filter import Filter
from artisan.execution.models.execution_chain import ChainIntermediates


class TestChainBuilderAdd:
    def test_add_creator_succeeds(self) -> None:
        builder = ChainBuilder(pipeline=None)
        result = builder.add(DataTransformer)
        assert result is builder
        assert len(builder._operations) == 1

    def test_add_multiple_creators(self) -> None:
        builder = ChainBuilder(pipeline=None)
        builder.add(DataTransformer).add(MetricCalculator)
        assert len(builder._operations) == 2
        assert len(builder._role_mappings) == 1

    def test_reject_curator(self) -> None:
        builder = ChainBuilder(pipeline=None)
        with pytest.raises(TypeError, match="Curator operations cannot be used"):
            builder.add(Filter)

    def test_empty_chain_raises_on_run(self) -> None:
        builder = ChainBuilder(pipeline=None)
        with pytest.raises(ValueError, match="Cannot execute an empty chain"):
            builder.submit()

    def test_fluent_chaining(self) -> None:
        builder = ChainBuilder(pipeline=None)
        result = builder.add(DataTransformer).add(MetricCalculator)
        assert result is builder


class TestRoleCompatibility:
    def test_identity_mapping_compatible(self) -> None:
        """DataTransformer outputs 'dataset' (data), MetricCalculator inputs 'dataset' (data)."""
        _validate_role_compatibility(
            DataTransformer.outputs,
            MetricCalculator.inputs,
            role_mapping=None,
        )

    def test_explicit_mapping_compatible(self) -> None:
        _validate_role_compatibility(
            DataTransformer.outputs,
            MetricCalculator.inputs,
            role_mapping={"dataset": "dataset"},
        )

    def test_explicit_mapping_unknown_output_role(self) -> None:
        with pytest.raises(TypeError, match="unknown output role 'nonexistent'"):
            _validate_role_compatibility(
                DataTransformer.outputs,
                MetricCalculator.inputs,
                role_mapping={"nonexistent": "dataset"},
            )

    def test_explicit_mapping_unknown_input_role(self) -> None:
        with pytest.raises(TypeError, match="unknown input role 'nonexistent'"):
            _validate_role_compatibility(
                DataTransformer.outputs,
                MetricCalculator.inputs,
                role_mapping={"dataset": "nonexistent"},
            )

    def test_type_mismatch_raises(self) -> None:
        """MetricCalculator outputs 'metrics' (metric), DataTransformer inputs 'dataset' (data)."""
        with pytest.raises(TypeError, match="Type mismatch"):
            _validate_role_compatibility(
                MetricCalculator.outputs,
                DataTransformer.inputs,
                role_mapping={"metrics": "dataset"},
            )

    def test_no_overlap_passes(self) -> None:
        """Identity mapping with no overlapping role names is valid (no checks needed)."""
        _validate_role_compatibility(
            MetricCalculator.outputs,
            DataTransformer.inputs,
            role_mapping=None,
        )


class TestChainBuilderStepName:
    def test_default_name_joins_operations(self) -> None:
        builder = ChainBuilder(pipeline=None)
        builder.add(DataTransformer).add(MetricCalculator)
        # Access the internal name computation (matches submit() logic)
        name = "_chain_".join(
            op_cls.name for op_cls, _, _ in builder._operations
        )
        assert name == "data_transformer_chain_metric_calculator"

    def test_custom_name(self) -> None:
        builder = ChainBuilder(pipeline=None, name="my_chain")
        builder.add(DataTransformer)
        assert builder._name == "my_chain"
