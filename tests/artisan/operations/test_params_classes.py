"""Tests for framework operation Params classes.

Validates that each Params class:
- Can be constructed with defaults (where all fields have defaults)
- Validates required fields
- Round-trips through model_dump()
- Enforces validation constraints (ge, gt, etc.)
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.operations.curator.filter import Filter
from artisan.operations.examples.data_generator import DataGenerator
from artisan.operations.examples.data_transformer import DataTransformer
from artisan.operations.examples.data_transformer_config import DataTransformerConfig

# ---------------------------------------------------------------------------
# FilterParams
# ---------------------------------------------------------------------------


class TestFilterParams:
    """Tests for FilterParams."""

    def test_default_construction(self) -> None:
        params = Filter.Params()
        assert params.criteria == []

    def test_custom_criteria(self) -> None:
        from artisan.operations.curator.filter import Criterion

        params = Filter.Params(
            criteria=[
                Criterion(metric="quality.score", operator="gt", value=0.8),
            ]
        )
        assert len(params.criteria) == 1
        assert params.criteria[0].metric == "quality.score"
        assert params.criteria[0].operator == "gt"
        assert params.criteria[0].value == 0.8

    def test_model_dump_round_trip(self) -> None:
        from artisan.operations.curator.filter import Criterion

        params = Filter.Params(
            criteria=[
                Criterion(metric="quality.confidence", operator="ge", value=0.7),
            ]
        )
        data = params.model_dump()
        restored = Filter.Params(**data)
        assert restored == params


# ---------------------------------------------------------------------------
# DataGenerator.Params
# ---------------------------------------------------------------------------


class TestDataGeneratorParams:
    """Tests for DataGenerator.Params."""

    def test_default_construction(self) -> None:
        params = DataGenerator.Params()
        assert params.count == 1
        assert params.rows_per_file == 10
        assert params.seed is None

    def test_model_dump_round_trip(self) -> None:
        params = DataGenerator.Params(count=10, seed=42)
        data = params.model_dump()
        restored = DataGenerator.Params(**data)
        assert restored == params

    def test_count_ge_1(self) -> None:
        with pytest.raises(ValidationError):
            DataGenerator.Params(count=0)


# ---------------------------------------------------------------------------
# DataTransformer.Params
# ---------------------------------------------------------------------------


class TestDataTransformerParams:
    """Tests for DataTransformer.Params."""

    def test_default_construction(self) -> None:
        params = DataTransformer.Params()
        assert params.scale_factor == 1.5
        assert params.noise_amplitude == 0.1
        assert params.variants == 1
        assert params.seed is None

    def test_model_dump_round_trip(self) -> None:
        params = DataTransformer.Params(scale_factor=2.5, variants=3, seed=7)
        data = params.model_dump()
        restored = DataTransformer.Params(**data)
        assert restored == params

    def test_scale_factor_ge_0(self) -> None:
        params = DataTransformer.Params(scale_factor=0.0)
        assert params.scale_factor == 0.0

    def test_scale_factor_negative(self) -> None:
        with pytest.raises(ValidationError):
            DataTransformer.Params(scale_factor=-0.1)

    def test_variants_ge_1(self) -> None:
        with pytest.raises(ValidationError):
            DataTransformer.Params(variants=0)


# ---------------------------------------------------------------------------
# DataTransformerConfig.Params
# ---------------------------------------------------------------------------


class TestDataTransformerConfigParams:
    """Tests for DataTransformerConfig.Params."""

    def test_default_construction(self) -> None:
        params = DataTransformerConfig.Params()
        assert params.scale_factors == [1.0]
        assert params.noise_amplitudes == [0.0]
        assert params.seed is None

    def test_custom_values(self) -> None:
        params = DataTransformerConfig.Params(
            scale_factors=[0.5, 1.0, 2.0],
            noise_amplitudes=[0.0, 0.1],
            seed=42,
        )
        assert params.scale_factors == [0.5, 1.0, 2.0]
        assert params.noise_amplitudes == [0.0, 0.1]
        assert params.seed == 42

    def test_model_dump_round_trip(self) -> None:
        params = DataTransformerConfig.Params(
            scale_factors=[0.1, 0.2],
            noise_amplitudes=[0.0, 0.5],
            seed=99,
        )
        data = params.model_dump()
        restored = DataTransformerConfig.Params(**data)
        assert restored == params

    def test_independent_defaults(self) -> None:
        """Default lists are independent between instances."""
        p1 = DataTransformerConfig.Params()
        p2 = DataTransformerConfig.Params()
        p1.scale_factors.append(99.0)
        assert 99.0 not in p2.scale_factors
