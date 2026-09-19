"""Tests for execution-batch dimensions."""

from __future__ import annotations

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
