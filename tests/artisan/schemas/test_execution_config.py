"""Tests for ExecutionConfig."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.execution.execution_config import ExecutionConfig


class TestExecutionConfig:
    def test_defaults(self):
        ec = ExecutionConfig()
        assert ec.artifacts_per_unit == 1
        assert ec.units_per_worker == 1
        assert ec.max_workers is None
        assert ec.estimated_seconds is None
        assert ec.job_name is None

    def test_non_default(self):
        ec = ExecutionConfig(
            artifacts_per_unit=8,
            units_per_worker=10,
            max_workers=4,
            estimated_seconds=300.0,
            job_name="tool_c",
        )
        assert ec.artifacts_per_unit == 8
        assert ec.units_per_worker == 10
        assert ec.max_workers == 4
        assert ec.estimated_seconds == 300.0
        assert ec.job_name == "tool_c"

    def test_artifacts_per_unit_ge_1(self):
        with pytest.raises(ValidationError):
            ExecutionConfig(artifacts_per_unit=0)

    def test_units_per_worker_ge_1(self):
        with pytest.raises(ValidationError):
            ExecutionConfig(units_per_worker=0)

    def test_model_copy_update(self):
        ec = ExecutionConfig()
        updated = ec.model_copy(update={"job_name": "tool_b"})
        assert updated.job_name == "tool_b"
        assert ec.job_name is None

    def test_round_trip(self):
        ec = ExecutionConfig(artifacts_per_unit=5, job_name="test")
        data = ec.model_dump()
        restored = ExecutionConfig.model_validate(data)
        assert restored == ec

    def test_negative_values_rejected(self):
        with pytest.raises(ValidationError):
            ExecutionConfig(artifacts_per_unit=-1)
