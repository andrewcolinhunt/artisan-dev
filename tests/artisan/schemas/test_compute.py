"""Tests for Compute configuration model."""

from __future__ import annotations

import pytest

from artisan.schemas.operation_config.compute import (
    Compute,
    ComputeConfig,
    LocalComputeConfig,
)


class TestCompute:
    def test_defaults(self):
        compute = Compute()
        assert compute.active == "local"
        assert isinstance(compute.local, LocalComputeConfig)

    def test_current_returns_active(self):
        compute = Compute()
        current = compute.current()
        assert isinstance(current, LocalComputeConfig)

    def test_current_unconfigured_raises(self):
        compute = Compute(active="modal")
        with pytest.raises(ValueError, match="not configured"):
            compute.current()

    def test_available_default(self):
        compute = Compute()
        assert compute.available() == ["local"]

    def test_model_copy_switch_active(self):
        compute = Compute()
        updated = compute.model_copy(update={"active": "modal"})
        assert updated.active == "modal"
        assert compute.active == "local"

    def test_round_trip(self):
        compute = Compute()
        data = compute.model_dump()
        restored = Compute.model_validate(data)
        assert restored == compute

    def test_current_returns_correct_base_type(self):
        compute = Compute()
        current = compute.current()
        assert isinstance(current, ComputeConfig)
        assert isinstance(current, LocalComputeConfig)
