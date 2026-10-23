"""Tests for the ``ComputeResources`` schema (operation_config)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.operation_config.compute_resources import ComputeResources


class TestFieldDefaults:
    """All fields default to None (defer to provider)."""

    def test_all_defaults_none(self):
        cr = ComputeResources()
        assert cr.gpu is None
        assert cr.cpu is None
        assert cr.memory_gb is None
        assert cr.timeout is None


class TestGpuField:
    """``gpu`` accepts arbitrary strings or None."""

    @pytest.mark.parametrize("gpu", ["A10G", "A100", "H100"])
    def test_valid_gpu_strings(self, gpu):
        cr = ComputeResources(gpu=gpu)
        assert cr.gpu == gpu

    def test_gpu_none(self):
        cr = ComputeResources(gpu=None)
        assert cr.gpu is None

    def test_gpu_wrong_type_rejected(self):
        with pytest.raises(ValidationError):
            ComputeResources(gpu=123)


class TestCpuField:
    """``cpu`` is a fractional float > 0 or None."""

    @pytest.mark.parametrize("cpu", [0.125, 0.5, 1.0, 2.5, 8.0])
    def test_valid_cpu(self, cpu):
        cr = ComputeResources(cpu=cpu)
        assert cr.cpu == cpu

    def test_cpu_none(self):
        cr = ComputeResources(cpu=None)
        assert cr.cpu is None

    def test_cpu_zero_rejected(self):
        """gt=0 constraint rejects 0."""
        with pytest.raises(ValidationError):
            ComputeResources(cpu=0)

    def test_cpu_negative_rejected(self):
        with pytest.raises(ValidationError):
            ComputeResources(cpu=-0.5)

    def test_cpu_wrong_type_rejected(self):
        with pytest.raises(ValidationError):
            ComputeResources(cpu="two")


class TestMemoryGbField:
    """``memory_gb`` is an int >= 1 or None."""

    @pytest.mark.parametrize("memory", [1, 8, 16, 64, 1024])
    def test_valid_memory(self, memory):
        cr = ComputeResources(memory_gb=memory)
        assert cr.memory_gb == memory

    def test_memory_none(self):
        cr = ComputeResources(memory_gb=None)
        assert cr.memory_gb is None

    def test_memory_zero_rejected(self):
        """ge=1 constraint rejects 0."""
        with pytest.raises(ValidationError):
            ComputeResources(memory_gb=0)

    def test_memory_negative_rejected(self):
        with pytest.raises(ValidationError):
            ComputeResources(memory_gb=-4)


class TestTimeoutField:
    """``timeout`` is an int >= 1 or None."""

    @pytest.mark.parametrize("timeout", [1, 60, 3600, 7200])
    def test_valid_timeout(self, timeout):
        cr = ComputeResources(timeout=timeout)
        assert cr.timeout == timeout

    def test_timeout_none(self):
        cr = ComputeResources(timeout=None)
        assert cr.timeout is None

    def test_timeout_zero_rejected(self):
        with pytest.raises(ValidationError):
            ComputeResources(timeout=0)

    def test_timeout_negative_rejected(self):
        with pytest.raises(ValidationError):
            ComputeResources(timeout=-30)


class TestUnknownKeys:
    """Pydantic's default is to reject unknown keys for this BaseModel."""

    def test_unknown_field_rejected(self):
        with pytest.raises(ValidationError):
            ComputeResources(unknown_field="x")

    def test_unknown_field_with_known_field_rejected(self):
        with pytest.raises(ValidationError):
            ComputeResources(gpu="A100", bogus=42)


class TestRoundTrip:
    """``model_dump`` ↔ ``model_validate`` is lossless."""

    def test_round_trip_all_fields_set(self):
        original = ComputeResources(gpu="A100", cpu=2.5, memory_gb=32, timeout=3600)
        dumped = original.model_dump()
        restored = ComputeResources.model_validate(dumped)
        assert restored == original

    def test_round_trip_all_defaults(self):
        original = ComputeResources()
        dumped = original.model_dump()
        restored = ComputeResources.model_validate(dumped)
        assert restored == original

    def test_round_trip_partial(self):
        original = ComputeResources(memory_gb=16)
        dumped = original.model_dump()
        restored = ComputeResources.model_validate(dumped)
        assert restored == original
        assert restored.gpu is None
        assert restored.timeout is None

    def test_round_trip_json_mode(self):
        """``mode='json'`` round-trip — relevant for hash payload."""
        original = ComputeResources(gpu="H100", memory_gb=64, timeout=1800)
        dumped = original.model_dump(mode="json")
        restored = ComputeResources.model_validate(dumped)
        assert restored == original
