"""Tests for RunnerResources."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.operation_config.runner_resources import RunnerResources


class TestRunnerResources:
    def test_defaults(self):
        rc = RunnerResources()
        assert rc.cpus == 1
        assert rc.memory_gb == 4
        assert rc.gpus == 0
        assert rc.time_limit == "01:00:00"
        assert rc.extra == {}

    def test_non_default(self):
        rc = RunnerResources(
            cpus=4,
            memory_gb=32,
            gpus=2,
            time_limit="04:00:00",
            extra={"partition": "gpu", "constraint": "fast"},
        )
        assert rc.cpus == 4
        assert rc.memory_gb == 32
        assert rc.gpus == 2
        assert rc.time_limit == "04:00:00"
        assert rc.extra == {"partition": "gpu", "constraint": "fast"}

    def test_cpus_ge_1(self):
        with pytest.raises(ValidationError):
            RunnerResources(cpus=0)

    def test_memory_gb_ge_1(self):
        with pytest.raises(ValidationError):
            RunnerResources(memory_gb=0)

    def test_gpus_ge_0(self):
        with pytest.raises(ValidationError):
            RunnerResources(gpus=-1)

    def test_model_copy_update(self):
        rc = RunnerResources()
        updated = rc.model_copy(update={"memory_gb": 64})
        assert updated.memory_gb == 64
        assert rc.memory_gb == 4  # original unchanged

    def test_round_trip(self):
        rc = RunnerResources(cpus=4, gpus=1, memory_gb=16)
        data = rc.model_dump()
        restored = RunnerResources.model_validate(data)
        assert restored == rc

    def test_extra_isolation(self):
        rc1 = RunnerResources()
        rc2 = RunnerResources()
        rc1.extra["key"] = "value"
        assert rc2.extra == {}
