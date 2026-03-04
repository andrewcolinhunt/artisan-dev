"""Tests for ResourceConfig."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.operation_config.resource_config import ResourceConfig


class TestResourceConfig:
    def test_defaults(self):
        rc = ResourceConfig()
        assert rc.partition == "cpu"
        assert rc.gres is None
        assert rc.cpus_per_task == 1
        assert rc.mem_gb == 4
        assert rc.time_limit == "01:00:00"
        assert rc.extra_slurm_kwargs == {}

    def test_non_default(self):
        rc = ResourceConfig(
            partition="gpu",
            gres="gpu:a4000:1",
            cpus_per_task=4,
            mem_gb=32,
            time_limit="04:00:00",
            extra_slurm_kwargs={"constraint": "fast"},
        )
        assert rc.partition == "gpu"
        assert rc.gres == "gpu:a4000:1"
        assert rc.cpus_per_task == 4
        assert rc.mem_gb == 32
        assert rc.time_limit == "04:00:00"
        assert rc.extra_slurm_kwargs == {"constraint": "fast"}

    def test_cpus_per_task_ge_1(self):
        with pytest.raises(ValidationError):
            ResourceConfig(cpus_per_task=0)

    def test_mem_gb_ge_1(self):
        with pytest.raises(ValidationError):
            ResourceConfig(mem_gb=0)

    def test_model_copy_update(self):
        rc = ResourceConfig()
        updated = rc.model_copy(update={"mem_gb": 64})
        assert updated.mem_gb == 64
        assert rc.mem_gb == 4  # original unchanged

    def test_round_trip(self):
        rc = ResourceConfig(partition="gpu", gres="gpu:1", mem_gb=16)
        data = rc.model_dump()
        restored = ResourceConfig.model_validate(data)
        assert restored == rc

    def test_extra_slurm_kwargs_isolation(self):
        rc1 = ResourceConfig()
        rc2 = ResourceConfig()
        rc1.extra_slurm_kwargs["key"] = "value"
        assert rc2.extra_slurm_kwargs == {}
