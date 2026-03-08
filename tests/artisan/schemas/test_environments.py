"""Tests for Environments model."""

from __future__ import annotations

from pathlib import Path

import pytest

from artisan.schemas.operation_config.environment_spec import (
    ApptainerEnvironmentSpec,
    DockerEnvironmentSpec,
    EnvironmentSpec,
    LocalEnvironmentSpec,
    PixiEnvironmentSpec,
)
from artisan.schemas.operation_config.environments import Environments


class TestEnvironments:
    def test_defaults(self):
        envs = Environments()
        assert envs.active == "local"
        assert isinstance(envs.local, LocalEnvironmentSpec)
        assert envs.docker is None
        assert envs.apptainer is None
        assert envs.pixi is None

    def test_current_returns_active(self):
        envs = Environments()
        current = envs.current()
        assert isinstance(current, LocalEnvironmentSpec)

    def test_current_with_docker(self):
        envs = Environments(
            active="docker",
            docker=DockerEnvironmentSpec(image="img:latest"),
        )
        current = envs.current()
        assert isinstance(current, DockerEnvironmentSpec)
        assert current.image == "img:latest"

    def test_current_unconfigured_raises(self):
        envs = Environments(active="docker")
        with pytest.raises(ValueError, match="not configured"):
            envs.current()

    def test_available_default(self):
        envs = Environments()
        assert envs.available() == ["local"]

    def test_available_multiple(self):
        envs = Environments(
            docker=DockerEnvironmentSpec(image="img:latest"),
            apptainer=ApptainerEnvironmentSpec(image=Path("/img.sif")),
        )
        available = envs.available()
        assert "local" in available
        assert "docker" in available
        assert "apptainer" in available
        assert "pixi" not in available

    def test_model_copy_switch_active(self):
        envs = Environments(
            docker=DockerEnvironmentSpec(image="img:latest"),
        )
        updated = envs.model_copy(update={"active": "docker"})
        assert updated.active == "docker"
        assert envs.active == "local"

    def test_round_trip(self):
        envs = Environments(
            active="docker",
            docker=DockerEnvironmentSpec(image="img:latest", gpu=True),
            pixi=PixiEnvironmentSpec(pixi_environment="ml"),
        )
        data = envs.model_dump()
        restored = Environments.model_validate(data)
        assert restored == envs

    def test_current_returns_correct_subclass(self):
        envs = Environments(
            active="apptainer",
            apptainer=ApptainerEnvironmentSpec(image=Path("/img.sif")),
        )
        current = envs.current()
        assert isinstance(current, ApptainerEnvironmentSpec)
        assert isinstance(current, EnvironmentSpec)
