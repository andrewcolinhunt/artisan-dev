"""Synthetic immutable container targets for environment unit tests."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from artisan.execution.environments.container_runtime import DockerRuntime
from artisan.schemas.execution.container_environment import (
    ContainerLauncherIdentity,
    OCIDerivation,
    PreparedOCIEnvironment,
    PreparedSIFEnvironment,
)
from artisan.schemas.execution.prepared_environment import (
    EnvironmentFile,
    EnvironmentSource,
    PreparedNativeEnvironment,
)


@pytest.fixture
def oci() -> PreparedOCIEnvironment:
    workspace = Path("/opt/artisan/environment")
    native = PreparedNativeEnvironment(
        source=EnvironmentSource(
            manifest_name="pixi.toml",
            name="default",
            files=(
                EnvironmentFile(path="pixi.lock", sha256="a" * 64),
                EnvironmentFile(path="pixi.toml", sha256="b" * 64),
            ),
        ),
        platform="linux-64",
        workspace=workspace,
        prefix=workspace / ".pixi/envs/default",
    )
    return PreparedOCIEnvironment(
        native=native,
        image_id="sha256:" + "d" * 64,
        base_image_id="sha256:" + "e" * 64,
        recipe_sha256="f" * 64,
        pixi_sha256="1" * 64,
        build_runtime=ContainerLauncherIdentity(
            kind="docker",
            version="28.5.1/28.5.1",
            executable_sha256="2" * 64,
            engine_id="synthetic-engine",
        ),
    )


@pytest.fixture
def docker(tmp_path: Path) -> DockerRuntime:
    binary = tmp_path / "docker"
    binary.write_text("#!/bin/sh\nexit 0\n")
    binary.chmod(0o755)
    return DockerRuntime(binary)


@pytest.fixture
def sif(tmp_path: Path, oci: PreparedOCIEnvironment) -> PreparedSIFEnvironment:
    path = tmp_path / "image.sif"
    path.write_bytes(b"synthetic image")
    target = PreparedSIFEnvironment(
        oci=oci,
        sif=path,
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        derivation=OCIDerivation(
            source_image_id=oci.image_id,
            source_config_sha256="3" * 64,
            rootfs_sha256="4" * 64,
            flattened_image_id="sha256:" + "5" * 64,
            archive_sha256="6" * 64,
        ),
        build_runtime=ContainerLauncherIdentity(
            kind="apptainer",
            version="apptainer version 1.5.4",
            executable_sha256="7" * 64,
        ),
    )

    (path.parent / "binding.json").write_text(target.model_dump_json())
    return target
