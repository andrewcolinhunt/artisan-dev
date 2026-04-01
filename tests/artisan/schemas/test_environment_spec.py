"""Tests for EnvironmentSpec hierarchy."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from artisan.schemas.operation_config.environment_spec import (
    ApptainerEnvironmentSpec,
    DockerEnvironmentSpec,
    EnvironmentSpec,
    LocalEnvironmentSpec,
    PixiEnvironmentSpec,
    _find_free_port,
)


class TestEnvironmentSpec:
    def test_default_env(self):
        spec = EnvironmentSpec()
        assert spec.env == {}

    def test_wrap_command_passthrough(self):
        spec = EnvironmentSpec()
        cmd = ["samtools", "sort", "in.bam"]
        assert spec.wrap_command(cmd) == cmd

    def test_prepare_env_none_when_empty(self):
        spec = EnvironmentSpec()
        assert spec.prepare_env() is None

    def test_prepare_env_merges(self):
        spec = EnvironmentSpec(env={"FOO": "bar"})
        env = spec.prepare_env()
        assert env is not None
        assert env["FOO"] == "bar"

    def test_validate_environment_noop(self):
        spec = EnvironmentSpec()
        spec.validate_environment()  # should not raise


class TestLocalEnvironmentSpec:
    def test_defaults(self):
        spec = LocalEnvironmentSpec()
        assert spec.venv_path is None
        assert spec.env == {}

    def test_prepare_env_no_venv(self):
        spec = LocalEnvironmentSpec()
        assert spec.prepare_env() is None

    def test_prepare_env_with_venv(self):
        spec = LocalEnvironmentSpec(venv_path=Path("/envs/.venv"))
        env = spec.prepare_env()
        assert env is not None
        assert env["VIRTUAL_ENV"] == "/envs/.venv"
        assert str(Path("/envs/.venv/bin")) in env["PATH"]

    def test_prepare_env_venv_plus_extra(self):
        spec = LocalEnvironmentSpec(
            venv_path=Path("/envs/.venv"),
            env={"CUDA_VISIBLE_DEVICES": "0"},
        )
        env = spec.prepare_env()
        assert env["CUDA_VISIBLE_DEVICES"] == "0"
        assert env["VIRTUAL_ENV"] == "/envs/.venv"

    def test_isinstance(self):
        spec = LocalEnvironmentSpec()
        assert isinstance(spec, EnvironmentSpec)


class TestDockerEnvironmentSpec:
    def test_construction(self):
        spec = DockerEnvironmentSpec(image="samtools:1.17")
        assert spec.image == "samtools:1.17"
        assert spec.gpu is False
        assert spec.binds == []

    def test_wrap_command_minimal(self):
        spec = DockerEnvironmentSpec(image="samtools:1.17")
        result = spec.wrap_command(["samtools", "sort"])
        assert result[:3] == ["docker", "run", "--rm"]
        assert "samtools:1.17" in result
        assert result[-2:] == ["samtools", "sort"]

    def test_wrap_command_with_gpu(self):
        spec = DockerEnvironmentSpec(image="img:latest", gpu=True)
        result = spec.wrap_command(["cmd"])
        assert "--gpus" in result
        assert "all" in result

    def test_wrap_command_with_cwd(self):
        spec = DockerEnvironmentSpec(image="img:latest")
        result = spec.wrap_command(["cmd"], cwd=Path("/data/sandbox/step0"))
        assert "--volume" in result
        assert "/data/sandbox:/data/sandbox" in result

    def test_wrap_command_with_binds(self):
        spec = DockerEnvironmentSpec(
            image="img:latest",
            binds=[(Path("/host/data"), Path("/container/data"))],
        )
        result = spec.wrap_command(["cmd"])
        assert "/host/data:/container/data" in result

    def test_wrap_command_with_env(self):
        spec = DockerEnvironmentSpec(image="img:latest", env={"KEY": "val"})
        result = spec.wrap_command(["cmd"])
        assert "--env" in result
        assert "KEY=val" in result

    @patch("shutil.which", return_value=None)
    def test_validate_not_installed(self, mock_which):
        spec = DockerEnvironmentSpec(image="img:latest")
        with pytest.raises(FileNotFoundError, match="Docker"):
            spec.validate_environment()

    def test_isinstance(self):
        spec = DockerEnvironmentSpec(image="img:latest")
        assert isinstance(spec, EnvironmentSpec)

    def test_round_trip(self):
        spec = DockerEnvironmentSpec(
            image="img:latest",
            gpu=True,
            binds=[(Path("/a"), Path("/b"))],
            env={"K": "V"},
        )
        data = spec.model_dump()
        restored = DockerEnvironmentSpec.model_validate(data)
        assert restored == spec


class TestApptainerEnvironmentSpec:
    def test_construction(self):
        spec = ApptainerEnvironmentSpec(image=Path("/img.sif"))
        assert spec.image == Path("/img.sif")
        assert spec.gpu is False

    def test_wrap_command_minimal(self):
        spec = ApptainerEnvironmentSpec(image=Path("/img.sif"))
        result = spec.wrap_command(["samtools", "sort"])
        assert result[:2] == ["apptainer", "exec"]
        assert "/img.sif" in result
        assert result[-2:] == ["samtools", "sort"]

    def test_wrap_command_with_gpu(self):
        spec = ApptainerEnvironmentSpec(image=Path("/img.sif"), gpu=True)
        result = spec.wrap_command(["cmd"])
        assert "--nv" in result

    def test_wrap_command_with_cwd(self):
        spec = ApptainerEnvironmentSpec(image=Path("/img.sif"))
        result = spec.wrap_command(["cmd"], cwd=Path("/data/sandbox/step0"))
        assert "--bind" in result
        assert "/data/sandbox:/data/sandbox" in result

    def test_wrap_command_with_binds_and_env(self):
        spec = ApptainerEnvironmentSpec(
            image=Path("/img.sif"),
            binds=[(Path("/host"), Path("/container"))],
            env={"KEY": "val"},
        )
        result = spec.wrap_command(["cmd"])
        assert "--bind" in result
        assert "--env" in result

    @patch("shutil.which", return_value=None)
    def test_validate_not_installed(self, mock_which):
        spec = ApptainerEnvironmentSpec(image=Path("/img.sif"))
        with pytest.raises(FileNotFoundError, match="Apptainer"):
            spec.validate_environment()

    @patch("shutil.which", return_value="/usr/bin/apptainer")
    def test_validate_missing_image(self, mock_which):
        spec = ApptainerEnvironmentSpec(image=Path("/nonexistent.sif"))
        with pytest.raises(FileNotFoundError, match="Container image not found"):
            spec.validate_environment()

    def test_isinstance(self):
        spec = ApptainerEnvironmentSpec(image=Path("/img.sif"))
        assert isinstance(spec, EnvironmentSpec)


class TestPixiEnvironmentSpec:
    def test_defaults(self):
        spec = PixiEnvironmentSpec()
        assert spec.pixi_environment == "default"
        assert spec.manifest_path is None

    def test_wrap_command(self):
        spec = PixiEnvironmentSpec(pixi_environment="ml")
        result = spec.wrap_command(["python", "train.py"])
        assert result[:4] == ["pixi", "run", "-e", "ml"]
        assert result[4:] == ["python", "train.py"]

    def test_wrap_command_with_manifest(self):
        spec = PixiEnvironmentSpec(manifest_path=Path("/project/pixi.toml"))
        result = spec.wrap_command(["cmd"])
        assert "--manifest-path" in result
        assert "/project/pixi.toml" in result

    @patch("shutil.which", return_value=None)
    def test_validate_not_installed(self, mock_which):
        spec = PixiEnvironmentSpec()
        with pytest.raises(FileNotFoundError, match="Pixi"):
            spec.validate_environment()

    def test_isinstance(self):
        spec = PixiEnvironmentSpec()
        assert isinstance(spec, EnvironmentSpec)


class TestFindFreePort:
    def test_returns_positive_int(self):
        port = _find_free_port()
        assert isinstance(port, int)
        assert port > 0

    def test_consecutive_calls_return_different_ports(self):
        ports = {_find_free_port() for _ in range(10)}
        assert len(ports) > 1


class TestMasterPortInjection:
    """MASTER_PORT/MASTER_ADDR auto-injection for GPU container specs."""

    def test_apptainer_gpu_injects_master_port(self):
        spec = ApptainerEnvironmentSpec(image=Path("/img.sif"), gpu=True)
        result = spec.wrap_command(["cmd"])
        env_pairs = {result[i + 1] for i, v in enumerate(result) if v == "--env"}
        master_ports = [p for p in env_pairs if p.startswith("MASTER_PORT=")]
        master_addrs = [p for p in env_pairs if p.startswith("MASTER_ADDR=")]
        assert len(master_ports) == 1
        assert int(master_ports[0].split("=")[1]) > 0
        assert master_addrs == ["MASTER_ADDR=127.0.0.1"]

    def test_apptainer_gpu_respects_explicit_master_port(self):
        spec = ApptainerEnvironmentSpec(
            image=Path("/img.sif"), gpu=True, env={"MASTER_PORT": "12345"}
        )
        result = spec.wrap_command(["cmd"])
        env_pairs = [result[i + 1] for i, v in enumerate(result) if v == "--env"]
        master_ports = [p for p in env_pairs if p.startswith("MASTER_PORT=")]
        assert master_ports == ["MASTER_PORT=12345"]

    def test_apptainer_no_gpu_no_injection(self):
        spec = ApptainerEnvironmentSpec(image=Path("/img.sif"), gpu=False)
        result = spec.wrap_command(["cmd"])
        joined = " ".join(result)
        assert "MASTER_PORT" not in joined
        assert "MASTER_ADDR" not in joined

    def test_docker_gpu_injects_master_port(self):
        spec = DockerEnvironmentSpec(image="img:latest", gpu=True)
        result = spec.wrap_command(["cmd"])
        env_pairs = {result[i + 1] for i, v in enumerate(result) if v == "--env"}
        master_ports = [p for p in env_pairs if p.startswith("MASTER_PORT=")]
        master_addrs = [p for p in env_pairs if p.startswith("MASTER_ADDR=")]
        assert len(master_ports) == 1
        assert int(master_ports[0].split("=")[1]) > 0
        assert master_addrs == ["MASTER_ADDR=127.0.0.1"]

    def test_docker_gpu_respects_explicit_master_port(self):
        spec = DockerEnvironmentSpec(
            image="img:latest", gpu=True, env={"MASTER_PORT": "54321"}
        )
        result = spec.wrap_command(["cmd"])
        env_pairs = [result[i + 1] for i, v in enumerate(result) if v == "--env"]
        master_ports = [p for p in env_pairs if p.startswith("MASTER_PORT=")]
        assert master_ports == ["MASTER_PORT=54321"]

    def test_docker_no_gpu_no_injection(self):
        spec = DockerEnvironmentSpec(image="img:latest", gpu=False)
        result = spec.wrap_command(["cmd"])
        joined = " ".join(result)
        assert "MASTER_PORT" not in joined
        assert "MASTER_ADDR" not in joined
