"""Tests for CommandSpec hierarchy."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from artisan.schemas.operation_config.command_spec import (
    ApptainerCommandSpec,
    CommandSpec,
    DockerCommandSpec,
    LocalCommandSpec,
    PixiCommandSpec,
)
from artisan.utils.external_tools import ArgStyle


class TestCommandSpec:
    def test_minimal(self):
        cs = CommandSpec(script=Path("/app/run.py"), arg_style=ArgStyle.HYDRA)
        assert cs.script == Path("/app/run.py")
        assert cs.arg_style == ArgStyle.HYDRA
        assert cs.subcommand is None
        assert cs.interpreter is None

    def test_full(self):
        cs = CommandSpec(
            script=Path("/app/run.py"),
            arg_style=ArgStyle.ARGPARSE,
            subcommand="design",
            interpreter="python -u",
        )
        assert cs.subcommand == "design"
        assert cs.interpreter == "python -u"

    def test_script_parts_no_interpreter(self):
        cs = CommandSpec(script=Path("/app/run.py"), arg_style=ArgStyle.HYDRA)
        assert cs.script_parts() == ["/app/run.py"]

    def test_script_parts_with_interpreter(self):
        cs = CommandSpec(
            script=Path("/app/run.py"),
            arg_style=ArgStyle.HYDRA,
            interpreter="python -u",
        )
        assert cs.script_parts() == ["python", "-u", "/app/run.py"]

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            CommandSpec()

    def test_model_copy(self):
        cs = CommandSpec(script=Path("/app/run.py"), arg_style=ArgStyle.HYDRA)
        updated = cs.model_copy(update={"subcommand": "train"})
        assert updated.subcommand == "train"
        assert cs.subcommand is None

    def test_round_trip(self):
        cs = CommandSpec(
            script=Path("/app/run.py"),
            arg_style=ArgStyle.HYDRA,
            subcommand="design",
        )
        data = cs.model_dump()
        restored = CommandSpec.model_validate(data)
        assert restored == cs


class TestApptainerCommandSpec:
    def test_construction(self):
        spec = ApptainerCommandSpec(
            script=Path("/opt/bin/tool_c"),
            arg_style=ArgStyle.HYDRA,
            image=Path("/path/to/container.sif"),
            gpu=True,
        )
        assert spec.image == Path("/path/to/container.sif")
        assert spec.gpu is True
        assert spec.binds == []
        assert spec.env == {}

    def test_isinstance(self):
        spec = ApptainerCommandSpec(
            script=Path("/opt/bin/tool_c"),
            arg_style=ArgStyle.HYDRA,
            image=Path("/path/to/container.sif"),
        )
        assert isinstance(spec, CommandSpec)

    def test_round_trip(self):
        spec = ApptainerCommandSpec(
            script=Path("/opt/bin/tool_c"),
            arg_style=ArgStyle.HYDRA,
            image=Path("/path/to/container.sif"),
            gpu=True,
            binds=[(Path("/data"), Path("/mnt/data"))],
            env={"CUDA": "1"},
        )
        data = spec.model_dump()
        restored = ApptainerCommandSpec.model_validate(data)
        assert restored == spec


class TestDockerCommandSpec:
    def test_construction(self):
        spec = DockerCommandSpec(
            script=Path("/app/run.py"),
            arg_style=ArgStyle.ARGPARSE,
            image=Path("myregistry/model:latest"),
            gpu=True,
        )
        assert spec.image == Path("myregistry/model:latest")
        assert isinstance(spec, CommandSpec)


class TestLocalCommandSpec:
    def test_defaults(self):
        spec = LocalCommandSpec(script=Path("/app/run.py"), arg_style=ArgStyle.ARGPARSE)
        assert spec.venv_path is None
        assert spec.env == {}
        assert isinstance(spec, CommandSpec)

    def test_with_venv(self):
        spec = LocalCommandSpec(
            script=Path("/app/run.py"),
            arg_style=ArgStyle.ARGPARSE,
            venv_path=Path("/envs/.venv"),
        )
        assert spec.venv_path == Path("/envs/.venv")


class TestPixiCommandSpec:
    def test_defaults(self):
        spec = PixiCommandSpec(script=Path("/app/run.py"), arg_style=ArgStyle.HYDRA)
        assert spec.environment == "default"
        assert spec.manifest_path is None
        assert isinstance(spec, CommandSpec)

    def test_non_default(self):
        spec = PixiCommandSpec(
            script=Path("/app/run.py"),
            arg_style=ArgStyle.HYDRA,
            environment="ml",
            manifest_path=Path("/project/pixi.toml"),
        )
        assert spec.environment == "ml"
        assert spec.manifest_path == Path("/project/pixi.toml")


class TestCommandSpecDispatch:
    def test_match_apptainer(self):
        spec = ApptainerCommandSpec(
            script=Path("/run.py"),
            arg_style=ArgStyle.HYDRA,
            image=Path("/img.sif"),
        )
        assert self._dispatch(spec) == "apptainer"

    def test_match_docker(self):
        spec = DockerCommandSpec(
            script=Path("/run.py"),
            arg_style=ArgStyle.HYDRA,
            image=Path("img:latest"),
        )
        assert self._dispatch(spec) == "docker"

    def test_match_local(self):
        spec = LocalCommandSpec(script=Path("/run.py"), arg_style=ArgStyle.HYDRA)
        assert self._dispatch(spec) == "local"

    def test_match_pixi(self):
        spec = PixiCommandSpec(script=Path("/run.py"), arg_style=ArgStyle.HYDRA)
        assert self._dispatch(spec) == "pixi"

    @staticmethod
    def _dispatch(spec: CommandSpec) -> str:
        match spec:
            case ApptainerCommandSpec():
                return "apptainer"
            case DockerCommandSpec():
                return "docker"
            case LocalCommandSpec():
                return "local"
            case PixiCommandSpec():
                return "pixi"
        return "unknown"
