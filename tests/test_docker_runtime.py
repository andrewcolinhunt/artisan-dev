"""Docker qualification preparation, without contacting an external service."""

from __future__ import annotations

import json
import os
import subprocess
from typing import Any

import pytest
from fixtures import docker_runtime

_IMAGE_ID = "sha256:" + "a" * 64
_ENDPOINT = "unix:///selected-engine.sock"


class FakeDocker:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], dict[str, Any]]] = []
        self.cached = True
        self.server: Any = {"Os": "linux", "Arch": "amd64"}
        self.image: Any = {
            "Id": _IMAGE_ID,
            "Os": "linux",
            "Architecture": "amd64",
            "Config": {"WorkingDir": "/image-default"},
        }
        self.failure: tuple[str, str] | None = None
        self.timeout: str | None = None

    @staticmethod
    def command(arguments: list[str]) -> list[str]:
        return arguments[3:] if arguments[1:2] == ["--host"] else arguments[1:]

    @property
    def commands(self) -> list[list[str]]:
        return [self.command(arguments) for arguments, _ in self.calls]

    def run(self, arguments: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
        self.calls.append((arguments, kwargs))
        command = self.command(arguments)
        kind = command[0]
        if kind == self.timeout:
            raise subprocess.TimeoutExpired(arguments, kwargs["timeout"])
        if self.failure is not None and kind == self.failure[0]:
            raise subprocess.CalledProcessError(1, arguments, stderr=self.failure[1])
        if kind == "context":
            output = json.dumps({"Host": _ENDPOINT})
        elif kind == "version":
            output = json.dumps(self.server)
        elif kind == "image":
            if not self.cached:
                raise subprocess.CalledProcessError(
                    1, arguments, stderr=f"Error: No such image: {command[-1]}"
                )
            output = json.dumps([self.image])
        elif kind == "pull":
            output = "pulled"
        elif kind == "build":
            assert kwargs["input"]
            self.cached = True
            output = "built"
        else:
            message = f"Unexpected Docker command: {arguments}"
            raise AssertionError(message)
        return subprocess.CompletedProcess(arguments, 0, stdout=output, stderr="")


@pytest.fixture
def fake_docker(monkeypatch: pytest.MonkeyPatch) -> FakeDocker:
    for name in (
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_DEFAULT_PLATFORM",
        "ARTISAN_DOCKER_CWD_TEST_IMAGE",
    ):
        monkeypatch.delenv(name, raising=False)
    docker = FakeDocker()
    monkeypatch.setattr(docker_runtime.shutil, "which", lambda command: "/fake/docker")
    monkeypatch.setattr(docker_runtime.subprocess, "run", docker.run)
    return docker


def test_cached_default_image_requires_no_pull_or_build(
    fake_docker: FakeDocker,
) -> None:
    runtime = docker_runtime.prepare_docker_runtime()

    assert runtime == docker_runtime.DockerRuntime(_ENDPOINT, _IMAGE_ID)
    assert [command[0] for command in fake_docker.commands] == [
        "context",
        "version",
        "image",
    ]
    assert fake_docker.commands[-1][-1].startswith("artisan-docker-test:")
    for arguments, kwargs in fake_docker.calls:
        assert kwargs["timeout"] > 0
        assert kwargs["check"] is True
        if FakeDocker.command(arguments)[0] != "context":
            assert arguments[:3] == ["docker", "--host", _ENDPOINT]
            assert "DOCKER_HOST" not in kwargs["env"]
            assert "DOCKER_CONTEXT" not in kwargs["env"]


def test_default_cache_miss_builds_pinned_recipe_once(fake_docker: FakeDocker) -> None:
    fake_docker.cached = False

    assert docker_runtime.prepare_docker_runtime().image_id == _IMAGE_ID
    assert docker_runtime.prepare_docker_runtime().image_id == _IMAGE_ID

    pulls = [command for command in fake_docker.commands if command[0] == "pull"]
    builds = [
        (FakeDocker.command(arguments), options)
        for arguments, options in fake_docker.calls
        if FakeDocker.command(arguments)[0] == "build"
    ]
    assert pulls == [["pull", "--platform", "linux/amd64", docker_runtime.BASE_IMAGE]]
    assert "@sha256:" in docker_runtime.BASE_IMAGE
    assert len(builds) == 1
    command, options = builds[0]
    assert command[1:3] == ["--platform", "linux/amd64"]
    assert "--network=none" in command
    assert "--pull=false" in command
    assert command[-1] == "-"
    recipe = options["input"]
    assert f"FROM {docker_runtime.BASE_IMAGE}\n" in recipe
    assert "WORKDIR /image-default\n" in recipe
    cache_reference = command[command.index("--tag") + 1]
    inspections = [
        command[-1] for command in fake_docker.commands if command[0] == "image"
    ]
    assert inspections == [cache_reference] * 3


def test_default_cache_is_separate_for_engine_architectures(
    fake_docker: FakeDocker,
) -> None:
    docker_runtime.prepare_docker_runtime()
    fake_docker.server["Arch"] = "arm64"
    fake_docker.image["Architecture"] = "arm64"
    docker_runtime.prepare_docker_runtime()

    references = [
        command[-1] for command in fake_docker.commands if command[0] == "image"
    ]
    assert len(set(references)) == 2


@pytest.mark.parametrize("context", [None, "selected-context"])
@pytest.mark.parametrize("host", [None, "unix:///environment-engine.sock"])
def test_context_precedence_and_endpoint_pinning(
    fake_docker: FakeDocker,
    monkeypatch: pytest.MonkeyPatch,
    context: str | None,
    host: str | None,
) -> None:
    if context is not None:
        monkeypatch.setenv("DOCKER_CONTEXT", context)
    if host is not None:
        monkeypatch.setenv("DOCKER_HOST", host)
    original_environment = dict(os.environ)

    runtime = docker_runtime.prepare_docker_runtime()

    assert dict(os.environ) == original_environment
    assert runtime.endpoint == (host if host and not context else _ENDPOINT)
    contexts = [command for command in fake_docker.commands if command[0] == "context"]
    if host and not context:
        assert not contexts
    else:
        assert len(contexts) == 1
        assert contexts[0][2:-2] == ([context] if context else [])
    for arguments, kwargs in fake_docker.calls:
        if FakeDocker.command(arguments)[0] != "context":
            assert arguments[1:3] == ["--host", runtime.endpoint]
            assert "DOCKER_CONTEXT" not in kwargs["env"]
            assert "DOCKER_HOST" not in kwargs["env"]


def test_explicit_image_is_inspected_and_pinned_without_rebuilding(
    fake_docker: FakeDocker, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ARTISAN_DOCKER_CWD_TEST_IMAGE", "local-custom:latest")

    runtime = docker_runtime.prepare_docker_runtime()

    assert runtime.image_id == _IMAGE_ID
    assert fake_docker.commands[-1] == ["image", "inspect", "local-custom:latest"]
    assert not any(command[0] in {"pull", "build"} for command in fake_docker.commands)


def test_missing_override_does_not_fall_back_to_default_image(
    fake_docker: FakeDocker, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ARTISAN_DOCKER_CWD_TEST_IMAGE", "missing-override")
    fake_docker.cached = False

    with pytest.raises(docker_runtime.DockerPreparationError, match="missing-override"):
        docker_runtime.prepare_docker_runtime()

    assert not any(command[0] in {"pull", "build"} for command in fake_docker.commands)


@pytest.mark.parametrize(
    "invalid_metadata",
    [
        {"Id": "mutable:tag"},
        {"Os": "windows"},
        {"Config": {"WorkingDir": ""}},
        {"Config": None},
    ],
)
def test_invalid_image_metadata_fails_without_replacing_image(
    fake_docker: FakeDocker, invalid_metadata: dict[str, Any]
) -> None:
    fake_docker.image.update(invalid_metadata)

    with pytest.raises(docker_runtime.DockerPreparationError):
        docker_runtime.prepare_docker_runtime()

    assert not any(command[0] in {"pull", "build"} for command in fake_docker.commands)


def test_missing_cli_fails_without_running_commands(
    fake_docker: FakeDocker, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(docker_runtime.shutil, "which", lambda command: None)

    with pytest.raises(docker_runtime.DockerUnavailable, match="Docker CLI"):
        docker_runtime.prepare_docker_runtime()

    assert not fake_docker.calls


def test_remote_engine_is_rejected_before_image_preparation(
    fake_docker: FakeDocker, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DOCKER_HOST", "tcp://remote-engine:2375")

    with pytest.raises(docker_runtime.DockerUnavailable, match="local Unix endpoint"):
        docker_runtime.prepare_docker_runtime()

    assert not fake_docker.calls


@pytest.mark.parametrize(
    "server", [None, {"Os": "windows", "Arch": "amd64"}, {"Os": "linux"}]
)
def test_invalid_engine_metadata_prevents_image_preparation(
    fake_docker: FakeDocker, server: Any
) -> None:
    fake_docker.server = server

    with pytest.raises(docker_runtime.DockerUnavailable):
        docker_runtime.prepare_docker_runtime()

    assert not any(command[0] == "image" for command in fake_docker.commands)


@pytest.mark.parametrize("phase", ["context", "version", "image", "pull", "build"])
def test_timeout_is_actionable_and_never_silently_skipped(
    fake_docker: FakeDocker, phase: str
) -> None:
    fake_docker.timeout = phase
    fake_docker.cached = phase not in {"pull", "build"}
    exception = (
        docker_runtime.DockerUnavailable
        if phase in {"context", "version"}
        else docker_runtime.DockerPreparationError
    )

    with pytest.raises(exception, match="timed out"):
        docker_runtime.prepare_docker_runtime()


@pytest.mark.parametrize("phase", ["version", "image", "pull", "build"])
def test_command_failure_reports_cause_without_retrying_unrelated_work(
    fake_docker: FakeDocker, phase: str
) -> None:
    fake_docker.failure = (phase, "sentinel permission denied")
    fake_docker.cached = phase not in {"pull", "build"}
    exception = (
        docker_runtime.DockerUnavailable
        if phase == "version"
        else docker_runtime.DockerPreparationError
    )

    with pytest.raises(exception, match="sentinel permission denied"):
        docker_runtime.prepare_docker_runtime()

    assert fake_docker.commands[-1][0] == phase
    if phase == "image":
        assert not any(
            command[0] in {"pull", "build"} for command in fake_docker.commands
        )


@pytest.mark.parametrize("cached", [False, True])
def test_inherited_builder_cannot_redirect_selected_engine(
    fake_docker: FakeDocker, monkeypatch: pytest.MonkeyPatch, cached: bool
) -> None:
    monkeypatch.setenv("BUILDX_BUILDER", "unrelated-remote-builder")
    fake_docker.cached = cached

    assert docker_runtime.prepare_docker_runtime().endpoint == _ENDPOINT

    assert os.environ["BUILDX_BUILDER"] == "unrelated-remote-builder"
    assert all("BUILDX_BUILDER" not in kwargs["env"] for _, kwargs in fake_docker.calls)
