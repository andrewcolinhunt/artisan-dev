"""Prepare the local Docker engine and pinned image used by integration tests."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BASE_IMAGE = (
    "docker.io/library/alpine@"
    "sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8"
)
IMAGE_WORKDIR = "/image-default"
DOCKERFILE = f"FROM {BASE_IMAGE}\nWORKDIR {IMAGE_WORKDIR}\n"
_PROBE_TIMEOUT = 10
_PREPARE_TIMEOUT = 180


@dataclass(frozen=True)
class DockerRuntime:
    """The selected local engine and immutable test image identity."""

    endpoint: str
    image_id: str


class DockerUnavailable(RuntimeError):
    """The selected Docker CLI or local Linux engine is unavailable."""


class DockerPreparationError(RuntimeError):
    """The selected engine could not provide a valid test image."""


def _run_docker(
    arguments: list[str],
    environment: dict[str, str],
    *,
    input_text: str | None = None,
    timeout: int = _PROBE_TIMEOUT,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *arguments],
        env=environment,
        input=input_text,
        capture_output=True,
        text=True,
        check=True,
        timeout=timeout,
    )


def _failure_message(error: Exception) -> str:
    if isinstance(error, subprocess.CalledProcessError):
        return error.stderr.strip() or str(error)
    if isinstance(error, subprocess.TimeoutExpired):
        return f"Docker command timed out after {error.timeout} seconds"
    return str(error)


def _resolve_engine() -> tuple[str, str, dict[str, str]]:
    if shutil.which("docker") is None:
        msg = "Docker CLI is not installed or not on PATH"
        raise DockerUnavailable(msg)
    environment = dict(os.environ)
    # Keep image builds on the same engine as the probes and test commands.
    environment.pop("BUILDX_BUILDER", None)
    context_name = environment.get("DOCKER_CONTEXT")
    endpoint = environment.get("DOCKER_HOST") if not context_name else None
    if not endpoint:
        context = _run_docker(
            [
                "context",
                "inspect",
                *([context_name] if context_name else []),
                "--format",
                "{{json .Endpoints.docker}}",
            ],
            environment,
        )
        details = json.loads(context.stdout)
        endpoint = details.get("Host") if isinstance(details, dict) else None
    if not isinstance(endpoint, str) or not endpoint.startswith("unix://"):
        msg = (
            "Docker qualification requires a local Unix endpoint; "
            "select a local Docker context or set DOCKER_HOST to its unix:// socket"
        )
        raise DockerUnavailable(msg)
    environment.pop("DOCKER_CONTEXT", None)
    environment.pop("DOCKER_HOST", None)
    version = _run_docker(
        ["--host", endpoint, "version", "--format", "{{json .Server}}"],
        environment,
    )
    server = json.loads(version.stdout)
    if not isinstance(server, dict) or server.get("Os") != "linux":
        msg = "Docker qualification requires a Linux engine"
        raise DockerUnavailable(msg)
    architecture = server.get("Arch")
    if not isinstance(architecture, str) or not architecture:
        msg = "Docker engine did not report its architecture"
        raise DockerUnavailable(msg)
    return endpoint, f"linux/{architecture}", environment


def _inspect_image(
    reference: str, endpoint: str, environment: dict[str, str]
) -> dict[str, Any]:
    inspected = _run_docker(
        ["--host", endpoint, "image", "inspect", reference], environment
    )
    images = json.loads(inspected.stdout)
    if (
        not isinstance(images, list)
        or len(images) != 1
        or not isinstance(images[0], dict)
    ):
        msg = f"Docker returned invalid image metadata: {reference}"
        raise DockerPreparationError(msg)
    return images[0]


def _prepare_image(endpoint: str, platform: str, environment: dict[str, str]) -> str:
    override = environment.get("ARTISAN_DOCKER_CWD_TEST_IMAGE")
    recipe_id = hashlib.sha256(f"{platform}\n{DOCKERFILE}".encode()).hexdigest()[:20]
    reference = override or f"artisan-docker-test:{recipe_id}"
    try:
        image = _inspect_image(reference, endpoint, environment)
    except subprocess.CalledProcessError as error:
        if override or "no such image" not in error.stderr.lower():
            raise
        _run_docker(
            ["--host", endpoint, "pull", "--platform", platform, BASE_IMAGE],
            environment,
            timeout=_PREPARE_TIMEOUT,
        )
        _run_docker(
            [
                "--host",
                endpoint,
                "build",
                "--platform",
                platform,
                "--network=none",
                "--pull=false",
                "--tag",
                reference,
                "-",
            ],
            environment,
            input_text=DOCKERFILE,
            timeout=_PREPARE_TIMEOUT,
        )
        image = _inspect_image(reference, endpoint, environment)
    image_id = image.get("Id")
    config = image.get("Config")
    workdir = config.get("WorkingDir") if isinstance(config, dict) else None
    if (
        not isinstance(image_id, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None
    ):
        msg = f"Docker image has no immutable ID: {reference}"
        raise DockerPreparationError(msg)
    if image.get("Os") != "linux" or not isinstance(workdir, str) or not workdir:
        msg = f"Docker test image must be Linux with a declared WORKDIR: {reference}"
        raise DockerPreparationError(msg)
    return image_id


def prepare_docker_runtime() -> DockerRuntime:
    """Resolve one local engine and reuse or prepare its cached test image.

    ARTISAN_DOCKER_CWD_TEST_IMAGE selects an existing image. Otherwise build
    the pinned Alpine recipe on cache misses. Neither path changes the user's
    selected Docker context or environment.

    Returns:
        The Unix endpoint and immutable image ID for all test subprocesses.

    Raises:
        DockerUnavailable: If the CLI or selected local Linux engine is unusable.
        DockerPreparationError: If image preparation or validation fails.
    """
    try:
        endpoint, platform, environment = _resolve_engine()
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        msg = (
            f"Docker engine is unavailable: {_failure_message(error)}. "
            "Start a local Linux Docker engine and retry."
        )
        raise DockerUnavailable(msg) from error
    try:
        image_id = _prepare_image(endpoint, platform, environment)
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        msg = f"Could not prepare Docker test image: {_failure_message(error)}"
        raise DockerPreparationError(msg) from error
    return DockerRuntime(endpoint=endpoint, image_id=image_id)


def main() -> int:
    """Prepare Docker for required suites and optionally record the image ID."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image-id-file", type=Path)
    args = parser.parse_args()
    try:
        runtime = prepare_docker_runtime()
        if args.image_id_file is not None:
            args.image_id_file.write_text(f"{runtime.image_id}\n")
    except (DockerUnavailable, DockerPreparationError, OSError) as error:
        print(f"Docker test setup failed: {error}", file=sys.stderr)
        return 1
    print(
        f"Docker test runtime ready: {runtime.image_id} ({runtime.endpoint})",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
