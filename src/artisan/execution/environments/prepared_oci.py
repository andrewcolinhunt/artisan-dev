"""Explicit OCI preparation and read-only lookup from canonical Pixi source."""

from __future__ import annotations

import hashlib
import subprocess
import tempfile
from pathlib import Path

from artisan.execution.environments.container_runtime import DockerRuntime, file_digest
from artisan.execution.environments.container_source import (
    CONTAINER_WORKSPACE,
    capture_container_inputs,
    container_recipe_digest,
    write_container_recipe,
)
from artisan.execution.environments.native import (
    _capture_current_source,
    _verify_current_source,
    native_platform,
    wheel_builders_match,
)
from artisan.execution.environments.pixi import pixi_executable, pixi_version
from artisan.execution.environments.source import read_environment_inputs
from artisan.schemas.execution.container_environment import (
    ContainerLauncherIdentity,
    PreparedOCIEnvironment,
)
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.operation_config.environment import Environment
from artisan.utils.hashing import canonical_json_bytes


def prepare_oci_environment(
    declaration: Environment,
    *,
    cache_root: Path,
    docker: DockerRuntime,
    base_image: str,
) -> PreparedOCIEnvironment:
    """Build source-derived software at final image paths and measure the result."""
    cache_root = cache_root.resolve()
    cache_root.mkdir(parents=True, exist_ok=True)
    runtime = docker.identity()
    base = docker.inspect_image(base_image)
    expected_arch = {"linux-64": "amd64", "linux-aarch64": "arm64"}[native_platform()]
    if (
        base["Os"] != "linux"
        or base["Architecture"] != expected_arch
        or base["Config"].get("Volumes")
        or base["Config"].get("OnBuild")
    ):
        msg = "OCI base requires the native Linux architecture and no implicit volumes or build hooks"
        raise ValueError(msg)
    reused = _reuse_oci_environment(
        declaration, cache_root, docker, base["Id"], runtime
    )
    if reused is not None:
        return reused
    with tempfile.TemporaryDirectory(prefix="oci-context-", dir=cache_root) as scratch:
        context = Path(scratch)
        inputs = capture_container_inputs(declaration, context, cache_root=cache_root)
        base_tag = _bind_base(docker, base["Id"])
        recipe = write_container_recipe(inputs, context, base_image=base_tag)
        key = hashlib.sha256(
            canonical_json_bytes(
                {
                    "source": inputs.captured.source.digest,
                    "wheels": [
                        wheel.model_dump(mode="json") for wheel in inputs.wheels
                    ],
                    "base": base["Id"],
                    "recipe": recipe,
                    "pixi": inputs.pixi_sha256,
                    "runtime": runtime.model_dump(mode="json"),
                }
            )
        ).hexdigest()
        binding = cache_root / "oci-v2" / key / "binding.json"
        if binding.exists():
            target = PreparedOCIEnvironment.model_validate_json(binding.read_bytes())
            verify_oci_environment(target, docker=docker)
            if (
                target.native.source != inputs.captured.source
                or target.native.wheels != inputs.wheels
                or target.native.platform != native_platform()
                or target.base_image_id != base["Id"]
                or target.recipe_sha256 != recipe
                or target.pixi_sha256 != inputs.pixi_sha256
                or target.build_runtime != runtime
            ):
                msg = "Prepared OCI binding disagrees with requested preparation inputs"
                raise ValueError(msg)
            return target
        image_file = context / "image-id"
        subprocess.run(
            [
                *docker.command,
                "build",
                "--pull=false",
                "--iidfile",
                str(image_file),
                "--tag",
                f"artisan-environment:{key}",
                str(context),
            ],
            check=True,
            timeout=1200,
        )
        image = image_file.read_text().strip()
        actual_layers = docker.inspect_image(image)["RootFS"]["Layers"]
        base_layers = base["RootFS"]["Layers"]
        if (
            actual_layers[: len(base_layers)] != base_layers
            or docker.inspect_image(base_tag)["Id"] != base["Id"]
        ):
            msg = "OCI build did not retain the exact qualified base image"
            raise ValueError(msg)
        payload = PreparedNativeEnvironment(
            source=inputs.captured.source,
            platform=native_platform(),
            wheels=inputs.wheels,
            workspace=CONTAINER_WORKSPACE,
            prefix=CONTAINER_WORKSPACE / ".pixi/envs" / inputs.captured.source.name,
        )
        _verify_current_source(declaration, inputs.captured.source, native_platform())
        target = PreparedOCIEnvironment(
            native=payload,
            image_id=image,
            base_image_id=base["Id"],
            recipe_sha256=recipe,
            pixi_sha256=inputs.pixi_sha256,
            build_runtime=runtime,
        )
        target = _seal_image_binding(target, docker=docker)
        binding.parent.mkdir(parents=True, exist_ok=False)
        temporary_binding = binding.with_suffix(".json.tmp")
        temporary_binding.write_text(target.model_dump_json(indent=2))
        temporary_binding.replace(binding)
        return target


def lookup_oci_environment(
    declaration: Environment, *, cache_root: Path, docker: DockerRuntime
) -> PreparedOCIEnvironment:
    """Resolve unchanged authored software and an available immutable local image."""
    inputs = read_environment_inputs(declaration, platform=native_platform())
    matches = []
    for binding in sorted(cache_root.glob("oci-v2/*/binding.json")):
        target = PreparedOCIEnvironment.model_validate_json(binding.read_bytes())
        if (
            target.native.platform != native_platform()
            or target.native.source.name != declaration.name
            or not wheel_builders_match(target.native)
        ):
            continue
        try:
            captured = _capture_current_source(inputs, target.native.source)
        except (ValueError, FileNotFoundError):
            continue
        if captured.source == target.native.source:
            verify_oci_environment(target, docker=docker)
            matches.append(target)
    if len(matches) != 1:
        msg = "Expected one compatible prepared OCI environment; prepare it explicitly in an unambiguous cache"
        raise ValueError(msg)
    return matches[0]


def _reuse_oci_environment(
    declaration: Environment,
    cache_root: Path,
    docker: DockerRuntime,
    base_image_id: str,
    runtime: ContainerLauncherIdentity,
) -> PreparedOCIEnvironment | None:
    """Reuse complete matching build inputs before invoking any package builder."""
    pixi_version()
    pixi_digest = file_digest(Path(pixi_executable()))
    inputs = read_environment_inputs(declaration, platform=native_platform())
    for binding in sorted(cache_root.glob("oci-v2/*/binding.json")):
        target = PreparedOCIEnvironment.model_validate_json(binding.read_bytes())
        if (
            target.native.platform != native_platform()
            or target.native.source.name != declaration.name
            or not wheel_builders_match(target.native)
            or target.base_image_id != base_image_id
            or target.build_runtime != runtime
            or target.pixi_sha256 != pixi_digest
        ):
            continue
        try:
            source = _capture_current_source(inputs, target.native.source).source
        except (ValueError, FileNotFoundError):
            continue
        recipe = container_recipe_digest(
            source,
            target.native.wheels,
            base_image=f"artisan-environment-base:{base_image_id.removeprefix('sha256:')}",
        )
        if source == target.native.source and recipe == target.recipe_sha256:
            verify_oci_environment(target, docker=docker)
            return target
    return None


def verify_oci_environment(
    target: PreparedOCIEnvironment, *, docker: DockerRuntime
) -> None:
    """Verify exact local image availability without building, pulling or fallback."""
    target = PreparedOCIEnvironment.model_validate(target)
    observed = docker.inspect_image(target.image_id)
    if (
        observed["Id"] != target.image_id
        or observed["Os"] != "linux"
        or (observed["Config"].get("Labels") or {}).get("org.artisan.prepared.payload")
        != target.payload_digest
    ):
        msg = "Prepared OCI image identity changed; prepare it explicitly"
        raise ValueError(msg)


def _seal_image_binding(
    target: PreparedOCIEnvironment, *, docker: DockerRuntime
) -> PreparedOCIEnvironment:
    container = docker.output(
        ["create", "--entrypoint", "/bin/true", target.image_id]
    ).strip()
    try:
        image = docker.output(
            [
                "commit",
                "--change",
                f"LABEL org.artisan.prepared.payload={target.payload_digest}",
                container,
            ]
        ).strip()
    finally:
        subprocess.run(
            [*docker.command, "rm", "-v", container],
            check=True,
            capture_output=True,
            timeout=30,
        )
    result = target.model_copy(update={"image_id": image})
    subprocess.run(
        [*docker.command, "tag", image, f"artisan-environment:{target.payload_digest}"],
        check=True,
        timeout=30,
    )
    verify_oci_environment(result, docker=docker)
    return result


def _bind_base(docker: DockerRuntime, image_id: str) -> str:
    """Give BuildKit a local content-named alias and reject conflicting aliases."""
    tag = f"artisan-environment-base:{image_id.removeprefix('sha256:')}"
    try:
        metadata = docker.inspect_image(tag)
    except subprocess.CalledProcessError:
        subprocess.run([*docker.command, "tag", image_id, tag], check=True, timeout=30)
    else:
        if metadata["Id"] != image_id:
            msg = "Content-named OCI base alias points at another image"
            raise ValueError(msg)
    return tag
