"""Generate container packaging from captured canonical source and derived wheels."""

from __future__ import annotations

import hashlib
import json
import shutil
import tarfile
from dataclasses import dataclass
from pathlib import Path

from artisan.execution.environments.local_packages import (
    select_local_package,
    validate_source_profile,
)
from artisan.execution.environments.native import (
    _build_wheels,
    _package_root,
    _prepare_builder,
    _verify_current_source,
    native_platform,
)
from artisan.execution.environments.native_target import (
    native_executable,
)
from artisan.execution.environments.pixi import pixi_executable, pixi_version
from artisan.execution.environments.source import (
    CapturedEnvironment,
    capture_environment_source,
    read_environment_inputs,
)
from artisan.schemas.execution.prepared_environment import (
    NATIVE_BINDING_FILENAME,
    NATIVE_PENDING_FILENAME,
    DerivedWheel,
    EnvironmentSource,
    PreparedNativeEnvironment,
)
from artisan.schemas.operation_config.environment import Environment

CONTAINER_WORKSPACE = Path("/opt/artisan/environment")
CONTAINER_PIXI = Path("/usr/local/bin/artisan-pixi")


@dataclass(frozen=True)
class ContainerBuildInputs:
    """Captured packaging files, never a copied installed native prefix."""

    captured: CapturedEnvironment
    wheels: tuple[DerivedWheel, ...]
    wheel_paths: tuple[Path, ...]
    builder: PreparedNativeEnvironment | None
    pixi_sha256: str


def capture_container_inputs(
    declaration: Environment, context: Path, *, cache_root: Path
) -> ContainerBuildInputs:
    """Select static sources once and build ordinary wheels in the owned context."""
    pixi_version()
    inputs = read_environment_inputs(declaration, platform=native_platform())
    for package in inputs.packages:
        validate_source_profile(_package_root(inputs, package.root), package)
    builder = (
        _prepare_builder(cache_root, native_platform()) if inputs.packages else None
    )
    selections = tuple(
        select_local_package(
            _package_root(inputs, package.root),
            package,
            builder_python=native_executable(builder, "python"),
        )
        for package in inputs.packages
        if builder is not None
    )
    captured = capture_environment_source(
        inputs, tuple(item.source for item in selections)
    )
    workspace = context / "workspace"
    captured.write(workspace)
    wheels, paths = _build_wheels(captured, workspace, selections, builder)
    pixi = Path(pixi_executable())
    shutil.copyfile(pixi, context / "pixi")
    (context / "pixi").chmod(0o755)
    with pixi.open("rb") as stream:
        pixi_digest = hashlib.file_digest(stream, "sha256").hexdigest()
    _verify_current_source(declaration, captured.source, native_platform())
    return ContainerBuildInputs(captured, wheels, paths, builder, pixi_digest)


def verify_container_source(
    declaration: Environment, expected: EnvironmentSource
) -> None:
    """Check canonical source bytes and membership without building or installing."""
    _verify_current_source(declaration, expected, native_platform())


def write_container_payload(
    inputs: ContainerBuildInputs,
    context: Path,
    *,
    workspace: Path,
    builder_workspace: Path = Path("/opt/artisan/builder"),
) -> tuple[tuple[str, ...], ...]:
    """Archive canonical build inputs and return exact final-path install commands.

    Provider implementations select these internal locations. No installed prefix
    is copied, and the canonical dependency manifest and lock remain unchanged.
    """
    for location in (workspace, builder_workspace):
        if not location.is_absolute() or ".." in location.parts:
            msg = "Image installation locations must be absolute canonical paths"
            raise ValueError(msg)
    if workspace.is_relative_to(builder_workspace) or builder_workspace.is_relative_to(
        workspace
    ):
        msg = "Worker/tool and builder workspaces must be disjoint"
        raise ValueError(msg)
    target = PreparedNativeEnvironment(
        source=inputs.captured.source,
        platform=native_platform(),
        wheels=inputs.wheels,
        workspace=workspace,
        prefix=workspace / ".pixi/envs" / inputs.captured.source.name,
    )
    (context / "workspace" / NATIVE_PENDING_FILENAME).write_text(
        target.model_dump_json(indent=2)
    )
    with tarfile.open(context / "payload.tar.xz", "w:xz", preset=3) as archive:
        archive.add(context / "workspace", arcname=str(workspace).lstrip("/"))
        if inputs.builder is not None:
            for record in inputs.builder.source.files:
                archive.add(
                    inputs.builder.workspace / record.path,
                    arcname=str(builder_workspace / record.path).lstrip("/"),
                )
    return _install_commands(
        inputs.captured.source, inputs.wheels, workspace, builder_workspace
    )


def _install_commands(
    source: EnvironmentSource,
    wheels: tuple[DerivedWheel, ...],
    workspace: Path,
    builder_workspace: Path,
) -> tuple[tuple[str, ...], ...]:
    commands = [
        [
            str(CONTAINER_PIXI),
            "install",
            "--no-config",
            "--locked",
            "--manifest-path",
            str(workspace / source.manifest_name),
            "--environment",
            source.name,
            *[item for wheel in wheels for item in ("--skip", wheel.name)],
        ]
    ]
    if wheels:
        commands.append(
            [
                str(CONTAINER_PIXI),
                "install",
                "--no-config",
                "--locked",
                "--manifest-path",
                str(builder_workspace / "pixi.toml"),
            ]
        )
        commands.append(
            [
                "/usr/bin/env",
                "PIP_CONFIG_FILE=/dev/null",
                str(builder_workspace / ".pixi/envs/default/bin/python"),
                "-I",
                "-m",
                "pip",
                "--isolated",
                "--python",
                str(workspace / ".pixi/envs" / source.name / "bin/python"),
                "install",
                "--no-index",
                "--no-deps",
                "--no-compile",
                *[
                    str(workspace / ".artisan-wheels" / wheel.filename)
                    for wheel in wheels
                ],
            ]
        )
    commands.append(
        [
            "/bin/mv",
            str(workspace / NATIVE_PENDING_FILENAME),
            str(workspace / NATIVE_BINDING_FILENAME),
        ]
    )
    return tuple(tuple(command) for command in commands)


def write_container_recipe(
    inputs: ContainerBuildInputs, context: Path, *, base_image: str
) -> str:
    """Emit the OCI recipe from shared canonical final-path installation inputs."""
    write_container_payload(inputs, context, workspace=CONTAINER_WORKSPACE)
    encoded = _container_recipe(inputs.captured.source, inputs.wheels, base_image)
    (context / "Dockerfile").write_bytes(encoded)
    return hashlib.sha256(encoded).hexdigest()


def container_recipe_digest(
    source: EnvironmentSource, wheels: tuple[DerivedWheel, ...], *, base_image: str
) -> str:
    """Identify current build commands without building wheels or writing payloads."""
    return hashlib.sha256(_container_recipe(source, wheels, base_image)).hexdigest()


def _container_recipe(
    source: EnvironmentSource, wheels: tuple[DerivedWheel, ...], base_image: str
) -> bytes:
    commands = _install_commands(
        source, wheels, CONTAINER_WORKSPACE, Path("/opt/artisan/builder")
    )
    lines = [
        f"FROM {base_image}",
        "USER 0",
        f"COPY --chmod=755 pixi {CONTAINER_PIXI}",
        "ADD payload.tar.xz /",
        *["RUN " + json.dumps(command) for command in commands],
        'ENTRYPOINT ["/usr/bin/env"]',
        "CMD []",
    ]
    return ("\n".join(lines) + "\n").encode()
