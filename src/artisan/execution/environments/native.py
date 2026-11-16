"""Explicit native preparation and pure lookup of canonical Pixi software."""

from __future__ import annotations

import hashlib
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

from artisan.execution.environments.local_packages import (
    BACKEND_SCRIPT,
    HATCH_VERSION,
    LocalPackageSelection,
    backend_command,
    select_local_package,
    validate_source_profile,
)
from artisan.execution.environments.native_target import (
    native_executable,
    publish_native_environment,
    verify_native_environment,
)
from artisan.execution.environments.pixi import pixi_executable, pixi_version
from artisan.execution.environments.preparation_lock import preparation_lock
from artisan.execution.environments.source import (
    CapturedEnvironment,
    EnvironmentInputs,
    capture_environment_source,
    read_environment_inputs,
    source_entry,
)
from artisan.execution.environments.source_membership import source_membership
from artisan.execution.environments.wheels import (
    verify_wheel_installation,
    verify_wheel_metadata,
)
from artisan.schemas.execution.prepared_environment import (
    NATIVE_BINDING_FILENAME,
    NATIVE_WHEEL_DIRECTORY,
    DerivedWheel,
    EnvironmentSource,
    PreparedNativeEnvironment,
)
from artisan.schemas.operation_config.environment import Environment
from artisan.utils.hashing import canonical_json_bytes

BINDING_FILENAME = NATIVE_BINDING_FILENAME


def native_platform() -> str:
    """Return the supported native Pixi platform without probing or provisioning."""
    platforms = {
        ("linux", "x86_64"): "linux-64",
        ("linux", "aarch64"): "linux-aarch64",
    }
    try:
        return platforms[sys.platform, platform.machine()]
    except KeyError as error:
        msg = "Native environment preparation is unsupported on this platform"
        raise ValueError(msg) from error


def prepare_native_environment(
    declaration: Environment, *, cache_root: Path
) -> PreparedNativeEnvironment:
    """Install a bounded snapshot once at its final path, then publish evidence.

    Local static Hatch packages are built from captured bytes and installed as
    ordinary wheels. The authored manifest and lock are never rewritten.
    """
    cache_root = cache_root.resolve()
    selected_platform = native_platform()
    pixi_version()
    inputs = read_environment_inputs(declaration, platform=selected_platform)
    for package in inputs.packages:
        validate_source_profile(_package_root(inputs, package.root), package)
    builder = (
        _prepare_builder(cache_root, selected_platform) if inputs.packages else None
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
    workspace = _workspace(cache_root, captured.source, selected_platform)
    with preparation_lock(workspace):
        if (workspace / BINDING_FILENAME).exists():
            return _verify_binding(workspace, captured.source, selected_platform)
        captured.write(workspace)
        try:
            _validate_manifest(workspace / captured.source.manifest_name)
            wheels, paths = _build_wheels(captured, workspace, selections, builder)
            _install_locked(
                workspace / captured.source.manifest_name,
                declaration.name,
                skip=tuple(package.name for package in inputs.packages),
            )
            prefix = workspace / ".pixi/envs" / declaration.name
            if paths:
                assert builder is not None
                _install_wheels(
                    native_executable(builder, "python"),
                    prefix / "bin/python",
                    paths,
                )
                verify_wheel_installation(paths, interpreter=prefix / "bin/python")
            target = PreparedNativeEnvironment(
                source=captured.source,
                platform=selected_platform,
                wheels=wheels,
                workspace=workspace,
                prefix=prefix,
            )
            _verify_current_source(declaration, target.source, selected_platform)
            publish_native_environment(target)
            verify_native_environment(target)
            return target
        except Exception:
            shutil.rmtree(workspace)
            raise


def lookup_native_environment(
    declaration: Environment, *, cache_root: Path
) -> PreparedNativeEnvironment:
    """Find exact prepared software with no subprocess, build, solve or install.

    Lookup checks captured files and a conservative bounded source membership
    inventory. New excluded files also require preparation; ordinary Python
    interpreter caches do not. No missing target falls back to host software.
    """
    selected_platform = native_platform()
    inputs = read_environment_inputs(declaration, platform=selected_platform)
    root = cache_root.resolve() / "native-v2"
    for path in sorted(root.glob(f"*/{BINDING_FILENAME}")):
        target = PreparedNativeEnvironment.model_validate_json(path.read_bytes())
        if (
            target.platform != selected_platform
            or target.source.name != declaration.name
            or not wheel_builders_match(target)
        ):
            continue
        try:
            captured = _capture_current_source(inputs, target.source)
        except (ValueError, FileNotFoundError):
            continue
        if captured.source == target.source:
            return _verify_binding(path.parent, captured.source, selected_platform)
    msg = "No compatible prepared native environment; call prepare explicitly"
    raise ValueError(msg)


def _capture_current_source(
    inputs: EnvironmentInputs, expected: EnvironmentSource
) -> CapturedEnvironment:
    packages = tuple(
        package.model_copy(
            update={
                "membership_sha256": source_membership(
                    _package_root(inputs, package.root), package.scan_roots
                )
            }
        )
        for package in expected.packages
    )
    return capture_environment_source(inputs, packages)


def _verify_current_source(
    declaration: Environment, expected: EnvironmentSource, selected_platform: str
) -> None:
    inputs = read_environment_inputs(declaration, platform=selected_platform)
    if _capture_current_source(inputs, expected).source != expected:
        msg = "Environment source changed during preparation"
        raise ValueError(msg)


def _workspace(
    cache_root: Path, source: EnvironmentSource, selected_platform: str
) -> Path:
    descriptor = f"{source.digest}\0{selected_platform}"
    if source.packages:
        descriptor += f"\0{wheel_builder_digest(selected_platform)}"
    key = hashlib.sha256(descriptor.encode()).hexdigest()
    return cache_root / "native-v2" / key


def _verify_binding(
    workspace: Path, source: EnvironmentSource, selected_platform: str
) -> PreparedNativeEnvironment:
    target = PreparedNativeEnvironment.model_validate_json(
        (workspace / BINDING_FILENAME).read_bytes()
    )
    if (
        target.workspace != workspace
        or target.source != source
        or target.platform != selected_platform
        or not wheel_builders_match(target)
    ):
        msg = "Prepared native binding does not match its source or final location"
        raise ValueError(msg)
    verify_native_environment(target)
    return target


def _package_root(inputs: EnvironmentInputs, name: str) -> Path:
    root = inputs.root if name == "." else source_entry(inputs.root, name)
    if not isinstance(root, Path):
        msg = "Local source builds require filesystem packages; use a published wheel for packaged resources"
        raise ValueError(msg)
    return root


def _prepare_builder(
    cache_root: Path, selected_platform: str
) -> PreparedNativeEnvironment:
    manifest_bytes = _builder_manifest(selected_platform)
    profile = hashlib.sha256(manifest_bytes).hexdigest()
    workspace = cache_root / "builders-v2" / profile
    with preparation_lock(workspace):
        if (workspace / BINDING_FILENAME).exists():
            if (workspace / "pixi.toml").read_bytes() != manifest_bytes:
                msg = "Prepared build backend differs from the qualified static profile"
                raise ValueError(msg)
            inputs = read_environment_inputs(
                Environment.from_pixi(workspace / "pixi.toml"),
                platform=selected_platform,
            )
            return _verify_binding(
                workspace, capture_environment_source(inputs).source, selected_platform
            )
        workspace.mkdir(parents=True, exist_ok=False)
        try:
            manifest = workspace / "pixi.toml"
            manifest.write_bytes(manifest_bytes)
            _pixi(["lock", "--no-config"], manifest)
            _install_locked(manifest, "default")
            inputs = read_environment_inputs(
                Environment.from_pixi(manifest), platform=selected_platform
            )
            prefix = workspace / ".pixi/envs/default"
            target = PreparedNativeEnvironment(
                source=capture_environment_source(inputs).source,
                platform=selected_platform,
                workspace=workspace,
                prefix=prefix,
            )
            publish_native_environment(target)
            verify_native_environment(target)
            return target
        except Exception:
            shutil.rmtree(workspace)
            raise


def _build_wheels(
    captured: CapturedEnvironment,
    workspace: Path,
    selections: tuple[LocalPackageSelection, ...],
    builder: PreparedNativeEnvironment | None,
) -> tuple[tuple[DerivedWheel, ...], tuple[Path, ...]]:
    if not selections:
        return (), ()
    assert builder is not None
    directory = workspace / NATIVE_WHEEL_DIRECTORY
    directory.mkdir()
    records, paths = [], []
    builder_digest = wheel_builder_digest(builder.platform)
    for selection in selections:
        result = backend_command(
            native_executable(builder, "python"),
            "build",
            workspace / selection.source.root,
            output=directory,
        )
        if len(result["wheels"]) != 1:
            msg = "Static Hatch preparation must produce one ordinary wheel per package"
            raise ValueError(msg)
        wheel = Path(result["wheels"][0]).resolve(strict=True)
        if wheel.parent != directory:
            msg = "Build backend produced a wheel outside its owned output directory"
            raise ValueError(msg)
        verify_wheel_metadata(
            wheel, selection.locked, source_version=selection.source.version
        )
        selected = {
            str(Path(selection.source.root) / name) for name in selection.source.files
        }
        source_digest = hashlib.sha256(
            canonical_json_bytes(
                [
                    item.model_dump(mode="json")
                    for item in captured.source.files
                    if item.path in selected
                ]
            )
        ).hexdigest()
        records.append(
            DerivedWheel(
                name=selection.locked.name,
                version=selection.source.version,
                source_root=selection.source.root,
                source_digest=source_digest,
                builder_digest=builder_digest,
                filename=wheel.name,
                sha256=hashlib.sha256(wheel.read_bytes()).hexdigest(),
            )
        )
        paths.append(wheel)
    return tuple(records), tuple(paths)


def wheel_builder_digest(selected_platform: str) -> str:
    """Identify declared build inputs without depending on an installed builder.

    The prepared v2 wheel descriptor uses this recipe identity and the exact
    output wheel hash. Builder cache cleanup does not invalidate prepared tools.
    """
    return hashlib.sha256(
        b"artisan.static-wheel-builder-v2\0"
        + canonical_json_bytes(
            {
                "manifest": hashlib.sha256(
                    _builder_manifest(selected_platform)
                ).hexdigest(),
                "backend": hashlib.sha256(BACKEND_SCRIPT.read_bytes()).hexdigest(),
            }
        )
    ).hexdigest()


def wheel_builders_match(target: PreparedNativeEnvironment) -> bool:
    """Check recorded local wheels against the current declared build recipe."""
    if not target.wheels:
        return True
    expected = wheel_builder_digest(target.platform)
    return all(wheel.builder_digest == expected for wheel in target.wheels)


def _builder_manifest(selected_platform: str) -> bytes:
    return (
        '[workspace]\nname="artisan-static-hatch-builder"\nchannels=["conda-forge"]\n'
        f'platforms=["{selected_platform}"]\nrequires-pixi="==0.83.0"\n'
        '[dependencies]\npython="==3.12.12"\npip="==25.3"\n'
        f'[pypi-dependencies]\nhatchling="=={HATCH_VERSION}"\n'
    ).encode()


def _install_wheels(
    builder_python: Path, target_python: Path, wheels: tuple[Path, ...]
) -> None:
    subprocess.run(
        [
            str(builder_python),
            "-I",
            "-m",
            "pip",
            "--isolated",
            "--python",
            str(target_python),
            "install",
            "--no-index",
            "--no-deps",
            "--no-compile",
            *map(str, wheels),
        ],
        env={
            "PATH": f"{builder_python.parent}{os.pathsep}/usr/bin{os.pathsep}/bin",
            "PIP_CONFIG_FILE": os.devnull,
        },
        check=True,
        text=True,
        capture_output=True,
        timeout=180,
    )


def _install_locked(manifest: Path, name: str, *, skip: tuple[str, ...] = ()) -> None:
    arguments = ["install", "--no-config", "--locked", "--environment", name]
    for package in skip:
        arguments.extend(("--skip", package))
    _pixi(arguments, manifest)


def _validate_manifest(manifest: Path) -> None:
    _pixi(["workspace", "name", "--no-config", "get"], manifest)


def _pixi(arguments: list[str], manifest: Path) -> None:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("PIXI_") or key == "PIXI_CACHE_DIR"
    }
    subprocess.run(
        [
            pixi_executable(),
            *arguments,
            "--manifest-path",
            str(manifest),
        ],
        cwd=manifest.parent,
        env=environment,
        check=True,
        text=True,
        capture_output=True,
        timeout=300,
    )
