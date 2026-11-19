"""Capture canonical manifest inputs without copying an arbitrary project tree."""

from __future__ import annotations

import hashlib
import stat
from dataclasses import dataclass
from importlib.resources.abc import Traversable
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Literal, cast

import tomli
import yaml  # type: ignore[import-untyped]
from packaging.utils import canonicalize_name

from artisan.schemas.execution.prepared_environment import (
    NATIVE_BINDING_FILENAME,
    NATIVE_PENDING_FILENAME,
    EnvironmentFile,
    EnvironmentSource,
    LocalPackageSource,
)
from artisan.schemas.operation_config.environment import Environment


@dataclass(frozen=True)
class LocalPackageInput:
    """One local package selected by the canonical lock for this platform."""

    root: str
    name: str
    version: str | None
    requirements: tuple[str, ...]
    requires_python: str | None


@dataclass(frozen=True)
class EnvironmentInputs:
    """Resolved references awaiting bounded backend source selection."""

    declaration: Environment
    root: Path | Traversable
    paths: tuple[str, ...]
    packages: tuple[LocalPackageInput, ...]


@dataclass(frozen=True)
class CapturedEnvironment:
    """Immutable file bytes paired with their portable content descriptor."""

    source: EnvironmentSource
    contents: tuple[bytes, ...]

    def write(self, destination: Path) -> None:
        """Write captured bytes to a new owned workspace without reading sources."""
        destination.mkdir(parents=True, exist_ok=False)
        for record, content in zip(self.source.files, self.contents, strict=True):
            target = destination / record.path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            target.chmod(0o644 | record.executable)


def relative_path(value: str, *, allow_root: bool = False) -> str:
    """Require canonical in-root names without shell expansion or traversal."""
    path = PurePosixPath(value)
    if allow_root and value in {".", "./"}:
        return "."
    if (
        not value
        or value == "."
        or path.is_absolute()
        or PureWindowsPath(value).drive
        or ".." in path.parts
        or "\\" in value
        or ":" in value
        or "__pycache__" in path.parts
        or path.parts[0]
        in {
            ".pixi",
            NATIVE_BINDING_FILENAME,
            NATIVE_PENDING_FILENAME,
            ".artisan-wheels",
        }
        or any(character in value for character in "*?[]")
        or str(path) != value
    ):
        msg = "Environment inputs must use exact canonical relative paths inside their root"
        raise ValueError(msg)
    return value


def source_entry(root: Path | Traversable, name: str) -> Path | Traversable:
    """Resolve a declared input while rejecting filesystem symbolic links."""
    relative_path(name)
    result = root
    for part in PurePosixPath(name).parts:
        result = result.joinpath(part)
        if isinstance(result, Path) and result.is_symlink():
            msg = "Environment source inputs cannot contain symbolic links"
            raise ValueError(msg)
    return result


def read_environment_inputs(
    declaration: Environment, *, platform: str
) -> EnvironmentInputs:
    """Read exact files for one environment and unambiguous physical subdir."""
    root = cast("Path | Traversable", getattr(declaration.manifest, "parent", None))
    if isinstance(root, Path) and root.resolve() != root:
        msg = "Environment source root must be resolved and free of symbolic links"
        raise ValueError(msg)
    manifest = source_entry(root, declaration.manifest.name)
    document = tomli.loads(manifest.read_bytes().decode("utf-8"))
    project = (
        document
        if declaration.manifest.name == "pixi.toml"
        else document["tool"]["pixi"]
    )
    sections = _selected_sections(project, declaration.name)
    paths = {declaration.manifest.name, "pixi.lock", *_extra_assets(document)}
    for section in sections:
        activation = _table(section.get("activation", {}), "activation")
        paths.update(_strings(activation.get("scripts", []), "activation scripts"))
    lock = yaml.safe_load(source_entry(root, "pixi.lock").read_bytes())
    local_files, packages = _locked_local_inputs(lock, declaration.name, platform)
    _validate_unselected_metadata(root, local_files)
    paths.update(local_files)
    return EnvironmentInputs(
        declaration=declaration,
        root=root,
        paths=tuple(sorted(relative_path(path) for path in paths)),
        packages=packages,
    )


def capture_environment_source(
    inputs: EnvironmentInputs,
    packages: tuple[LocalPackageSource, ...] = (),
) -> CapturedEnvironment:
    """Capture inferred inputs and the qualified backend's selected package files."""
    if {package.root for package in packages} != {
        package.root for package in inputs.packages
    }:
        msg = "Every locked local package requires a bounded source snapshot"
        raise ValueError(msg)
    paths = set(inputs.paths)
    for package in packages:
        for path in package.files:
            paths.add(str(PurePosixPath(package.root) / relative_path(path)))
    records, contents = [], []
    for name in sorted(paths):
        entry = source_entry(inputs.root, name)
        if not entry.is_file():
            msg = f"Declared environment input is not a regular file: {name}"
            raise ValueError(msg)
        content = entry.read_bytes()
        records.append(
            EnvironmentFile(
                path=name,
                sha256=hashlib.sha256(content).hexdigest(),
                executable=(stat.S_IMODE(entry.stat().st_mode) & 0o111)
                if isinstance(entry, Path)
                else 0,
            )
        )
        contents.append(content)
    return CapturedEnvironment(
        EnvironmentSource(
            manifest_name=cast(
                "Literal['pixi.toml', 'pyproject.toml']",
                inputs.declaration.manifest.name,
            ),
            name=inputs.declaration.name,
            files=tuple(records),
            packages=packages,
        ),
        tuple(contents),
    )


def _table(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        msg = f"Expected a {label} table"
        raise ValueError(msg)
    return value


def _strings(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        msg = f"Expected a list of strings for {label}"
        raise ValueError(msg)
    return tuple(value)


def _extra_assets(document: dict[str, Any]) -> tuple[str, ...]:
    tool = _table(document.get("tool", {}), "tool")
    artisan = _table(tool.get("dexterity-artisan", {}), "dexterity-artisan")
    config = _table(artisan.get("environment", {}), "environment assets")
    if config.keys() - {"assets"}:
        msg = "Environment packaging accepts only the assets option"
        raise ValueError(msg)
    paths = _strings(config.get("assets", []), "environment assets")
    if len(paths) != len(set(paths)):
        msg = "Environment assets must not contain duplicates"
        raise ValueError(msg)
    return tuple(relative_path(path) for path in paths)


def _selected_sections(
    project: dict[str, Any], name: str
) -> tuple[dict[str, Any], ...]:
    environments = _table(project.get("environments", {}), "environments")
    if name not in environments and name != "default":
        msg = f"Unknown Pixi environment: {name}"
        raise ValueError(msg)
    selected = environments.get(name, {})
    config = (
        {"features": selected}
        if isinstance(selected, list)
        else _table(selected, "environment")
    )
    features = _strings(config.get("features", []), "environment features")
    declared = _table(project.get("feature", {}), "features")
    sections = [] if config.get("no-default-feature", False) else [project]
    for feature in features:
        if feature not in declared:
            msg = f"Unknown Pixi feature: {feature}"
            raise ValueError(msg)
        sections.append(_table(declared[feature], "feature"))
    sections.append(config)
    # Capture direct files from every target block rather than reimplementing
    # Pixi's platform, family and variant selector precedence.
    return tuple(
        entry
        for section in sections
        for entry in (
            section,
            *(
                _table(target, "target section")
                for target in _table(section.get("target", {}), "target").values()
            ),
        )
    )


def _locked_local_inputs(
    lock: object, environment: str, platform: str
) -> tuple[tuple[str, ...], tuple[LocalPackageInput, ...]]:
    document = _table(lock, "Pixi lock")
    if document.get("version") != 7:
        msg = "Environment preparation requires Pixi lock format 7"
        raise ValueError(msg)
    environments = _table(document.get("environments"), "locked environments")
    try:
        selected_environment = _table(environments[environment], "locked environment")
        by_platform = _table(
            selected_environment.get("packages"), "locked platform packages"
        )
    except KeyError as error:
        msg = f"Lock has no {environment} environment for {platform}"
        raise ValueError(msg) from error
    selected_platform = _locked_platform_key(document, by_platform, platform)
    references = by_platform[selected_platform]
    selected = {reference["pypi"] for reference in references if "pypi" in reference}
    local_files, packages = [], []
    for record in document.get("packages", []):
        path = record.get("pypi")
        if path is None or path.startswith(
            (
                "https://",
                "http://",
                "direct+https://",
                "direct+http://",
                "git+https://",
                "git+http://",
                "git+ssh://",
            )
        ):
            continue
        is_selected = path in selected
        if path.startswith("./"):
            path = path[2:] or "."
        path = relative_path(path, allow_root=True)
        if path.endswith(".whl"):
            local_files.append(path)
            continue
        if not is_selected:
            local_files.append(str(PurePosixPath(path) / "pyproject.toml"))
            continue
        packages.append(
            LocalPackageInput(
                root=path,
                name=canonicalize_name(record["name"]),
                version=record.get("version"),
                requirements=_strings(
                    record.get("requires_dist", []), "locked requirements"
                ),
                requires_python=record.get("requires_python"),
            )
        )
    return tuple(sorted(set(local_files))), tuple(
        sorted(packages, key=lambda item: item.root)
    )


def _locked_platform_key(
    document: dict[str, Any], packages: dict[str, Any], requested: str
) -> str:
    """Select one locked platform without ranking host virtual-package variants."""
    if "platforms" not in document:
        # Keep exact-key capture for plain locks without a descriptor table.
        candidates = [requested] if requested in packages else []
    else:
        subdirs = _locked_platform_subdirs(document["platforms"])
        if packages.keys() - subdirs.keys():
            msg = "Locked environment references unknown platform descriptor names"
            raise ValueError(msg)
        # Preparation requests an OS/architecture subdir, not a variant name.
        # A bare-name entry has no priority over another matching rich entry.
        candidates = [name for name in packages if subdirs[name] == requested]
    if len(candidates) != 1:
        msg = f"Lock requires exactly one platform for {requested!r} in the selected environment; found {candidates}"
        raise ValueError(msg)
    return candidates[0]


def _locked_platform_subdirs(value: object) -> dict[str, str]:
    """Validate the format-7 platform names used by environment package maps."""
    if not isinstance(value, list):
        msg = "Expected a list of locked platform descriptors"
        raise ValueError(msg)
    subdirs: dict[str, str] = {}
    for entry in value:
        descriptor = _table(entry, "locked platform descriptor")
        name = descriptor.get("name")
        subdir = descriptor.get("subdir", name)
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(subdir, str)
            or not subdir
        ):
            msg = "Locked platform descriptors require nonempty name and subdir strings"
            raise ValueError(msg)
        if name in subdirs:
            msg = "Locked platform descriptors contain duplicate names"
            raise ValueError(msg)
        subdirs[name] = subdir
    return subdirs


def _validate_unselected_metadata(
    root: Path | Traversable, paths: tuple[str, ...]
) -> None:
    """Supply only static metadata needed by Pixi's whole-workspace lock check."""
    for path in paths:
        if PurePosixPath(path).name != "pyproject.toml":
            continue
        document = tomli.loads(source_entry(root, path).read_bytes().decode("utf-8"))
        project, build = document.get("project", {}), document.get("build-system", {})
        hatch = document.get("tool", {}).get("hatch", {})
        if (
            project.get("dynamic")
            or not isinstance(project.get("name"), str)
            or not isinstance(project.get("version"), str)
            or build.get("build-backend") != "hatchling.build"
            or "backend-path" in build
            or hatch.get("version")
            or hatch.get("metadata", {}).get("hooks")
            or hatch.get("build", {}).get("hooks")
        ):
            msg = "Unselected local dependencies require static Hatch metadata for canonical lock validation; supply an ordinary wheel"
            raise ValueError(msg)
