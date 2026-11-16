"""Bound local source snapshots using a qualified static Hatch wheel profile."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import tomli
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

from artisan.execution.environments.source import (
    LocalPackageInput,
    relative_path,
    source_entry,
)
from artisan.execution.environments.source_membership import source_membership
from artisan.schemas.execution.prepared_environment import (
    LocalPackageSource,
    SourcePayload,
)

HATCH_VERSION = "1.31.0"
BACKEND_SCRIPT = Path(__file__).with_name("_hatch_backend.py")


@dataclass(frozen=True)
class LocalPackageSelection:
    """Bounded source membership and its mapping to installed import payloads."""

    locked: LocalPackageInput
    source: LocalPackageSource

    def editable_files(self, root: Path) -> dict[str, Path]:
        """Supply the common runtime fingerprint with backend-selected paths."""
        return {
            payload.path: cast("Path", source_entry(root, payload.source))
            for payload in self.source.payloads
        }


def validate_source_profile(root: Path, locked: LocalPackageInput) -> dict[str, Any]:
    """Reject source builds whose inputs cannot be captured by the static profile."""
    project_file = source_entry(root, "pyproject.toml")
    document = tomli.loads(project_file.read_bytes().decode("utf-8"))
    build = document.get("build-system", {})
    if build.get("build-backend") != "hatchling.build" or "backend-path" in build:
        _unsupported("custom build backend")
    requirements = [Requirement(value) for value in build.get("requires", [])]
    if (
        len(requirements) != 1
        or canonicalize_name(requirements[0].name) != "hatchling"
        or requirements[0].extras
        or requirements[0].marker is not None
        or requirements[0].url is not None
        or Version(HATCH_VERSION) not in requirements[0].specifier
    ):
        _unsupported(f"build requirements outside qualified Hatchling {HATCH_VERSION}")
    project = document.get("project", {})
    if project.get("dynamic") or not isinstance(project.get("version"), str):
        _unsupported("dynamic package metadata")
    if canonicalize_name(project["name"]) != locked.name:
        msg = "Local project name differs from the canonical lock"
        raise ValueError(msg)
    if locked.version is not None and Version(project["version"]) != Version(
        locked.version
    ):
        msg = "Local project version differs from the canonical lock"
        raise ValueError(msg)
    license_patterns = project.get("license-files", [])
    if not isinstance(license_patterns, list) or any(
        not isinstance(pattern, str) or "/" in pattern or "\\" in pattern
        for pattern in license_patterns
    ):
        _unsupported("license discovery outside the package root")
    hatch = document.get("tool", {}).get("hatch", {})
    if hatch.get("version") or hatch.get("metadata", {}).get("hooks"):
        _unsupported("dynamic Hatch metadata")
    build_config = hatch.get("build", {})
    if build_config.keys() - {"targets"}:
        _unsupported("global Hatch selectors or build hooks")
    wheel = build_config.get("targets", {}).get("wheel", {})
    if wheel.keys() - {"packages", "only-include", "exclude", "force-include"}:
        _unsupported("wheel selectors or build hooks outside the static profile")
    roots = [*wheel.get("packages", []), *wheel.get("only-include", [])]
    if not roots:
        _unsupported("implicit or whole-project package selection")
    for directory in roots:
        if "__pycache__" in Path(directory).parts:
            _unsupported("interpreter cache packaging")
        if not source_entry(root, relative_path(directory)).is_dir():
            _unsupported("missing package root")
    for source, target in wheel.get("force-include", {}).items():
        if "__pycache__" in Path(source).parts or "__pycache__" in Path(target).parts:
            _unsupported("interpreter cache packaging")
        entry = source_entry(root, relative_path(source))
        if not (entry.is_file() or entry.is_dir()):
            _unsupported("missing forced package input")
        relative_path(target)
        if any(part.endswith((".data", ".dist-info")) for part in Path(target).parts):
            _unsupported("forced wheel metadata or data schemes")
    return document


def select_local_package(
    root: Path, locked: LocalPackageInput, *, builder_python: Path
) -> LocalPackageSelection:
    """Ask the qualified backend for actual membership without building a wheel."""
    document = validate_source_profile(root, locked)
    wheel = document["tool"]["hatch"]["build"]["targets"]["wheel"]
    scan_roots = tuple(
        sorted(
            {
                *wheel.get("packages", []),
                *wheel.get("only-include", []),
                *wheel.get("force-include", {}).keys(),
            }
        )
    )
    membership = source_membership(root, scan_roots)
    result = backend_command(builder_python, "select", root)
    selected: dict[str, str] = {}
    for record in result["files"]:
        source, target = (
            relative_path(record["source"]),
            relative_path(record["target"]),
        )
        if "__pycache__" in Path(source).parts or "__pycache__" in Path(target).parts:
            _unsupported("interpreter cache packaging")
        if target in selected:
            _unsupported("overlapping wheel payload destinations")
        source_entry(root, source)
        selected[target] = source
    files = {"pyproject.toml", *selected.values()}
    files.update(relative_path(value) for value in result["license_files"])
    readme = document["project"].get("readme")
    if isinstance(readme, str):
        files.add(relative_path(readme))
    elif readme is not None:
        _unsupported("non-file README metadata")
    for ignore in (".gitignore", ".hgignore"):
        if (root / ignore).is_file():
            files.add(ignore)
        elif any((parent / ignore).exists() for parent in root.parents):
            _unsupported("ignore rules outside the local package root")
    for name in files:
        if not source_entry(root, name).is_file():
            _unsupported("non-file source input")
    return LocalPackageSelection(
        locked=locked,
        source=LocalPackageSource(
            root=locked.root,
            name=locked.name,
            version=document["project"]["version"],
            files=tuple(sorted(files)),
            payloads=tuple(
                SourcePayload(path=target, source=source)
                for target, source in sorted(selected.items())
            ),
            scan_roots=scan_roots,
            membership_sha256=membership,
        ),
    )


def backend_command(
    interpreter: Path, action: str, root: Path, *, output: Path | None = None
) -> dict[str, Any]:
    """Run the fixed backend bridge without inherited Python or build overrides."""
    command = [str(interpreter), "-I", str(BACKEND_SCRIPT), action, str(root)]
    if output is not None:
        command.extend(("--output", str(output)))
    environment = {"PATH": f"{interpreter.parent}{os.pathsep}/usr/bin{os.pathsep}/bin"}
    result = subprocess.run(
        command,
        cwd=root,
        env=environment,
        text=True,
        capture_output=True,
        check=True,
        timeout=120,
    )
    value = json.loads(result.stdout)
    if not isinstance(value, dict):
        msg = "Qualified build backend returned invalid evidence"
        raise ValueError(msg)
    return value


def _unsupported(detail: str) -> None:
    msg = (
        f"Unsupported local source profile: {detail}; supply an ordinary wheel instead"
    )
    raise ValueError(msg)
