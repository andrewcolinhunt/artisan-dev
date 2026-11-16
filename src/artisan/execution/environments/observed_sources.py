"""Observe bounded installed editable import roots without inventing build inputs."""

from __future__ import annotations

import importlib.machinery
import importlib.metadata
import json
from collections.abc import Iterator, Sequence
from pathlib import Path
from types import ModuleType
from urllib.parse import unquote, urlparse

from packaging.utils import canonicalize_name


def observed_editable_owners(modules: Sequence[ModuleType]) -> tuple[str, ...]:
    """Find candidate literal editable owners for already imported live modules.

    Plain Hatch editables may advertise only a path file, without top-level module
    metadata. This lookup does not qualify those paths; the ordinary full source
    observation and exact loaded-file ownership check still follow.
    """
    paths = {
        Path(filename).resolve()
        for module in modules
        if (filename := getattr(module, "__file__", None)) is not None
    }
    if not paths:
        return ()
    names = set()
    for distribution in importlib.metadata.distributions():
        direct = json.loads(distribution.read_text("direct_url.json") or "{}")
        if not direct.get("dir_info", {}).get("editable", False):
            continue
        location = urlparse(str(direct.get("url", "")))
        if location.scheme != "file" or location.netloc:
            continue
        project = Path(unquote(location.path)).resolve()
        if not any(path.is_relative_to(project) for path in paths):
            continue
        if any(
            Path(line).is_absolute()
            and any(path.is_relative_to(Path(line).resolve()) for path in paths)
            for line in _path_lines(distribution)
        ) and (name := distribution.metadata.get("Name")):
            names.add(canonicalize_name(name))
    return tuple(sorted(names))


def observed_editable_sources(names: Sequence[str]) -> dict[str, dict[str, Path]]:
    """Measure literal path-based editables from their installed import metadata.

    These are observations of the existing interpreter, not canonical package
    selections or a promise of equivalence to a built wheel. Executable import
    hooks and source paths outside their declared project are unsupported.
    """
    owners = importlib.metadata.packages_distributions()
    sources: dict[str, dict[str, Path]] = {}
    for name in names:
        normalized = canonicalize_name(name)
        distribution = importlib.metadata.distribution(normalized)
        direct = json.loads(distribution.read_text("direct_url.json") or "{}")
        if not direct.get("dir_info", {}).get("editable", False):
            continue
        modules = tuple(
            sorted(
                module
                for module, packages in owners.items()
                if normalized in {canonicalize_name(item) for item in packages}
            )
        )
        sources[normalized] = _editable_files(distribution, direct, modules)
    return sources


def _editable_files(
    distribution: importlib.metadata.Distribution,
    direct: dict[str, object],
    modules: tuple[str, ...],
) -> dict[str, Path]:
    location = urlparse(str(direct.get("url", "")))
    if location.scheme != "file" or location.netloc:
        msg = "Observed editable code requires local installed import roots"
        raise ValueError(msg)
    project = Path(unquote(location.path)).resolve(strict=True)
    roots = _editable_roots(distribution, project)
    if not modules:
        modules = _import_names(roots)
    payload: dict[str, Path] = {}
    for root in sorted(roots):
        for module in modules:
            if not module.isidentifier():
                msg = "Observed editable import root must be a module name"
                raise ValueError(msg)
            directory = root / module
            if directory.is_dir():
                for path in sorted(directory.rglob("*")):
                    if "__pycache__" not in path.relative_to(root).parts:
                        _include(payload, root, path)
            for suffix in importlib.machinery.all_suffixes():
                path = root / f"{module}{suffix}"
                if path.exists() or path.is_symlink():
                    _include(payload, root, path)
    if not payload:
        msg = "Observed editable import payload is empty"
        raise ValueError(msg)
    return payload


def _path_lines(distribution: importlib.metadata.Distribution) -> Iterator[str]:
    for record in distribution.files or ():
        if record.suffix == ".pth":
            yield from (
                line
                for line in Path(str(distribution.locate_file(record)))
                .read_text()
                .splitlines()
                if line and not line.startswith("#")
            )


def _editable_roots(
    distribution: importlib.metadata.Distribution, project: Path
) -> set[Path]:
    roots = set()
    for line in _path_lines(distribution):
        path = Path(line)
        if (
            line.startswith(("import ", "import\t"))
            or not path.is_absolute()
            or path.resolve(strict=True) != path
            or not path.is_relative_to(project)
            or not path.is_dir()
        ):
            msg = "Observed worker cannot qualify executable or indirect editable hooks; prepare an explicit worker"
            raise ValueError(msg)
        roots.add(path)
    if not roots:
        msg = "Observed editable code has no literal installed path mapping"
        raise ValueError(msg)
    return roots


def _import_names(roots: set[Path]) -> tuple[str, ...]:
    names = set()
    suffixes = importlib.machinery.all_suffixes()
    for root in roots:
        for child in root.iterdir():
            if child.is_dir() and any(
                (child / f"__init__{suffix}").is_file() for suffix in suffixes
            ):
                names.add(child.name)
            elif child.is_file():
                names.update(
                    child.name.removesuffix(suffix)
                    for suffix in suffixes
                    if child.name.endswith(suffix)
                )
    return tuple(sorted(name for name in names if name.isidentifier()))


def _include(payload: dict[str, Path], root: Path, path: Path) -> None:
    if path.is_symlink() or path.resolve() != path or not path.is_relative_to(root):
        msg = "Observed editable imports cannot contain symbolic links"
        raise ValueError(msg)
    if path.is_dir():
        return
    if not path.is_file():
        msg = "Observed editable imports must be regular files"
        raise ValueError(msg)
    relative = path.relative_to(root).as_posix()
    if relative in payload and payload[relative] != path:
        msg = "Observed editable import mappings overlap"
        raise ValueError(msg)
    payload[relative] = path
