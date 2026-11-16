"""Conservative source membership checks without executing a build backend."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

from artisan.execution.environments.source import source_entry
from artisan.utils.hashing import canonical_json_bytes


def source_membership(root: Path, scan_roots: tuple[str, ...]) -> str:
    """Hash bounded names and types while leaving excluded file contents unread.

    Preparation uses Hatch's actual selection. Unchanged selector metadata,
    selected bytes and this inventory permit pure lookup without reevaluating
    its patterns. Adding an excluded file causes a conservative miss. Interpreter
    cache directories are ignored and cannot be packaged by this source profile.
    """
    entries: dict[str, str] = {}
    for member in root.iterdir():
        if member.name in {
            ".git",
            ".pixi",
            "__pycache__",
            ".artisan-native.json",
            ".artisan-wheels",
        }:
            continue
        kind = (
            "symlink"
            if member.is_symlink()
            else "directory"
            if member.is_dir()
            else "file"
            if member.is_file()
            else "other"
        )
        entries[member.name] = kind
    for name in scan_roots:
        entry = source_entry(root, name)
        if not isinstance(entry, Path):
            msg = "Local source membership requires a filesystem package"
            raise TypeError(msg)
        _record(entries, root, entry)
        if entry.is_dir():
            for directory, children, files in os.walk(entry, followlinks=False):
                children[:] = sorted(
                    child for child in children if child != "__pycache__"
                )
                for child in [*children, *files]:
                    _record(entries, root, Path(directory) / child)
    for name in (".gitignore", ".hgignore"):
        entry = root / name
        if entry.exists() or entry.is_symlink():
            _record(entries, root, entry)
        else:
            if any((parent / name).exists() for parent in root.parents):
                msg = (
                    "Local source ignore rules outside the package root are unsupported"
                )
                raise ValueError(msg)
            entries[name] = "missing"
    return hashlib.sha256(canonical_json_bytes(entries)).hexdigest()


def _record(entries: dict[str, str], root: Path, path: Path) -> None:
    if path.is_symlink():
        msg = "Bounded local source trees cannot contain symbolic links"
        raise ValueError(msg)
    if path.is_dir():
        kind = "directory"
    elif path.is_file():
        kind = "file"
    else:
        msg = "Bounded local source input is missing or not a regular file"
        raise ValueError(msg)
    entries[path.relative_to(root).as_posix()] = kind
