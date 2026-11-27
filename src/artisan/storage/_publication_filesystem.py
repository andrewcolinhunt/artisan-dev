"""Read-only local filesystem confined to one portable inspection bundle."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, BinaryIO, cast

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem


class _PublicationFileSystem(AbstractFileSystem):  # type: ignore[misc]
    """Map published locators into one bundle without host-path fallback."""

    cachable = False  # codespell:ignore cachable
    protocol = ("file", "local")

    def __init__(self, directory: Path) -> None:
        super().__init__()
        self.directory = directory.resolve()
        self.local = LocalFileSystem()

    def _mapped(self, path: str) -> str:
        value = os.fspath(path)
        target = Path(value).resolve()
        if not target.is_relative_to(self.directory):
            msg = "Path is outside the publication bundle"
            raise PermissionError(msg)
        return str(target)

    def _open(self, path: str, mode: str = "rb", **kwargs: Any) -> BinaryIO:
        if mode != "rb":
            msg = "Publication files are read-only"
            raise PermissionError(msg)
        return cast(BinaryIO, self.local.open(self._mapped(path), mode=mode, **kwargs))

    def exists(self, path: str, **kwargs: Any) -> bool:
        return cast(bool, self.local.exists(self._mapped(path), **kwargs))

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        return cast(dict[str, Any], self.local.info(self._mapped(path), **kwargs))

    def isfile(self, path: str) -> bool:
        return cast(bool, self.local.isfile(self._mapped(path)))

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[Any]:
        return cast(
            list[Any], self.local.ls(self._mapped(path), detail=detail, **kwargs)
        )

    def mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        msg = "Publication files are read-only"
        raise PermissionError(msg)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        msg = "Publication files are read-only"
        raise PermissionError(msg)

    def rmdir(self, path: str) -> None:
        msg = "Publication files are read-only"
        raise PermissionError(msg)

    def rm_file(self, path: str) -> None:
        msg = "Publication files are read-only"
        raise PermissionError(msg)
