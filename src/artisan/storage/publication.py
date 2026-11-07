"""Portable, sanitized inspection copies of completed current-format stores."""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, cast

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from pydantic import BaseModel, ConfigDict, Field

from artisan.storage._publication_policy import PublicationPolicy
from artisan.storage.core.artifact_store import ArtifactStore

PUBLICATION_MANIFEST = "_artisan/publication.json"


class _File(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    artifact_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    path: str
    uri: str
    content_hash: str = Field(pattern=r"^[0-9a-f]{32}$")
    size_bytes: int = Field(ge=0)


class _Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: int = 1
    purpose: str = "sanitized_inspection"
    files: list[_File]


# Fsspec does not provide type stubs for its filesystem base classes.
class _PublicationFileSystem(AbstractFileSystem):  # type: ignore[misc]
    """Map published locators into one bundle without host-path fallback."""

    cachable = False
    protocol = ("file", "local")

    def __init__(self, directory: Path, files: Sequence[_File]) -> None:
        super().__init__()
        self.directory = directory.resolve()
        self.locations = {item.uri: str(self.directory / item.path) for item in files}
        self.local = LocalFileSystem()

    def _mapped(self, path: str) -> str:
        value = os.fspath(path)
        target = Path(self.locations.get(value, value)).resolve()
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


@dataclass(frozen=True)
class PublicationExport:
    """Location of a portable inspection bundle, independent of its original root."""

    directory: Path

    @property
    def delta_root(self) -> Path:
        """Return the new current-format Delta root."""
        return self.directory / "delta"

    @property
    def files_root(self) -> Path:
        """Return the verified copies of externally backed artifact bytes."""
        return self.directory / "files"

    @property
    def staging_root(self) -> Path:
        """Return the retained, newly sealed publication evidence."""
        return self.directory / "staging"

    def open_store(self) -> ArtifactStore:
        """Open this bundle with its confined portable file-location mapping.

        Pass the returned store's ``filesystem`` to FileRef reads and
        materialization. Application artifact types must already be registered.
        """
        manifest = _Manifest.model_validate_json(
            (self.delta_root / PUBLICATION_MANIFEST).read_bytes()
        )
        if manifest.schema_version != 1 or manifest.purpose != "sanitized_inspection":
            msg = "Unsupported publication manifest"
            raise ValueError(msg)
        for item in manifest.files:
            path = Path(item.path)
            if path.is_absolute() or ".." in path.parts or path.parts[:1] != ("files",):
                msg = "Invalid publication file path"
                raise ValueError(msg)
            if (
                len(path.parts) != 3
                or path.parts[1] != item.artifact_id
                or str(path) != item.path
                or "\\" in item.path
                or "\x00" in item.path
                or item.uri != f"/publication/{item.path}"
            ):
                msg = "Invalid publication file locator"
                raise ValueError(msg)
        if len({item.artifact_id for item in manifest.files}) != len(manifest.files):
            msg = "Duplicate publication artifact"
            raise ValueError(msg)
        if len({item.uri for item in manifest.files}) != len(manifest.files):
            msg = "Duplicate publication file locator"
            raise ValueError(msg)
        return ArtifactStore(
            str(self.delta_root),
            fs=_PublicationFileSystem(self.directory, manifest.files),
            files_root=str(self.files_root),
        )


def _destination(source: ArtifactStore, destination: str | Path) -> Path:
    path = Path(destination).absolute()
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        msg = "Publication destination must not traverse symlinks"
        raise ValueError(msg)
    path = path.resolve()
    roots = [source.base_path, source.files_root]
    for root in filter(None, roots):
        if "://" in root and not root.startswith(("file://", "local://")):
            continue
        original = Path(LocalFileSystem._strip_protocol(root)).resolve()
        if path.is_relative_to(original) or original.is_relative_to(path):
            msg = "Publication destination overlaps the source store"
            raise ValueError(msg)
    if path.exists() and (not path.is_dir() or any(path.iterdir())):
        msg = "Publication destination must be new or empty"
        raise ValueError(msg)
    return path


def export_publication(
    source: ArtifactStore,
    destination: str | Path,
    *,
    redact: Sequence[str],
) -> PublicationExport:
    """Export completed evidence into a new sanitized inspection store.

    Artifact content, metadata, IDs, lineage, metrics and historical run IDs
    remain unchanged. File locators become portable. Matching replay config
    values become explicit required replacements; operational logs and literal
    input references are redacted. Matching protected evidence causes failure.
    This is not a general secret scanner: review scientific payloads separately.

    Args:
        source: Open, quiescent current-format store with registered artifact
            types and a filesystem capable of reading its external files.
        destination: New or empty local directory outside source roots.
        redact: Explicit substrings to omit from supported operational fields.
            Neither these values nor original filesystem paths enter the receipt.

    Returns:
        A portable bundle containing Delta tables, verified external files and
        newly sealed staging evidence. Its store supports inspection, not resume.

    Raises:
        ValueError: Destination/policy is invalid or redaction overlaps protected
            evidence. A failed export removes only its own new bundle contents.
        StoreIntegrityError: Source evidence is incomplete, corrupt or changing,
            or the rebuilt destination fails integrity verification.
        ArtifactIntegrityError: Scientific artifact identity or file bytes fail
            verification. No source file is modified.
    """
    from artisan.storage._publication_writer import write_publication

    policy = PublicationPolicy.create(redact)
    target = _destination(source, destination)
    existed = target.exists()
    target.mkdir(parents=True, exist_ok=True)
    try:
        files = write_publication(source, target, policy)
        manifest = _Manifest(files=files)
        (target / "delta" / PUBLICATION_MANIFEST).write_text(
            json.dumps(manifest.model_dump(), indent=2) + "\n"
        )
    except BaseException:
        shutil.rmtree(target)
        if existed:
            target.mkdir()
        raise
    return PublicationExport(target)
