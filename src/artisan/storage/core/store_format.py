"""Strict manifest gate for the coordinated Artisan store format."""

from __future__ import annotations

import json
from typing import Any

from fsspec import AbstractFileSystem

from artisan.errors import IncompatibleStoreError
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.utils.path import uri_join

STORE_MANIFEST = {
    "store_format": 2,
    "artifact_identity": 1,
    "cache_identity": 2,
}
STORE_MANIFEST_PATH = "_artisan/store.json"


def assert_store_format(delta_root: str, fs: AbstractFileSystem) -> None:
    """Require the exact supported store manifest before normal access."""
    manifest_path = uri_join(delta_root, STORE_MANIFEST_PATH)
    if not fs.exists(manifest_path):
        detail = "missing manifest"
        raise _incompatible(detail)
    try:
        with fs.open(manifest_path, "r") as stream:
            found = json.load(stream)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        detail = f"malformed manifest: {exc}"
        raise _incompatible(detail) from exc
    if found != STORE_MANIFEST:
        detail = f"found {found!r}"
        raise _incompatible(detail)


def prepare_store_initialization(
    delta_root: str,
    fs: AbstractFileSystem,
) -> bool:
    """Validate an existing store or approve creation at an empty root.

    Returns:
        True when initialization must publish a new manifest after creating
        tables; False when the supported manifest already exists.
    """
    manifest_path = uri_join(delta_root, STORE_MANIFEST_PATH)
    if fs.exists(manifest_path):
        assert_store_format(delta_root, fs)
        return False
    if fs.exists(delta_root) and fs.ls(delta_root, detail=False):
        detail = "Delta root is not empty and has no manifest"
        raise _incompatible(detail)
    if any(fs.exists(uri_join(delta_root, path)) for path in _known_table_paths()):
        detail = "known Artisan tables exist without a manifest"
        raise _incompatible(detail)
    return True


def publish_store_manifest(delta_root: str, fs: AbstractFileSystem) -> None:
    """Publish the exact supported manifest after empty tables exist."""
    manifest_path = uri_join(delta_root, STORE_MANIFEST_PATH)
    parent = uri_join(delta_root, "_artisan")
    fs.makedirs(parent, exist_ok=True)
    with fs.open(manifest_path, "w") as stream:
        json.dump(STORE_MANIFEST, stream, sort_keys=True, separators=(",", ":"))


def _known_table_paths() -> list[str]:
    """Return framework and registered content-table paths."""
    return [
        *(member.value for member in TablePath),
        *(type_def.table_path for type_def in ArtifactTypeDef.get_all().values()),
    ]


def _incompatible(detail: str) -> IncompatibleStoreError:
    """Build the consistent clean-break error message."""
    required: dict[str, Any] = STORE_MANIFEST
    return IncompatibleStoreError(
        f"Incompatible Artisan store ({detail}); required {required!r}. "
        "Use a new Delta root for this release."
    )
