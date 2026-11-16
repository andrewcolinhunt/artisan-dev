"""The sole supported Artisan store domain; historical roots are never relabeled."""

from __future__ import annotations

from fsspec import AbstractFileSystem

from artisan.errors import IncompatibleStoreError
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.utils.hashing import canonical_json_bytes
from artisan.utils.path import uri_join

STORE_MANIFEST_PATH = "_artisan/store.json"
_MANIFEST_BYTES = canonical_json_bytes(
    {"domain": "artisan.retained-store", "schema_version": 1}
)


def assert_retained_store(root: str, fs: AbstractFileSystem) -> None:
    """Require the exact supported domain without initializing or relabeling it."""
    path = uri_join(root, STORE_MANIFEST_PATH)
    if not fs.exists(path):
        msg = "Retained-result store has no domain manifest"
        raise IncompatibleStoreError(msg)
    with fs.open(path, "rb", cache_type="none") as stream:
        found = stream.read()
    if found != _MANIFEST_BYTES:
        msg = "Store is outside the retained-result domain; relabeling is not migration"
        raise IncompatibleStoreError(msg)


def initialize_retained_store(root: str, fs: AbstractFileSystem) -> None:
    """Claim an empty root; component initializers then create their own tables.

    Re-entry is explicit and idempotent. A manifest alone does not assert that
    component tables exist; their readers must validate their physical contracts.
    """
    path = uri_join(root, STORE_MANIFEST_PATH)
    if fs.exists(path):
        assert_retained_store(root, fs)
        return
    if fs.exists(root) and fs.ls(root, detail=False):
        msg = "Cannot initialize retained results in an unlabeled nonempty root"
        raise IncompatibleStoreError(msg)
    publish_immutable_bytes(fs, path, _MANIFEST_BYTES)
    assert_retained_store(root, fs)


def assert_writable_store(delta_root: str, fs: AbstractFileSystem) -> None:
    """Reject pipeline writes and repairs to a labeled inspection export."""
    if fs.exists(uri_join(delta_root, "_artisan", "publication.json")):
        msg = "Publication stores are for inspection; start a new pipeline root"
        raise IncompatibleStoreError(msg)
