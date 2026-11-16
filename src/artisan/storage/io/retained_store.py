"""Explicit initialization and read-only opening of the retained store domain."""

from __future__ import annotations

import polars as pl
from fsspec import AbstractFileSystem, get_filesystem_class
from fsspec.utils import get_protocol

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.external import validate_persistable_uri
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.core.assignment_schema import ASSIGNMENT_DECISIONS_PATH
from artisan.storage.core.store_format import (
    assert_writable_store,
    initialize_retained_store,
)
from artisan.storage.core.table_schemas import (
    FRAMEWORK_SCHEMAS,
    get_physical_schema_for_path,
)
from artisan.storage.io.assignment_journal import AssignmentJournal
from artisan.storage.io.delta_files import DeltaSnapshot
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_plan import retained_table_paths
from artisan.utils.path import uri_join, uri_parent


def resolve_store_files_root(
    delta_root: str,
    files_root: str | None,
    *,
    protocols: str | tuple[str, ...],
) -> str:
    """Validate a read binding, defaulting only local stores to sibling files.

    This performs no filesystem access. A custom or nonlocal carrier root must
    come from the caller, never from artifact locations stored in the database.
    """
    protocols = (protocols,) if isinstance(protocols, str) else protocols
    if not delta_root.strip():
        msg = "Storage requires nonempty Delta and files roots"
        raise ValueError(msg)
    if files_root is None:
        if "file" not in protocols:
            msg = "Nonlocal storage requires an explicit files_root"
            raise ValueError(msg)
        files_root = uri_join(uri_parent(delta_root), "files")
    for root in (delta_root, files_root):
        if not root.strip():
            msg = "Storage requires nonempty Delta and files roots"
            raise ValueError(msg)
        validate_persistable_uri(root)
        if get_protocol(root) not in protocols:
            msg = "Root protocol differs from the configured storage backend"
            raise ValueError(msg)
    return files_root


def open_configured_store(
    delta_root: str,
    *,
    files_root: str | None = None,
    storage: StorageConfig | None = None,
    fs: AbstractFileSystem | None = None,
    storage_options: dict[str, str] | None = None,
) -> RetainedCommitter:
    """Open a configured read binding without initialization or recovery."""
    if storage is not None and (fs is not None or storage_options is not None):
        msg = "Supply StorageConfig or filesystem/storage_options, not both"
        raise ValueError(msg)
    delta_root = str(delta_root)
    files_root = None if files_root is None else str(files_root)
    storage = StorageConfig() if storage is None else storage
    files_root = resolve_store_files_root(
        delta_root,
        files_root,
        protocols=fs.protocol
        if fs is not None
        else get_filesystem_class(storage.protocol).protocol,
    )
    return open_retained_store(
        delta_root,
        files_root,
        fs=storage.filesystem() if fs is None else fs,
        storage_options=storage.delta_storage_options()
        if storage_options is None
        else storage_options,
    )


def create_retained_store(
    delta_root: str,
    files_root: str,
    *,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
) -> RetainedCommitter:
    """Create all registered tables or finish the same domain's initialization.

    The caller must register trusted application schemas first and exclusively own
    initialization. Missing tables can be created only while every existing table
    is empty. Foreign stores, inspection publications and unlabeled nonempty roots
    reject. This does not recover selected results or accept staged deliveries.
    """
    _validate_roots(delta_root, files_root, fs)
    initialize_retained_store(delta_root, fs)
    committer = _committer(delta_root, files_root, fs, storage_options)
    required = _required_tables()
    existing = tuple(
        _validate_table(committer, path)
        for path in required
        if fs.exists(uri_join(delta_root, path, "_delta_log"))
    )
    if len(existing) != len(required) and any(_has_rows(table) for table in existing):
        msg = "Cannot initialize missing tables after retained store data was written"
        raise StoreIntegrityError(msg)
    committer.journal.initialize()
    committer.initialize()
    initialize_framework_tables(committer)
    return committer


def open_retained_store(
    delta_root: str,
    files_root: str,
    *,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
) -> RetainedCommitter:
    """Validate every registered physical table without changing the store.

    Roots and backend options are supplied explicitly; no locator or recorded
    module authorizes their discovery. Opening checks schemas, not all historical
    payloads. Selected reads and mutations retain their own exact evidence checks.
    Missing tables reject rather than triggering initialization or recovery.
    """
    _validate_roots(delta_root, files_root, fs)
    committer = _committer(delta_root, files_root, fs, storage_options)
    for path in _required_tables():
        if not fs.exists(uri_join(delta_root, path, "_delta_log")):
            msg = f"Retained store is missing required table {path!r}"
            raise StoreIntegrityError(msg)
        _validate_table(committer, path)
    return committer


def initialize_framework_tables(committer: RetainedCommitter) -> None:
    """Explicitly initialize the shared curator and lifecycle physical tables."""
    for table in FRAMEWORK_SCHEMAS:
        path = uri_join(committer.delta_root, table.value)
        if not committer.fs.exists(uri_join(path, "_delta_log")):
            pl.DataFrame(schema=get_physical_schema_for_path(table)).write_delta(
                path, storage_options=committer.storage_options
            )
        committer._snapshot(table.value)


def _required_tables() -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                *retained_table_paths(),
                *(table.value for table in FRAMEWORK_SCHEMAS),
                ASSIGNMENT_DECISIONS_PATH,
            }
        )
    )


def _validate_table(committer: RetainedCommitter, path: str) -> DeltaSnapshot:
    if path == ASSIGNMENT_DECISIONS_PATH:
        return committer.journal._snapshot()
    return committer._snapshot(path)


def _has_rows(table: DeltaSnapshot) -> bool:
    return any(
        not frame.is_empty()
        for frame in table.iter_frames(columns=[next(iter(table.schema))])
    )


def _validate_roots(delta_root: str, files_root: str, fs: AbstractFileSystem) -> None:
    if not delta_root.strip() or not files_root.strip():
        msg = "Retained stores require explicit nonempty Delta and files roots"
        raise ValueError(msg)
    validate_persistable_uri(delta_root)
    validate_persistable_uri(files_root)
    assert_writable_store(delta_root, fs)


def _committer(
    delta_root: str,
    files_root: str,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None,
) -> RetainedCommitter:
    options = dict(storage_options or {})
    journal = AssignmentJournal(delta_root, fs=fs, storage_options=options)
    return RetainedCommitter(
        delta_root, files_root, journal, fs=fs, storage_options=options
    )
