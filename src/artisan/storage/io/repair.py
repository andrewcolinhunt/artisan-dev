"""Inspect supported stores and explicitly finish already selected publications."""

from __future__ import annotations

from fsspec import AbstractFileSystem
from fsspec.utils import get_protocol

from artisan.schemas.artifact.external import validate_persistable_uri
from artisan.storage.io.repair_report import RepairReport
from artisan.storage.io.retained_recovery import apply_retained_repair
from artisan.storage.io.retained_repair import inspect_retained_store
from artisan.storage.io.retained_store import open_configured_store

__all__ = ["RepairReport", "repair_store"]


def repair_store(
    *,
    delta_root: str,
    staging_root: str,
    fs: AbstractFileSystem,
    storage_options: dict[str, str] | None = None,
    apply: bool = False,
    preserve_staging: bool = False,
    files_root: str | None = None,
) -> RepairReport:
    """Report evidence or finish durable selections under exclusive offline ownership.

    Inspection changes no storage. Applying repair first audits the store, then
    finishes only existing winners and immutable plans using ordinary recovery.
    Unclaimed staging and unresolved worker ownership remain unchanged. Repair
    never chooses a different winner, infers worker exit or retries science.

    Args:
        delta_root: Existing supported store.
        staging_root: Worker staging location on the configured backend.
        fs: Filesystem for metadata and artifact carriers.
        storage_options: Delta-rs options for the same backend.
        apply: Explicitly finish recoverable selected publications after audit.
        preserve_staging: Keep verified staging after successful recovery.
        files_root: Managed carrier root; required for nonlocal or custom layouts.

    Returns:
        Evidence findings, including unresolved and blocking conditions.
    """
    validate_persistable_uri(staging_root)
    protocols = fs.protocol
    protocols = (protocols,) if isinstance(protocols, str) else protocols
    if not staging_root.strip() or get_protocol(staging_root) not in protocols:
        msg = "Repair staging root must use the configured storage backend"
        raise ValueError(msg)
    committer = open_configured_store(
        delta_root, files_root=files_root, fs=fs, storage_options=storage_options
    )
    if apply:
        return apply_retained_repair(
            committer, staging_root=staging_root, preserve_staging=preserve_staging
        )
    return inspect_retained_store(committer, staging_root=staging_root)
