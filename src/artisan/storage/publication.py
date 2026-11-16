"""Portable, sanitized inspection copies without execution or writer authority."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from artisan.storage._retained_publication import PublicationExport
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.core.store_read_scope import RetainedStoreReadScope

__all__ = ["PublicationExport", "export_publication"]


def export_publication(
    source: ArtifactStore,
    destination: str | Path,
    *,
    redact: Sequence[str],
) -> PublicationExport:
    """Copy verified results and run history into a portable inspection bundle.

    Scientific payloads, identities and declared lineage remain unchanged. The
    bundle omits worker locators and control authority. Operational fields may be
    redacted; matching protected evidence rejects the export. It can be moved and
    opened for inspection, but cannot be resumed, repaired or used as a cache.

    Args:
        source: Open supported store. Its registered schemas and configured
            filesystem must remain available; the source must be quiescent.
        destination: New or empty local directory outside the source roots.
        redact: Explicit substrings to omit from supported operational fields.

    Returns:
        An inspection bundle with verified carrier copies and an open_store method.

    Raises:
        ValueError: Source or destination is unsuitable, or redaction would change
            protected scientific evidence.
        StoreIntegrityError: Required evidence is inconsistent or changes during
            export. A failed export removes only its own incomplete bundle.
    """
    from artisan.orchestration.engine.publication_runs import export_retained_runs

    scope = source._read_scope
    if not isinstance(scope, RetainedStoreReadScope):
        msg = "Publication export requires an ordinary live store read scope"
        raise ValueError(msg)
    return export_retained_runs(
        RetainedReader(scope.committer), Path(destination), redact=tuple(redact)
    )
