"""Observe complete logical views during authored curator work and stream drain."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.execution.curator_cache import (
    CuratorCacheExclusion,
    CuratorCacheProof,
    CuratorCarrierRead,
    CuratorViewFingerprint,
)
from artisan.storage.cache.curator_identity import curator_cache_key
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.io.curator_external_reads import verify_curator_carrier_read
from artisan.utils.hashing import compute_stream_digest


class CuratorReadRecorder:
    """Observe separate authored scopes, then finish only after complete success.

    The caller scopes initial execution and each iterator next/close separately,
    leaving framework validation and staging outside those scopes. The caller
    must establish successful stream exhaustion before requesting finish.
    """

    def __init__(self, view: CuratorView, admission: CuratorAdmission) -> None:
        if view.snapshot != admission.view:
            msg = "Curator observation requires its exact admitted view"
            raise ValueError(msg)
        key = curator_cache_key(admission)
        if key is None:
            msg = "Curator cache observation requires admitted code and software qualification"
            raise ValueError(msg)
        view.bind_admission(admission)
        self._view, self._admission, self._key = view, admission, key
        self.views: set[tuple[str, bool]] = set()
        self.exclusions: set[CuratorCacheExclusion] = set()
        self._proof: CuratorCacheProof | None = None
        self._failed = False
        self._started = False
        self._pending_carrier_reads = 0
        self._carrier_reads: dict[tuple[str, str], CuratorCarrierRead] = {}
        if _external_associations(admission):
            self.exclusions.add("untracked_external_content")

    def begin_carrier_read(self) -> None:
        """Require evidence for one conversion that exposes an external descriptor.

        Declared content IDs do not expose bytes. Each actual conversion scope
        counts separately so reading a duplicate cannot erase an earlier bypass.
        """
        if self._view._read_recorder is not self or self._failed:
            msg = "Curator carrier capture requires an active authored scope"
            raise RuntimeError(msg)
        self._pending_carrier_reads += 1

    def record_carrier_read(self, read: CuratorCarrierRead) -> None:
        """Qualify a supported read only after its capture scope closes normally."""
        if (
            self._view._read_recorder is not self
            or self._failed
            or self._pending_carrier_reads == 0
        ):
            msg = "Curator carrier reads require an active successful authored scope"
            raise RuntimeError(msg)
        key = read.artifact_id, read.uri
        if self._carrier_reads.setdefault(key, read) != read:
            msg = "Curator read changed one carrier identity within its invocation"
            raise StoreIntegrityError(msg)
        self._pending_carrier_reads -= 1

    @contextmanager
    def authored(self) -> Iterator[None]:
        """Observe one authored call, including normal iterator exhaustion."""
        if (
            self._view._read_recorder is not None
            or self._proof is not None
            or self._failed
        ):
            msg = "Curator observation requires an unfinished recorder and no active scope"
            raise RuntimeError(msg)
        self._view._read_recorder = self
        self._started = True
        try:
            yield
        except BaseException as error:
            if not isinstance(error, StopIteration):
                self._failed = True
            raise
        finally:
            self._view._read_recorder = None

    @property
    def proof(self) -> CuratorCacheProof:
        """Return evidence only after explicit successful completion."""
        if self._proof is None:
            msg = "Curator read observation has not completed successfully"
            raise RuntimeError(msg)
        return self._proof

    def finish(self) -> CuratorCacheProof:
        """Seal observed reads after successful authored execution and stream closure."""
        if self._view._read_recorder is not None or self._failed or not self._started:
            msg = "Unstarted, active or failed curator observation cannot finish"
            raise RuntimeError(msg)
        if self._proof is not None:
            return self._proof
        fingerprints = tuple(
            fingerprint_curator_view(self._view, table, direct=direct)
            for table, direct in sorted(self.views)
        )
        if any(item.content_digest is None for item in fingerprints):
            self.exclusions.add("unsupported_view_schema")
        if self._pending_carrier_reads:
            self.exclusions.add("untracked_external_content")
        assert self._admission.definition_digest is not None
        assert self._admission.software_digest is not None
        self._proof = CuratorCacheProof(
            definition_digest=self._admission.definition_digest,
            software_digest=self._admission.software_digest,
            cache_key=self._key,
            snapshot_digest=self._view.snapshot.digest,
            views=fingerprints,
            carrier_reads=tuple(
                self._carrier_reads[key] for key in sorted(self._carrier_reads)
            ),
            ineligible_reasons=tuple(sorted(self.exclusions)),
        )
        return self._proof


@contextmanager
def observe_curator_reads(
    view: CuratorView,
    admission: CuratorAdmission,
) -> Iterator[CuratorReadRecorder]:
    """Observe one contiguous authored scope and finish after it returns normally."""
    recorder = CuratorReadRecorder(view, admission)
    with recorder.authored():
        yield recorder
    recorder.finish()


def fingerprint_curator_view(
    view: CuratorView, table_path: str, *, direct: bool = False
) -> CuratorViewFingerprint:
    """Hash canonical scalar rows without retaining Python payload rows.

    IPC string views can preserve backing-buffer order after logical sorting.
    Normalize values in bulk before streaming JSON; the separate original schema
    binds their types, and temporal physical integers preserve nanoseconds.
    """
    if direct and table_path != TablePath.ARTIFACT_EDGES.value:
        msg = "Direct view is only defined for artifact relationships"
        raise ValueError(msg)
    frame = view.artifact_edges(direct=True) if direct else view.scan(table_path)
    schema = frame.collect_schema()
    columns = sorted(schema.names())
    digest = None
    if all(_sortable(dtype) for dtype in schema.dtypes()):
        with TemporaryDirectory(prefix="artisan-curator-query-") as directory:
            path = Path(directory) / "view.jsonl"
            frame.select(
                _canonical_scalar(name, schema[name]) for name in columns
            ).sort(columns).sink_ndjson(path, maintain_order=True, engine="streaming")
            with path.open("rb") as stream:
                digest, _ = compute_stream_digest(stream)
    return CuratorViewFingerprint(
        table_path=table_path,
        direct=direct,
        present=view.snapshot.table(table_path) is not None,
        schema_signature=tuple((name, str(schema[name])) for name in columns),
        content_digest=digest,
    )


def _canonical_scalar(name: str, dtype: pl.DataType) -> pl.Expr:
    value = pl.col(name)
    if dtype == pl.Binary:
        return value.bin.encode("hex")
    if dtype.is_temporal():
        value = value.to_physical()
    return value.cast(pl.String)


def validate_curator_cache_proof(
    proof: CuratorCacheProof, view: CuratorView, *, admission: CuratorAdmission
) -> None:
    """Recheck claimed dependencies against the exact admitted source snapshot.

    Completeness of observation belongs to the trusted closed execution return;
    this validator cannot infer that an arbitrary empty proof observed no reads.
    Primary IDs alone require no byte witness. Actual conversion scopes and
    untracked hydration record their obligations before exposing file content.
    """
    proof = CuratorCacheProof.model_validate(proof)
    if (
        _external_associations(admission)
        and "untracked_external_content" not in proof.ineligible_reasons
    ):
        msg = "Prehydrated external associations lack qualified read evidence"
        raise StoreIntegrityError(msg)
    if (
        view.snapshot != admission.view
        or proof.snapshot_digest != admission.view.digest
        or proof.definition_digest != admission.definition_digest
        or proof.software_digest != admission.software_digest
        or proof.cache_key != curator_cache_key(admission)
    ):
        msg = "Curator cache proof differs from its admitted science or snapshot"
        raise StoreIntegrityError(msg)
    view.bind_admission(admission)
    if not curator_views_match(proof, view):
        msg = "Curator cache proof differs from its exact admitted query results"
        raise StoreIntegrityError(msg)


def curator_views_match(proof: CuratorCacheProof, view: CuratorView) -> bool:
    """Compare recorded views with a separately frozen current visibility boundary."""
    if not all(
        fingerprint_curator_view(view, item.table_path, direct=item.direct) == item
        for item in proof.views
    ):
        return False
    for read in proof.carrier_reads:
        verify_curator_carrier_read(view, read)
    return True


def _external_associations(admission: CuratorAdmission) -> set[str]:
    return {
        artifact_id
        for association in admission.associations
        for artifact_id in association.artifact_ids
        if cast(
            type[Artifact], ArtifactTypeDef.get(association.artifact_type).model
        ).EXTERNALLY_BACKED
    }


def _sortable(dtype: pl.DataType) -> bool:
    return (
        dtype.is_numeric()
        or dtype.is_temporal()
        or dtype in (pl.Boolean, pl.Binary, pl.String, pl.Null)
    )
