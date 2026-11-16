"""Assignment and all-units cache lookup over the same accepted-result evidence."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import partial

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.storage.cache.retained_identity import retained_cache_key
from artisan.storage.core.retained_reader import (
    ResultSelection,
    RetainedReader,
    _validate_unit_order,
)
from artisan.storage.io.admission import capture_admission, read_admission_record
from artisan.storage.io.retained_plan import RetainedCommitPlan
from artisan.storage.io.retained_read import RetainedReadSnapshot


@dataclass(frozen=True)
class RetainedCacheHit:
    """Verified source for current admitted work; this does not publish reuse."""

    admission: AdmissionReference
    cache_key: str
    receipt: SealedDelivery

    @property
    def selection(self) -> ResultSelection:
        return ResultSelection(self.admission.attempt.assignment, self.receipt)


def lookup_assignment(
    admission: AdmissionReference, *, reader: RetainedReader
) -> RetainedCacheHit | None:
    """Return a matching complete winner after verifying both frozen input snapshots."""
    with reader.committer._read_snapshot() as snapshot:
        key = _admitted_key(admission, reader, _read=snapshot)
        if key is None:
            return None
        return _lookup(
            admission, key, reader._completed_plans(snapshot), reader, snapshot
        )


def lookup_step(
    admissions: Sequence[AdmissionReference], *, reader: RetainedReader
) -> tuple[RetainedCacheHit, ...] | None:
    """Return all current ordered units only when every unit has an eligible hit.

    Sources may come from different historical steps, including successful units
    of a partially successful step. This neither asserts historical step completion
    nor copies historical counts or metadata. The controller must durably publish
    the current finite unit selection before making it visible.
    """
    if not admissions:
        return None
    _validate_unit_order(tuple(item.attempt.assignment for item in admissions))
    with reader.committer._read_snapshot() as snapshot:
        keys = [_admitted_key(item, reader, _read=snapshot) for item in admissions]
        if any(key is None for key in keys):
            return None
        candidates = reader._completed_plans(snapshot)
        hits = []
        for admission, key in zip(admissions, keys, strict=True):
            assert key is not None
            hit = _lookup(admission, key, candidates, reader, snapshot)
            if hit is None:
                return None
            hits.append(hit)
        return tuple(hits)


def _admitted_key(
    admission: AdmissionReference,
    reader: RetainedReader,
    *,
    _read: RetainedReadSnapshot | None = None,
) -> str | None:
    admission = AdmissionReference.model_validate(admission)
    committer = reader.committer
    if _read is not None:
        _read.require_committer(committer)
    state = committer.journal.state(admission.attempt.assignment)
    if state is None or (state.attempt, state.admission_digest) != (
        admission.attempt,
        admission.record_digest,
    ):
        msg = "Cache lookup requires the exact current journal-bound admission"
        raise StoreIntegrityError(msg)
    if state.cancelled:
        return None
    with capture_admission(
        admission,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
        verify_producer=partial(committer._verify_producer, _read=_read),
    ) as captured:
        return retained_cache_key(
            captured.record.recipe, admission.attempt, captured.record.boundary
        )


def _lookup(
    admission: AdmissionReference,
    key: str,
    candidates: Sequence[RetainedCommitPlan],
    reader: RetainedReader,
    snapshot: RetainedReadSnapshot,
) -> RetainedCacheHit | None:
    committer = reader.committer
    for candidate in candidates:
        metadata = read_admission_record(
            candidate.admission, delta_root=committer.delta_root, fs=committer.fs
        )
        if (
            retained_cache_key(metadata.recipe, metadata.attempt, metadata.boundary)
            != key
        ):
            continue
        with reader._capture_inputs(candidate.receipt, snapshot):
            return RetainedCacheHit(admission, key, candidate.receipt)
    return None
