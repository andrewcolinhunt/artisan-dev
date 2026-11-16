"""Verify the exact proposed reuse source independently of its journal decision."""

from __future__ import annotations

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.storage.cache.retained_identity import retained_cache_key
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.admission import capture_admission


def verify_cache_source(
    admission: AdmissionReference,
    receipt: SealedDelivery,
    *,
    reader: RetainedReader,
) -> None:
    """Prove this source's complete result and exact admitted scientific identity.

    This never substitutes another matching source. Cancellation ordering belongs
    to the journal, which rechecks the current attempt and admission under its
    lock after this content verification. Already durable reuse can consequently
    be revalidated after cancellation without permitting a new selection.
    """
    admission = AdmissionReference.model_validate(admission)
    receipt = SealedDelivery.model_validate(receipt)
    committer = reader.committer
    state = committer.journal.state(admission.attempt.assignment)
    if state is None or (state.attempt, state.admission_digest) != (
        admission.attempt,
        admission.record_digest,
    ):
        msg = "Cache reuse requires the exact current journal-bound admission"
        raise StoreIntegrityError(msg)
    with capture_admission(
        admission,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
        verify_producer=committer._verify_producer,
    ) as current:
        key = retained_cache_key(
            current.record.recipe, admission.attempt, current.record.boundary
        )
    if key is None:
        msg = "Current assignment is ineligible for ordinary cache reuse"
        raise StoreIntegrityError(msg)
    with reader.capture_inputs(receipt) as source:
        source_key = retained_cache_key(
            source.record.recipe, source.record.attempt, source.record.boundary
        )
        if source_key != key:
            msg = "Proposed cache source does not match admitted scientific identity"
            raise StoreIntegrityError(msg)
