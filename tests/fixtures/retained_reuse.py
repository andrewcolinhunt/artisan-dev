"""Crash a real controller immediately after its durable cache selection."""

from __future__ import annotations

import json
import os
import sys
from functools import partial

from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.cache.retained_reuse import verify_cache_source
from artisan.storage.core.retained_reader import RetainedReader
from fixtures.retained_acceptance import open_committer


def main() -> None:
    request = json.load(sys.stdin)
    storage = StorageConfig.model_validate(request["storage"])
    committer = open_committer(request["root"], storage.filesystem(), storage)
    admission = AdmissionReference.model_validate(request["admission"])
    receipt = SealedDelivery.model_validate(request["receipt"])
    committer.journal.reuse(
        admission,
        receipt,
        verify_source=partial(verify_cache_source, reader=RetainedReader(committer)),
    )
    os._exit(41)


if __name__ == "__main__":
    main()
