"""Small admitted results and a separate controller process for recovery tests."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from fsspec import AbstractFileSystem

from artisan.schemas.execution.assignment import (
    AssignmentIdentity,
    DeliveryIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.provenance.projection import BoundaryProjection
from artisan.storage.core.store_format import initialize_retained_store
from artisan.storage.io.admission import publish_admission
from artisan.storage.io.assignment_journal import AssignmentJournal
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_seal import stage_retained_result
from fixtures.retained_results import RetainedCase, retained_case


def open_committer(
    root: str,
    fs: AbstractFileSystem,
    storage: StorageConfig,
    *,
    initialize: bool = False,
) -> RetainedCommitter:
    if initialize:
        initialize_retained_store(root, fs)
    journal = AssignmentJournal(
        root, fs=fs, storage_options=storage.delta_storage_options()
    )
    if initialize:
        journal.initialize()
    committer = RetainedCommitter(
        root,
        f"{root}/files",
        journal,
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    if initialize:
        committer.initialize()
    return committer


def boundary_projection(case: RetainedCase) -> BoundaryProjection:
    projection = case.projection
    addresses = projection.boundary_addresses()
    return BoundaryProjection(
        boundary_inputs=projection.boundary_inputs,
        input_attribution=projection.input_attribution,
        occurrences=tuple(
            item for item in projection.occurrences if item.address in addresses
        ),
        exports=(),
        frozen_context=projection.frozen_context,
    )


def admit_case(
    committer: RetainedCommitter,
    scratch: Path,
    *,
    kind: str = "file",
    unit_index: int = 0,
) -> tuple[RetainedCase, SealedDelivery]:
    case = retained_case(scratch, kind=kind)
    return case, admit_prepared_case(committer, case, unit_index=unit_index)


def admit_prepared_case(
    committer: RetainedCommitter,
    case: RetainedCase,
    *,
    unit_index: int = 0,
    generation: int = 0,
) -> SealedDelivery:
    attempt = case.completion().delivery.attempt.model_copy(
        update={
            "assignment": AssignmentIdentity(
                step_run_id="a" * 32, unit_index=unit_index
            ),
            "attempt": generation,
            "generation": generation,
        }
    )
    reference = publish_admission(
        attempt,
        case.recipe,
        boundary_projection(case),
        case.boundary,
        step_number=7,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
        verify_producer=committer._verify_producer,
    )
    committer.journal.admit(attempt, admission_digest=reference.record_digest)
    return stage_retained_result(
        f"{committer.delta_root}/staging",
        SuccessfulDelivery(
            delivery=DeliveryIdentity.new(attempt),
            computation_complete=True,
            cleanup_complete=True,
        ),
        case.projection,
        case.artifacts,
        fs=committer.fs,
    )


def main() -> None:
    request = json.load(sys.stdin)
    storage = StorageConfig.model_validate(request["storage"])
    committer = open_committer(request["root"], storage.filesystem(), storage)
    receipt = SealedDelivery.model_validate(request["receipt"])

    def checkpoint(
        boundary: str, _receipt: SealedDelivery, table_path: str | None
    ) -> None:
        if boundary == request["boundary"] and (
            request.get("table_path") is None or request["table_path"] == table_path
        ):
            os._exit(41)

    committer._checkpoint = checkpoint
    committer.accept(receipt)
    msg = "Requested controller crash boundary was not reached"
    raise RuntimeError(msg)


if __name__ == "__main__":
    main()
