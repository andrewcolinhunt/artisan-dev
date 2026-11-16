"""Admitted scientific fixtures shared by retained readers and cache tests."""

from __future__ import annotations

from fsspec import AbstractFileSystem

from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.io.admission import publish_admission
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_seal import stage_retained_result
from fixtures.retained_acceptance import boundary_projection
from fixtures.retained_results import RetainedCase

type Backend = tuple[AbstractFileSystem, StorageConfig, str]


def admit_read_case(
    committer: RetainedCommitter,
    case: RetainedCase,
    *,
    step_run_id: str,
    unit_index: int = 0,
    recipe: ExecutionRecipe | None = None,
    software_digest: str = "c" * 64,
    replay_of: str | None = None,
) -> tuple[AdmissionReference, SealedDelivery]:
    recipe = recipe or case.recipe
    attempt = AssignmentAttempt(
        assignment=AssignmentIdentity(step_run_id=step_run_id, unit_index=unit_index),
        attempt=0,
        generation=0,
        recipe_digest=recipe.digest,
        software_digest=software_digest,
        input_context_digest=case.projection.input_context_digest,
        replay_of_execution_run_id=replay_of,
    )
    reference = publish_admission(
        attempt,
        recipe,
        boundary_projection(case),
        case.boundary,
        step_number=7,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
        verify_producer=committer._verify_producer,
    )
    committer.journal.admit(attempt, admission_digest=reference.record_digest)
    receipt = stage_retained_result(
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
    return reference, receipt
