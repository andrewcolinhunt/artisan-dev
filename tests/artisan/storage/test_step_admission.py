"""Finite step admission is durable before units can dispatch or reuse results."""

from __future__ import annotations

import json

import pytest
from fsspec import AbstractFileSystem

from artisan.composites.base.compiler import compile_definition
from artisan.errors import StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.operations.examples import DataGenerator
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.execution.assignment import AssignmentAttempt, AssignmentIdentity
from artisan.schemas.execution.step_admission import StepAdmission
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.store_format import initialize_retained_store
from artisan.storage.io.admission import publish_admission
from artisan.storage.io.assignment_journal import AssignmentJournal
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.hashing import canonical_json_bytes

type Backend = tuple[AbstractFileSystem, StorageConfig, str]


def _prepare(
    backend: Backend,
    *,
    count: int = 2,
    step_replay: str | None = None,
    unit_replay: str | None = None,
    output_roles: str = '["datasets"]',
    output_types: str = '{"datasets":"csv"}',
    compute_options: str = "{}",
) -> tuple[RetainedStepTracker, StepAdmission]:
    fs, storage, root = backend
    delta_root, files_root = f"{root}/candidate", f"{root}/managed"
    initialize_retained_store(delta_root, fs)
    journal = AssignmentJournal(
        delta_root, fs=fs, storage_options=storage.delta_storage_options()
    )
    journal.initialize()
    committer = RetainedCommitter(
        delta_root,
        files_root,
        journal,
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    committer.initialize()
    tracker = RetainedStepTracker(committer, "synthetic-pipeline")
    recipe = compile_definition(DataGenerator, params={"seed": 17}).recipe
    step_id = "a" * 32
    tracker.create_attempt(
        StepStartRecord(
            step_run_id=step_id,
            replay_of_execution_run_id=step_replay,
            step_number=3,
            step_name="generate",
            operation_class=f"{recipe.definition.module}.{recipe.definition.qualname}",
            params_json=recipe.params_json,
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json=compute_options,
            output_roles_json=output_roles,
            output_types_json=output_types,
        )
    )
    references = []
    for index in range(count):
        identity = AssignmentIdentity(step_run_id=step_id, unit_index=index)
        boundary = OccurrenceGraph(boundary_invocation=identity.assignment_id).project(
            {}, {}
        )
        attempt = AssignmentAttempt(
            assignment=identity,
            attempt=0,
            generation=0,
            recipe_digest=recipe.digest,
            software_digest="b" * 64,
            input_context_digest=boundary.input_context_digest,
            replay_of_execution_run_id=unit_replay,
        )
        reference = publish_admission(
            attempt,
            recipe,
            boundary,
            {},
            step_number=3,
            delta_root=delta_root,
            files_root=files_root,
            fs=fs,
        )
        journal.admit(attempt, admission_digest=reference.record_digest)
        references.append(reference)
    return tracker, StepAdmission(
        step_run_id=step_id,
        step_number=3,
        recipe=recipe,
        software_digest="b" * 64 if count else None,
        assignments=tuple(references),
    )


@pytest.mark.parametrize(
    ("step_replay", "unit_replay"),
    [(None, "c" * 32), ("c" * 32, None), ("c" * 32, "d" * 32)],
)
def test_diagnostic_assignments_require_matching_pending_owner(
    raw_backend_fs: Backend, step_replay: str | None, unit_replay: str | None
) -> None:
    tracker, admission = _prepare(
        raw_backend_fs, step_replay=step_replay, unit_replay=unit_replay
    )
    with pytest.raises(StoreIntegrityError, match="pending scientific owner"):
        tracker.admit(admission)
    assert tracker.current_state(admission.step_run_id).status == StepStatus.PENDING


@pytest.mark.parametrize("count", [0, 2])
def test_diagnostic_admission_preserves_pending_owner(
    raw_backend_fs: Backend, count: int
) -> None:
    source = "c" * 32
    tracker, admission = _prepare(
        raw_backend_fs, count=count, step_replay=source, unit_replay=source
    )
    assert tracker.admit(admission).replay_of_execution_run_id == source
    assert tracker.read_admission(admission.step_run_id) == admission


@pytest.mark.parametrize(
    ("roles", "types", "options"),
    [
        ('["other"]', '{"other":"csv"}', "{}"),
        ('["datasets"]', '{"datasets":"metric"}', "{}"),
        ("[]", "{}", '{"dynamic_output_roles":true}'),
    ],
)
def test_admission_requires_pending_output_contract(
    raw_backend_fs: Backend, roles: str, types: str, options: str
) -> None:
    tracker, admission = _prepare(
        raw_backend_fs, output_roles=roles, output_types=types, compute_options=options
    )
    with pytest.raises(StoreIntegrityError, match="pending scientific owner"):
        tracker.admit(admission)


@pytest.mark.parametrize("count", [0, 2])
def test_finite_admission_survives_controller_recreation(
    raw_backend_fs: Backend, count: int
) -> None:
    tracker, admission = _prepare(raw_backend_fs, count=count)
    assert tracker.current_state(admission.step_run_id).status == StepStatus.PENDING
    with pytest.raises(StoreIntegrityError, match="no durable finite"):
        tracker.read_admission(admission.step_run_id)
    running = tracker.admit(admission)
    assert running.status == (StepStatus.RUNNING if count else StepStatus.SKIPPED)
    assert tracker.admit(admission) == running
    reopened = RetainedStepTracker(tracker.committer, "synthetic-pipeline")
    assert reopened.read_admission(admission.step_run_id) == admission
    assert reopened.current_state(admission.step_run_id).state_sequence == 1
    assert len(admission.assignments) == count


def test_replaced_finite_admission_rejects_even_with_self_consistent_content(
    raw_backend_fs: Backend,
) -> None:
    tracker, admission = _prepare(raw_backend_fs)
    tracker.admit(admission)
    path = f"{tracker.committer.delta_root}/_artisan/step-admissions/{admission.step_run_id}.json"
    with tracker.committer.fs.open(path, "rb") as stream:
        document = json.load(stream)
    document["assignments"] = document["assignments"][:1]
    StepAdmission.model_validate(document)
    with tracker.committer.fs.open(path, "wb") as stream:
        stream.write(canonical_json_bytes(document))
    with pytest.raises(StoreIntegrityError, match="controller-bound identity"):
        tracker.read_admission(admission.step_run_id)


@pytest.mark.parametrize("change", ["cancelled", "generation", "digest", "step_number"])
def test_finite_admission_requires_exact_active_units(
    raw_backend_fs: Backend, change: str
) -> None:
    tracker, admission = _prepare(raw_backend_fs)
    reference = admission.assignments[0]
    if change == "cancelled":
        tracker.committer.journal.cancel(reference.attempt)
    elif change == "generation":
        tracker.committer.journal.admit(
            reference.attempt.model_copy(update={"generation": 1}),
            admission_digest=reference.record_digest,
        )
    elif change == "digest":
        admission = admission.model_copy(
            update={
                "assignments": (
                    reference.model_copy(update={"record_digest": "c" * 32}),
                    admission.assignments[1],
                )
            }
        )
    else:
        admission = admission.model_copy(update={"step_number": 4})
    with pytest.raises(StoreIntegrityError, match="exact active|pending scientific"):
        tracker.admit(admission)
    assert tracker.current_state(admission.step_run_id).status == StepStatus.PENDING


@pytest.mark.parametrize("indices", [(1, 0), (0, 2), (0, 0)])
def test_finite_admission_rejects_missing_reordered_or_repeated_units(
    indices: tuple[int, int],
) -> None:
    recipe = compile_definition(DataGenerator).recipe
    references = tuple(
        {
            "attempt": {
                "assignment": {"step_run_id": "a" * 32, "unit_index": index},
                "attempt": 0,
                "generation": 0,
                "recipe_digest": recipe.digest,
                "software_digest": "b" * 64,
                "input_context_digest": "c" * 32,
            },
            "record_digest": "d" * 32,
        }
        for index in indices
    )
    with pytest.raises(ValueError, match="complete ordered units"):
        StepAdmission.model_validate(
            {
                "step_run_id": "a" * 32,
                "step_number": 3,
                "recipe": recipe,
                "software_digest": "b" * 64,
                "assignments": references,
            }
        )
