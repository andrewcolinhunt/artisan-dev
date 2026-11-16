"""Submission snapshots preserve exact sources through delayed input resolution."""

from __future__ import annotations

import json
from concurrent.futures import CancelledError
from pathlib import Path

import pytest
from fixtures.candidate_outputs import external_case, prepare_scientific
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator
from fixtures.retained_results import retained_case

from artisan.errors import (
    ArtifactIntegrityError,
    PersistenceIntegrityError,
    StoreIntegrityError,
)
from artisan.execution.recording.commands import CommandRecorder
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.engine.pipeline_inputs import (
    normalize_pipeline_inputs,
    resolve_creator_inputs,
    resolve_curator_inputs,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.retained_reader import AcceptedArtifact
from artisan.storage.io.input_registration_plan import read_input_registration
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.step_result_plan import StepUnitFailure


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    storage = StorageConfig()
    return open_committer(
        str(tmp_path / "store"), storage.filesystem(), storage, initialize=True
    )


def _record() -> StepStartRecord:
    return StepStartRecord(
        step_run_id="d" * 32,
        step_number=8,
        step_name="consume",
        operation_class="synthetic.Consume",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json="{}",
        output_roles_json='["data"]',
        output_types_json='{"data":"any"}',
    )


def _pending(committer: RetainedCommitter) -> StepStartRecord:
    record = _record()
    RetainedStepTracker(committer, "synthetic-pipeline").create_attempt(record)
    return record


def _check() -> None:
    pass


def _source(
    committer: RetainedCommitter, tmp_path: Path
) -> tuple[AcceptedArtifact, ...]:
    results, admission, receipts = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias")
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    return read_current_step_outputs(committer, admission.step_run_id)[0].occurrences


def test_snapshot_binds_attempts_and_copies_all_caller_containers() -> None:
    attempts = {7: "a" * 32}
    reference = OutputReference(source_step=7, role="data", artifact_type="csv")
    members = [reference, reference]
    inputs = {"data": members, "empty": []}
    frozen = normalize_pipeline_inputs(inputs, step_run_ids=attempts)
    members.clear()
    inputs.clear()
    attempts[7] = "b" * 32
    assert frozen.dependency_step_run_ids == ("a" * 32,)
    assert len(frozen.roles[0].members) == 2
    assert frozen.roles[1].members == ()
    assert (
        frozen.diagnostic_data()["data"]
        == [
            {
                "source_step": 7,
                "step_run_id": "a" * 32,
                "role": "data",
                "artifact_type": "csv",
            }
        ]
        * 2
    )


def test_explicit_empty_sequence_keeps_no_work_intent() -> None:
    absent = normalize_pipeline_inputs(None, step_run_ids={})
    assert normalize_pipeline_inputs({}, step_run_ids={}) == absent
    empty = normalize_pipeline_inputs([], step_run_ids={})
    assert empty != absent
    assert empty.raw_paths == ()
    assert empty.diagnostic_data() == {"raw_paths": []}


def test_artifact_snapshot_and_diagnostics_never_include_content() -> None:
    artifact = MetricArtifact.draft(
        {"private_body": "do-not-record"}, "input.json"
    ).finalize()
    frozen = normalize_pipeline_inputs({"data": [artifact]}, step_run_ids={})
    artifact.materialized_path = "later-mutation"
    copied = frozen.roles[0].members[0]
    assert isinstance(copied, Artifact)
    assert copied.materialized_path is None
    assert copied is not artifact
    assert "do-not-record" not in json.dumps(frozen.diagnostic_data())
    raw = normalize_pipeline_inputs(
        ["https://example.invalid/input?token=private-token"], step_run_ids={}
    )
    recorder = CommandRecorder()
    recorder.add_sensitive_data("private-token")
    assert "private-token" not in json.dumps(
        recorder.sanitize_json(raw.diagnostic_data())
    )


@pytest.mark.parametrize(
    "inputs", ["path.csv", ["file.csv", 1], {"data": "a" * 32}, {"data": ["not-an-id"]}]
)
def test_invalid_shapes_fail_without_storage_access(inputs: object) -> None:
    with pytest.raises(TypeError):
        normalize_pipeline_inputs(inputs, step_run_ids={})  # type: ignore[arg-type]


def test_reference_without_attempt_binding_never_defers_to_latest() -> None:
    with pytest.raises(ValueError, match="No submitted attempt"):
        normalize_pipeline_inputs(
            {"data": OutputReference(source_step=7, role="data")}, step_run_ids={}
        )


def test_exact_reference_expansion_preserves_duplicates_after_later_retry(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    expected = _source(committer, tmp_path)
    reference = OutputReference(source_step=7, role="data")
    attempts = {7: "a" * 32}
    frozen = normalize_pipeline_inputs(
        {"data": [reference, expected[0], reference], "empty": []},
        step_run_ids=attempts,
    )
    newer, new_admission, _ = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias"), step_run_id="b" * 32
    )
    newer.finish(
        new_admission.step_run_id, failures={0: StepUnitFailure(error="failed retry")}
    )
    attempts[7] = new_admission.step_run_id
    resolved = resolve_creator_inputs(
        frozen,
        record=_pending(committer),
        committer=committer,
        storage=StorageConfig(),
        check_cancelled=_check,
    )
    assert [item.reference for item in resolved["data"]] == [
        item.reference for item in (*expected, expected[0], *expected)
    ]
    assert resolved["empty"] == ()


def test_failed_selected_attempt_does_not_fall_back_to_old_success(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    _source(committer, tmp_path)
    failed, admission, _ = prepare_scientific(
        committer, retained_case(tmp_path, kind="alias"), step_run_id="b" * 32
    )
    failed.finish(admission.step_run_id, failures={0: StepUnitFailure(error="failure")})
    frozen = normalize_pipeline_inputs(
        [OutputReference(source_step=7, role="data")],
        step_run_ids={7: admission.step_run_id},
    )
    resolved = resolve_curator_inputs(
        frozen,
        record=_pending(committer),
        committer=committer,
        storage=StorageConfig(),
        check_cancelled=_check,
    )
    assert resolved == {"_merged_streams": ()}


@pytest.mark.parametrize(("role", "kind"), [("missing", "any"), ("data", "metric")])
def test_exact_reference_rejects_wrong_role_or_type(
    committer: RetainedCommitter,
    tmp_path: Path,
    role: str,
    kind: str,
) -> None:
    _source(committer, tmp_path)
    frozen = normalize_pipeline_inputs(
        {"data": OutputReference(source_step=7, role=role, artifact_type=kind)},
        step_run_ids={7: "a" * 32},
    )
    with pytest.raises((ValueError, StoreIntegrityError)):
        resolve_creator_inputs(
            frozen,
            record=_pending(committer),
            committer=committer,
            storage=StorageConfig(),
            check_cancelled=_check,
        )


def test_registered_ids_remain_unscoped_while_typed_accepted_inputs_keep_producers(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    accepted = _source(committer, tmp_path)[0]
    artifact_id = accepted.artifact.artifact_id
    assert artifact_id is not None
    frozen = normalize_pipeline_inputs(
        {"data": [artifact_id, accepted, artifact_id]}, step_run_ids={}
    )
    record = _pending(committer)
    scientific = resolve_creator_inputs(
        frozen,
        record=record,
        committer=committer,
        storage=StorageConfig(),
        check_cancelled=_check,
    )["data"]
    assert isinstance(scientific[0], Artifact)
    assert isinstance(scientific[1], AcceptedArtifact)
    assert isinstance(scientific[2], Artifact)
    assert scientific[1].reference == accepted.reference
    curator = resolve_curator_inputs(
        frozen,
        record=record,
        committer=committer,
        storage=StorageConfig(),
        check_cancelled=_check,
    )["data"]
    assert curator[0] == curator[2] == artifact_id
    assert isinstance(curator[1], AcceptedArtifact)


def test_curator_bare_artifacts_require_matching_existing_registration(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    accepted = _source(committer, tmp_path)[0]
    record = _pending(committer)
    frozen = normalize_pipeline_inputs({"data": [accepted.artifact]}, step_run_ids={})
    assert resolve_curator_inputs(
        frozen,
        record=record,
        committer=committer,
        storage=StorageConfig(),
        check_cancelled=_check,
    ) == {"data": (accepted.artifact.artifact_id,)}
    changed = type(accepted.artifact).from_row(
        {**accepted.artifact.to_row(), "origin_step_number": 99}
    )
    with pytest.raises(StoreIntegrityError, match="registered content"):
        resolve_curator_inputs(
            normalize_pipeline_inputs({"data": [changed]}, step_run_ids={}),
            record=record,
            committer=committer,
            storage=StorageConfig(),
            check_cancelled=_check,
        )
    absent = MetricArtifact.draft({"new": 1}, "new.json").finalize()
    snapshot = normalize_pipeline_inputs({"data": [absent]}, step_run_ids={})
    with pytest.raises(StoreIntegrityError, match="registered content"):
        resolve_curator_inputs(
            snapshot,
            record=record,
            committer=committer,
            storage=StorageConfig(),
            check_cancelled=_check,
        )
    assert isinstance(
        resolve_creator_inputs(
            snapshot,
            record=record,
            committer=committer,
            storage=StorageConfig(),
            check_cancelled=_check,
        )["data"][0],
        Artifact,
    )


def test_raw_paths_register_once_preserve_repeats_and_survive_source_deletion(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    path = tmp_path / "raw.csv"
    path.write_bytes(b"value\n1\n")
    paths = [str(path), str(path)]
    frozen = normalize_pipeline_inputs(paths, step_run_ids={})
    paths.clear()
    record = _pending(committer)

    def delete_after_registration() -> None:
        if (
            read_input_registration(
                committer.delta_root, record.step_run_id, committer.fs
            )
            is not None
        ):
            path.unlink(missing_ok=True)

    resolved = resolve_creator_inputs(
        frozen,
        record=record,
        committer=committer,
        storage=StorageConfig(),
        check_cancelled=delete_after_registration,
    )
    assert len(resolved["file"]) == 2
    first, second = resolved["file"]
    assert isinstance(first, FileRefArtifact)
    assert isinstance(second, FileRefArtifact)
    assert first.artifact_id == second.artifact_id
    assert first.read_content() == b"value\n1\n"
    assert not path.exists()


def test_post_registration_cancellation_stops_before_input_admission(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    path = tmp_path / "raw.csv"
    path.write_bytes(b"x\n1\n")
    frozen = normalize_pipeline_inputs([str(path)], step_run_ids={})
    record = _pending(committer)

    def stop() -> None:
        if (
            read_input_registration(
                committer.delta_root, record.step_run_id, committer.fs
            )
            is not None
        ):
            raise CancelledError()

    with pytest.raises(CancelledError):
        resolve_curator_inputs(
            frozen,
            record=record,
            committer=committer,
            storage=StorageConfig(),
            check_cancelled=stop,
        )
    assert (
        RetainedStepTracker(committer, "").current_state(record.step_run_id).status
        == StepStatus.PENDING
    )
    assert (
        read_input_registration(committer.delta_root, record.step_run_id, committer.fs)
        is not None
    )


def test_resolution_requires_durable_pending_before_raw_file_access(
    committer: RetainedCommitter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Raw input read before pending")

    monkeypatch.setattr(
        "artisan.orchestration.engine.pipeline_inputs.register_raw_inputs", forbidden
    )
    with pytest.raises(PersistenceIntegrityError, match="Unknown step"):
        resolve_creator_inputs(
            normalize_pipeline_inputs(["missing.csv"], step_run_ids={}),
            record=_record(),
            committer=committer,
            storage=StorageConfig(),
            check_cancelled=_check,
        )


def test_registered_external_creator_input_checks_the_entire_selected_carrier(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    results, admission, receipts = prepare_scientific(
        committer, external_case(tmp_path / "source", appendable=True)
    )
    committer.accept(receipts[0])
    results.finish(admission.step_run_id)
    accepted = read_current_step_outputs(committer, admission.step_run_id)[
        0
    ].occurrences[0]
    artifact_id = accepted.artifact.artifact_id
    assert artifact_id is not None
    carrier = Path(accepted.artifact.external_path)
    carrier.write_bytes(carrier.read_bytes().replace(b'"value": 2', b'"value": 9'))
    frozen = normalize_pipeline_inputs({"data": [artifact_id]}, step_run_ids={})
    record = _pending(committer)
    assert resolve_curator_inputs(
        frozen,
        record=record,
        committer=committer,
        storage=StorageConfig(),
        check_cancelled=_check,
    ) == {"data": (artifact_id,)}
    with pytest.raises(ArtifactIntegrityError):
        resolve_creator_inputs(
            frozen,
            record=record,
            committer=committer,
            storage=StorageConfig(),
            check_cancelled=_check,
        )


def test_successful_dynamic_empty_source_does_not_invent_a_missing_role(
    committer: RetainedCommitter,
) -> None:
    operation = DynamicCurator()
    operation.params.empty = True
    coordinator, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, step_number=7, **STAMPS
    )
    staged, terminal, _ = stage_observed(coordinator, admission, operation)
    coordinator.finish(reference, staged.execution_run_id, terminal)
    frozen = normalize_pipeline_inputs(
        {"data": OutputReference(source_step=7, role="invented")},
        step_run_ids={7: reference.step_run_id},
    )
    with pytest.raises(ValueError, match="no output role"):
        resolve_curator_inputs(
            frozen,
            record=_pending(committer),
            committer=committer,
            storage=StorageConfig(),
            check_cancelled=_check,
        )


def test_repeated_attempt_references_each_check_their_logical_step(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    _source(committer, tmp_path)
    frozen = normalize_pipeline_inputs(
        {
            "data": [
                OutputReference(source_step=7, role="data"),
                OutputReference(source_step=6, role="data"),
            ]
        },
        step_run_ids={7: "a" * 32, 6: "a" * 32},
    )
    with pytest.raises(StoreIntegrityError, match="selected step attempt"):
        resolve_creator_inputs(
            frozen,
            record=_pending(committer),
            committer=committer,
            storage=StorageConfig(),
            check_cancelled=_check,
        )
