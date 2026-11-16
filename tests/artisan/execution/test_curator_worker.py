"""Persisted curator worker admission uses the shared call and staged-evidence path."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path
from unittest.mock import Mock

import polars as pl
import pytest
from fixtures.curator_cache import DynamicCurator
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.curator_worker import (
    AdmittedCurator,
    PreparedCuratorSoftware,
    execute_admitted_curator,
)
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.registry.resolve import operation_identity
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.execution.replay import ReplayDiagnostic, ReplaySnapshot
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.curator_staging import read_staged_curator_result
from artisan.utils.path import uri_join, uri_parent
from artisan.visualization.retained_inspect import retained_failures

MODULE = "artisan.execution.curator_worker"


@dataclass
class CuratorCase:
    curator: AdmittedCurator
    admission: CuratorAdmission
    software: PreparedCuratorSoftware
    dispatch: AssignmentDispatch
    coordinator: CuratorResults


@pytest.fixture
def case(tmp_path: Path) -> CuratorCase:
    storage = StorageConfig()
    committer = open_committer(
        str(tmp_path / "candidate"), LocalFileSystem(), storage, initialize=True
    )
    operation = DynamicCurator()
    coordinator, admission, reference = admit_curator(
        committer,
        operation,
        {},
        step_run_id="a" * 32,
        software_digest="1" * 64,
        software_cacheable=False,
    )
    software = Mock(spec=PreparedCuratorSoftware)
    software.software_digest = admission.software_digest
    software.software_cacheable = False
    software.application_modules = ()
    dispatch = AssignmentDispatch(
        committer.delta_root,
        committer.files_root,
        coordinator.staging.staging_dir,
        storage,
        tmp_path / "worker",
        {"SYNTHETIC_LITERAL": "literal $HOME with spaces"},
    )
    return CuratorCase(
        AdmittedCurator(operation, reference),
        admission,
        software,
        dispatch,
        coordinator,
    )


@pytest.mark.parametrize("backend", ["local", pytest.param("s3", marks=pytest.mark.s3)])
def test_closed_curator_failure_publishes_log_on_configured_filesystem(
    tmp_path: Path, request: pytest.FixtureRequest, backend: str
) -> None:
    fs, storage, root = LocalFileSystem(), StorageConfig(), str(tmp_path / "case")
    if backend == "s3":
        fs, storage, root = request.getfixturevalue("s3_fs")
    committer = open_committer(root, fs, storage, initialize=True)
    operation = DynamicCurator(params=DynamicCurator.Params(fail=True))
    coordinator, admission, reference = admit_curator(
        committer,
        operation,
        {},
        step_run_id="a" * 32,
        software_digest="1" * 64,
        software_cacheable=False,
    )
    software = Mock(spec=PreparedCuratorSoftware)
    software.software_digest = admission.software_digest
    software.software_cacheable = False
    software.application_modules = ()
    dispatch = AssignmentDispatch(
        committer.delta_root,
        committer.files_root,
        coordinator.staging.staging_dir,
        storage,
        tmp_path / "worker",
        {},
    )
    staged = execute_admitted_curator(
        AdmittedCurator(operation, reference),
        software,
        dispatch,
        working_root=dispatch.working_root,
    )
    assert staged.execution_run_id is not None
    summary = read_staged_curator_result(
        admission,
        staged.execution_run_id,
        staging_root=dispatch.staging_root,
        committer=committer,
    )
    coordinator.finish(reference, staged.execution_run_id, summary)
    failure = retained_failures(committer).row(0, named=True)
    assert failure["execution_run_id"] == staged.execution_run_id
    assert failure["log"]
    log = uri_join(uri_parent(committer.delta_root), "logs", "failures", failure["log"])
    assert b"=== Execution Failure Log ===" in fs.cat_file(log)
    fs.rm(log)
    assert retained_failures(committer)["log"].to_list() == [None]


def test_worker_stages_stream_and_controller_accepts_ordinary_result(
    case: CuratorCase,
) -> None:
    previous = os.environ.get("SYNTHETIC_LITERAL")
    result = execute_admitted_curator(
        case.curator,
        case.software,
        case.dispatch,
        working_root=case.dispatch.working_root,
    )
    assert result.success, result.error
    assert result.execution_run_id is not None
    assert result.staging_path is not None
    recorded = pl.read_parquet(f"{result.staging_path}/executions.parquet")
    replay = ReplaySnapshot.model_validate_json(recorded["replay_snapshot"][0])
    assert replay.status == "requires_operation_class"
    assert replay.source is not None
    assert replay.source.execution_spec_id == case.admission.digest
    assert replay.required_runtime_keys == ("SYNTHETIC_LITERAL",)
    assert (
        case.dispatch.runtime_env["SYNTHETIC_LITERAL"] not in replay.model_dump_json()
    )
    assert os.environ.get("SYNTHETIC_LITERAL") == previous
    case.software.verify_worker.assert_called_once()  # type: ignore[attr-defined]
    summary = read_staged_curator_result(
        case.admission,
        result.execution_run_id,
        staging_root=case.dispatch.staging_root,
        committer=case.coordinator.committer,
    )
    case.coordinator.finish(case.curator.admission, result.execution_run_id, summary)
    accepted = CuratorReader(case.coordinator.committer).read_result(
        case.admission.record.step_run_id,
        case.coordinator.committer._control_plan(
            case.coordinator.committer._controls().row(-1, named=True)
        ).plan_digest,
    )
    assert accepted.result.succeeded_count == 1
    assert accepted.plan.cache_proof is not None
    assert not case.admission.software_cacheable


@pytest.mark.parametrize(
    "change", ["definition", "software", "eligibility", "cancelled"]
)
def test_worker_rejects_changed_admission_before_authored_entry(
    case: CuratorCase, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    if change == "definition":
        case.curator.operation.params.empty = True
    elif change == "software":
        case.software.software_digest = "2" * 64
    elif change == "eligibility":
        case.software.software_cacheable = True
    else:
        case.coordinator.record_cancellation(
            case.admission.record.step_run_id,
            CancellationAcknowledgement(CancellationStatus.REQUESTED),
        )
    monkeypatch.setattr(
        f"{MODULE}.run_curator_flow",
        lambda *args, **kwargs: pytest.fail("Invalid admission reached authored entry"),
    )
    with pytest.raises((ValueError, StoreIntegrityError)):
        execute_admitted_curator(
            case.curator,
            case.software,
            case.dispatch,
            working_root=case.dispatch.working_root,
        )


def test_runtime_values_are_scoped_and_unknown_cleanup_is_not_a_staged_success(
    case: CuratorCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.utils.command_supervisor import CommandOwnershipError

    def run(*args: object, **kwargs: object) -> StagingResult:
        assert (
            os.environ["SYNTHETIC_LITERAL"]
            == case.dispatch.runtime_env["SYNTHETIC_LITERAL"]
        )
        return StagingResult(
            success=False,
            cancellation_acknowledgement=CancellationAcknowledgement(
                CancellationStatus.UNKNOWN
            ),
        )

    monkeypatch.setattr(f"{MODULE}.run_curator_flow", run)
    with pytest.raises(CommandOwnershipError):
        execute_admitted_curator(
            case.curator,
            case.software,
            case.dispatch,
            working_root=case.dispatch.working_root,
        )
    assert "SYNTHETIC_LITERAL" not in os.environ


def test_provider_software_is_remeasured_before_authored_curator_entry(
    case: CuratorCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    software = Mock(spec=PreparedCuratorSoftware)
    software.software_digest = case.admission.software_digest
    software.software_cacheable = case.admission.software_cacheable
    software.verify_worker.side_effect = ValueError("SDK payload changed")
    monkeypatch.setattr(
        f"{MODULE}.run_curator_flow",
        lambda *a, **kw: pytest.fail("Changed provider reached authored code"),
    )
    with pytest.raises(ValueError, match="SDK payload changed"):
        execute_admitted_curator(
            case.curator,
            software,
            case.dispatch,
            working_root=case.dispatch.working_root,
        )
    software.verify_worker.assert_called_once()


@pytest.mark.parametrize("change", ["source", "unapproved_software"])
def test_worker_revalidates_replay_selection_before_authored_entry(
    case: CuratorCase, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    identity = operation_identity(type(case.curator.operation))
    diagnostic = ReplayDiagnostic(
        source_execution_run_id="b" * 32,
        source_execution_spec_id="c" * 64,
        source_identity=identity,
        selected_identity=identity,
        source_software_digest=case.admission.software_digest,
        selected_software_digest=case.admission.software_digest,
        allow_code_change=False,
        runner=case.admission.record.compute_backend,
        roots={},
    )
    if change == "unapproved_software":
        diagnostic = diagnostic.model_copy(
            update={"selected_software_digest": "2" * 64}
        )
    monkeypatch.setattr(
        f"{MODULE}.run_curator_flow",
        lambda *args, **kwargs: pytest.fail("Invalid replay reached authored entry"),
    )
    with pytest.raises(
        (ValueError, StoreIntegrityError),
        match="changed without permission|differs from its admitted worker",
    ):
        execute_admitted_curator(
            replace(case.curator, diagnostic=diagnostic),
            case.software,
            case.dispatch,
            working_root=case.dispatch.working_root,
        )
