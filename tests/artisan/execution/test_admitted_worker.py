"""Workers consume immutable admissions and localize complete managed carriers."""

from __future__ import annotations

import shutil
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar, Literal

import pytest
from botocore.exceptions import EndpointConnectionError
from fixtures.retained_acceptance import open_committer
from fixtures.retained_results import retained_case
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import compile_definition
from artisan.errors import StoreIntegrityError
from artisan.execution.admission_localization import localize_admission
from artisan.execution.admitted_worker import execute_admitted_assignment
from artisan.execution.assignment import Assignment
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.execution.composition import ExecuteAdapter
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.recording.delivery_diagnostics import read_delivery_diagnostics
from artisan.execution.recording.retained_replay import read_replay_configuration
from artisan.operations.base import OperationDefinition
from artisan.schemas import (
    Artifact,
    ArtifactResult,
    FileRefArtifact,
    InputSpec,
    OutputSpec,
)
from artisan.schemas.execution.assignment import AssignmentAttempt, AssignmentIdentity
from artisan.schemas.execution.delivery_diagnostics import DeliveryDiagnostics
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.storage.io.admission import (
    CapturedAdmission,
    capture_admission,
    publish_admission,
)
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.command_supervisor import CommandOwnershipError
from artisan.utils.hashing import compute_content_digest
from artisan.utils.log_paths import failure_log_relative_path
from artisan.utils.path import uri_join, uri_parent

type Backend = tuple[AbstractFileSystem, StorageConfig, str]


@pytest.fixture(params=["local", pytest.param("s3", marks=pytest.mark.s3)])
def backend_fs(request: pytest.FixtureRequest, tmp_path: Path) -> Backend:
    if request.param == "local":
        return LocalFileSystem(), StorageConfig(), str(tmp_path)
    return request.getfixturevalue("s3_fs")


def _path(artifact: Artifact) -> Path:
    return Path(getattr(artifact, next(iter(artifact.LOCATOR_FIELDS))))


class _CopyCarrier(OperationDefinition):
    name = "admitted_copy_carrier"

    class InputRole(StrEnum):
        items = "items"

    class OutputRole(StrEnum):
        files = "files"

    inputs: ClassVar = {"items": InputSpec(artifact_type="any", materialize=False)}
    outputs: ClassVar = {
        "files": OutputSpec(
            artifact_type="file_ref", derives_from={"inputs": ["items"]}
        )
    }
    per_artifact_dispatch = False
    observed: ClassVar[list[Path]] = []

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        paths = [_path(artifact) for artifact in inputs.input_artifacts["items"]]
        self.observed.extend(paths)
        assert len(set(paths)) == 1
        return {"content": paths[0].read_bytes()}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        assert inputs.files_dir is not None
        path = Path(inputs.files_dir) / "copied.bin"
        path.write_bytes(inputs.inputs["content"])
        return {"path": str(path)}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        path = Path(inputs.memory_outputs["path"][0])
        content = path.read_bytes()
        result = ArtifactResult()
        result.add_artifact(
            "files",
            FileRefArtifact.draft(
                str(path), compute_content_digest(content), len(content)
            ),
            sources={
                "items": [
                    inputs.ref("items", index=index)
                    for index in range(len(inputs.input_artifacts["items"]))
                ]
            },
        )
        return result


@dataclass(frozen=True)
class _DirectSoftware:
    """Exercise real worker admission and storage without pretending to test transport."""

    software_digest: str = "c" * 64
    software_cacheable: bool = True
    application_modules: tuple[str, ...] = ()

    def verify(self, plan: CompiledPlan) -> None:
        plan.instantiate()

    def create_adapter(
        self,
        assignment: Assignment,
        *,
        working_root: Path,
        runtime_env: Mapping[str, str],
    ) -> ExecuteAdapter:
        return lambda _operation, _path: LocalExecuteRouter()


def _admitted(
    backend_fs: Backend, tmp_path: Path, kind: str
) -> tuple[AdmittedAssignment, AssignmentDispatch, RetainedCommitter, bytes]:
    fs, storage, root = backend_fs
    committer = open_committer(f"{root}/candidate", fs, storage, initialize=True)
    scratch = tmp_path / "original"
    case = retained_case(scratch, kind=kind)
    artifacts = [
        artifact for artifact in case.artifacts.values() if artifact.EXTERNALLY_BACKED
    ]
    content = _path(artifacts[0]).read_bytes()
    plan = compile_definition(_CopyCarrier)
    identity = AssignmentIdentity(step_run_id="a" * 32, unit_index=0)
    graph = OccurrenceGraph(boundary_invocation=identity.assignment_id)
    for artifact in artifacts:
        graph.add_input("items", artifact)
    boundary = graph.project({}, {})
    attempt = AssignmentAttempt(
        assignment=identity,
        attempt=0,
        generation=0,
        recipe_digest=plan.recipe.digest,
        software_digest="c" * 64,
        input_context_digest=boundary.input_context_digest,
    )
    reference = publish_admission(
        attempt,
        plan.recipe,
        boundary,
        {
            address: graph.nodes[address].artifact
            for address in boundary.boundary_addresses()
        },
        step_number=7,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=fs,
    )
    committer.journal.admit(attempt, admission_digest=reference.record_digest)
    shutil.rmtree(scratch)
    dispatch = AssignmentDispatch(
        committer.delta_root,
        committer.files_root,
        f"{root}/staging",
        storage,
        tmp_path / "worker",
    )
    return AdmittedAssignment(plan, reference), dispatch, committer, content


@pytest.mark.parametrize("kind", ["file", "appendable"])
def test_localized_carriers_preserve_protected_rows_and_full_shared_bytes(
    backend_fs: Backend, tmp_path: Path, kind: str
) -> None:
    admitted, dispatch, committer, content = _admitted(backend_fs, tmp_path, kind)
    fs = dispatch.storage.filesystem()
    with capture_admission(
        admitted.admission,
        delta_root=dispatch.delta_root,
        files_root=dispatch.files_root,
        fs=fs,
        verify_producer=committer._verify_producer,
    ) as captured:
        rows = {
            address: artifact.to_row()
            for address, artifact in captured.artifacts.items()
        }
        locators = {
            address: str(_path(artifact))
            for address, artifact in captured.artifacts.items()
        }
        assignment = localize_admission(
            admitted,
            captured,
            files_root=dispatch.files_root,
            directory=tmp_path / "localized",
            fs=fs,
        )
        paths = {_path(artifact) for artifact in assignment.inputs["items"]}
        assert len(paths) == 1
        assert paths.pop().read_bytes() == content
        assert [artifact.to_row() for artifact in assignment.inputs["items"]] == [
            rows[address] for address in captured.record.boundary.input_roles()["items"]
        ]
        assert locators == {
            address: str(_path(artifact))
            for address, artifact in captured.artifacts.items()
        }


@pytest.mark.parametrize("kind", ["file", "appendable"])
@pytest.mark.parametrize("preserve", [False, True])
def test_admitted_worker_seals_before_scratch_cleanup_and_accepts_after_exit(
    backend_fs: Backend, tmp_path: Path, kind: str, preserve: bool
) -> None:
    admitted, dispatch, committer, content = _admitted(backend_fs, tmp_path, kind)
    dispatch = replace(dispatch, preserve_working=preserve)
    _CopyCarrier.observed.clear()
    receipt = execute_admitted_assignment(
        admitted, _DirectSoftware(), dispatch, working_root=dispatch.working_root
    )
    configuration = read_replay_configuration(
        receipt,
        admission=admitted.admission,
        delta_root=dispatch.delta_root,
        fs=dispatch.storage.filesystem(),
    )
    assert configuration.status == "available"
    assert configuration.required_runtime_keys == ()
    diagnostic = read_delivery_diagnostics(
        receipt.delivery,
        delta_root=dispatch.delta_root,
        fs=dispatch.storage.filesystem(),
        expected_seal_digest=receipt.seal_digest,
    )
    assert diagnostic is not None
    assert diagnostic.outcome == "success"
    assert len(diagnostic.invocations) == 1
    assert diagnostic.invocations[0].command_recording.status == "complete"
    assert diagnostic.invocations[0].command_recording.commands == []
    assert all(path.exists() == preserve for path in _CopyCarrier.observed)
    assert bool(list(dispatch.working_root.glob("admitted-inputs-*"))) == preserve
    assert bool(list(dispatch.working_root.rglob("copied.bin"))) == preserve
    plan = committer.accept(receipt)
    assert len(plan.carriers) == 1
    fs = dispatch.storage.filesystem()
    with fs.open(
        f"{dispatch.files_root}/{plan.carriers[0].relative_path}", "rb"
    ) as stream:
        assert stream.read() == content
    assert committer.accepted_projection(receipt).exports[0].role == "files"


@pytest.mark.parametrize("failure", ["none", "storage", "logger", "connection"])
def test_optional_replay_capture_keeps_private_values_out_of_persistent_results(
    backend_fs: Backend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failure: Literal["none", "storage", "logger", "connection"],
) -> None:
    admitted, dispatch, committer, _ = _admitted(backend_fs, tmp_path, "file")
    secret = "synthetic-runtime-value"
    dispatch = replace(dispatch, runtime_env={"SYNTHETIC_RUNTIME": secret})
    if failure != "none":

        def fail_publication(*args: object, **kwargs: object) -> None:
            if failure == "connection":
                raise EndpointConnectionError(endpoint_url=secret)
            raise OSError(secret)

        monkeypatch.setattr(
            "artisan.execution.recording.retained_replay.publish_replay_configuration",
            fail_publication,
        )
        if failure == "logger":
            monkeypatch.setattr(
                "artisan.execution.recording.retained_replay._LOGGER.warning",
                fail_publication,
            )
    receipt = execute_admitted_assignment(
        admitted, _DirectSoftware(), dispatch, working_root=dispatch.working_root
    )
    committer.accept(receipt)
    configuration = read_replay_configuration(
        receipt,
        admission=admitted.admission,
        delta_root=dispatch.delta_root,
        fs=committer.fs,
    )
    assert configuration.status == ("available" if failure == "none" else "unavailable")
    assert configuration.required_runtime_keys == (
        ("SYNTHETIC_RUNTIME",) if failure == "none" else ()
    )
    assert secret not in configuration.model_dump_json()
    assert secret not in caplog.text
    assert not list(dispatch.working_root.glob("admitted-inputs-*"))
    assert committer.accepted_projection(receipt).exports[0].role == "files"


@pytest.mark.parametrize("unavailable", [False, True])
def test_authored_failure_captures_runtime_keys_without_a_seal_or_acceptance(
    backend_fs: Backend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unavailable: bool,
) -> None:
    admitted, dispatch, committer, _ = _admitted(backend_fs, tmp_path, "file")
    secret = "synthetic-failure-private-value"
    dispatch = replace(dispatch, runtime_env={"SYNTHETIC_RUNTIME": secret})

    def fail_science(*args: object) -> None:
        raise ValueError(secret)

    def fail_capture(*args: object, **kwargs: object) -> None:
        raise OSError(secret)

    monkeypatch.setattr(_CopyCarrier, "preprocess", fail_science)
    if unavailable:
        monkeypatch.setattr(
            "artisan.execution.recording.retained_replay.publish_replay_configuration",
            fail_capture,
        )
    with pytest.raises(AssignmentExecutionError) as caught:
        execute_admitted_assignment(
            admitted, _DirectSoftware(), dispatch, working_root=dispatch.working_root
        )
    assert caught.value.delivery is not None
    configuration = read_replay_configuration(
        caught.value.delivery,
        admission=admitted.admission,
        delta_root=dispatch.delta_root,
        fs=committer.fs,
    )
    assert configuration.delivery == caught.value.delivery
    assert configuration.admission_digest == admitted.admission.record_digest
    assert configuration.seal_digest is None
    assert configuration.status == ("unavailable" if unavailable else "available")
    assert configuration.required_runtime_keys == (
        () if unavailable else ("SYNTHETIC_RUNTIME",)
    )
    assert secret not in configuration.model_dump_json()
    assert secret not in str(caught.value)
    diagnostic = read_delivery_diagnostics(
        caught.value.delivery, delta_root=dispatch.delta_root, fs=committer.fs
    )
    assert diagnostic is not None
    log = uri_join(
        uri_parent(dispatch.delta_root),
        "logs",
        "failures",
        failure_log_relative_path(
            caught.value.delivery.delivery_id, diagnostic.timestamp_start
        ),
    )
    content = committer.fs.cat_file(log).decode("utf-8")
    assert secret not in content
    assert "<redacted>" in content
    assert caught.value.delivery.delivery_id in content
    assert "Phase: preprocess" in content
    state = committer.journal.state(admitted.attempt.assignment)
    assert state is not None
    assert state.winner is None
    assert state.reuse is None
    assert not list(dispatch.working_root.glob("admitted-inputs-*"))


def test_optional_delivery_diagnostic_publication_failure_preserves_acceptance(
    backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    admitted, dispatch, committer, _ = _admitted(backend_fs, tmp_path, "file")

    def fail_publication(*args: object, **kwargs: object) -> None:
        msg = "diagnostic storage unavailable"
        raise OSError(msg)

    monkeypatch.setattr(
        "artisan.execution.admitted_worker.publish_delivery_diagnostics",
        fail_publication,
    )
    receipt = execute_admitted_assignment(
        admitted, _DirectSoftware(), dispatch, working_root=dispatch.working_root
    )
    assert committer.accept(receipt).receipt == receipt
    assert (
        read_delivery_diagnostics(
            receipt.delivery, delta_root=dispatch.delta_root, fs=committer.fs
        )
        is None
    )


def test_direct_admitted_diagnostics_redact_storage_values_without_transport(
    backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    admitted, dispatch, committer, _ = _admitted(backend_fs, tmp_path, "file")
    secret = "synthetic-storage-option-value"
    dispatch = replace(
        dispatch,
        storage=dispatch.storage.model_copy(
            update={"options": {**dispatch.storage.options, "neutral_option": secret}}
        ),
    )
    monkeypatch.setattr(StorageConfig, "filesystem", lambda _: committer.fs)

    def fail_sealing(*args: object, **kwargs: object) -> None:
        raise OSError(secret)

    monkeypatch.setattr(
        "artisan.execution.admitted_worker.stage_retained_result", fail_sealing
    )
    with pytest.raises(OSError, match=secret):
        execute_admitted_assignment(
            admitted, _DirectSoftware(), dispatch, working_root=dispatch.working_root
        )
    paths = committer.fs.glob(
        f"{dispatch.delta_root}/_artisan/diagnostics/*/*/*/*/execution.json"
    )
    assert len(paths) == 1
    with committer.fs.open(paths[0], "rb") as stream:
        encoded = stream.read()
    assert secret.encode() not in encoded
    diagnostic = DeliveryDiagnostics.model_validate_json(encoded)
    assert diagnostic.outcome == "infrastructure"
    assert diagnostic.seal_digest is None
    assert diagnostic.error is not None
    assert "<redacted>" in diagnostic.error
    assert not list(dispatch.working_root.glob("admitted-inputs-*"))


@pytest.mark.parametrize("unresolved", [False, True])
def test_admitted_worker_retains_input_carriers_only_for_unresolved_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, unresolved: bool
) -> None:
    backend = LocalFileSystem(), StorageConfig(), str(tmp_path)
    admitted, dispatch, _, content = _admitted(backend, tmp_path, "file")
    observed: list[Path] = []
    failure = CommandOwnershipError() if unresolved else RuntimeError("closed failure")

    def fail(assignment: Assignment, *args: object, **kwargs: object) -> None:
        observed.extend(_path(artifact) for artifact in assignment.inputs["items"])
        assert all(path.read_bytes() == content for path in observed)
        raise failure

    monkeypatch.setattr("artisan.execution.admitted_worker.execute_assignment", fail)
    with pytest.raises(type(failure)) as raised:
        execute_admitted_assignment(
            admitted, _DirectSoftware(), dispatch, working_root=dispatch.working_root
        )
    assert raised.value is failure
    assert observed
    assert all(path.exists() == unresolved for path in observed)
    if unresolved:
        assert all(path.read_bytes() == content for path in observed)
    else:
        assert not list(dispatch.working_root.glob("admitted-inputs-*"))


def test_admitted_worker_closes_metadata_capture_before_scientific_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend = LocalFileSystem(), StorageConfig(), str(tmp_path)
    admitted, dispatch, _, _ = _admitted(backend, tmp_path, "file")

    @contextmanager
    def failing_capture(*args: Any, **kwargs: Any) -> Iterator[CapturedAdmission]:
        with capture_admission(*args, **kwargs) as captured:
            yield captured
            message = "metadata cleanup failed"
            raise OSError(message)

    def unexpected_execution(*args: object, **kwargs: object) -> None:
        pytest.fail("Scientific execution began before metadata capture closed")

    monkeypatch.setattr(
        "artisan.execution.admitted_worker.capture_admission", failing_capture
    )
    monkeypatch.setattr(
        "artisan.execution.admitted_worker.execute_assignment", unexpected_execution
    )
    with pytest.raises(OSError, match="metadata cleanup failed"):
        execute_admitted_assignment(
            admitted, _DirectSoftware(), dispatch, working_root=dispatch.working_root
        )
    assert not list(dispatch.working_root.glob("admitted-inputs-*"))


@pytest.mark.parametrize("change", ["cancel", "generation", "admission", "carrier"])
def test_worker_rejects_obsolete_or_corrupt_admission_before_science(
    backend_fs: Backend, tmp_path: Path, change: str
) -> None:
    admitted, dispatch, committer, _ = _admitted(backend_fs, tmp_path, "file")
    if change == "cancel":
        committer.journal.cancel(admitted.attempt)
    elif change == "generation":
        committer.journal.admit(
            admitted.attempt.model_copy(update={"generation": 1, "attempt": 1}),
            admission_digest="b" * 32,
        )
    elif change == "admission":
        admitted = AdmittedAssignment(
            admitted.plan,
            admitted.admission.model_copy(update={"record_digest": "b" * 32}),
        )
    else:
        fs = dispatch.storage.filesystem()
        with capture_admission(
            admitted.admission,
            delta_root=dispatch.delta_root,
            files_root=dispatch.files_root,
            fs=fs,
            verify_producer=committer._verify_producer,
        ) as captured:
            path = f"{dispatch.files_root}/{captured.record.carriers[0].relative_path}"
        with fs.open(path, "wb") as stream:
            stream.write(b"changed")
    _CopyCarrier.observed.clear()
    with pytest.raises(StoreIntegrityError):
        execute_admitted_assignment(
            admitted, _DirectSoftware(), dispatch, working_root=dispatch.working_root
        )
    assert _CopyCarrier.observed == []
