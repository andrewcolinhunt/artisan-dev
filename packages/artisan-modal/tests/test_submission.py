"""Dispatch credentials and exact admission identity stay in the closed live call."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from artisan_modal import submission
from artisan_modal.bindings import PreparedModalImage
from artisan_modal.image_store import ImageSelection
from artisan_modal.resolution import bind_software
from artisan_modal.submission import prepare_call, validate_dispatch

from artisan.operations.curator.merge import Merge
from artisan.operations.examples.data_transformer import DataTransformer
from artisan.orchestration.runner_api import (
    AdmittedAssignment,
    AdmittedCurator,
    AssignmentAttempt,
    AssignmentDispatch,
    AssignmentReplyContext,
    CuratorAdmissionReference,
    CuratorReplyContext,
    Environment,
    RunnerResources,
    WorkerTransportHeader,
    compile_definition,
    execute_admitted_assignment,
    execute_admitted_curator,
)
from artisan.schemas.execution.storage_config import StorageConfig


def dispatch(root: Path) -> AssignmentDispatch:
    return AssignmentDispatch(
        "s3://test-store/delta",
        "s3://test-store/files",
        "s3://test-store/staging",
        StorageConfig(protocol="s3", options={"secret": "synthetic-storage-value"}),
        root,
        runtime_env={"TOKEN": "synthetic-runtime-value"},
        preserve_working=True,
    )


@pytest.mark.parametrize("protocol", ["file", "local", "memory", "gcs"])
def test_unqualified_storage_rejects_before_serialization(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    tmp_path: Path,
    protocol: str,
) -> None:
    image, _, _ = prepared
    software = bind_software(
        Merge(),
        ImageSelection(image, {}),
        image.startup.worker.compatibility,
        RunnerResources(cpus=1, memory_gb=1, gpus=0),
        (),
    )
    wrong = replace(dispatch(tmp_path), storage=StorageConfig(protocol=protocol))
    with pytest.raises(ValueError, match="shared S3"):
        validate_dispatch(wrong, software)
    with pytest.raises(ValueError, match="explicit S3"):
        validate_dispatch(
            replace(dispatch(tmp_path), files_root="/local/files"), software
        )


@pytest.mark.parametrize("kind", ["assignment", "curator"])
def test_prepare_binds_typed_reply_context_and_preservation_before_sdk_submit(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    image, _, _ = prepared
    plan = compile_definition(DataTransformer)
    software = bind_software(
        plan,
        ImageSelection(image, {}),
        image.startup.worker.compatibility,
        RunnerResources(cpus=1, memory_gb=1, gpus=0),
        (),
    )
    expected = dispatch(tmp_path)
    attempt = AssignmentAttempt.model_validate(
        {
            "assignment": {"step_run_id": "1" * 32, "unit_index": 0},
            "attempt": 0,
            "generation": 0,
            "recipe_digest": plan.recipe.digest,
            "software_digest": software.software_digest,
            "input_context_digest": "2" * 32,
        }
    )
    admitted = (
        AdmittedAssignment(plan, {"attempt": attempt, "record_digest": "3" * 32})
        if kind == "assignment"
        else AdmittedCurator(
            Merge(),
            CuratorAdmissionReference(step_run_id="1" * 32, admission_digest="4" * 32),
        )
    )
    payload = b"opaque qualified live envelope"
    header = WorkerTransportHeader(
        compatibility=software.compatibility,
        payload_sha256=hashlib.sha256(payload).hexdigest(),
    )
    serialize = Mock(return_value=SimpleNamespace(header=header, payload=payload))
    monkeypatch.setattr(submission, "serialize_worker_call", serialize)
    request = prepare_call(admitted, software, expected)
    assert request.preserve_working
    assert serialize.call_args.args == (
        execute_admitted_assignment
        if kind == "assignment"
        else execute_admitted_curator,
    )
    assert serialize.call_args.kwargs["args"] == (admitted, software, expected)
    assert "kwargs" not in serialize.call_args.kwargs
    assert (
        serialize.call_args.kwargs["application_modules"]
        == software.application_modules
    )
    context = (
        AssignmentReplyContext if kind == "assignment" else CuratorReplyContext
    ).model_validate_json(request.reply_context)
    context.verify(header)
    assert set(context.sensitive_values) == {
        "synthetic-storage-value",
        "synthetic-runtime-value",
    }
    if kind == "assignment":
        changed = replace(
            admitted,
            admission=admitted.admission.model_copy(
                update={
                    "attempt": attempt.model_copy(update={"software_digest": "0" * 64})
                }
            ),
        )
        serialize.reset_mock()
        with pytest.raises(ValueError, match="admitted software"):
            prepare_call(changed, software, expected)
        serialize.assert_not_called()
