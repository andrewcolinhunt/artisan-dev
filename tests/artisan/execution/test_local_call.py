"""Inert callers share supervised startup, context binding and cleanup semantics."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Literal
from unittest.mock import Mock

import pytest

from artisan.execution.curator_reply import CuratorReply, CuratorReplyContext
from artisan.execution.local_call import InertLocalCallHandle, InertWorkerCall
from artisan.execution.local_profile import LocalWorkerProfile
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.execution.worker_transport import (
    TRANSPORT_PACKAGES,
    DistributionMetadata,
    RuntimeCompatibility,
    WorkerTransportHeader,
)
from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    CommandSupervisor,
    OwnershipOutcome,
)


@pytest.fixture
def inert() -> InertWorkerCall:
    compatibility = RuntimeCompatibility(
        python_implementation="CPython",
        python_version="3.12.12",
        cache_tag="cpython-312",
        packages=tuple(
            DistributionMetadata(
                name=name,
                version="1",
            )
            for name in sorted(TRANSPORT_PACKAGES)
        ),
    )
    payload = b"opaque trusted live call"
    header = WorkerTransportHeader(
        compatibility=compatibility, payload_sha256=hashlib.sha256(payload).hexdigest()
    )
    context = CuratorReplyContext(
        admission=CuratorAdmissionReference(
            step_run_id="a" * 32, admission_digest="b" * 32
        ),
        payload_sha256=header.payload_sha256,
        sensitive_values=(),
    )
    return InertWorkerCall(header, payload, context.model_dump_json().encode())


def test_inert_envelope_rejects_changed_payload_and_reply_context(
    inert: InertWorkerCall,
) -> None:
    with pytest.raises(ValueError, match="exact header"):
        InertWorkerCall(inert.header, b"changed", inert.reply_context)
    changed = CuratorReplyContext.model_validate_json(inert.reply_context).model_copy(
        update={"payload_sha256": "e" * 64}
    )
    with pytest.raises(ValueError, match="exact worker payload"):
        InertWorkerCall(inert.header, inert.payload, changed.model_dump_json().encode())


@pytest.mark.parametrize("closure", ["closed", "unknown"])
@pytest.mark.parametrize("preserve", [False, True])
def test_inert_call_uses_shared_owner_and_never_exposes_unknown_success(
    inert: InertWorkerCall,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    closure: Literal["closed", "unknown"],
    preserve: bool,
) -> None:
    owner = Mock(spec=CommandSupervisor)
    owner.attachment = {"secret": "synthetic-owner"}
    owner.finish.return_value = OwnershipOutcome(closure)
    profile = Mock(spec=LocalWorkerProfile)
    profile.sensitive_values = ()
    monkeypatch.setattr(CommandSupervisor, "start", lambda: owner)

    def launch(handle: InertLocalCallHandle, directory: Path) -> bytes:
        (directory / "worker").mkdir()
        (directory / "worker" / "debug.txt").write_text("worker output")
        context = CuratorReplyContext.model_validate_json(
            (directory / "reply-context.json").read_bytes()
        )
        assert "synthetic-owner" in context.sensitive_values
        reply = CuratorReply.staged(
            context, StagingResult(success=False, error="synthetic failure")
        )
        return handle._decode_reply(reply.model_dump_json().encode())

    monkeypatch.setattr(InertLocalCallHandle, "_launch_worker", launch)
    working_root = tmp_path / "calls"
    handle = InertLocalCallHandle(
        inert,
        profile,
        working_root=working_root,
        runtime_env={},
        preserve_working=preserve,
    )
    if closure == "closed":
        result = handle.result(timeout=5)
        assert not CuratorReply.model_validate_json(result).staging.success
        assert bool(list(working_root.iterdir())) == preserve
        if preserve:
            directory = next(working_root.iterdir())
            assert [path.name for path in directory.iterdir()] == ["worker"]
            assert (directory / "worker" / "debug.txt").read_text() == "worker output"
    else:
        with pytest.raises(CommandOwnershipError):
            handle.result(timeout=5)
        assert list(working_root.iterdir())
    assert handle.ownership_state() == OwnershipState(closure)
    profile.verify.assert_called_once()


@pytest.mark.parametrize("kind", ["inert", "curator"])
def test_nested_native_calls_reject_before_allocating_an_independent_owner(
    inert: InertWorkerCall, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    from artisan.execution.assignment_transport import AssignmentDispatch
    from artisan.execution.curator_worker import AdmittedCurator, LocalCuratorHandle
    from artisan.execution.local_assignment import PreparedLocalBindings
    from artisan.operations.curator.merge import Merge
    from artisan.schemas.execution.storage_config import StorageConfig
    from artisan.utils.configuration_secrets import COMMAND_ATTACHMENT_ENV

    monkeypatch.setenv(COMMAND_ATTACHMENT_ENV[0], "ambient-parent-owner")
    monkeypatch.setattr(
        CommandSupervisor,
        "start",
        lambda: pytest.fail("Nested call created an independent owner"),
    )
    with pytest.raises(CommandOwnershipError, match="Nested native"):
        if kind == "inert":
            InertLocalCallHandle(
                inert,
                Mock(spec=LocalWorkerProfile),
                working_root=tmp_path / "calls",
                runtime_env={},
            )
        else:
            curator = AdmittedCurator(
                Merge(),
                CuratorReplyContext.model_validate_json(inert.reply_context).admission,
            )
            dispatch = AssignmentDispatch(
                str(tmp_path / "delta"),
                str(tmp_path / "files"),
                str(tmp_path / "staging"),
                StorageConfig(),
                tmp_path / "calls",
            )
            LocalCuratorHandle(
                curator,
                Mock(spec=PreparedLocalBindings),
                dispatch,
            )
