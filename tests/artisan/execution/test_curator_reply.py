"""Curator transport cannot swap owners, bypass bootstrap or expose runtime secrets."""

from __future__ import annotations

from pathlib import Path

import pytest

from artisan.execution.curator_reply import CuratorReply, CuratorReplyContext
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.execution.worker_reply import execute_call_directory
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.worker_transport import (
    RuntimeCompatibility,
    WorkerTransportHeader,
)
from artisan.utils.command_supervisor import CommandOwnershipError


def context() -> CuratorReplyContext:
    return CuratorReplyContext(
        admission=CuratorAdmissionReference(
            step_run_id="a" * 32, admission_digest="b" * 32
        ),
        payload_sha256="c" * 64,
        sensitive_values=("synthetic-secret",),
    )


def test_staged_failure_and_owner_are_preserved_without_exception_pickles() -> None:
    captured = context()
    original = StagingResult(
        success=False, error="synthetic-secret", execution_run_id="d" * 32
    )
    reply = CuratorReply.staged(captured, original)
    encoded = reply.model_dump_json()
    assert "synthetic-secret" not in encoded
    result = CuratorReply.model_validate_json(encoded).result(captured.admission)
    assert not result.success
    assert original.error == "synthetic-secret"
    with pytest.raises(ValueError, match="different curator"):
        reply.result(captured.admission.model_copy(update={"step_run_id": "e" * 32}))


@pytest.mark.parametrize("ownership", [False, True])
def test_failure_causes_are_redacted_and_ownership_remains_typed(
    ownership: bool,
) -> None:
    try:
        msg = "synthetic-secret"
        raise ValueError(msg)
    except ValueError as error:
        try:
            if ownership:
                raise CommandOwnershipError() from error
            msg = "outer failure"
            raise RuntimeError(msg) from error
        except Exception as outer:
            reply = CuratorReply.failed(context(), outer)
    assert "synthetic-secret" not in reply.model_dump_json()
    with pytest.raises(CommandOwnershipError if ownership else RuntimeError):
        reply.result(context().admission)


def test_swapped_payload_pair_fails_before_live_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.schemas.execution.worker_transport import (
        TRANSPORT_PACKAGES,
        DistributionMetadata,
    )

    compatibility = RuntimeCompatibility(
        python_implementation="CPython",
        python_version="3.12.12",
        cache_tag="cpython-312",
        packages=tuple(
            DistributionMetadata(name=name, version="1")
            for name in sorted(TRANSPORT_PACKAGES)
        ),
    )
    header = WorkerTransportHeader(compatibility=compatibility, payload_sha256="d" * 64)
    (tmp_path / "reply-context.json").write_text(context().model_dump_json())
    (tmp_path / "header.json").write_text(header.model_dump_json())
    (tmp_path / "payload.bin").write_bytes(b"not loaded")
    monkeypatch.setattr(
        "artisan.execution.worker_reply.execute_worker_call",
        lambda *args, **kwargs: pytest.fail("Swapped payload reached live decoder"),
    )
    reply = CuratorReply.model_validate_json(execute_call_directory(tmp_path))
    with pytest.raises(RuntimeError, match="exact worker payload"):
        reply.result(context().admission)


def test_unknown_reply_domain_rejects_before_worker_loading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "reply-context.json").write_text('{"domain":"application.path"}')
    monkeypatch.setattr(
        "artisan.execution.worker_reply.execute_worker_call",
        lambda *args, **kwargs: pytest.fail("Unknown domain loaded application code"),
    )
    with pytest.raises(ValueError, match="Unknown worker reply domain"):
        execute_call_directory(tmp_path)
