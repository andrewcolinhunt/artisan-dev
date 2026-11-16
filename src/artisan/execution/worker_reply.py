"""Execute closed worker reply domains without importing stored code names."""

from __future__ import annotations

import json
from contextlib import nullcontext
from pathlib import Path

from artisan.execution.assignment_reply import AssignmentReply, AssignmentReplyContext
from artisan.execution.curator_reply import CuratorReply, CuratorReplyContext
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.execution.worker_bootstrap import PreparedProcessCall, execute_worker_call
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.execution.worker_transport import WorkerTransportHeader
from artisan.utils.command_supervisor import attached_worker_unit


def parse_reply_context(encoded: bytes) -> AssignmentReplyContext | CuratorReplyContext:
    """Parse only registered framework domains before any live object is loaded."""
    domain = json.loads(encoded).get("domain")
    if domain == "assignment":
        return AssignmentReplyContext.model_validate_json(encoded)
    if domain == "curator":
        return CuratorReplyContext.model_validate_json(encoded)
    msg = "Unknown worker reply domain"
    raise ValueError(msg)


def validate_reply(
    encoded: bytes,
    context: AssignmentReplyContext | CuratorReplyContext,
    *,
    sensitive_values: tuple[str, ...] = (),
) -> None:
    """Verify the complete typed reply and owner before a transport returns bytes."""
    if isinstance(context, AssignmentReplyContext):
        AssignmentReply.model_validate_json(encoded).result(
            context.attempt, sensitive_values=sensitive_values
        )
    else:
        CuratorReply.model_validate_json(encoded).result(
            context.admission, sensitive_values=sensitive_values
        )


def execute_call_directory(
    directory: Path, *, attachment: dict[str, str] | None = None
) -> bytes:
    """Verify context, attachment and header before loading an authorized live call."""
    context = parse_reply_context((directory / "reply-context.json").read_bytes())
    try:
        with (
            attached_worker_unit(attachment)
            if attachment is not None
            else nullcontext()
        ):
            header = WorkerTransportHeader.model_validate_json(
                (directory / "header.json").read_bytes()
            )
            context.verify(header)
            result: object = execute_worker_call(
                PreparedProcessCall(header, (directory / "payload.bin").read_bytes()),
            )
        reply: AssignmentReply | CuratorReply
        if isinstance(context, AssignmentReplyContext):
            receipt = SealedDelivery.model_validate(result)
            reply = AssignmentReply(
                attempt=context.attempt, kind="success", receipt=receipt
            )
        else:
            if not isinstance(result, StagingResult):
                msg = "Curator worker returned an invalid staging result"
                raise TypeError(msg)
            reply = CuratorReply.staged(context, result)
    except Exception as error:
        reply = (
            AssignmentReply.failed(
                context.attempt, error, sensitive_values=context.sensitive_values
            )
            if isinstance(context, AssignmentReplyContext)
            else CuratorReply.failed(context, error)
        )
    return reply.model_dump_json().encode()
