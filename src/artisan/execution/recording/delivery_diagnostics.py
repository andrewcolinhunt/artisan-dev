"""Capture bounded diagnostics without creating scientific persistence boundaries."""

from __future__ import annotations

import logging
import time
from array import array
from collections.abc import Callable, Iterator
from concurrent.futures import CancelledError
from contextlib import ExitStack, contextmanager, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.recording.commands import CommandRecorder, capture_commands
from artisan.execution.recording.durable import (
    configuration_recorder,
    sanitize_error_envelope,
)
from artisan.execution.recording.recorder import _read_tool_output
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.execution.assignment import DeliveryIdentity, SealedDelivery
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.delivery_diagnostics import (
    MAX_DIAGNOSTIC_BYTES,
    MAX_DIAGNOSTIC_MESSAGE,
    DeliveryDiagnostics,
    DiagnosticOutcome,
    InvocationDiagnostic,
)
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.core.store_format import assert_retained_store
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.utils.command_supervisor import CommandOwnershipError
from artisan.utils.hashing import canonical_json_bytes
from artisan.utils.log_paths import failure_log_relative_path
from artisan.utils.path import uri_join, uri_parent
from artisan.utils.traceback import format_error

_LOGGER = logging.getLogger(__name__)
_LEFT_CUT = "[truncated]\n"
_RIGHT_CUT = "\n[truncated]"


@dataclass
class InvocationObservation:
    """Mutable phase measurements owned by one local lifecycle."""

    timings: dict[str, float] = field(default_factory=dict)
    log_path: str | None = None
    cleanup: Literal["closed", "unknown"] = "unknown"


class DeliveryRecorder:
    """Observe sequential child calls without retaining their artifact contents."""

    def __init__(
        self,
        *,
        sensitive_values: tuple[str, ...],
        storage: StorageConfig | None = None,
    ) -> None:
        self.timestamp_start = datetime.now(UTC)
        self.redactor = CommandRecorder()
        self.redactor.add_sensitive_data(sensitive_values)
        if storage is not None:
            self.redactor.add_sensitive_data(storage.options)
            self.redactor.add_sensitive_data(storage.delta_options)
        self.invocations: list[InvocationDiagnostic] = []
        self.receipt: SealedDelivery | None = None
        self.cleanup: Literal["closed", "unknown"] = "unknown"
        self.omitted = 0
        self._bytes = 0

    @contextmanager
    def invocation(
        self,
        operation: OperationDefinition,
        invocation_id: str,
        call_path: tuple[str, ...],
    ) -> Iterator[InvocationObservation]:
        """Collect only after the owned invocation scope has attempted closure."""
        observed = InvocationObservation()
        stack = ExitStack()
        commands = None
        try:
            candidate = stack.enter_context(capture_commands(operation))
            configuration_recorder(operation, recorder=candidate)
            candidate.add_sensitive_data(self.redactor.sensitive_values)
            commands = candidate
        except Exception:
            pass
        started = time.perf_counter()
        failure: BaseException | None = None
        try:
            yield observed
        except BaseException as error:
            failure = error
            raise
        finally:
            try:
                stack.close()
                if commands is None:
                    self.omitted += 1
                else:
                    self.redactor.add_sensitive_data(commands.sensitive_values)
                    self._append(
                        operation,
                        invocation_id,
                        call_path,
                        observed,
                        commands,
                        started,
                        failure,
                    )
            except Exception:
                self.omitted += 1

    def _append(
        self,
        operation: OperationDefinition,
        invocation_id: str,
        call_path: tuple[str, ...],
        observed: InvocationObservation,
        commands: CommandRecorder,
        started: float,
        failure: BaseException | None,
    ) -> None:
        closed = observed.cleanup == "closed"
        log = _read_tool_output(observed.log_path) if closed else None
        if log is not None and log.startswith(_LEFT_CUT):
            log = _LEFT_CUT + log.removeprefix(_LEFT_CUT).partition("\n")[2]
        record = InvocationDiagnostic(
            invocation_id=invocation_id,
            call_path=call_path,
            operation_name=operation.name,
            operation_version=operation.version,
            succeeded=failure is None,
            cleanup="closed" if closed else "unknown",
            duration_seconds=time.perf_counter() - started,
            timings=observed.timings,
            command_recording=commands.snapshot()
            if closed
            else CommandRecording.unavailable(),
            tool_output=_sanitize_text(log, commands) if log is not None else None,
            phase=failure.phase
            if isinstance(failure, AssignmentExecutionError)
            else None,
            error=_message(failure, commands),
            error_envelope=_bounded_envelope(failure.error_envelope, commands)
            if isinstance(failure, AssignmentExecutionError)
            else None,
        )
        size = len(canonical_json_bytes(record.model_dump(mode="json")))
        if self._bytes + size > MAX_DIAGNOSTIC_BYTES - 8 * MAX_DIAGNOSTIC_MESSAGE:
            self.omitted += 1
            return
        self.invocations.append(record)
        self._bytes += size

    def finish(
        self,
        delivery: DeliveryIdentity,
        *,
        started: float,
        failure: BaseException | None,
    ) -> DeliveryDiagnostics:
        """Apply all sensitivity learned during the complete assignment once more."""
        science = failure if isinstance(failure, AssignmentExecutionError) else None
        if isinstance(failure, CommandOwnershipError) and isinstance(
            failure.__cause__, AssignmentExecutionError
        ):
            science = failure.__cause__
        records = []
        omitted = self.omitted
        size = 0
        for item in self.invocations:
            record = InvocationDiagnostic.model_validate(
                {
                    **item.model_dump(),
                    "error_envelope": _bounded_envelope(
                        item.error_envelope, self.redactor
                    ),
                    "command_recording": self.redactor.sanitize_recording(
                        item.command_recording
                    ),
                    "tool_output": _sanitize_text(item.tool_output, self.redactor)
                    if item.tool_output is not None
                    else None,
                    "error": _bounded_message(_sanitize_text(item.error, self.redactor))
                    if item.error is not None
                    else None,
                }
            )
            encoded_size = len(canonical_json_bytes(record.model_dump(mode="json")))
            if size + encoded_size > MAX_DIAGNOSTIC_BYTES - 8 * MAX_DIAGNOSTIC_MESSAGE:
                omitted += 1
                continue
            records.append(record)
            size += encoded_size
        outcome: DiagnosticOutcome = "success"
        if isinstance(failure, CommandOwnershipError):
            outcome = "ownership"
        elif isinstance(failure, CancelledError):
            outcome = "cancelled"
        elif science is not None:
            outcome = "science"
        elif failure is not None:
            outcome = "infrastructure"
        return DeliveryDiagnostics(
            delivery=delivery,
            timestamp_start=self.timestamp_start,
            seal_digest=self.receipt.seal_digest if self.receipt is not None else None,
            outcome=outcome,
            cleanup=self.cleanup,
            duration_seconds=time.perf_counter() - started,
            invocations=tuple(records),
            omitted_invocations=omitted,
            phase=science.phase if science is not None else None,
            call_path=science.call_path if science is not None else (),
            error=_message(failure, self.redactor),
            error_envelope=_bounded_envelope(science.error_envelope, self.redactor)
            if science is not None
            else None,
        )


def _bounded_envelope(
    envelope: dict[str, Any] | None, recorder: CommandRecorder
) -> dict[str, Any] | None:
    clean = sanitize_error_envelope(recorder, envelope)
    if clean is None or len(canonical_json_bytes(clean)) > MAX_DIAGNOSTIC_MESSAGE:
        return None
    return clean


def _message(error: BaseException | None, recorder: CommandRecorder) -> str | None:
    if error is None:
        return None
    return _bounded_message(recorder.sanitize(format_error(error)))


def _bounded_message(text: str) -> str:
    if len(text) > MAX_DIAGNOSTIC_MESSAGE:
        return text[: MAX_DIAGNOSTIC_MESSAGE - len(_RIGHT_CUT)] + _RIGHT_CUT
    return text


def _sanitize_text(text: str, recorder: CommandRecorder) -> str:
    """Redact whole values and fragments crossing an explicitly bounded edge."""
    left, right = text.startswith(_LEFT_CUT), text.endswith(_RIGHT_CUT)
    body = text.removeprefix(_LEFT_CUT).removesuffix(_RIGHT_CUT)
    values = recorder.sensitive_values
    start = (
        max((_suffix_prefix_overlap(value, body) for value in values), default=0)
        if left
        else 0
    )
    removed = (
        max((_suffix_prefix_overlap(body, value) for value in values), default=0)
        if right
        else 0
    )
    body = (
        ("<redacted>" if start else "")
        + recorder.sanitize(body[start : max(start, len(body) - removed)])
        + ("<redacted>" if removed else "")
    )
    return (_LEFT_CUT if left else "") + body + (_RIGHT_CUT if right else "")


def _suffix_prefix_overlap(left: str, right: str) -> int:
    """Find the longest suffix/prefix match in linear time and bounded memory."""
    size = min(len(left), len(right))
    if not size:
        return 0
    pattern = right[:size]
    prefix = array("I", [0]) * size
    matched = 0
    for index in range(1, size):
        while matched and pattern[index] != pattern[matched]:
            matched = prefix[matched - 1]
        matched += pattern[index] == pattern[matched]
        prefix[index] = matched
    matched = 0
    for character in left[-size:]:
        while matched and (matched == size or character != pattern[matched]):
            matched = prefix[matched - 1]
        matched += character == pattern[matched]
    return matched


@contextmanager
def capture_delivery_diagnostics(
    delivery: DeliveryIdentity,
    sink: Callable[[DeliveryDiagnostics], None] | None,
    *,
    sensitive_values: tuple[str, ...],
    storage: StorageConfig | None = None,
) -> Iterator[DeliveryRecorder | None]:
    """Keep optional observation/publishing failures from replacing the real outcome."""
    if sink is None:
        yield None
        return
    try:
        recorder = DeliveryRecorder(sensitive_values=sensitive_values, storage=storage)
    except Exception as error:
        _unavailable(delivery, error)
        yield None
        return
    started = time.perf_counter()
    failure: BaseException | None = None
    try:
        yield recorder
    except BaseException as error:
        failure = error
        raise
    finally:
        try:
            sink(recorder.finish(delivery, started=started, failure=failure))
        except Exception as error:
            _unavailable(delivery, error)


def _unavailable(delivery: DeliveryIdentity, error: Exception) -> None:
    with suppress(Exception):
        _LOGGER.warning(
            "Execution diagnostics unavailable for delivery %s (%s)",
            delivery.delivery_id,
            type(error).__name__,
        )


def publish_delivery_diagnostics(
    diagnostic: DeliveryDiagnostics, *, delta_root: str, fs: AbstractFileSystem
) -> None:
    """Write optional delivery observations without admitting or accepting work."""
    assert_retained_store(delta_root, fs)
    diagnostic = DeliveryDiagnostics.model_validate(diagnostic)
    encoded = canonical_json_bytes(diagnostic.model_dump(mode="json"))
    if len(encoded) > MAX_DIAGNOSTIC_BYTES:
        msg = "Delivery diagnostics exceed their byte limit"
        raise ValueError(msg)
    publish_immutable_bytes(fs, _path(delta_root, diagnostic.delivery), encoded)
    if diagnostic.outcome == "science" and diagnostic.cleanup == "closed":
        relative = failure_log_relative_path(
            diagnostic.delivery.delivery_id, diagnostic.timestamp_start
        )
        path = uri_join(uri_parent(delta_root), "logs", "failures", relative)
        publish_immutable_bytes(fs, path, _failure_log(diagnostic))


def _failure_log(diagnostic: DeliveryDiagnostics) -> bytes:
    """Render bounded, already-redacted observations without reading worker files."""
    sections = [
        "=== Execution Failure Log ===",
        f"Run ID: {diagnostic.delivery.delivery_id}",
        f"Started: {diagnostic.timestamp_start.isoformat()}",
        f"Phase: {diagnostic.phase or 'unavailable'}",
        "",
        "=== Error ===",
        diagnostic.error or "unavailable",
    ]
    for invocation in diagnostic.invocations:
        sections.extend(
            ["", f"=== Invocation: {'/'.join(invocation.call_path) or 'root'} ==="]
        )
        if invocation.error:
            sections.append(invocation.error)
        if invocation.tool_output:
            sections.extend(["=== Tool Output ===", invocation.tool_output])
    encoded = "\n".join(sections).encode("utf-8")
    if len(encoded) <= MAX_DIAGNOSTIC_BYTES:
        return encoded
    cut = _RIGHT_CUT.encode("utf-8")
    prefix = encoded[: MAX_DIAGNOSTIC_BYTES - len(cut)]
    return prefix.decode("utf-8", errors="ignore").encode("utf-8") + cut


def read_delivery_diagnostics(
    delivery: DeliveryIdentity,
    *,
    delta_root: str,
    fs: AbstractFileSystem,
    expected_seal_digest: str | None = None,
) -> DeliveryDiagnostics | None:
    """Read exact diagnostic ownership; absent evidence is not command-free success."""
    assert_retained_store(delta_root, fs)
    path = _path(delta_root, delivery)
    if not fs.exists(path):
        return None
    with fs.open(path, "rb", cache_type="none") as stream:
        encoded = stream.read(MAX_DIAGNOSTIC_BYTES + 1)
    if len(encoded) > MAX_DIAGNOSTIC_BYTES:
        msg = "Delivery diagnostics exceed their byte limit"
        raise StoreIntegrityError(msg)
    try:
        diagnostic = DeliveryDiagnostics.model_validate_json(encoded)
    except ValueError:
        msg = "Delivery diagnostics are malformed"
        raise StoreIntegrityError(msg) from None
    if (
        diagnostic.delivery != delivery
        or (
            expected_seal_digest is not None
            and diagnostic.seal_digest != expected_seal_digest
        )
        or canonical_json_bytes(diagnostic.model_dump(mode="json")) != encoded
    ):
        msg = "Delivery diagnostics differ from their exact owner or canonical encoding"
        raise StoreIntegrityError(msg)
    return diagnostic


def _path(delta_root: str, delivery: DeliveryIdentity) -> str:
    return uri_join(
        delta_root,
        "_artisan",
        "diagnostics",
        delivery.attempt.assignment.assignment_id,
        str(delivery.attempt.attempt),
        str(delivery.attempt.generation),
        delivery.delivery_id,
        "execution.json",
    )
