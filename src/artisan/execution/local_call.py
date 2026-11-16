"""Shared supervised local call lifecycle for already-qualified worker transport."""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import tempfile
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from concurrent.futures import CancelledError, Future
from dataclasses import dataclass
from pathlib import Path

from artisan.execution.local_profile import LocalWorkerProfile
from artisan.schemas.execution.worker_ownership import OwnershipState, WorkerOwner
from artisan.schemas.execution.worker_transport import WorkerTransportHeader
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.utils._command_supervisor import CLEANUP_SECONDS
from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    CommandSupervisor,
    ManagedCommand,
    OwnershipOutcome,
    reject_nested_native_scope,
)
from artisan.utils.configuration_secrets import COMMAND_ATTACHMENT_ENV
from artisan.utils.external_tools import redact_values


class LocalCallStartupError[ResultT](CommandOwnershipError):
    """Retain the exact call owner when its dispatch thread cannot start."""

    def __init__(
        self, handle: LocalCallHandle[ResultT], outcome: OwnershipOutcome
    ) -> None:
        super().__init__("Local call dispatch could not start")
        self.handle = handle
        self.cleanup_outcome = outcome


class LocalCallHandle[ResultT](ABC):
    """Own preparation, worker launch and cleanup independently of result shape."""

    def __init__(
        self,
        profile: LocalWorkerProfile,
        *,
        working_root: Path,
        runtime_env: Mapping[str, str],
        preserve_working: bool = False,
        dispatch: Callable[[LocalCallHandle[ResultT], Path], ResultT] | None = None,
        worker_owner: WorkerOwner | None = None,
    ) -> None:
        reject_nested_native_scope()
        self._profile = profile
        self._working_root = working_root
        self._runtime_env = dict(runtime_env)
        self._preserve_working = preserve_working
        self._dispatch = dispatch
        self._worker_owner = worker_owner
        self._future: Future[ResultT] = Future()
        self._lock = threading.Lock()
        self._cancelled = False
        self._cancel_deadline: float | None = None
        self._cancel_unknown = False
        self._outcome: OwnershipOutcome | None = None
        self._owner = CommandSupervisor.start()
        self._thread = threading.Thread(target=self._run, name="artisan-local-call")
        try:
            self._thread.start()
        except BaseException as error:
            deadline = time.monotonic() + CLEANUP_SECONDS
            try:
                self._owner.request_close(deadline)
                outcome = self._owner.finish(deadline)
            except BaseException:
                outcome = OwnershipOutcome(
                    "unknown", reason="Local startup cleanup failed"
                )
            self._outcome = outcome
            self._future.set_exception(error)
            raise LocalCallStartupError(self, outcome) from error

    def result(self, timeout: float | None = None) -> ResultT:
        """Return a unit result after its owner closes; shared transport may remain."""
        return self._future.result(timeout=timeout)

    def done(self) -> bool:
        """Report result availability; shared transport cleanup is separate."""
        return self._future.done()

    def ownership_state(self) -> OwnershipState:
        """Combine unit cleanup with the retained shared transport owner's state."""
        with self._lock:
            if self._cancel_unknown:
                return OwnershipState.UNKNOWN
            unit = (
                OwnershipState(self._outcome.state)
                if self._outcome is not None
                else OwnershipState.PENDING
            )
        transport = (
            self._worker_owner.ownership_state()
            if self._worker_owner is not None
            else OwnershipState.CLOSED
        )
        if OwnershipState.UNKNOWN in (unit, transport):
            return OwnershipState.UNKNOWN
        if OwnershipState.PENDING in (unit, transport):
            return OwnershipState.PENDING
        return OwnershipState.CLOSED

    @property
    def worker_owner(self) -> WorkerOwner | None:
        """Expose whole-worker cleanup separately from this unit's cancellation."""
        return self._worker_owner

    def cancel(self) -> CancellationAcknowledgement:
        """Request this owner's closure without cancelling independent siblings."""
        with self._lock:
            if self._future.done():
                assert self._outcome is not None
                if self._outcome.state != "closed" or self._cancel_unknown:
                    return CancellationAcknowledgement(
                        CancellationStatus.UNKNOWN,
                        self._outcome.reason or "Local assignment closure is unknown",
                    )
                if not self._cancelled:
                    return CancellationAcknowledgement(
                        CancellationStatus.REJECTED,
                        "Local assignment already completed",
                    )
                return CancellationAcknowledgement(
                    CancellationStatus.CONFIRMED,
                    self._outcome.reason or "Local assignment owner stopped",
                )
            self._cancelled = True
            self._cancel_deadline = (
                self._cancel_deadline or time.monotonic() + CLEANUP_SECONDS
            )
            self._owner.request_close(self._cancel_deadline)
            if time.monotonic() >= self._cancel_deadline:
                self._cancel_unknown = True
                return CancellationAcknowledgement(
                    CancellationStatus.UNKNOWN,
                    "Local dispatch did not settle before its cleanup deadline",
                )
        return CancellationAcknowledgement(
            CancellationStatus.REQUESTED, "Waiting for local assignment owner closure"
        )

    def _check_cancelled(self) -> None:
        with self._lock:
            if self._cancelled:
                raise CancelledError()

    def _run(self) -> None:
        directory: Path | None = None
        receipt: ResultT | None = None
        error: BaseException | None = None
        try:
            self._check_cancelled()
            self._preflight()
            directory = Path(
                tempfile.mkdtemp(prefix="artisan-assignment-", dir=self._working_root)
            )
            receipt = self._invoke(directory)
        except BaseException as failure:
            error = failure
            self._owner.request_close(time.monotonic() + CLEANUP_SECONDS)
        finally:
            try:
                outcome = self._owner.finish(time.monotonic() + CLEANUP_SECONDS)
            except BaseException as failure:
                error = failure
                outcome = OwnershipOutcome(
                    "unknown", reason="Local owner collection failed"
                )
            if outcome.state == "closed" and directory is not None:
                try:
                    self._cleanup(directory)
                except OSError as failure:
                    error = failure
                    outcome = OwnershipOutcome(
                        "unknown", reason="Local assignment scratch cleanup failed"
                    )
            with self._lock:
                self._outcome = outcome
                if outcome.state != "closed":
                    error = CommandOwnershipError(
                        outcome.reason or "Local worker closure is unknown"
                    )
                elif self._cancelled:
                    error = CancelledError()
                if error is not None:
                    self._future.set_exception(error)
                else:
                    assert receipt is not None
                    self._future.set_result(receipt)

    def _cleanup(self, directory: Path) -> None:
        """Keep requested worker files without retaining transport credentials."""
        worker = directory / "worker"
        if not self._preserve_working or not worker.exists():
            shutil.rmtree(directory)
            return
        for path in directory.iterdir():
            if path == worker:
                continue
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
            else:
                path.unlink()

    def _invoke(self, directory: Path) -> ResultT:
        self._write_payload(directory)
        if self._dispatch is not None:
            return self._dispatch(self, directory)
        return self._launch_worker(directory)

    def _launch_worker(self, directory: Path) -> ResultT:
        environment = self._profile.environment(
            directory / "launcher", self._runtime_env
        )
        command = environment.wrap_command(
            [
                "python",
                "-I",
                "-m",
                "artisan.execution.local_call",
                str(directory / "header.json"),
                str(directory / "payload.bin"),
                str(directory / "result.json"),
            ]
        )
        self._check_cancelled()
        process = ManagedCommand(
            self._owner.attachment,
            command,
            environment.prepare_env(),
            str(directory),
            merged=False,
            external=False,
        )
        try:
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                sensitive = (
                    *self._profile.sensitive_values,
                    *self._runtime_env.values(),
                    self._owner.attachment["secret"],
                )
                message = redact_values(stderr or stdout, sensitive)
                msg = f"Prepared local worker failed ({process.returncode}): {message}"
                raise RuntimeError(msg)
        except BaseException as error:
            process.interrupt(error)
            raise
        finally:
            process.dispose()
        self._check_cancelled()
        return self._decode_reply((directory / "result.json").read_bytes())

    @property
    def sensitive_values(self) -> tuple[str, ...]:
        """Redact the same captured runtime and ownership values in every reply."""
        return (
            *self._profile.sensitive_values,
            *self._runtime_env.values(),
            *self._owner.attachment.values(),
        )

    @abstractmethod
    def _preflight(self) -> None:
        """Verify the trusted request before writing live-call transport."""

    @abstractmethod
    def _write_payload(self, directory: Path) -> None:
        """Write one exact inert header, payload and closed reply context."""

    @abstractmethod
    def _decode_reply(self, encoded: bytes) -> ResultT:
        """Validate the domain-specific authority before exposing its result."""


@dataclass(frozen=True)
class InertWorkerCall:
    """Closed header, exact payload bytes and framework reply context for transport."""

    header: WorkerTransportHeader
    payload: bytes
    reply_context: bytes

    def __post_init__(self) -> None:
        from artisan.execution.worker_reply import parse_reply_context

        parse_reply_context(self.reply_context).verify(self.header)
        if hashlib.sha256(self.payload).hexdigest() != self.header.payload_sha256:
            msg = "Inert call payload differs from its exact header"
            raise ValueError(msg)

    def write(self, directory: Path, *, sensitive_values: tuple[str, ...] = ()) -> None:
        """Add local ownership redaction without changing the submitted call bytes."""
        from artisan.execution.worker_reply import parse_reply_context

        context = parse_reply_context(self.reply_context)
        context = context.model_copy(
            update={"sensitive_values": (*context.sensitive_values, *sensitive_values)}
        )
        _write_private(
            directory / "reply-context.json", context.model_dump_json().encode()
        )
        _write_private(
            directory / "header.json", self.header.model_dump_json().encode()
        )
        _write_private(directory / "payload.bin", self.payload)


class InertLocalCallHandle(LocalCallHandle[bytes]):
    """Launch already serialized trusted calls under the shared owner lifecycle.

    Providers retain their own full software identity inside the trusted payload;
    this node-local profile only supplies qualified interpreter activation.
    """

    def __init__(
        self,
        call: InertWorkerCall,
        profile: LocalWorkerProfile,
        *,
        working_root: Path,
        runtime_env: Mapping[str, str],
        preserve_working: bool = False,
        dispatch: Callable[[LocalCallHandle[bytes], Path], bytes] | None = None,
        worker_owner: WorkerOwner | None = None,
    ) -> None:
        from artisan.execution.compute.prepared import freeze_runtime_injections

        self._call = call
        working_root.mkdir(parents=True, exist_ok=True)
        super().__init__(
            profile,
            working_root=working_root,
            runtime_env=freeze_runtime_injections(runtime_env),
            preserve_working=preserve_working,
            dispatch=dispatch,
            worker_owner=worker_owner,
        )

    def _preflight(self) -> None:
        self._profile.verify()

    def _write_payload(self, directory: Path) -> None:
        self._check_cancelled()
        self._call.write(directory, sensitive_values=self.sensitive_values)

    def _decode_reply(self, encoded: bytes) -> bytes:
        from artisan.execution.worker_reply import parse_reply_context, validate_reply

        validate_reply(
            encoded,
            parse_reply_context(self._call.reply_context),
            sensitive_values=self.sensitive_values,
        )
        return encoded


def _write_private(path: Path, content: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(content)


def main() -> None:
    """Execute one closed reply domain after validating inert call evidence."""
    from artisan.execution.worker_reply import execute_call_directory

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("header", type=Path)
    parser.add_argument("payload", type=Path)
    parser.add_argument("result", type=Path)
    args = parser.parse_args()
    if not all(key in os.environ for key in COMMAND_ATTACHMENT_ENV):
        msg = "Local worker requires its call ownership attachment"
        raise ValueError(msg)
    encoded = execute_call_directory(args.header.parent)
    temporary = args.result.with_suffix(".pending")
    _write_private(temporary, encoded)
    temporary.replace(args.result)


if __name__ == "__main__":
    main()
