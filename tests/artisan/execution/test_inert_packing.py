"""Inert provider calls reuse real packed workers and independent unit ownership."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from collections.abc import Callable
from concurrent.futures import CancelledError
from contextlib import suppress
from pathlib import Path
from typing import Literal
from unittest.mock import Mock

import pytest

from artisan.execution.assignment_reply import AssignmentReply, AssignmentReplyContext
from artisan.execution.environments.observed_worker import ObservedWorker
from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.local_call import InertLocalCallHandle, InertWorkerCall
from artisan.execution.local_packing import LocalPackStartupError, launch_inert_calls
from artisan.execution.local_profile import LocalWorkerProfile
from artisan.execution.runtime_compatibility import capture_runtime_compatibility
from artisan.execution.worker_bootstrap import serialize_worker_call
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
    SealedDelivery,
)
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus
from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    CommandSupervisor,
    strip_internal_attachment,
)
from artisan.utils.configuration_secrets import COMMAND_ATTACHMENT_ENV

type ProbeMode = Literal["success", "science", "native", "command"]


@pytest.fixture
def worker() -> LocalWorkerProfile:
    compatibility = capture_runtime_compatibility(TRANSPORT_PACKAGES)
    executable = Path(sys.executable).absolute()
    with executable.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    observed = ObservedWorker(
        executable,
        Path(sys.prefix),
        Path(sys.base_prefix),
        digest,
        compatibility,
        strip_internal_attachment(os.environ),
    )
    return LocalWorkerProfile(
        observed, Path("/unused/pixi"), Path("/usr/bin/env"), None
    )


def _call(
    worker: LocalWorkerProfile,
    evidence: Path,
    index: int,
    mode: ProbeMode = "success",
) -> InertWorkerCall:
    profile = worker
    attempt = AssignmentAttempt(
        assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=index),
        attempt=0,
        generation=0,
        recipe_digest="b" * 32,
        software_digest="c" * 64,
        software_cacheable=False,
        input_context_digest="d" * 32,
    )

    def run() -> SealedDelivery:
        from artisan.utils.command_supervisor import managed_invocation
        from artisan.utils.external_tools import run_command

        class ProbeCommandEnvironment:
            cancellation_scope = "process_group"

            def wrap_command(
                self, command: list[str], cwd: str | None = None
            ) -> list[str]:
                return list(command)

            def prepare_env(self) -> dict[str, str]:
                return {"PATH": os.defpath}

        with managed_invocation():
            evidence.mkdir(parents=True, exist_ok=True)
            (evidence / f"unit-{index}.json").write_text(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "cwd": os.getcwd(),
                        "job_id": os.environ.get("SLURM_JOB_ID"),
                        "literal": os.environ.get("SYNTHETIC_ALLOCATION_VALUE"),
                    }
                )
            )
            if mode == "native":
                deadline = time.monotonic() + 20
                while not (evidence / "release").exists():
                    if time.monotonic() >= deadline:
                        msg = "Probe was not released"
                        raise TimeoutError(msg)
                    time.sleep(0.01)
            elif mode == "command":
                run_command(
                    ProbeCommandEnvironment(),
                    [
                        sys.executable,
                        "-c",
                        "import time; from pathlib import Path; "
                        f"Path({str(evidence / 'command-started')!r}).touch(); "
                        "time.sleep(20)",
                    ],
                )
        if mode == "science":
            msg = f"Expected science failure: {os.environ.get('SYNTHETIC_ALLOCATION_VALUE')}"
            raise AssignmentExecutionError(
                msg, phase="execute", delivery=DeliveryIdentity.new(attempt)
            )
        return SealedDelivery(
            delivery=DeliveryIdentity.new(attempt),
            seal_uri=f"memory://transport-probe/{index}",
            seal_digest="e" * 32,
        )

    assert profile.worker.compatibility is not None
    prepared = serialize_worker_call(run, compatibility=profile.worker.compatibility)
    context = AssignmentReplyContext(
        attempt=attempt,
        payload_sha256=prepared.header.payload_sha256,
        sensitive_values=(),
    )
    return InertWorkerCall(
        prepared.header, prepared.payload, context.model_dump_json().encode()
    )


def _wait_for(predicate: Callable[[], bool]) -> None:
    deadline = time.monotonic() + 15
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("Worker observation did not settle")
        time.sleep(0.01)


def _close(handles: tuple[InertLocalCallHandle, ...], evidence: Path) -> None:
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / "release").touch()
    owner = handles[0].worker_owner
    assert owner is not None
    owner.request_close(time.monotonic() + 5)
    _wait_for(lambda: owner.ownership_state() != OwnershipState.PENDING)
    for handle in handles:
        with suppress(Exception):
            handle.result(timeout=5)


@pytest.mark.parametrize("second", ["success", "science"])
def test_inert_pack_preserves_order_fresh_deliveries_and_one_actual_process(
    worker: LocalWorkerProfile,
    tmp_path: Path,
    second: ProbeMode,
) -> None:
    evidence, scratch = tmp_path / "evidence", tmp_path / "workers"
    calls = (_call(worker, evidence, 0), _call(worker, evidence, 1, second))
    identity = worker.worker.digest
    literal = "literal $HOME ; $(not-executed)"
    handles = launch_inert_calls(
        calls,
        worker,
        working_root=scratch,
        runtime_env={"SLURM_JOB_ID": "12345", "SYNTHETIC_ALLOCATION_VALUE": literal},
    )
    try:
        first = AssignmentReply.model_validate_json(handles[0].result(timeout=15))
        assert first.receipt is not None
        assert first.attempt.assignment.unit_index == 0
        if second == "science":
            with pytest.raises(
                AssignmentExecutionError, match="Expected science"
            ) as error:
                handles[1].result(timeout=15)
            assert literal not in str(error.value)
            assert error.value.delivery is not None
            assert error.value.delivery.attempt.assignment.unit_index == 1
            assert error.value.delivery != first.receipt.delivery
        else:
            last = AssignmentReply.model_validate_json(handles[1].result(timeout=15))
            assert last.receipt is not None
            assert last.attempt.assignment.unit_index == 1
            assert (
                first.receipt.delivery.delivery_id != last.receipt.delivery.delivery_id
            )
        _wait_for(lambda: handles[0].ownership_state() == OwnershipState.CLOSED)
        records = [
            json.loads((evidence / f"unit-{index}.json").read_text())
            for index in (0, 1)
        ]
        assert records[0]["pid"] == records[1]["pid"] != os.getpid()
        assert all(record["job_id"] == "12345" for record in records)
        assert all(record["literal"] == literal for record in records)
        assert worker.worker.digest == identity
        assert handles[0]._owner is not handles[1]._owner
        assert handles[0].worker_owner is handles[1].worker_owner
        assert not list(scratch.iterdir())
    finally:
        _close(handles, evidence)


@pytest.mark.parametrize("mode", ["command", "native"])
def test_inert_unit_cancel_keeps_sibling_and_native_unknown(
    worker: LocalWorkerProfile,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: ProbeMode,
) -> None:
    evidence = tmp_path / "evidence"
    handles = launch_inert_calls(
        (_call(worker, evidence, 0, mode), _call(worker, evidence, 1)),
        worker,
        working_root=tmp_path / "workers",
    )
    try:
        marker = evidence / ("command-started" if mode == "command" else "unit-0.json")
        _wait_for(marker.exists)
        if mode == "native":
            monkeypatch.setattr("artisan.execution.local_call.CLEANUP_SECONDS", 0.1)
        assert handles[0].cancel().status == CancellationStatus.REQUESTED
        if mode == "native":
            time.sleep(0.15)
            assert handles[0].cancel().status == CancellationStatus.UNKNOWN
            assert not handles[0].done()
            assert not handles[1].done()
            (evidence / "release").touch()
        with pytest.raises(
            CommandOwnershipError if mode == "native" else CancelledError
        ):
            handles[0].result(timeout=15)
        assert AssignmentReply.model_validate_json(
            handles[1].result(timeout=15)
        ).receipt
        if mode == "native":
            assert handles[0].ownership_state() == OwnershipState.UNKNOWN
    finally:
        _close(handles, evidence)


def test_inert_pack_rejects_mismatched_header_before_allocating_owners(
    worker: LocalWorkerProfile,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    call = _call(worker, tmp_path, 0)
    changed = call.header.compatibility.model_copy(update={"python_version": "3.12.99"})
    call = InertWorkerCall(
        call.header.model_copy(update={"compatibility": changed}),
        call.payload,
        call.reply_context,
    )
    monkeypatch.setattr(
        CommandSupervisor, "start", lambda: pytest.fail("Created owner")
    )
    with pytest.raises(ValueError, match="exact qualified worker"):
        launch_inert_calls((call,), worker, working_root=tmp_path)


def test_inert_pack_retains_zero_handle_transport_owner_on_startup_failure(
    worker: LocalWorkerProfile,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    call = _call(worker, tmp_path, 0)

    def fail(*args: object, **kwargs: object) -> InertLocalCallHandle:
        msg = "Unit constructor failed"
        raise RuntimeError(msg)

    monkeypatch.setattr("artisan.execution.local_packing.InertLocalCallHandle", fail)
    scratch = tmp_path / "workers"
    with pytest.raises(LocalPackStartupError) as caught:
        launch_inert_calls((call,), worker, working_root=scratch)
    assert caught.value.handles == ()
    assert len(caught.value.worker_owners) == 1
    assert caught.value.worker_owners[0].ownership_state() == OwnershipState.CLOSED
    assert not list(scratch.iterdir())


@pytest.mark.parametrize(
    "injections",
    [
        {"PATH": "/unqualified"},
        {"PYTHONPATH": "/unqualified"},
        {"LD_PRELOAD": "/unqualified.so"},
        {COMMAND_ATTACHMENT_ENV[0]: "foreign-owner"},
        {"SLURM_JOB_ID": "invalid\0value"},
    ],
)
def test_inert_allocation_injections_reject_controls_before_starting_owners(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, injections: dict[str, str]
) -> None:
    monkeypatch.setattr(
        CommandSupervisor, "start", lambda: pytest.fail("Created owner")
    )
    with pytest.raises(ValueError, match="Runtime injections"):
        launch_inert_calls(
            (),
            Mock(spec=LocalWorkerProfile),
            working_root=tmp_path,
            runtime_env=injections,
        )
