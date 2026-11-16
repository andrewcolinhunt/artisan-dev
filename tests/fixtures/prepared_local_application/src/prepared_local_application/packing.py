"""Assert independent outcomes inside actual same-process local transport packs."""

from __future__ import annotations

import json
import time
from concurrent.futures import CancelledError
from pathlib import Path

import fsspec

from artisan.composites.base.compiler import compile_definition
from artisan.execution.local_assignment import (
    PreparedLocalBindings,
    bind_local_software,
)
from artisan.execution.local_packing import (
    LocalAssignmentRequest,
    launch_local_assignments,
)
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus
from artisan.storage.io.retained_seal import capture_retained_delivery
from artisan.utils.command_supervisor import CommandOwnershipError
from prepared_local_application import NativeIncrement, TwoToolSequence
from prepared_local_application.probe import Seal, admit, assert_dead, inspect


def _wait(path: Path) -> None:
    deadline = time.monotonic() + 90
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert path.exists()


def run_packing(
    bindings: PreparedLocalBindings,
    manifest: Path,
    cache: Path,
    work: Path,
    runtime: dict[str, str],
) -> dict[str, object]:
    """Prove same PID, independent seals/failures, and truthful native cancellation."""
    plan = compile_definition(TwoToolSequence, params={"manifest": str(manifest)})
    sealer = Seal(str(work / "sealed"))
    native = compile_definition(NativeIncrement)
    native_bindings = bind_local_software(
        native,
        bindings.worker,
        cache_root=cache,
        pixi=bindings.pixi,
        system_env=bindings.system_env,
    )
    assignments = [admit(plan, bindings.software_digest, unit) for unit in (10, 11)]
    restored = admit(native, native_bindings.software_digest, 18)
    literals = [runtime, {**runtime, "PROBE_CREDENTIAL": "synthetic-second"}]
    handles = launch_local_assignments(
        [
            LocalAssignmentRequest(assignment, bindings, sealer, values)
            for assignment, values in zip(assignments, literals, strict=True)
        ]
        + [LocalAssignmentRequest(restored, native_bindings, sealer)],
        working_root=work / "workers",
        units_per_worker=3,
    )
    pair = [
        inspect(
            handle.result(timeout=120),
            assignment,
            16,
            selected=values["PROBE_CREDENTIAL"],
        )
        for handle, assignment, values in zip(
            handles[:2], assignments, literals, strict=True
        )
    ]
    assert pair[0]["worker_pid"] == pair[1]["worker_pid"]
    assert pair[0]["delivery_id"] != pair[1]["delivery_id"]
    assert pair[0]["tools"][0]["scratch"] != pair[1]["tools"][0]["scratch"]
    restored_receipt = handles[2].result(timeout=120)
    boundary = {
        entry.boundary: restored.inputs[entry.boundary.role][entry.boundary.index]
        for entry in restored.input_attribution
    }
    with capture_retained_delivery(
        restored_receipt,
        boundary,
        expected_recipe=restored.plan.recipe,
        fs=fsspec.filesystem("file"),
    ) as captured:
        metric = captured.artifacts[captured.projection.export_roles()["metrics"][0]]
        assert metric.values["worker_pid"] == pair[0]["worker_pid"]
        assert metric.values["native_selected"] is None
        assert metric.values["native_literal"] is None

    failure = compile_definition(NativeIncrement, params={"fail": True})
    good, bad = (
        admit(native, native_bindings.software_digest, 12),
        admit(failure, native_bindings.software_digest, 13),
    )
    first, second = launch_local_assignments(
        [LocalAssignmentRequest(item, native_bindings, sealer) for item in (good, bad)],
        working_root=work / "workers",
        units_per_worker=2,
    )
    receipt = first.result(timeout=120)
    try:
        second.result(timeout=120)
    except RuntimeError:
        pass
    else:
        msg = "Failing packed unit returned success"
        raise AssertionError(msg)
    assert Path(receipt.seal_uri).is_file()

    marker = work / "packed-command-descendants.json"
    held_plan = compile_definition(
        TwoToolSequence,
        params={"manifest": str(manifest), "hold": 60, "marker": str(marker)},
    )
    held, sibling = (
        admit(held_plan, bindings.software_digest, 14),
        admit(plan, bindings.software_digest, 15),
    )
    cancelled, survivor = launch_local_assignments(
        [
            LocalAssignmentRequest(item, bindings, sealer, runtime)
            for item in (held, sibling)
        ],
        working_root=work / "workers",
        units_per_worker=2,
    )
    _wait(marker)
    assert cancelled.cancel().status == CancellationStatus.REQUESTED
    try:
        cancelled.result(timeout=20)
    except CancelledError:
        pass
    else:
        msg = "Cancelled packed command returned success"
        raise AssertionError(msg)
    assert cancelled.cancel().status == CancellationStatus.CONFIRMED
    for pid in json.loads(marker.read_text()).values():
        assert_dead(pid)
    survivor_evidence = inspect(survivor.result(timeout=120), sibling, 16)

    native_marker, release = work / "native-entered", work / "native-release"
    blocked_plan = compile_definition(
        NativeIncrement, params={"marker": str(native_marker), "release": str(release)}
    )
    blocked, after = (
        admit(blocked_plan, native_bindings.software_digest, 16),
        admit(native, native_bindings.software_digest, 17),
    )
    blocked_handle, after_handle = launch_local_assignments(
        [
            LocalAssignmentRequest(item, native_bindings, sealer)
            for item in (blocked, after)
        ],
        working_root=work / "workers",
        units_per_worker=2,
    )
    _wait(native_marker)
    assert blocked_handle.cancel().status == CancellationStatus.REQUESTED
    time.sleep(10.2)
    assert blocked_handle.cancel().status == CancellationStatus.UNKNOWN
    assert not blocked_handle.done()
    release.write_text("release")
    try:
        blocked_handle.result(timeout=30)
    except (CancelledError, CommandOwnershipError):
        pass
    else:
        msg = "Blocked cancelled native unit returned success"
        raise AssertionError(msg)
    assert blocked_handle.cancel().status == CancellationStatus.UNKNOWN
    after_receipt = after_handle.result(timeout=120)
    boundary = {
        entry.boundary: after.inputs[entry.boundary.role][entry.boundary.index]
        for entry in after.input_attribution
    }
    with capture_retained_delivery(
        after_receipt,
        boundary,
        expected_recipe=after.plan.recipe,
        fs=fsspec.filesystem("file"),
    ) as captured:
        metric = captured.artifacts[captured.projection.export_roles()["metrics"][0]]
        assert metric.values["worker_pid"] == int(native_marker.read_text())
    return {
        "same_process": pair,
        "prior_success_preserved": receipt.delivery.delivery_id,
        "command_cancel_sibling": survivor_evidence,
        "blocked_native_cancel": "unknown",
    }
