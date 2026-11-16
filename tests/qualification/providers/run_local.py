"""Exercise real prepared interpreters, transport fencing, and process ownership."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import time
from pathlib import Path

import bootstrap
import cloudpickle
import sequence
from application_schema import ProbeInput

from artisan.schemas.artifact.metric import MetricArtifact


class DeserializeCanary:
    """Write one harmless marker only if a payload is deserialized."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def __reduce__(self) -> tuple[object, tuple[str]]:
        return eval, (
            f"__import__('pathlib').Path({str(self.path)!r}).write_text('unpickled')",
        )


def launch(
    root: Path,
    prepared: Path,
    delivery_id: str,
    *,
    canary: bool = False,
    mismatch: str | None = None,
    hold: float = 0,
    delay: float = 0,
    target_kind: str = "native",
    target_id: str = "prepared-pixi",
) -> subprocess.Popen[str]:
    """Launch an inert-header bootstrap with a separately serialized payload."""
    assignment = root / delivery_id
    assignment.mkdir()
    identity = bootstrap.runtime_identity(
        [
            "cloudpickle",
            "pydantic",
            "dexterity-artisan",
            "polars",
            "deltalake",
            "pyarrow",
        ]
    )
    if mismatch in identity:
        identity[mismatch] = "0.0.invalid"
    receipt = json.loads((prepared / "receipt.json").read_text())
    header = {
        "runtime": identity,
        "framework_digest": bootstrap.framework_digest(),
        "application_digest": bootstrap.application_digest(),
        "prepared": str(prepared),
        "lock_digest": receipt["lock_digest"],
        "selected_tools": ["a", "b"],
    }
    if mismatch == "source":
        header["framework_digest"] = "stale source"
    if mismatch == "application":
        header["application_digest"] = "stale application schema"
    if mismatch == "lock":
        header["lock_digest"] = "stale lock"
    if mismatch == "selection":
        header["selected_tools"] = ["unprepared-tool"]
    descriptor = bootstrap.software_descriptor(
        header, bootstrap.target_identity(target_kind, target_id)
    )
    (assignment / "software-descriptor.json").write_text(
        json.dumps(descriptor, indent=2) + "\n"
    )
    (assignment / "header.json").write_text(json.dumps(header))
    arguments = {
        "delivery_root": str(assignment / "delivery"),
        "tool_script": str(Path(__file__).with_name("tool.py").resolve()),
        "environments": json.loads((prepared / "tool-environments.json").read_text()),
        "tool_hold_seconds": hold,
        "delay_seconds": delay,
        "input_artifact": MetricArtifact.draft({"value": 5}, "input.json", 1),
        "input_schema": ProbeInput(
            metric=MetricArtifact.draft({"value": 5}, "input.json", 1)
        ),
        "software_descriptor": descriptor,
        "envelope": {
            "unit_id": "unit",
            "attempt": 0,
            "generation": 0,
            "delivery_id": delivery_id,
            "recipe_digest": "two-tool-sequence",
            "software_digest": bootstrap.software_digest(descriptor),
        },
    }
    operation = (
        DeserializeCanary(assignment / "unpickled")
        if canary
        else sequence.execute_sequence
    )
    (assignment / "payload.bin").write_bytes(cloudpickle.dumps((operation, arguments)))
    command = [
        str(prepared / ".pixi/envs/worker/bin/python"),
        "-I",
        str(Path(__file__).with_name("bootstrap.py").resolve()),
        str(assignment / "header.json"),
        str(assignment / "payload.bin"),
        str(assignment / "result.json"),
    ]
    return subprocess.Popen(
        command,
        env={
            **os.environ,
            "PYTHONPATH": "/worker-only/python",
            "LD_LIBRARY_PATH": "/worker-only/lib",
            "CUDA_VISIBLE_DEVICES": "qualification-visible-device",
        },
        start_new_session=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def wait_success(process: subprocess.Popen[str]) -> None:
    """Require actual process completion before inspecting staged results."""
    stdout, stderr = process.communicate(timeout=45)
    assert process.returncode == 0, stdout + stderr


def assert_dead(pid: int) -> None:
    """Allow a reaped-or-zombie descendant but reject a surviving process."""
    status = Path(f"/proc/{pid}/status")
    assert not status.exists() or "State:\tZ" in status.read_text(), (
        f"process survived: {pid}"
    )


def qualify(
    root: Path,
    prepared: Path,
    *,
    target_kind: str = "native",
    target_id: str = "prepared-pixi",
) -> dict[str, object]:
    """Run success, negative bootstrap, cancellation, sibling, and reentry probes."""
    cloudpickle.register_pickle_by_value(sequence)
    root.mkdir(parents=True, exist_ok=False)
    from functools import partial

    submit = partial(launch, target_kind=target_kind, target_id=target_id)
    wait_success(submit(root, prepared, "first"))
    evidence = json.loads((root / "first/result.json").read_text())
    assert evidence["value"] == 16
    assert len({phase["pid"] for phase in evidence["phases"]}) == 1
    assert [tool["dependency_version"] for tool in evidence["tools"]] == [
        "24.2",
        "25.0",
    ]
    for tool in evidence["tools"]:
        assert tool["activation"] == tool["role"]
        assert tool["literal"] == "a literal $value with spaces"
        assert tool["python_path"] is None
        assert tool["loader_path"] is None
        assert tool["visible_gpu"] == "qualification-visible-device"
        assert tool["selected_credential"] == "synthetic-selected"
        assert tool["owner"] == evidence["delivery_id"]
        assert (
            str(prepared / f".pixi/envs/tool-{tool['role']}") in tool["dependency_path"]
        )
    assert not (root / "first/delivery/scratch").exists()
    for mismatch in (
        "python",
        "cloudpickle",
        "pydantic",
        "source",
        "application",
        "lock",
        "selection",
    ):
        process = submit(root, prepared, mismatch, canary=True, mismatch=mismatch)
        _, stderr = process.communicate(timeout=15)
        assert process.returncode != 0
        assert "mismatch" in stderr or "not prepared" in stderr
        assert not (root / mismatch / "unpickled").exists()
    control = submit(root, prepared, "canary-control", canary=True)
    control.communicate(timeout=15)
    assert (root / "canary-control/unpickled").exists()
    cancelled = submit(root, prepared, "cancelled", hold=20)
    sibling = submit(root, prepared, "sibling", delay=1)
    marker = root / "cancelled/delivery/scratch/processes.json"
    deadline = time.monotonic() + 10
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert marker.exists()
    descendants = json.loads(marker.read_text())
    os.killpg(cancelled.pid, signal.SIGTERM)
    cancelled.communicate(timeout=10)
    assert cancelled.returncode == -signal.SIGTERM
    time.sleep(0.1)
    for pid in descendants.values():
        assert_dead(pid)
    assert not (root / "cancelled/result.json").exists()
    wait_success(sibling)
    wait_success(submit(root, prepared, "redelivery"))
    assert (root / "first/delivery/evidence.json").read_bytes() != (
        root / "redelivery/delivery/evidence.json"
    ).read_bytes()
    return {
        "status": "passed",
        "runtime": bootstrap.runtime_identity(["cloudpickle", "pydantic"]),
        "first_delivery": evidence,
        "bootstrap_mismatches_rejected": [
            "python",
            "cloudpickle",
            "pydantic",
            "source",
            "application",
            "lock",
            "selection",
        ],
        "canary_positive_control": True,
        "cancelled_descendants": descendants,
        "sibling_completed": True,
        "redelivery_isolated": True,
        "gpu_visibility": "synthetic environment preservation only; hardware not exercised",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("prepared", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--target-kind", default="native")
    parser.add_argument("--target-id", default="prepared-pixi")
    args = parser.parse_args()
    report = qualify(
        args.output.resolve(),
        args.prepared.resolve(),
        target_kind=args.target_kind,
        target_id=args.target_id,
    )
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
