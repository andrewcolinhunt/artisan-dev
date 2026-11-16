"""Qualify native, Docker, and Apptainer execution with the same locked software."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from probe import LITERAL, harness_digest


def runtime_environment() -> dict[str, str]:
    """Exercise literal env transport while making unwanted inheritance visible."""
    return {
        "QUALIFICATION_RUNTIME_LITERAL": LITERAL,
        "PYTHONPATH": "/worker-only/python",
        "LD_LIBRARY_PATH": "/worker-only/lib",
        "CUDA_VISIBLE_DEVICES": "qualification-visible-device",
    }


def command(
    backend: str,
    root: Path,
    prepared: Path,
    image: str,
    apptainer: Path,
    sif: Path,
    delivery: str,
    *,
    hold: float = 0,
    harness: Path,
) -> list[str]:
    """Build literal argv with exactly the needed mounts and cwd."""
    if backend == "native":
        executable = prepared / ".pixi/envs/worker/bin/python"
        probe = harness / "containers/probe.py"
        source, output, cwd = (
            root / "inputs/value.json",
            root / "outputs" / delivery,
            root / "work",
        )
        prefix: list[str] = []
    else:
        prepared = Path("/opt/provider-probe")
        executable = prepared / ".pixi/envs/worker/bin/python"
        probe = Path("/opt/qualification/containers/probe.py")
        source, output, cwd = (
            Path("/inputs/value.json"),
            Path("/outputs") / delivery,
            Path("/work"),
        )
        if backend == "docker":
            prefix = [
                "docker",
                "run",
                "--rm",
                "--name",
                f"{root.name}-{delivery}",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "--network",
                "none",
                "--read-only",
                "--cpus",
                "1",
                "--memory",
                "1g",
                "--workdir",
                "/work",
                "--entrypoint",
                str(executable),
                "--tmpfs",
                "/tmp:rw,nosuid,size=64m",
            ]
            prefix.extend(
                [
                    "--mount",
                    f"type=bind,source={harness},target=/opt/qualification,readonly",
                ]
            )
            for name in ("inputs", "outputs", "work"):
                mount = f"type=bind,source={root / name},target=/{name}"
                prefix.extend(
                    ["--mount", mount + (",readonly" if name == "inputs" else "")]
                )
            for name, value in runtime_environment().items():
                prefix.extend(["--env", f"{name}={value}"])
            prefix.append(image)
        else:
            prefix = [
                str(apptainer),
                "exec",
                "--contain",
                "--cleanenv",
                "--no-eval",
                "--pwd",
                "/work",
            ]
            prefix.extend(["--bind", f"{harness}:/opt/qualification:ro"])
            for name in ("inputs", "outputs", "work"):
                prefix.extend(
                    [
                        "--bind",
                        f"{root / name}:/{name}"
                        + (":ro" if name == "inputs" else ":rw"),
                    ]
                )
            for name, value in runtime_environment().items():
                prefix.extend(["--env", f"{name}={value}"])
            prefix.extend([str(sif), str(executable)])
    target_id = image if backend == "docker" else "linux-native"
    launcher_version = sys.version.split()[0]
    if backend == "docker":
        launcher_version = subprocess.check_output(
            [
                "docker",
                "version",
                "--format",
                "{{.Client.Version}}/{{.Server.Version}}",
            ],
            text=True,
        ).strip()
    if backend == "apptainer":
        assert image.startswith("sha256:"), (
            "SIF qualification requires its source image identity"
        )
        launcher_version = subprocess.check_output(
            [str(apptainer), "--version"], text=True
        ).strip()
        with sif.open("rb") as stream:
            target_id = hashlib.file_digest(stream, "sha256").hexdigest()
    invocation = [
        "-I",
        str(probe),
        "--prepared",
        str(prepared),
        "--input",
        str(source),
        "--output",
        str(output),
        "--cwd",
        str(cwd),
        "--hold",
        str(hold),
        "--target-kind",
        backend,
        "--target-id",
        target_id,
        "--launcher-version",
        launcher_version,
        "--harness-digest",
        harness_digest(harness),
        "--source-image-id",
        image if backend != "native" else "",
    ]
    return ([str(executable)] if backend == "native" else prefix) + invocation


def verify(
    root: Path, delivery: str, prepared: Path, backend: str
) -> dict[str, object]:
    """Verify command isolation and hydrate exported bytes through acceptance."""
    import fsspec
    from qualification.persistence.prototype import CandidateStore, Envelope
    from qualification.providers.bootstrap import software_digest

    output = root / "outputs" / delivery
    report = json.loads((output / "container-report.json").read_text())
    assert report["value"] == 16
    assert report["runtime_literal"] == LITERAL
    assert report["input_read_only"]
    assert report["scratch_removed"]
    assert len({phase["pid"] for phase in report["phases"]}) == 1
    assert [tool["dependency_version"] for tool in report["tools"]] == ["24.2", "25.0"]
    expected_prefix = str(prepared) if backend == "native" else "/opt/provider-probe"
    for tool in report["tools"]:
        assert tool["activation"] == tool["role"]
        assert tool["literal"] == "a literal $value with spaces"
        assert tool["python_path"] is None
        assert tool["loader_path"] is None
        assert tool["dependency_path"].startswith(expected_prefix)
        assert tool["selected_credential"] == "synthetic-selected"
        assert tool["owner"] == delivery
    assert not (output / "retained-output.json").exists()
    candidate = output / "candidate"
    envelope = Envelope.model_validate(
        json.loads((candidate / "seal.json").read_text())["envelope"]
    )
    assert envelope.software_digest == software_digest(report["software_descriptor"])
    assert report["software_descriptor"]["target"]["kind"] == backend
    from qualification.lineage.projection import BoundaryProjection

    projection = BoundaryProjection.model_validate(report["projection"])
    store = CandidateStore(
        str(root / f"store-{delivery}"), fs=fsspec.filesystem("file")
    )
    store.activate(envelope, projection)
    store.accept(str(candidate))
    accepted = store.result(envelope.unit_id)
    assert accepted is not None
    assert accepted.exports == projection.exports
    assert store.cache(envelope, projection) == [accepted]
    changed_target = envelope.model_copy(update={"software_digest": "other-target"})
    assert store.cache(changed_target, projection) == []
    if backend != "native":
        changed_descriptor = json.loads(json.dumps(report["software_descriptor"]))
        changed_descriptor["target"]["identity"]["launcher_version"] += "-changed"
        changed_runtime = envelope.model_copy(
            update={"software_digest": software_digest(changed_descriptor)}
        )
        assert store.cache(changed_runtime, projection) == []
    file_occurrence = next(
        item for item in accepted.occurrences if item.artifact_type == "file_ref"
    )
    artifact = store.hydrate(envelope.unit_id, file_occurrence.artifact_id)
    assert json.loads(Path(artifact.path).read_text()) == {"value": 16}
    report["accepted_external_bytes"] = True
    return report


def qualify(
    backend: str,
    root: Path,
    prepared: Path,
    image: str,
    apptainer: Path,
    sif: Path,
    harness: Path,
) -> dict[str, object]:
    """Exercise completion, cancellation, and an independent sibling assignment."""
    if backend == "docker":
        image = subprocess.check_output(
            ["docker", "image", "inspect", image, "--format", "{{.Id}}"],
            text=True,
        ).strip()
    root.mkdir(parents=True, exist_ok=False)
    for name in ("inputs", "outputs", "work"):
        (root / name).mkdir()
    source = root / "inputs/value.json"
    source.write_text('{"value": 5}\n')
    source.chmod(0o444)
    environment = {**os.environ, **runtime_environment()}
    arguments = (backend, root, prepared, image, apptainer, sif)
    subprocess.run(
        command(*arguments, "success", harness=harness),
        cwd=root / "work",
        env=environment,
        check=True,
        timeout=90,
    )
    report = verify(root, "success", prepared, backend)
    with (
        (root / "cancelled.log").open("w") as cancelled_log,
        (root / "sibling.log").open("w") as sibling_log,
    ):
        cancelled = subprocess.Popen(
            command(*arguments, "cancelled", hold=20, harness=harness),
            cwd=root / "work",
            env=environment,
            start_new_session=True,
            stdout=cancelled_log,
            stderr=subprocess.STDOUT,
        )
        sibling = subprocess.Popen(
            command(*arguments, "sibling", harness=harness),
            cwd=root / "work",
            env=environment,
            start_new_session=True,
            stdout=sibling_log,
            stderr=subprocess.STDOUT,
        )
        try:
            marker = root / "outputs/cancelled/scratch/processes.json"
            deadline = time.monotonic() + 35
            while (
                not marker.exists()
                and cancelled.poll() is None
                and time.monotonic() < deadline
            ):
                time.sleep(0.05)
            assert marker.exists(), (root / "cancelled.log").read_text()
            descendants = json.loads(marker.read_text())
            if backend == "docker":
                subprocess.run(
                    ["docker", "stop", "--time", "2", f"{root.name}-cancelled"],
                    check=True,
                    capture_output=True,
                    timeout=10,
                )
            else:
                os.killpg(cancelled.pid, signal.SIGTERM)
            assert cancelled.wait(timeout=15) != 0
            assert sibling.wait(timeout=90) == 0, (root / "sibling.log").read_text()
            if backend != "docker":
                for pid in descendants.values():
                    status = Path(f"/proc/{pid}/status")
                    assert not status.exists() or "State:\tZ" in status.read_text()
            assert not (root / "outputs/cancelled/candidate/seal.json").exists()
            verify(root, "sibling", prepared, backend)
        finally:
            for process in (cancelled, sibling):
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
            if backend == "docker":
                for delivery in ("cancelled", "sibling"):
                    subprocess.run(
                        ["docker", "rm", "-f", f"{root.name}-{delivery}"],
                        capture_output=True,
                        check=False,
                        timeout=10,
                    )
    report.update(
        {"backend": backend, "cancelled_no_seal": True, "sibling_completed": True}
    )
    (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("backend", choices=("native", "docker", "apptainer"))
    parser.add_argument("output", type=Path)
    parser.add_argument("--native-prepared", type=Path, required=True)
    parser.add_argument("--image", default="")
    parser.add_argument("--apptainer", type=Path, default=Path("apptainer"))
    parser.add_argument("--sif", type=Path, default=Path("image.sif"))
    parser.add_argument("--harness", type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    qualify(
        args.backend,
        args.output.resolve(),
        args.native_prepared.resolve(),
        args.image,
        args.apptainer,
        args.sif,
        args.harness.resolve(),
    )
