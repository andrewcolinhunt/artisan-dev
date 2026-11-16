"""Qualify prepared tool containers with real ownership, binds and cancellation."""

from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import time
import uuid
from pathlib import Path
from typing import Any

from artisan.execution.environments.command_witness import (
    WitnessInvocation,
    lookup_command_witness,
)
from artisan.execution.environments.container_launch import (
    PreparedContainerCommandEnvironment,
    validate_container_bind_path,
)
from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
    capture_gpu_driver,
)
from artisan.execution.environments.prepared_oci import lookup_oci_environment
from artisan.execution.environments.prepared_sif import lookup_sif_source_environment
from artisan.schemas.execution.command_witness import PreparedCommandWitness
from artisan.schemas.execution.container_environment import (
    PreparedOCIEnvironment,
    PreparedSIFEnvironment,
)
from artisan.schemas.operation_config.environment import Environment
from artisan.utils.command_supervisor import (
    CommandSupervisor,
    initialize_native_worker,
    managed_invocation,
)
from artisan.utils.external_tools import run_command

LITERAL = 'literal $value, spaces and "quotes"\nsecond line'
PROGRAM = r"""
import errno,json,os,pathlib,subprocess,sys,time
from native_probe import transform
source,hold,token=sys.argv[1:]
source=pathlib.Path(source)
value=json.loads(source.read_text())["value"]
try:
    source.write_text("modified")
except OSError as error:
    assert error.errno==errno.EROFS,error
else:
    raise AssertionError("Input mount is writable")
assert os.environ.get("PYTHONPATH") is None
assert os.environ.get("LD_LIBRARY_PATH") is None
assert os.environ["PROBE_CREDENTIAL"]=="synthetic-selected"
if float(hold):
    child=subprocess.Popen([sys.executable,"-c","import time; time.sleep(120)",token],start_new_session=True,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    pathlib.Path("started.json").write_text(json.dumps({"child":child.pid}))
time.sleep(float(hold))
pathlib.Path("result.json").write_text(json.dumps({"value":transform(value),"activation":os.environ.get("PROBE_ACTIVATION"),"literal":os.environ["PROBE_LITERAL"],"cwd":str(pathlib.Path.cwd())}))
"""


def _worker(
    target: PreparedOCIEnvironment | PreparedSIFEnvironment,
    runtime: DockerRuntime | ApptainerRuntime,
    sandbox: Path,
    attachment: dict[str, str],
    hold: float,
    token: str,
    witness: PreparedCommandWitness,
) -> None:
    initialize_native_worker(attachment)
    environment = PreparedContainerCommandEnvironment(
        target,
        runtime,
        sandbox,
        {"PROBE_LITERAL": LITERAL, "PROBE_CREDENTIAL": "synthetic-selected"},
        witness=WitnessInvocation(witness, sandbox / "command-status"),
    )
    os.environ.update(
        PYTHONPATH="/worker-only/python", LD_LIBRARY_PATH="/worker-only/libraries"
    )
    try:
        with managed_invocation():
            run_command(
                environment,
                [
                    "python",
                    "-I",
                    "-c",
                    PROGRAM,
                    str(sandbox / "preprocess/input.json"),
                    str(hold),
                    token,
                ],
                cwd=str(sandbox / "execute"),
            )
    except Exception as error:
        (sandbox / "failure.json").write_text(json.dumps({"error": str(error)}))
        raise


def _start(
    target: PreparedOCIEnvironment | PreparedSIFEnvironment,
    runtime: DockerRuntime | ApptainerRuntime,
    root: Path,
    *,
    hold: float,
    token: str,
    witness: PreparedCommandWitness,
) -> tuple[CommandSupervisor, Any, Path]:
    sandbox = root / "invocation"
    for name in ("preprocess", "execute"):
        (sandbox / name).mkdir(parents=True)
    (sandbox / "preprocess/input.json").write_text('{"value":6}')
    owner = CommandSupervisor.start()
    process = multiprocessing.get_context("spawn").Process(
        target=_worker,
        args=(target, runtime, sandbox, owner.attachment, hold, token, witness),
    )
    process.start()
    return owner, process, sandbox


def _processes(token: str) -> set[int]:
    found = set()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            command = (entry / "cmdline").read_bytes().split(b"\0")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if token.encode() in command:
            found.add(int(entry.name))
    return found


def qualify(
    backend: str,
    cache: Path,
    output: Path,
    apptainer: Path | None,
    temporary: Path | None,
    gpu_devices: tuple[str, ...] | None = None,
) -> None:
    """Run one normal result and cancel another while an independent sibling succeeds."""
    declaration = Environment.from_pixi(Path(__file__).parent / "source/pyproject.toml")
    runtime: DockerRuntime | ApptainerRuntime
    target: PreparedOCIEnvironment | PreparedSIFEnvironment
    if backend == "docker":
        runtime = DockerRuntime(Path("/usr/bin/docker"), gpu_devices=gpu_devices)
        target = lookup_oci_environment(declaration, cache_root=cache, docker=runtime)
    else:
        if apptainer is None:
            msg = "An explicit Apptainer executable is required"
            raise ValueError(msg)
        runtime = ApptainerRuntime(apptainer, temporary, gpu_devices)
        target = lookup_sif_source_environment(
            declaration, cache_root=cache, apptainer=runtime
        )
    validate_container_bind_path(output, runtime)
    witness = lookup_command_witness(cache)
    output.mkdir(parents=True, exist_ok=False)
    if gpu_devices:
        _qualify_gpu(target, runtime, output, witness)
        return
    token = "artisan-container-probe-" + uuid.uuid4().hex
    running = []
    try:
        owner, process, sandbox = _start(
            target, runtime, output / "normal", hold=0, token="", witness=witness
        )
        running.append((owner, process))
        process.join(90)
        assert process.exitcode == 0, (sandbox / "failure.json").read_text()
        assert owner.finish(time.monotonic() + 10).state == "closed"
        result = json.loads((sandbox / "execute/result.json").read_text())
        assert result == {
            "value": 18,
            "activation": "qualified",
            "literal": LITERAL,
            "cwd": str(sandbox / "execute"),
        }
        cancelled, child, cancel_sandbox = _start(
            target,
            runtime,
            output / "cancelled",
            hold=120,
            token=token,
            witness=witness,
        )
        sibling, sibling_process, sibling_sandbox = _start(
            target, runtime, output / "sibling", hold=0, token="", witness=witness
        )
        running.extend(((cancelled, child), (sibling, sibling_process)))
        deadline = time.monotonic() + 90
        while (
            not (cancel_sandbox / "execute/started.json").exists()
            and child.is_alive()
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
        assert (cancel_sandbox / "execute/started.json").exists()
        descendants = _processes(token)
        assert descendants
        cancelled.request_close(time.monotonic() + 10)
        child.join(15)
        assert child.exitcode is not None
        assert child.exitcode != 0
        cancelled_outcome = cancelled.finish(time.monotonic() + 10)
        assert cancelled_outcome.state == "closed", cancelled_outcome
        assert not _processes(token)
        assert not (cancel_sandbox / "execute/result.json").exists()
        sibling_process.join(90)
        assert sibling_process.exitcode == 0, (
            sibling_sandbox / "failure.json"
        ).read_text()
        assert sibling.finish(time.monotonic() + 10).state == "closed"
        assert (
            json.loads((sibling_sandbox / "execute/result.json").read_text())["value"]
            == 18
        )
        report = {
            "status": "passed",
            "backend": backend,
            "target_digest": target.digest,
            "launcher": runtime.identity().model_dump(mode="json"),
            "normal": result,
            "cancelled_descendants_stopped": True,
            "sibling_completed": True,
            "readonly_input": True,
            "clean_tool_environment": True,
        }
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
    finally:
        for owner, process in running:
            if process.is_alive():
                owner.request_close(time.monotonic() + 10, immediate=True)
                process.join(15)
            owner.finish(time.monotonic() + 10)


def _qualify_gpu(
    target: PreparedOCIEnvironment | PreparedSIFEnvironment,
    runtime: DockerRuntime | ApptainerRuntime,
    output: Path,
    witness: PreparedCommandWitness,
) -> None:
    driver = capture_gpu_driver(runtime.gpu_devices)
    assert driver is not None
    sandbox = output / "invocation"
    execute = sandbox / "execute"
    execute.mkdir(parents=True)
    environment = PreparedContainerCommandEnvironment(
        target,
        runtime,
        sandbox,
        {
            "QUALIFICATION_SELECTED_CREDENTIAL": "synthetic-selected",
            "QUALIFICATION_OWNER": "owned-probe",
        },
        witness=WitnessInvocation(witness, sandbox / "command-status"),
    )
    program = (Path(__file__).parents[1] / "providers/gpu_probe.py").read_text()
    with managed_invocation():
        completed = run_command(
            environment, ["python", "-I", "-c", program], cwd=str(execute)
        )
    report = json.loads(completed.stdout)
    assert report["output"] == 42
    assert report["device"] == "NVIDIA A10"
    assert report["device_count"] == 1
    assert report["pythonpath"] is None
    assert report["loader_path"] == (
        "/.singularity.d/libs" if isinstance(runtime, ApptainerRuntime) else None
    )
    assert report["selected_credential"] == "synthetic-selected"
    assert report["owner"] == "owned-probe"
    report.update(
        target_digest=target.digest,
        driver=driver.model_dump(mode="json"),
        launcher=runtime.identity().model_dump(mode="json"),
    )
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


def main() -> None:
    """Read explicit local cache, launcher and disposable output paths."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("backend", choices=("docker", "apptainer"))
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apptainer", type=Path)
    parser.add_argument("--temporary-root", type=Path)
    parser.add_argument("--gpu-device", action="append")
    args = parser.parse_args()
    qualify(
        args.backend,
        args.cache_root.resolve(),
        args.output.resolve(),
        args.apptainer,
        args.temporary_root,
        tuple(args.gpu_device) if args.gpu_device is not None else None,
    )


if __name__ == "__main__":
    main()
