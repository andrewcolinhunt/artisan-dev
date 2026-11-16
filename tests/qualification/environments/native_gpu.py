"""Exercise native GPU launch with existing observed and prepared interpreters.

This probe installs nothing. It validates launcher/device/driver behavior; a full
installed assignment transport run remains a separate qualification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

from artisan.execution.compute.prepared import PreparedCommandEnvironment
from artisan.execution.environments.native_gpu import (
    capture_native_gpu,
    verify_native_gpu,
)
from artisan.execution.environments.native_target import verify_native_environment
from artisan.execution.environments.observed_worker import (
    ObservedCommandEnvironment,
    qualify_observed_worker,
)
from artisan.execution.local_profile import LocalLauncherIdentity
from artisan.execution.recording.commands import capture_commands
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES
from artisan.utils.command_supervisor import managed_invocation
from artisan.utils.external_tools import run_command

PROGRAM = """
import hashlib,json,runpy,sys
from pathlib import Path
if sys.argv[4]=='observed':
    from artisan.execution.environments.native_gpu import verify_native_gpu_loader
    from artisan.schemas.execution.gpu import NativeGPUAllocation
    verify_native_gpu_loader(NativeGPUAllocation.model_validate_json(sys.argv[3]))
probe=runpy.run_path(sys.argv[1],run_name='qualified_gpu_probe')
result=probe['run_kernel']()
expected=json.loads(sys.argv[2])
mapped={line.split(maxsplit=5)[5] for line in Path('/proc/self/maps').read_text().splitlines() if len(line.split(maxsplit=5))==6}
for library in expected:
    assert library['path'] in mapped
    assert hashlib.sha256(Path(library['path']).read_bytes()).hexdigest()==library['sha256']
result['driver_payload_verified']=True
result['current_worker_preflight_verified']=sys.argv[4]=='observed'
print('ARTISAN_GPU_PROBE='+json.dumps(result))
"""


def run(prepared_target: Path, scratch: Path, pixi: Path) -> dict[str, object]:
    """Run the same four-byte kernel through both production environment wrappers."""
    if scratch.exists():
        msg = "Qualification scratch must be a fresh owned directory"
        raise ValueError(msg)
    allocation = capture_native_gpu(1)
    assert allocation is not None
    worker = qualify_observed_worker(TRANSPORT_PACKAGES)
    prepared = PreparedNativeEnvironment.model_validate_json(
        prepared_target.read_bytes()
    )
    verify_native_environment(prepared)
    launcher = LocalLauncherIdentity.capture(pixi, Path("/usr/bin/env"))
    scratch.mkdir(parents=True)
    probe = Path(__file__).parents[1] / "providers/gpu_probe.py"
    expected = json.dumps(
        [item.model_dump(mode="json") for item in allocation.libraries]
    )
    injections = {
        "QUALIFICATION_SELECTED_CREDENTIAL": "synthetic-selected",
        "QUALIFICATION_OWNER": "gpu-qualification",
    }
    profiles = {
        "observed": ObservedCommandEnvironment(
            worker, scratch / "observed", injections, gpu=allocation
        ),
        "prepared": PreparedCommandEnvironment(
            prepared,
            pixi,
            Path("/usr/bin/env"),
            scratch / "prepared",
            injections,
            gpu=allocation,
        ),
    }
    results = {}
    for name, environment in profiles.items():
        verify_native_gpu(allocation)
        with managed_invocation(), capture_commands() as recorder:
            result = run_command(
                environment,
                [
                    "python",
                    "-I",
                    "-c",
                    PROGRAM,
                    str(probe),
                    expected,
                    allocation.model_dump_json(),
                    name,
                ],
                cwd=str(scratch),
            )
        value = json.loads(
            next(
                line.removeprefix("ARTISAN_GPU_PROBE=")
                for line in result.stdout.splitlines()
                if line.startswith("ARTISAN_GPU_PROBE=")
            )
        )
        assert value["output"] == 42
        assert value["device_count"] == 1
        assert value["cuda_visible_devices"] == ",".join(allocation.devices)
        assert (
            value["selected_credential"]
            == injections["QUALIFICATION_SELECTED_CREDENTIAL"]
        )
        assert value["loader_path"].split(":")[0] == str(scratch / name / "gpu-driver")
        assert {path.name for path in (scratch / name / "gpu-driver").iterdir()} == {
            item.name for item in allocation.libraries
        }
        results[name] = {
            "kernel": value,
            "commands": recorder.snapshot().model_dump(mode="json"),
        }
    shutil.rmtree(scratch)
    return {
        "status": "passed",
        "profiles": results,
        "allocation": allocation.model_dump(mode="json"),
        "prepared_target_digest": prepared.digest,
        "observed_worker_digest": worker.digest,
        "prepared_launcher": launcher.descriptor(),
        "probe_sha256": hashlib.sha256(probe.read_bytes()).hexdigest(),
        "qualification_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scratch_closed": not scratch.exists(),
        "scope": "native driver and production launch wrappers; no full assignment transport",
    }


def main() -> None:
    """Read existing target evidence and write an exact live qualification report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("prepared_target", type=Path)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--pixi", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    args.report.write_text(
        json.dumps(run(args.prepared_target, args.scratch, args.pixi), indent=2)
    )


if __name__ == "__main__":
    main()
