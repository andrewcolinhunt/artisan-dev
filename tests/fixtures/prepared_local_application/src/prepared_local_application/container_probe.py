"""Run the same installed scientific sequence with prepared container tools."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from artisan.composites.base.compiler import compile_definition
from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
)
from artisan.execution.local_assignment import bind_local_software, qualify_local_worker
from artisan.execution.local_worker import launch_local_assignment
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES
from prepared_local_application import TwoToolSequence
from prepared_local_application.probe import Seal, admit, inspect


def run(args: argparse.Namespace) -> dict[str, object]:
    """Reuse prepared tools and seal exact file/metric results after worker closure."""
    raw = PreparedNativeEnvironment.model_validate_json(args.worker.read_bytes())
    worker = qualify_local_worker(
        raw,
        [*TRANSPORT_PACKAGES, "prepared-local-application"],
        pixi=args.pixi,
        system_env=Path("/usr/bin/env"),
    )
    runtime = (
        DockerRuntime(Path("/usr/bin/docker"))
        if args.backend == "docker"
        else ApptainerRuntime(args.apptainer, args.temporary_root)
    )
    plan = compile_definition(TwoToolSequence, params={"manifest": str(args.manifest)})
    bindings = bind_local_software(
        plan,
        worker,
        cache_root=args.cache,
        pixi=args.pixi,
        system_env=Path("/usr/bin/env"),
        tool_backend=args.backend,
        docker=runtime if isinstance(runtime, DockerRuntime) else None,
        apptainer=runtime if isinstance(runtime, ApptainerRuntime) else None,
    )
    args.work.mkdir(parents=True, exist_ok=False)
    (args.work / "workers").mkdir()
    sealer = Seal(str(args.work / "sealed"))
    evidence = []
    for index, increment in enumerate((1, 2)):
        selected = compile_definition(
            TwoToolSequence,
            params={"manifest": str(args.manifest), "increment": increment},
        )
        assignment = admit(selected, bindings.software_digest, index)
        receipt = launch_local_assignment(
            assignment,
            bindings,
            sealer,
            working_root=args.work / "workers",
            runtime_env={
                "PROBE_LITERAL": "a literal $value with spaces",
                "PROBE_CREDENTIAL": "synthetic-selected",
            },
        ).result(timeout=180)
        evidence.append(inspect(receipt, assignment, 14 + 2 * increment))
    assert not list((args.work / "workers").iterdir())
    assert len({item["delivery_id"] for item in evidence}) == 2
    return {
        "status": "passed",
        "backend": args.backend,
        "software_digest": bindings.software_digest,
        "worker_compatibility": bindings.compatibility.model_dump(mode="json"),
        "tool_targets": [
            {"call_path": tool.call_path, "digest": tool.target.digest}
            for tool in bindings.tools
        ],
        "launcher": runtime.identity().model_dump(mode="json"),
        "evidence": evidence,
        "numeric_change_reuses_prepared_software": True,
        "file_hydrated_after_worker_exit": True,
    }


def main() -> None:
    """Read explicit prepared bindings; this command does not install or build."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("backend", choices=("docker", "apptainer"))
    for name in ("worker", "manifest", "cache", "pixi", "work", "report"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--apptainer", type=Path)
    parser.add_argument("--temporary-root", type=Path)
    args = parser.parse_args()
    if args.backend == "apptainer" and args.apptainer is None:
        parser.error("--apptainer is required for the SIF backend")
    report = run(args)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                "status": report["status"],
                "backend": args.backend,
                "software_digest": report["software_digest"],
            }
        )
    )


if __name__ == "__main__":
    main()
