"""Run the same inert-header sequence through real sbatch and srun commands."""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import time
from pathlib import Path

import bootstrap
import cloudpickle
import sequence
from application_schema import ProbeInput

from artisan.schemas.artifact.metric import MetricArtifact


def assignment(root: Path, prepared: Path, name: str, image_id: str) -> list[str]:
    """Write an assignment whose tools and worker use final prepared prefixes."""
    directory = root / name
    directory.mkdir()
    receipt = json.loads((prepared / "receipt.json").read_text())
    header = {
        "runtime": bootstrap.runtime_identity(
            [
                "cloudpickle",
                "pydantic",
                "dexterity-artisan",
                "polars",
                "deltalake",
                "pyarrow",
            ]
        ),
        "framework_digest": bootstrap.framework_digest(),
        "application_digest": bootstrap.application_digest(),
        "prepared": str(prepared),
        "lock_digest": receipt["lock_digest"],
        "selected_tools": ["a", "b"],
    }
    target = bootstrap.target_identity(
        "slurm-srun" if name == "srun" else "slurm-sbatch",
        image_id,
        scheduler=subprocess.check_output(["srun", "--version"], text=True).strip(),
    )
    descriptor = bootstrap.software_descriptor(header, target)
    (directory / "software-descriptor.json").write_text(
        json.dumps(descriptor, indent=2) + "\n"
    )
    arguments = {
        "delivery_root": str(directory / "delivery"),
        "scratch_root": f"/tmp/provider-node-scratch/{root.name}",
        "tool_script": str(Path(__file__).with_name("tool.py")),
        "environments": json.loads((prepared / "tool-environments.json").read_text()),
        "input_schema": ProbeInput(
            metric=MetricArtifact.draft({"value": 5}, "input.json", 1)
        ),
        "software_descriptor": descriptor,
        "wait_for_requeue": name == "requeue",
        "envelope": {
            "unit_id": "slurm-unit",
            "attempt": 0,
            "generation": 0,
            "delivery_id": name,
            "recipe_digest": "two-tool-sequence",
            "software_digest": bootstrap.software_digest(descriptor),
        },
    }
    operation = sequence.execute_scheduler_sequence
    (directory / "header.json").write_text(json.dumps(header))
    (directory / "payload.bin").write_bytes(cloudpickle.dumps((operation, arguments)))
    return [
        sys.executable,
        "-I",
        str(Path(__file__).with_name("bootstrap.py")),
        str(directory / "header.json"),
        str(directory / "payload.bin"),
        str(directory / "result.json"),
    ]


def accounting(job_id: str) -> str:
    """Retain exact scheduler rows, including job.step identifiers."""
    return subprocess.check_output(
        [
            "sacct",
            "--noheader",
            "--parsable2",
            "--jobs",
            job_id,
            "--format=JobID%64,State%40,Start,End,ExitCode",
        ],
        text=True,
    )


def wait_job(job_id: str) -> str:
    """Bound the real scheduler wait and return accounting evidence."""
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        output = subprocess.check_output(
            ["squeue", "--noheader", "--jobs", job_id], text=True
        )
        if not output.strip():
            return accounting(job_id)
        time.sleep(0.2)
    msg = f"owned job did not finish: {job_id}"
    raise TimeoutError(msg)


def qualify(root: Path, prepared: Path, image_id: str) -> dict[str, object]:
    """Prove two launch modes and actual scheduler requeue delivery isolation."""
    root.mkdir(parents=True, exist_ok=False)
    cloudpickle.register_pickle_by_value(sequence)
    reports = {}
    owned_jobs = []
    try:
        command = assignment(root, prepared, "sbatch", image_id)
        submitted = subprocess.check_output(
            [
                "sbatch",
                "--parsable",
                "--partition=debug",
                "--cpus-per-task=1",
                "--mem=1G",
                "--time=00:02:00",
                f"--output={root}/sbatch.log",
                "--wrap",
                shlex.join(command),
            ],
            text=True,
        ).strip()
        job_id = submitted.split(";")[0]
        owned_jobs.append(job_id)
        reports["sbatch_accounting"] = wait_job(job_id)
        reports["sbatch"] = json.loads((root / "sbatch/result.json").read_text())
        command = assignment(root, prepared, "srun", image_id)
        subprocess.run(
            [
                "salloc",
                "--partition=debug",
                "-n2",
                "--mem=2G",
                "--time=00:02:00",
                "srun",
                "--exact",
                "--mpi=none",
                "-n1",
                "-c1",
                "--mem=1G",
                *command,
            ],
            check=True,
            timeout=100,
        )
        reports["srun"] = json.loads((root / "srun/result.json").read_text())
        reports["srun_accounting"] = accounting(str(reports["srun"]["slurm_job_id"]))
        command = assignment(root, prepared, "requeue", image_id)
        submitted = subprocess.check_output(
            [
                "sbatch",
                "--parsable",
                "--requeue",
                "--partition=debug",
                "--cpus-per-task=1",
                "--mem=1G",
                "--time=00:02:00",
                f"--output={root}/requeue.log",
                "--wrap",
                shlex.join(command),
            ],
            text=True,
        ).strip()
        job_id = submitted.split(";")[0]
        owned_jobs.append(job_id)
        deadline = time.monotonic() + 30
        while (
            not list((root / "requeue").glob("restart-0-*/worker-started.json"))
            and time.monotonic() < deadline
        ):
            time.sleep(0.1)
        assert list((root / "requeue").glob("restart-0-*/worker-started.json"))
        subprocess.run(["scontrol", "requeue", job_id], check=True, timeout=10)
        reports["requeue_accounting"] = wait_job(job_id)
        reports["requeue"] = json.loads((root / "requeue/result.json").read_text())
        assert reports["requeue"]["slurm_restart_count"] == "1"
        assert len(list((root / "requeue").glob("restart-*-*"))) == 2
        for name in ("sbatch", "srun", "requeue"):
            assert reports[name]["value"] == 16
            assert len({phase["pid"] for phase in reports[name]["phases"]}) == 1
        assert reports["srun"]["slurm_step_id"] == "0"
        (root / "report.json").write_text(json.dumps(reports, indent=2) + "\n")
        return reports
    finally:
        if owned_jobs:
            subprocess.run(["scancel", *owned_jobs], check=False, timeout=10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("prepared", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--image-id", required=True)
    arguments = parser.parse_args()
    qualify(
        arguments.output.resolve(), arguments.prepared.resolve(), arguments.image_id
    )
