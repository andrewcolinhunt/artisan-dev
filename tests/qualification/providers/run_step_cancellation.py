"""Observe real queued-launcher and exact job.step cancellation in one allocation."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def heartbeat(root: Path) -> None:
    """Expose this step and descendant until its owner stops the fixture."""
    root.mkdir()
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    identity = {
        "job": os.environ["SLURM_JOB_ID"],
        "step": os.environ["SLURM_STEP_ID"],
        "pid": os.getpid(),
        "child_pid": child.pid,
    }
    (root / "identity.json").write_text(json.dumps(identity))
    try:
        deadline = time.monotonic() + 60
        while not (root / "stop").exists() and time.monotonic() < deadline:
            (root / "heartbeat").write_text(str(time.monotonic()))
            time.sleep(0.05)
    finally:
        child.terminate()
        child.wait(timeout=5)


def wait_marker(path: Path) -> None:
    """Wait for positive readiness rather than guessing from elapsed time."""
    deadline = time.monotonic() + 20
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert path.exists(), str(path)


def qualify(root: Path) -> None:
    """Cancel an exact step while another step in the same allocation continues."""
    from artisan_submitit.cancellation import cancel_jobs

    root.mkdir(parents=True)
    processes = {}
    try:
        for name in ("target", "sibling", "queued"):
            log = (root / f"{name}.log").open("w")
            processes[name] = subprocess.Popen(
                [
                    "srun",
                    "--exact",
                    "--mpi=none",
                    "-n1",
                    "-c1",
                    "--mem=512M",
                    sys.executable,
                    __file__,
                    "--heartbeat",
                    str(root / name),
                ],
                start_new_session=True,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            log.close()
            if name != "queued":
                wait_marker(root / name / "heartbeat")
        time.sleep(0.5)
        assert processes["queued"].poll() is None
        assert not (root / "queued/identity.json").exists()
        os.killpg(processes["queued"].pid, signal.SIGTERM)
        processes["queued"].wait(timeout=15)
        target = json.loads((root / "target/identity.json").read_text())
        sibling = json.loads((root / "sibling/identity.json").read_text())
        target_id = f"{target['job']}.{target['step']}"
        error = cancel_jobs([target_id], timeout=10)
        assert error is None
        processes["target"].wait(timeout=30)
        before = (root / "sibling/heartbeat").read_text()
        time.sleep(0.3)
        assert (root / "sibling/heartbeat").read_text() != before
        assert not (root / "queued/identity.json").exists()
        (root / "sibling/stop").touch()
        assert processes["sibling"].wait(timeout=15) == 0
        child_status = Path(f"/proc/{target['child_pid']}/status")
        assert not child_status.exists() or "State:\tZ" in child_status.read_text()
        accounting = subprocess.check_output(
            [
                "sacct",
                "--noheader",
                "--parsable2",
                "--jobs",
                target["job"],
                "--format=JobID%64,State%40,Start,End,ExitCode",
            ],
            text=True,
        )
        assert any(
            line.startswith(target_id + "|CANCELLED")
            for line in accounting.splitlines()
        )
        report = {
            "target": target,
            "sibling": sibling,
            "actual_provider_cancel_arguments": [target_id],
            "queued_launcher_cancelled_before_start": True,
            "target_descendant_dead": True,
            "sibling_completed": True,
            "accounting": accounting,
        }
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    finally:
        for process in processes.values():
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--heartbeat", type=Path)
    parser.add_argument("--inside", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.heartbeat:
        heartbeat(args.heartbeat)
    elif args.inside:
        qualify(args.inside)
    else:
        subprocess.run(
            [
                "salloc",
                "-p",
                "debug",
                "-n2",
                "--mem=2G",
                "--time=00:02:00",
                sys.executable,
                __file__,
                "--inside",
                str(args.output),
            ],
            check=True,
            timeout=120,
        )
