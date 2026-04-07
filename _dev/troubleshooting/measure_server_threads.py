"""Measure Prefect server thread count before, during, and after pipeline work.

Run with: ~/.pixi/bin/pixi run python _dev/troubleshooting/measure_server_threads.py
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path


def server_thread_count(pid: int) -> int:
    """Count threads in the Prefect server process."""
    try:
        # macOS: ps -M shows one line per thread
        result = subprocess.run(
            ["ps", "-M", "-p", str(pid)],
            capture_output=True, text=True,
        )
        # First line is header, rest are threads
        lines = result.stdout.strip().split("\n")
        return max(0, len(lines) - 1)
    except Exception:
        return -1


def client_snapshot(label: str) -> None:
    """Print client-side thread count."""
    threads = threading.enumerate()
    print(f"  Client threads: {len(threads)}  [{', '.join(sorted(t.name for t in threads))}]")


def server_snapshot(label: str, pid: int) -> None:
    """Print server-side thread count."""
    count = server_thread_count(pid)
    print(f"  Server threads (PID {pid}): {count}")


# -- Find server PID --
SERVER_PID = 83196  # from prefect-start output
print(f"Prefect server PID: {SERVER_PID}")

# -- Baseline --
print(f"\n[0. Before any pipeline work]")
server_snapshot("baseline", SERVER_PID)
client_snapshot("baseline")

# -- Import and create pipelines --
from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.utils import tutorial_setup

print(f"\n[1. After imports]")
server_snapshot("imports", SERVER_PID)
client_snapshot("imports")

# -- Run 5 pipeline lifecycles to stress the server --
for i in range(5):
    env = tutorial_setup(f"server_test_{i}", base_dir=Path(__file__).parent)
    p = PipelineManager.create(
        name=f"server_test_{i}",
        delta_root=env.delta_root,
        staging_root=env.staging_root,
        working_root=env.working_root,
    )
    out = p.output
    p.run(operation=DataGenerator, name="gen", params={"count": 5, "seed": i})
    p.run(
        operation=DataTransformer, name="xform",
        inputs={"dataset": out("gen", "datasets")},
        params={"scale_factor": 2.0, "variants": 2, "seed": i * 10},
    )
    p.finalize()
    print(f"\n[Pipeline {i} complete]")
    server_snapshot(f"after pipeline {i}", SERVER_PID)
    client_snapshot(f"after pipeline {i}")

# -- Now leave 3 pipelines un-finalized --
print("\n--- Creating 3 leaked pipelines ---")
for i in range(3):
    env = tutorial_setup(f"server_leak_{i}", base_dir=Path(__file__).parent)
    p = PipelineManager.create(
        name=f"server_leak_{i}",
        delta_root=env.delta_root,
        staging_root=env.staging_root,
        working_root=env.working_root,
    )
    p.run(operation=DataGenerator, name="gen", params={"count": 3, "seed": i + 100})

print(f"\n[After 5 finalized + 3 leaked pipelines]")
server_snapshot("after all", SERVER_PID)
client_snapshot("after all")

# -- Wait and measure again --
print("\nWaiting 15s for settling...")
time.sleep(15)
print(f"\n[After 15s idle]")
server_snapshot("settled", SERVER_PID)
client_snapshot("settled")
