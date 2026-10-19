"""Synthetic remote ownership probe, deployed only through an explicit command."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Literal

from pydantic import Field

import artisan
from artisan.operations.examples import WaitTool
from artisan.schemas.operation_config.compute import ComputeProvider, ModalComputeConfig
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.tool_spec import ToolSpec

APP_NAME = "artisan-tool-managed_cancellation_probe"
MARKER = "ARTISAN_CANCELLATION_QUALIFICATION "


def source_digest() -> str:
    """Digest the actual overlaid framework source files, excluding generated version."""
    root = Path(artisan.__file__).parent
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        if path.name == "_version.py":
            continue
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(path.read_bytes() + b"\0")
    return digest.hexdigest()


def marker(event: str, token: str, **values: Any) -> None:
    """Print only synthetic correlation and process evidence."""
    print(
        MARKER
        + json.dumps(
            {
                "event": event,
                "token": token,
                "task_id": os.environ.get("MODAL_TASK_ID"),
                "pid": os.getpid(),
                "wall_time": time.time(),
                **values,
            },
            sort_keys=True,
        ),
        flush=True,
    )


class ManagedCancellationProbe(WaitTool):
    """Exercise production endpoint ownership with bounded synthetic processes."""

    name = "managed_cancellation_probe"
    description = "Temporary bounded remote cancellation qualification"

    class Params(WaitTool.Params):
        """Choose a finite lifetime and one deterministic cancellation boundary."""

        seconds: int = Field(
            default=60, ge=1, le=120, description="Finite command lifetime."
        )
        token: str = Field(
            pattern=r"^[a-f0-9]{32}$",
            description="Synthetic invocation correlation token.",
        )
        mode: Literal["normal", "stubborn", "startup"] = Field(
            default="normal", description="Controlled cancellation boundary."
        )

    params: Params = Params(token="0" * 32)
    tool: ToolSpec = ToolSpec(executable="python3", interpreter=None)
    compute_resources: ComputeResources = ComputeResources(
        cpu=1, memory_gb=1, timeout=180
    )
    compute_provider: ComputeProvider = ComputeProvider(
        modal=ModalComputeConfig(
            local_python_sources=["artisan", "managed_cancellation_probe"],
            min_containers=0,
            max_containers=3,
            scaledown_window=15,
            retries=0,
            secrets=[],
            volumes={},
        )
    )

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        """Exercise the unmodified command path after an optional start barrier."""
        import modal

        marker(
            "worker_ready",
            self.params.token,
            source_digest=source_digest(),
            call_id=modal.current_function_call_id()
            if os.environ.get("MODAL_TASK_ID")
            else None,
        )
        if self.params.mode == "startup":
            time.sleep(90)
        return [
            *self.tool.parts(),
            "-u",
            "-m",
            "managed_cancellation_probe",
            "command",
            self.params.model_dump_json(),
            inputs["dataset"],
        ]


def run_command_probe(params: dict[str, Any], source: str) -> None:
    """Start a same-group child and preserve a strict finite fallback lifetime."""
    stubborn = params["mode"] == "stubborn"
    if stubborn:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    child = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-m",
            "managed_cancellation_probe",
            "child",
            json.dumps(params),
        ]
    )
    marker("command_ready", params["token"], child_pid=child.pid)
    try:
        child.wait(timeout=params["seconds"] + 5)
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)
    path = Path(source)
    Path(path.stem + "_waited.csv").write_text(
        "seconds,host,source\n"
        f"{params['seconds']},{os.environ.get('MODAL_TASK_ID', 'local')},{path.name}\n"
    )
    marker("natural_completion", params["token"])


def run_child(params: dict[str, Any]) -> None:
    """Provide a real child, with TERM resistance when selected."""
    if params["mode"] == "stubborn":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    marker("child_ready", params["token"])
    deadline = time.monotonic() + params["seconds"]
    while time.monotonic() < deadline:
        time.sleep(0.1)


if __name__ == "__main__":
    arguments = json.loads(sys.argv[2])
    if sys.argv[1] == "child":
        run_child(arguments)
    elif sys.argv[1] == "command":
        run_command_probe(arguments, sys.argv[3])
    else:
        msg = "unknown qualification mode"
        raise ValueError(msg)
