"""Tiny command tool with a separately installed dependency and file output."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import packaging


def main() -> None:
    """Transform one value and record the actual command runtime."""
    source, destination, role = sys.argv[1:]
    value = json.loads(Path(source).read_text())["value"]
    hold_seconds = float(os.environ.get("QUALIFICATION_HOLD_SECONDS", "0"))
    if hold_seconds:
        child = subprocess.Popen(
            [sys.executable, "-c", f"import time; time.sleep({hold_seconds!r})"]
        )
        Path(os.environ["TMPDIR"], "processes.json").write_text(
            json.dumps({"tool_pid": os.getpid(), "child_pid": child.pid})
        )
        time.sleep(hold_seconds)
        child.wait(timeout=5)
    transformed = value + 3 if role == "a" else value * 2
    Path(destination).write_text(
        json.dumps(
            {
                "value": transformed,
                "role": role,
                "pid": os.getpid(),
                "parent_pid": os.getppid(),
                "executable": sys.executable,
                "dependency_version": packaging.__version__,
                "dependency_path": packaging.__file__,
                "activation": os.environ.get("QUALIFICATION_TOOL"),
                "literal": os.environ.get("QUALIFICATION_LITERAL"),
                "visible_gpu": os.environ.get("CUDA_VISIBLE_DEVICES"),
                "scratch": os.environ.get("TMPDIR"),
                "python_path": os.environ.get("PYTHONPATH"),
                "loader_path": os.environ.get("LD_LIBRARY_PATH"),
                "selected_credential": os.environ.get(
                    "QUALIFICATION_SELECTED_CREDENTIAL"
                ),
                "owner": os.environ.get("QUALIFICATION_OWNER"),
            }
        )
    )


if __name__ == "__main__":
    main()
