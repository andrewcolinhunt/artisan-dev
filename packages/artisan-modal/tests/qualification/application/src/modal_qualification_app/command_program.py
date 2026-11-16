"""Tiny explicit Python tool; the canonical tool recipe includes s3fs/packaging."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4


def signal_ready(value: int, *, child: int | None = None) -> dict[str, object]:
    """Publish exact live-dispatch readiness without putting capabilities in params."""
    import s3fs  # type: ignore[import-untyped]

    channel = json.loads(os.environ["ARTISAN_QUAL_CHANNEL"])
    fs = s3fs.S3FileSystem(**channel["options"])
    payload = {
        "nonce": channel["nonce"],
        "attempt": channel["attempts"][str(value)],
        "pid": os.getpid(),
        "child": child,
        "child_start": _start(child) if child is not None else None,
    }
    fs.pipe_file(
        f"{channel['root']}/entered/{uuid4().hex}.json",
        json.dumps(payload).encode(),
        mode="create",
    )
    fs.pipe_file(
        f"{channel['root']}/{value}.json", json.dumps(payload).encode(), mode="create"
    )
    return payload


def _start(pid: int) -> str | None:
    try:
        return Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()[19]
    except FileNotFoundError:
        return None


def verify_cancelled_child() -> None:
    import s3fs

    channel = json.loads(os.environ["ARTISAN_QUAL_CHANNEL"])
    fs = s3fs.S3FileSystem(**channel["options"])
    record = json.loads(fs.cat_file(f"{channel['root']}/0.json"))
    assert record["nonce"] == channel["nonce"]
    assert record["attempt"] == channel["attempts"]["0"]
    assert _start(record["child"]) != record["child_start"]


def command(kernel: Callable[[], dict[str, object]]) -> None:
    import packaging

    source, target, role, mode, gpu = sys.argv[1:]
    data = json.loads(Path(source).read_text())
    original = int(data["input"])
    if mode == "fail" and original == 1:
        raise SystemExit(7)
    if mode == "hold":
        if original == 0:
            child = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(90)"],
                start_new_session=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            signal_ready(original, child=child.pid)
            time.sleep(30)
        else:
            verify_cancelled_child()
    if gpu == "true":
        data.setdefault("gpu", []).append(kernel())
        data["value"] = 42
    else:
        data["value"] = data["value"] + 1 if role == "a" else data["value"] * 2
    data.setdefault("tools", []).append(
        {
            "role": role,
            "python": sys.executable,
            "packaging": packaging.__version__,
            "activation": os.environ.get("PROBE_ACTIVATION"),
        }
    )
    Path(target).write_text(json.dumps(data))
