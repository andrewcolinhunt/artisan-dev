"""Freeze reviewed sources locally so concurrent repository edits cannot drift."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

import artisan


def main() -> None:
    """Copy only framework and reviewed synthetic harness sources; no network."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True, type=Path)
    args = parser.parse_args()
    args.snapshot.mkdir(exist_ok=False)
    source = Path(artisan.__file__).parent
    for path in source.rglob("*"):
        if path.is_file() and (
            path.suffix in {".py", ".pyi"} or path.name == "py.typed"
        ):
            destination = args.snapshot / "src" / "artisan" / path.relative_to(source)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
    destination = args.snapshot / "harness"
    destination.mkdir()
    files = (
        "README.md",
        "managed_cancellation_probe.py",
        "prepare_snapshot.py",
        "deploy_probe.py",
        "test_modal_cancellation.py",
        "run_qualification.py",
        "local_smoke.py",
    )
    for name in files:
        path = Path(__file__).parent / name
        shutil.copy2(path, destination / path.name)
    report = {
        "git_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "diff_digest": hashlib.sha256(
            subprocess.check_output(["git", "diff", "HEAD"])
        ).hexdigest(),
        "source": str(source),
    }
    (args.snapshot / "snapshot.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
