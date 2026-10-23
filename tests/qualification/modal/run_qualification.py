"""Bound the entire live qualification and always stop its exact dedicated app."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    """Run prepared tests with a wall-clock bound and unconditional app cleanup."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    assert manifest["app_name"] == "artisan-tool-managed_cancellation_probe"
    assert manifest["app_id"].startswith("ap-")
    env = dict(os.environ)
    env["ARTISAN_REMOTE_QUALIFICATION_MANIFEST"] = str(args.manifest)
    root = Path(__file__).parent
    result = 1
    try:
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                str(root / "test_modal_cancellation.py"),
                "-v",
                "--tb=short",
                "-o",
                "markers=modal: requires an explicitly owned live Modal deployment",
                f"--junitxml={root / 'qualification.xml'}",
            ],
            env=env,
            timeout=900,
            check=False,
        )
        result = completed.returncode
    finally:
        stopped = subprocess.run(
            [sys.executable, "-m", "modal", "app", "stop", manifest["app_id"]],
            timeout=45,
            capture_output=True,
            check=False,
        )
        cleanup = {"app_id": manifest["app_id"], "stop_returncode": stopped.returncode}
        (root / "app-cleanup.json").write_text(json.dumps(cleanup, indent=2) + "\n")
        if stopped.returncode:
            msg = "failed to stop exact temporary qualification app"
            raise RuntimeError(msg)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
