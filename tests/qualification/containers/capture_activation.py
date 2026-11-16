"""Capture activation after installing an unchanged lock at its image prefix."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path


def capture(prepared: Path, pixi: Path) -> None:
    """Record only activation values needed by the two qualified commands."""
    environments = []
    for role in "ab":
        name = f"tool-{role}"
        output = subprocess.check_output(
            [
                str(pixi),
                "shell-hook",
                "--locked",
                "--manifest-path",
                str(prepared / "pixi.toml"),
                "-e",
                name,
                "--json",
            ],
            text=True,
        )
        activation = json.loads(output)["environment_variables"]
        environments.append(
            {
                "PATH": str(prepared / ".pixi/envs" / name / "bin") + ":/usr/bin:/bin",
                "PYTHON": str(prepared / ".pixi/envs" / name / "bin/python"),
                "QUALIFICATION_TOOL": activation["QUALIFICATION_TOOL"],
                "QUALIFICATION_LITERAL": "a literal $value with spaces",
            }
        )
    (prepared / "tool-environments.json").write_text(
        json.dumps(environments, indent=2) + "\n"
    )
    receipt = {
        "lock_digest": hashlib.sha256(
            (prepared / "pixi.lock").read_bytes()
        ).hexdigest(),
        "tools": ["a", "b"],
    }
    (prepared / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("prepared", type=Path)
    parser.add_argument("--pixi", type=Path, default=Path("/usr/local/bin/pixi"))
    arguments = parser.parse_args()
    capture(arguments.prepared, arguments.pixi)
