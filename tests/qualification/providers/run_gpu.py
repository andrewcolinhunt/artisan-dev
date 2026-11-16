"""Launch the tiny GPU command through the same constructed tool environment."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

from sequence import command_environment


def run(
    tool_python: Path,
    script: Path,
    scratch: Path,
    *,
    driver_library_path: Path | None = None,
) -> dict[str, object]:
    """Keep qualified GPU visibility/ownership while removing accidental paths."""
    scratch.mkdir(parents=True, exist_ok=True)
    environment = command_environment(
        {
            "PYTHON": str(tool_python),
            "PATH": f"{tool_python.parent}:/usr/bin:/bin",
            "QUALIFICATION_TOOL": "gpu",
            "QUALIFICATION_LITERAL": "a literal $value with spaces",
        },
        scratch,
        "gpu-qualification",
    )
    if driver_library_path is not None:
        assert driver_library_path.is_absolute()
        assert driver_library_path.is_dir()
        environment["LD_LIBRARY_PATH"] = str(driver_library_path)
    environment.update(
        {
            key: os.environ[key]
            for key in (
                "SLURM_JOB_ID",
                "SLURM_STEP_ID",
                "SLURM_JOB_GPUS",
                "SLURM_STEP_GPUS",
            )
            if key in os.environ
        }
    )
    output = subprocess.check_output(
        [str(tool_python), str(script)],
        env=environment,
        cwd=scratch,
        text=True,
        timeout=20,
    )
    result = json.loads(output)
    assert result["pythonpath"] is None
    assert result["loader_path"] == (
        str(driver_library_path) if driver_library_path else None
    )
    assert result["selected_credential"] == "synthetic-selected"
    assert result["owner"] == "gpu-qualification"
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("tool_python", type=Path)
    parser.add_argument(
        "--script", type=Path, default=Path(__file__).with_name("gpu_probe.py")
    )
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--driver-library-path", type=Path)
    args = parser.parse_args()
    print(
        json.dumps(
            run(
                args.tool_python,
                args.script,
                args.scratch,
                driver_library_path=args.driver_library_path,
            ),
            indent=2,
        )
    )
