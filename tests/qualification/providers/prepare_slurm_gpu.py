"""Configure only a fresh disposable Slurm container for one allocated GPU."""

from __future__ import annotations

import os
from pathlib import Path


def main() -> None:
    """Keep prepared prefixes read-only and all ownership changes task-owned."""
    assert Path("/.dockerenv").is_file()
    assert os.environ.get("QUALIFICATION_DISPOSABLE_CONTAINER") == "1"
    assert os.environ["ARTISAN_DOCKER_UID"] == str(Path("/gpu-control").stat().st_uid)
    assert os.environ["ARTISAN_DOCKER_GID"] == str(Path("/gpu-control").stat().st_gid)
    configuration = Path("/etc/slurm/slurm.conf")
    lines = configuration.read_text().splitlines()
    lines = [
        line + " Gres=gpu:a10:1" if line.startswith("NodeName=") else line
        for line in lines
    ]
    configuration.write_text("\n".join([*lines, "GresTypes=gpu"]) + "\n")
    Path("/etc/slurm/gres.conf").write_text("Name=gpu Type=a10 File=/dev/nvidia0\n")
    source = Path("/entrypoint.sh").read_text()
    old = "chown -R docker:docker /home/docker /pixi-env"
    assert source.count(old) == 1
    source = source.replace(old, "chown -R docker:docker /home/docker /gpu-control")
    source = source.replace(
        "runuser -u docker -- test -w /pixi-env",
        "runuser -u docker -- test -r /pixi-env",
    )
    target = Path("/entrypoint-qualification-gpu.sh")
    target.write_text(source)
    os.execv("/bin/bash", ["/bin/bash", str(target), "sleep", "infinity"])


if __name__ == "__main__":
    main()
