"""Locate the qualified Pixi executable used by canonical preparation."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


def pixi_executable() -> str:
    """Locate Pixi independently of an activated project environment."""
    executable = shutil.which("pixi") or str(Path.home() / ".pixi/bin/pixi")
    if not Path(executable).is_file():
        msg = "Pixi is required; install Pixi 0.83 before preparation"
        raise FileNotFoundError(msg)
    return executable


def pixi_version() -> str:
    """Read and qualify the executable used to activate prepared environments."""
    version = subprocess.check_output(
        [pixi_executable(), "--version"], text=True
    ).strip()
    if version != "pixi 0.83.0":
        msg = "Managed Pixi environments require qualified Pixi 0.83.0"
        raise ValueError(msg)
    return version
