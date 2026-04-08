"""Tutorial setup utility for reducing notebook boilerplate.

Provides :func:`tutorial_setup` which handles directory creation and cleanup
in a single call.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import NamedTuple

from artisan.utils.path import get_caller_dir


class TutorialEnv(NamedTuple):
    """Paths returned by tutorial_setup()."""

    runs_dir: str
    delta_root: str
    staging_root: str
    working_root: str


def tutorial_setup(
    name: str,
    *,
    base_dir: Path | None = None,
    clean: bool = True,
) -> TutorialEnv:
    """Set up a tutorial environment with standard directory layout.

    Args:
        name: Tutorial name, used as subdirectory under runs/.
        base_dir: Base directory. Defaults to caller's directory.
        clean: Remove existing runs directory if True.

    Returns:
        TutorialEnv with runs_dir, delta_root, staging_root, working_root.
    """
    base_dir = base_dir or get_caller_dir(stack_level=2)
    runs_dir = os.path.join(str(base_dir), "runs", name)

    if clean and os.path.exists(runs_dir):
        shutil.rmtree(runs_dir)

    delta_root = os.path.join(runs_dir, "delta")
    staging_root = os.path.join(runs_dir, "staging")
    working_root = os.path.join(runs_dir, "working")

    for d in [delta_root, staging_root, working_root]:
        os.makedirs(d, exist_ok=True)

    return TutorialEnv(
        runs_dir=runs_dir,
        delta_root=delta_root,
        staging_root=staging_root,
        working_root=working_root,
    )
