"""Path utilities for directory resolution and execution directory management.

Provides :func:`get_caller_dir` and :func:`find_project_root` for robust
path resolution across environments (scripts, VS Code notebooks, JupyterHub),
plus sharded directory helpers for execution staging.
"""

from __future__ import annotations

import inspect
import os
import posixpath
from pathlib import Path

# ---------------------------------------------------------------------------
# Environment-aware path resolution
# ---------------------------------------------------------------------------


def get_caller_dir(stack_level: int = 1) -> Path:
    """Get the directory of the calling file (script or notebook).

    Tries multiple strategies so the same code works in a plain Python script,
    a VS Code Jupyter notebook, and a JupyterHub/JupyterLab notebook:

    1. **Script** — the caller's ``__file__`` via frame inspection.
    2. **VS Code notebook** — ``__vsc_ipynb_file__`` injected into kernel
       globals by the VS Code Jupyter extension.
    3. **JupyterHub / JupyterLab** — ``JPY_SESSION_NAME`` env-var set by
       ``ipykernel`` ≥ 6.9 with the notebook path.
    4. **Fallback** — ``Path.cwd()``.

    Args:
        stack_level: How many frames to walk up. 1 (default) means
            "my caller", 2 means "my caller's caller", etc.

    Returns:
        Absolute path to the directory containing the running file.
    """
    frame = inspect.currentframe()
    try:
        caller = frame
        for _ in range(stack_level):
            caller = caller.f_back if caller else None
        if caller:
            # Strategy 1: script with __file__
            caller_file = caller.f_globals.get("__file__")
            if caller_file:
                p = Path(caller_file).resolve()
                if p.exists() and p.suffix == ".py":
                    return p.parent

            # Strategy 2: VS Code injects __vsc_ipynb_file__
            vsc_file = caller.f_globals.get("__vsc_ipynb_file__")
            if vsc_file:
                return Path(vsc_file).resolve().parent
    finally:
        del frame

    # Strategy 3: JupyterHub / JupyterLab (ipykernel 6.9+)
    session = os.environ.get("JPY_SESSION_NAME", "")
    if session:
        p = Path(session)
        if not p.is_absolute():
            p = Path.home() / p
        p = p.resolve()
        if p.exists():
            return p.parent

    # Strategy 4: fallback
    return Path.cwd().resolve()


def find_project_root() -> Path:
    """Find the project root directory (the one containing ``pyproject.toml``).

    Strategies (in order):

    1. Walk up from :func:`get_caller_dir`.
    2. Resolve from the installed package location (editable install).
       ``path.py`` lives at ``<root>/src/artisan/utils/path.py``, so
       ``parents[3]`` is the project root.
    3. Read the ``ARTISAN_ROOT`` environment variable.

    Returns:
        Absolute path to the project root.

    Raises:
        RuntimeError: If none of the strategies find a valid root.
    """
    # Strategy 1: walk up from caller dir
    p = get_caller_dir()
    while p != p.parent:
        if (p / "pyproject.toml").exists():
            return p
        p = p.parent

    # Strategy 2: editable install — this file is at <root>/src/artisan/utils/path.py
    root = Path(__file__).resolve().parents[3]
    if (root / "pyproject.toml").exists():
        return root

    # Strategy 3: explicit env var
    if env_root := os.environ.get("ARTISAN_ROOT"):
        root = Path(env_root).resolve()
        if (root / "pyproject.toml").exists():
            return root

    msg = (
        "Project root not found. Either run from within the project "
        "directory or set the ARTISAN_ROOT environment variable."
    )
    raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# Execution directory helpers
# ---------------------------------------------------------------------------


def step_dir_name(step_number: int, operation_name: str) -> str:
    """Build a human-readable step directory name.

    Args:
        step_number: Pipeline step number.
        operation_name: Operation name (e.g. "ingest", "tool_c").

    Returns:
        Directory name like ``"0_ingest"`` or ``"1_tool_c"``.
    """
    return f"{step_number}_{operation_name}"


def shard_uri(
    root: str,
    execution_run_id: str,
    step_number: int | None = None,
    operation_name: str | None = None,
) -> str:
    """Create sharded URI from execution_run_id, optionally partitioned by step.

    Works with local paths and cloud URIs (s3://, gcs://).

    Args:
        root: Base URI or path string.
        execution_run_id: 32-character hex hash.
        step_number: Optional pipeline step number for partitioning.
        operation_name: Optional operation name. When provided together with
            step_number, the step directory becomes ``{step_number}_{operation_name}``.

    Returns:
        Sharded URI string.
    """
    prefix = execution_run_id[:2]
    shard = execution_run_id[2:4]
    if step_number is not None:
        step_segment = (
            step_dir_name(step_number, operation_name)
            if operation_name
            else str(step_number)
        )
        return f"{root}/{step_segment}/{prefix}/{shard}/{execution_run_id}"
    return f"{root}/{prefix}/{shard}/{execution_run_id}"


# ---------------------------------------------------------------------------
# URI-safe path operations
# ---------------------------------------------------------------------------


def uri_join(base: str, *parts: str) -> str:
    """Join URI/path segments. Works for local paths and cloud URIs.

    Uses ``posixpath.join`` which operates on string structure without
    assuming OS conventions. Both local POSIX paths and cloud URIs
    (``s3://``, ``gcs://``) use ``/`` as the separator.

    Args:
        base: Base URI or path.
        *parts: Segments to append.

    Returns:
        Joined URI string.
    """
    return posixpath.join(base, *parts)


def uri_parent(uri: str) -> str:
    """Parent directory of a URI/path. Works for local paths and cloud URIs.

    Args:
        uri: URI or path string.

    Returns:
        Parent directory URI string.
    """
    return posixpath.dirname(uri)
