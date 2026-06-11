"""Environment lookup with a ``.env``-file fallback.

Mirrors the ``~/.modal.toml`` model for credentials the framework needs at
call time: a file any process can find — Jupyter kernels, cron, IDE
runners — regardless of which shell launched it. Process environment
variables stay authoritative (CI injects secrets as env and must never be
shadowed by a local file).
"""

from __future__ import annotations

import os

from dotenv import dotenv_values

_cache: dict[str, dict[str, str | None]] = {}


def env_or_dotenv(name: str) -> str | None:
    """Process env value for ``name``, else the nearest ``.env`` walking up from cwd.

    The file is parsed with ``dotenv_values`` — values are returned, never
    written into ``os.environ``. Returns None when the key is in neither
    place.

    The upward search for the nearest ``.env`` runs on every call (it is
    cheap); only the parsed file contents are memoized, keyed by the file's
    resolved absolute path, so callers that change directory read the
    correct file.

    Args:
        name: Environment variable name (e.g. "MODAL_PROXY_TOKEN_ID").

    Returns:
        The value, or None.
    """
    value = os.environ.get(name)
    if value is not None:
        return value
    path = _nearest_dotenv()
    if path is None:
        return None
    if path not in _cache:
        _cache[path] = dotenv_values(path)
    return _cache[path].get(name)


def _nearest_dotenv() -> str | None:
    """Absolute path of the closest ``.env`` at or above cwd, or None."""
    current = os.path.abspath(os.getcwd())
    while True:
        candidate = os.path.join(current, ".env")
        if os.path.isfile(candidate):
            return candidate
        parent = os.path.dirname(current)
        if parent == current:
            return None
        current = parent
