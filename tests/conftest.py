"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import rootutils

# Setup project root once for all tests
rootutils.setup_root(
    search_from=__file__,
    indicator=[".project-root", "pyproject.toml"],
    pythonpath=True,
    dotenv=True,
    cwd=False,
)
