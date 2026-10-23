"""Tests for sandbox directory creation."""

from __future__ import annotations

import os
from pathlib import Path

from artisan.execution.context.sandbox import create_sandbox


class TestCreateSandbox:
    """Tests for create_sandbox directory creation."""

    def test_creates_subdirs(self, tmp_path: Path) -> None:
        """create_sandbox creates preprocess, execute, postprocess under given path."""
        sandbox = str(tmp_path / "sandbox")
        create_sandbox(sandbox)

        assert os.path.isdir(os.path.join(sandbox, "preprocess"))
        assert os.path.isdir(os.path.join(sandbox, "execute"))
        assert os.path.isdir(os.path.join(sandbox, "postprocess"))

    def test_returns_correct_paths(self, tmp_path: Path) -> None:
        """Return tuple matches (sandbox_path, preprocess, execute, postprocess)."""
        sandbox = str(tmp_path / "sandbox")
        result = create_sandbox(sandbox)

        assert result == (
            sandbox,
            os.path.join(sandbox, "preprocess"),
            os.path.join(sandbox, "execute"),
            os.path.join(sandbox, "postprocess"),
        )

    def test_idempotent(self, tmp_path: Path) -> None:
        """Calling create_sandbox twice on the same path does not raise."""
        sandbox = str(tmp_path / "sandbox")
        create_sandbox(sandbox)
        create_sandbox(sandbox)

        assert os.path.isdir(sandbox)

    def test_creates_parents(self, tmp_path: Path) -> None:
        """Nested sandbox_path with nonexistent parents is created."""
        sandbox = str(tmp_path / "a" / "b" / "c" / "sandbox")
        create_sandbox(sandbox)

        assert os.path.isdir(sandbox)
        assert os.path.isdir(os.path.join(sandbox, "preprocess"))
