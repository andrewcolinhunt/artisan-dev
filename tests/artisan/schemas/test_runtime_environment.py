"""Tests for RuntimeEnvironment.files_root field."""

from __future__ import annotations

from pathlib import Path

from artisan.schemas.execution.runtime_environment import RuntimeEnvironment


class TestRuntimeEnvironmentFilesRoot:
    """Tests for the files_root field on RuntimeEnvironment."""

    def test_files_root_defaults_to_none(self, tmp_path: Path) -> None:
        """files_root is None when not provided."""
        env = RuntimeEnvironment(
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
        )
        assert env.files_root is None

    def test_files_root_accepts_value(self, tmp_path: Path) -> None:
        """files_root stores the provided path."""
        files_root = str(tmp_path / "files")
        env = RuntimeEnvironment(
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            files_root=files_root,
        )
        assert env.files_root == files_root

    def test_files_root_frozen(self, tmp_path: Path) -> None:
        """files_root cannot be mutated (frozen model)."""
        env = RuntimeEnvironment(
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            files_root=str(tmp_path / "files"),
        )
        try:
            env.files_root = str(tmp_path / "other")  # type: ignore[misc]
            assert False, "Should have raised"  # noqa: B011
        except Exception:
            pass  # Expected: ValidationError on frozen model
