"""Tests for RuntimeEnvironment.files_root_path field."""

from __future__ import annotations

from pathlib import Path

from artisan.schemas.execution.runtime_environment import RuntimeEnvironment


class TestRuntimeEnvironmentFilesRoot:
    """Tests for the files_root_path field on RuntimeEnvironment."""

    def test_files_root_path_defaults_to_none(self, tmp_path: Path) -> None:
        """files_root_path is None when not provided."""
        env = RuntimeEnvironment(
            delta_root_path=tmp_path / "delta",
            staging_root_path=tmp_path / "staging",
        )
        assert env.files_root_path is None

    def test_files_root_path_accepts_value(self, tmp_path: Path) -> None:
        """files_root_path stores the provided path."""
        files_root = tmp_path / "files"
        env = RuntimeEnvironment(
            delta_root_path=tmp_path / "delta",
            staging_root_path=tmp_path / "staging",
            files_root_path=files_root,
        )
        assert env.files_root_path == files_root

    def test_files_root_path_frozen(self, tmp_path: Path) -> None:
        """files_root_path cannot be mutated (frozen model)."""
        env = RuntimeEnvironment(
            delta_root_path=tmp_path / "delta",
            staging_root_path=tmp_path / "staging",
            files_root_path=tmp_path / "files",
        )
        try:
            env.files_root_path = tmp_path / "other"  # type: ignore[misc]
            assert False, "Should have raised"  # noqa: B011
        except Exception:
            pass  # Expected: ValidationError on frozen model
