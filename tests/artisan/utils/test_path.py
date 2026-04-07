"""Tests for path utilities: get_caller_dir and find_project_root.

Covers all four strategies in get_caller_dir (script __file__, VS Code
notebook, JupyterHub session, cwd fallback) and all three strategies in
find_project_root (walk-up, editable install, env var).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import artisan.utils.path as path_module
from artisan.utils.path import find_project_root, get_caller_dir, shard_path, uri_join, uri_parent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_frame(caller_globals: dict) -> MagicMock:
    """Build a mock frame chain: currentframe() -> f_back (caller)."""
    caller_frame = MagicMock()
    caller_frame.f_globals = caller_globals

    current_frame = MagicMock()
    current_frame.f_back = caller_frame
    return current_frame


# ===================================================================
# get_caller_dir — Strategy 1: script __file__
# ===================================================================


class TestGetCallerDirScript:
    """Strategy 1: caller's __file__ via frame inspection."""

    def test_returns_this_test_files_directory(self):
        """Direct call from a .py file returns its directory."""
        result = get_caller_dir()
        assert result == Path(__file__).resolve().parent

    def test_returns_absolute_path(self):
        result = get_caller_dir()
        assert result.is_absolute()

    def test_real_py_file_returns_parent(self, tmp_path):
        """__file__ pointing to an existing .py file returns its parent."""
        script = tmp_path / "my_script.py"
        script.touch()

        frame = _make_mock_frame({"__file__": str(script)})
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result == tmp_path.resolve()

    def test_nonexistent_py_file_is_skipped(self):
        """__file__ pointing to a .py file that doesn't exist is skipped."""
        frame = _make_mock_frame({"__file__": "/no/such/script.py"})
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        # Falls through to cwd because the file doesn't exist
        assert result == Path.cwd().resolve()

    def test_non_py_extension_is_skipped(self, tmp_path):
        """__file__ with non-.py extension (e.g. .pyc) is ignored."""
        pyc = tmp_path / "module.pyc"
        pyc.touch()

        frame = _make_mock_frame({"__file__": str(pyc)})
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result == Path.cwd().resolve()

    def test_ipython_temp_file_skipped_if_missing(self):
        """IPython temp __file__ that doesn't exist on disk is skipped."""
        frame = _make_mock_frame({"__file__": "/tmp/ipykernel_9999/12345.py"})
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result == Path.cwd().resolve()


# ===================================================================
# get_caller_dir — Strategy 2: VS Code __vsc_ipynb_file__
# ===================================================================


class TestGetCallerDirVSCode:
    """Strategy 2: __vsc_ipynb_file__ injected by VS Code."""

    def test_returns_notebook_parent_dir(self, tmp_path):
        nb = tmp_path / "notebooks" / "demo.ipynb"
        nb.parent.mkdir()
        nb.touch()

        frame = _make_mock_frame({"__vsc_ipynb_file__": str(nb)})
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result == nb.parent.resolve()

    def test_resolves_to_absolute_path(self, tmp_path):
        nb = tmp_path / "test.ipynb"
        nb.touch()

        frame = _make_mock_frame({"__vsc_ipynb_file__": str(nb)})
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result.is_absolute()

    def test_path_does_not_need_to_exist(self, tmp_path):
        """VS Code may set the variable before the file is saved."""
        nb = tmp_path / "unsaved" / "new.ipynb"

        frame = _make_mock_frame({"__vsc_ipynb_file__": str(nb)})
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result == nb.resolve().parent


# ===================================================================
# get_caller_dir — Strategy 3: JupyterHub JPY_SESSION_NAME
# ===================================================================


class TestGetCallerDirJupyterHub:
    """Strategy 3: JPY_SESSION_NAME environment variable (ipykernel ≥ 6.9)."""

    def test_absolute_session_path(self, tmp_path, monkeypatch):
        nb = tmp_path / "work" / "analysis.ipynb"
        nb.parent.mkdir()
        nb.touch()

        monkeypatch.setenv("JPY_SESSION_NAME", str(nb))
        frame = _make_mock_frame({})  # No __file__ or __vsc_ipynb_file__
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result == nb.parent.resolve()

    def test_relative_session_path_resolved_via_home(self, tmp_path, monkeypatch):
        """Relative path is resolved relative to Path.home()."""
        rel = "projects/notebooks/demo.ipynb"
        nb = tmp_path / rel
        nb.parent.mkdir(parents=True)
        nb.touch()

        monkeypatch.setenv("JPY_SESSION_NAME", rel)
        frame = _make_mock_frame({})
        with (
            patch("artisan.utils.path.inspect.currentframe", return_value=frame),
            patch("artisan.utils.path.Path.home", return_value=tmp_path),
        ):
            result = get_caller_dir()

        assert result == nb.parent.resolve()

    def test_nonexistent_session_file_falls_to_cwd(self, monkeypatch):
        """If JPY_SESSION_NAME file doesn't exist, falls to cwd."""
        monkeypatch.setenv("JPY_SESSION_NAME", "/no/such/notebook.ipynb")
        frame = _make_mock_frame({})
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result == Path.cwd().resolve()

    def test_empty_session_name_ignored(self, monkeypatch):
        """Empty string is treated as not set."""
        monkeypatch.setenv("JPY_SESSION_NAME", "")
        frame = _make_mock_frame({})
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result == Path.cwd().resolve()


# ===================================================================
# get_caller_dir — Strategy 4: cwd fallback
# ===================================================================


class TestGetCallerDirFallback:
    """Strategy 4: Path.cwd() fallback when nothing else works."""

    def test_returns_cwd_with_empty_globals(self, monkeypatch):
        monkeypatch.delenv("JPY_SESSION_NAME", raising=False)
        frame = _make_mock_frame({})
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result == Path.cwd().resolve()

    def test_returns_cwd_when_currentframe_is_none(self, monkeypatch):
        monkeypatch.delenv("JPY_SESSION_NAME", raising=False)
        with patch("artisan.utils.path.inspect.currentframe", return_value=None):
            result = get_caller_dir()

        assert result == Path.cwd().resolve()

    def test_returns_cwd_when_f_back_is_none(self, monkeypatch):
        monkeypatch.delenv("JPY_SESSION_NAME", raising=False)
        frame = MagicMock()
        frame.f_back = None
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result == Path.cwd().resolve()


# ===================================================================
# get_caller_dir — priority ordering
# ===================================================================


class TestGetCallerDirPriority:
    """Verify strategies are tried in the documented order."""

    def test_script_file_beats_vsc(self, tmp_path):
        """__file__ (Strategy 1) wins over __vsc_ipynb_file__ (Strategy 2)."""
        script = tmp_path / "run.py"
        script.touch()
        nb = tmp_path / "notebooks" / "test.ipynb"
        nb.parent.mkdir()
        nb.touch()

        frame = _make_mock_frame(
            {
                "__file__": str(script),
                "__vsc_ipynb_file__": str(nb),
            }
        )
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result == script.parent.resolve()

    def test_vsc_beats_jpy_session(self, tmp_path, monkeypatch):
        """__vsc_ipynb_file__ (Strategy 2) wins over JPY_SESSION_NAME (Strategy 3)."""
        nb_vsc = tmp_path / "vscode" / "test.ipynb"
        nb_vsc.parent.mkdir()
        nb_vsc.touch()
        nb_jpy = tmp_path / "hub" / "test.ipynb"
        nb_jpy.parent.mkdir()
        nb_jpy.touch()

        monkeypatch.setenv("JPY_SESSION_NAME", str(nb_jpy))
        frame = _make_mock_frame({"__vsc_ipynb_file__": str(nb_vsc)})
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result == nb_vsc.parent.resolve()

    def test_jpy_session_beats_cwd(self, tmp_path, monkeypatch):
        """JPY_SESSION_NAME (Strategy 3) wins over cwd (Strategy 4)."""
        nb = tmp_path / "hub" / "test.ipynb"
        nb.parent.mkdir()
        nb.touch()

        monkeypatch.setenv("JPY_SESSION_NAME", str(nb))
        frame = _make_mock_frame({})
        with patch("artisan.utils.path.inspect.currentframe", return_value=frame):
            result = get_caller_dir()

        assert result == nb.parent.resolve()


# ===================================================================
# find_project_root — Strategy 1: walk up from caller dir
# ===================================================================


class TestFindProjectRootWalkUp:
    """Strategy 1: walk up directory tree from get_caller_dir()."""

    def test_finds_root_from_test_directory(self):
        """Normal case: test file is inside the project tree."""
        result = find_project_root()
        assert (result / "pyproject.toml").exists()
        assert (result / "src" / "artisan").is_dir()

    def test_returns_absolute_path(self):
        assert find_project_root().is_absolute()

    def test_finds_root_from_nested_dir(self, tmp_path):
        """pyproject.toml found by walking up from a deeply nested dir."""
        (tmp_path / "pyproject.toml").touch()
        nested = tmp_path / "a" / "b" / "c" / "d"
        nested.mkdir(parents=True)

        with patch("artisan.utils.path.get_caller_dir", return_value=nested):
            result = find_project_root()

        assert result == tmp_path

    def test_finds_nearest_pyproject(self, tmp_path):
        """If multiple pyproject.toml exist, the nearest (innermost) wins."""
        (tmp_path / "pyproject.toml").touch()
        inner = tmp_path / "subproject"
        inner.mkdir()
        (inner / "pyproject.toml").touch()
        child = inner / "src"
        child.mkdir()

        with patch("artisan.utils.path.get_caller_dir", return_value=child):
            result = find_project_root()

        assert result == inner


# ===================================================================
# find_project_root — Strategy 2: editable install
# ===================================================================


class TestFindProjectRootEditableInstall:
    """Strategy 2: resolve from installed package __file__ location."""

    def test_finds_root_when_caller_outside_project(self, tmp_path):
        """Even if caller is in /tmp, editable install locates the root."""
        with patch("artisan.utils.path.get_caller_dir", return_value=tmp_path):
            result = find_project_root()

        # The actual project root (editable install always works in dev)
        assert (result / "pyproject.toml").exists()
        assert (result / "src" / "artisan").is_dir()

    def test_finds_root_when_caller_at_filesystem_root(self):
        """Caller at / can't walk up, but editable install works."""
        with patch("artisan.utils.path.get_caller_dir", return_value=Path("/")):
            result = find_project_root()

        assert (result / "pyproject.toml").exists()


# ===================================================================
# find_project_root — Strategy 3: ARTISAN_ROOT env var
# ===================================================================


class TestFindProjectRootEnvVar:
    """Strategy 3: ARTISAN_ROOT environment variable."""

    def test_env_var_used_when_other_strategies_fail(self, tmp_path, monkeypatch):
        """ARTISAN_ROOT is the last resort."""
        fake_root = tmp_path / "my_project"
        fake_root.mkdir()
        (fake_root / "pyproject.toml").touch()

        monkeypatch.setenv("ARTISAN_ROOT", str(fake_root))

        # Break Strategy 1: caller outside any project
        # Break Strategy 2: fake __file__ so parents[3] has no pyproject.toml
        fake_file = tmp_path / "x" / "y" / "z" / "w" / "path.py"
        fake_file.parent.mkdir(parents=True)
        fake_file.touch()

        with patch("artisan.utils.path.get_caller_dir", return_value=Path("/")):
            monkeypatch.setattr(path_module, "__file__", str(fake_file))
            result = find_project_root()

        assert result == fake_root.resolve()

    def test_env_var_without_pyproject_is_skipped(self, tmp_path, monkeypatch):
        """ARTISAN_ROOT pointing to a dir without pyproject.toml is skipped."""
        no_toml = tmp_path / "empty"
        no_toml.mkdir()
        monkeypatch.setenv("ARTISAN_ROOT", str(no_toml))

        # Also break Strategy 2
        fake_file = tmp_path / "x" / "y" / "z" / "w" / "path.py"
        fake_file.parent.mkdir(parents=True)
        fake_file.touch()

        with patch("artisan.utils.path.get_caller_dir", return_value=Path("/")):
            monkeypatch.setattr(path_module, "__file__", str(fake_file))
            with pytest.raises(RuntimeError, match="Project root not found"):
                find_project_root()

    def test_env_var_nonexistent_dir_is_skipped(self, tmp_path, monkeypatch):
        """ARTISAN_ROOT pointing to a nonexistent dir is skipped."""
        monkeypatch.setenv("ARTISAN_ROOT", "/no/such/directory")

        fake_file = tmp_path / "x" / "y" / "z" / "w" / "path.py"
        fake_file.parent.mkdir(parents=True)
        fake_file.touch()

        with patch("artisan.utils.path.get_caller_dir", return_value=Path("/")):
            monkeypatch.setattr(path_module, "__file__", str(fake_file))
            with pytest.raises(RuntimeError, match="Project root not found"):
                find_project_root()


# ===================================================================
# find_project_root — error case
# ===================================================================


class TestFindProjectRootError:
    """RuntimeError when all strategies fail."""

    def test_raises_with_clear_message(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ARTISAN_ROOT", raising=False)

        fake_file = tmp_path / "x" / "y" / "z" / "w" / "path.py"
        fake_file.parent.mkdir(parents=True)
        fake_file.touch()

        with patch("artisan.utils.path.get_caller_dir", return_value=Path("/")):
            monkeypatch.setattr(path_module, "__file__", str(fake_file))
            with pytest.raises(RuntimeError, match="ARTISAN_ROOT"):
                find_project_root()


# ===================================================================
# Existing shard_path tests (regression)
# ===================================================================


class TestShardPath:
    """Ensure existing shard_path behavior is not broken."""

    def test_basic_sharding(self):
        result = shard_path(Path("/tmp"), "abcdef1234567890")
        assert result == Path("/tmp/ab/cd/abcdef1234567890")

    def test_with_step_number(self):
        result = shard_path(Path("/tmp"), "abcdef1234567890", step_number=3)
        assert result == Path("/tmp/3/ab/cd/abcdef1234567890")

    def test_with_step_number_and_operation(self):
        result = shard_path(
            Path("/tmp"), "abcdef1234567890", step_number=3, operation_name="tool_c"
        )
        assert result == Path("/tmp/3_tool_c/ab/cd/abcdef1234567890")


# ===================================================================
# uri_join
# ===================================================================


class TestUriJoin:
    """URI-safe path joining for local and cloud URIs."""

    def test_local_path(self):
        assert uri_join("/data/delta", "executions") == "/data/delta/executions"

    def test_local_path_multiple_parts(self):
        assert uri_join("/data", "delta", "executions") == "/data/delta/executions"

    def test_s3_uri(self):
        assert uri_join("s3://bucket/delta", "executions") == "s3://bucket/delta/executions"

    def test_gcs_uri(self):
        assert uri_join("gcs://bucket/delta", "executions") == "gcs://bucket/delta/executions"

    def test_trailing_slash_on_base(self):
        assert uri_join("/data/delta/", "executions") == "/data/delta/executions"

    def test_absolute_part_replaces_base(self):
        """posixpath.join behavior: absolute part resets the path."""
        assert uri_join("/data/delta", "/other") == "/other"


# ===================================================================
# uri_parent
# ===================================================================


class TestUriParent:
    """URI-safe parent directory for local and cloud URIs."""

    def test_local_path(self):
        assert uri_parent("/data/pipelines/delta") == "/data/pipelines"

    def test_s3_uri(self):
        assert uri_parent("s3://bucket/project/delta") == "s3://bucket/project"

    def test_gcs_uri(self):
        assert uri_parent("gcs://bucket/project/delta") == "gcs://bucket/project"

    def test_trailing_slash(self):
        assert uri_parent("/data/delta/") == "/data/delta"

    def test_root_path(self):
        assert uri_parent("/") == "/"

    def test_bare_s3_bucket(self):
        # posixpath treats // as separator, so s3://bucket → s3:
        # In practice, URIs always have a path component (s3://bucket/key)
        assert uri_parent("s3://bucket") == "s3:"
