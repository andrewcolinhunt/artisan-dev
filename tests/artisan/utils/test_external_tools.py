"""Tests for external tool execution utilities."""

from __future__ import annotations

import signal
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from artisan.schemas.operation_config.environment_spec import (
    DockerEnvironmentSpec,
    EnvironmentSpec,
    LocalEnvironmentSpec,
)
from artisan.utils.external_tools import (
    ExternalToolError,
    _kill_process_group,
    format_args,
    run_command,
    to_cli_value,
)


class TestToCLIValue:
    """Tests for to_cli_value function."""

    def test_none_returns_empty_string(self):
        assert to_cli_value(None) == ""

    def test_bool_true(self):
        assert to_cli_value(True) == "true"

    def test_bool_false(self):
        assert to_cli_value(False) == "false"

    def test_path_converts_to_string(self):
        assert to_cli_value(Path("/tmp/out")) == "/tmp/out"

    def test_list_json_serializes(self):
        assert to_cli_value([1, 2, 3]) == "[1, 2, 3]"

    def test_dict_json_serializes(self):
        result = to_cli_value({"key": "value"})
        assert result == '{"key": "value"}'

    def test_int_converts_to_string(self):
        assert to_cli_value(42) == "42"

    def test_float_converts_to_string(self):
        assert to_cli_value(3.14) == "3.14"


class TestFormatArgs:
    """Tests for format_args function."""

    def test_booleans_as_flags(self):
        """Booleans become flags."""
        result = format_args({"verbose": True, "quiet": False})
        assert result == ["--verbose"]

    def test_key_value_pairs(self):
        """--key value pairs."""
        result = format_args({"batch-size": 16, "lr": 0.001})
        assert result == ["--batch-size", "16", "--lr", "0.001"]

    def test_skips_none(self):
        result = format_args({"a": 1, "b": None})
        assert result == ["--a", "1"]

    def test_mixed(self):
        result = format_args({"batch-size": 16, "verbose": True, "seed": 42})
        assert result == ["--batch-size", "16", "--verbose", "--seed", "42"]

    def test_with_path(self):
        result = format_args({"output": Path("/tmp/out")})
        assert result == ["--output", "/tmp/out"]


class TestExternalToolError:
    """Tests for ExternalToolError exception."""

    def test_str_representation(self):
        err = ExternalToolError(
            message="Command failed",
            command=["python", "run.py"],
            return_code=1,
            stdout="",
            stderr="error",
            runtime=None,
        )

        result = str(err)
        assert "Command failed" in result
        assert "exit code 1" in result
        assert "python run.py" in result

    def test_is_exception(self):
        err = ExternalToolError(
            message="test",
            command=[],
            return_code=1,
            stdout="",
            stderr="",
            runtime=None,
        )

        assert isinstance(err, Exception)

    def test_str_includes_stderr_tail(self):
        """ExternalToolError.__str__ includes last 20 lines of stderr."""
        err = ExternalToolError(
            message="Command failed",
            command=["tool"],
            return_code=1,
            stdout="",
            stderr="stderr line 1\nstderr line 2",
            runtime=None,
        )
        result = str(err)
        assert "--- stderr (last 20 lines) ---" in result
        assert "stderr line 2" in result

    def test_str_includes_stdout_tail(self):
        """ExternalToolError.__str__ includes last 20 lines of stdout."""
        err = ExternalToolError(
            message="Command failed",
            command=["tool"],
            return_code=1,
            stdout="stdout line 1\nstdout line 2",
            stderr="",
            runtime=None,
        )
        result = str(err)
        assert "--- stdout (last 20 lines) ---" in result
        assert "stdout line 2" in result

    def test_str_handles_empty_output(self):
        """No extra sections when stdout/stderr are empty."""
        err = ExternalToolError(
            message="Command failed",
            command=["tool"],
            return_code=1,
            stdout="",
            stderr="",
            runtime=None,
        )
        result = str(err)
        assert "--- stderr" not in result
        assert "--- stdout" not in result

    def test_str_truncates_long_output(self):
        """Only last 20 lines shown from 100 lines of stderr."""
        lines = [f"line {i}" for i in range(100)]
        err = ExternalToolError(
            message="Command failed",
            command=["tool"],
            return_code=1,
            stdout="",
            stderr="\n".join(lines),
            runtime=None,
        )
        result = str(err)
        assert "line 80" in result
        assert "line 99" in result
        assert "line 0\n" not in result


class TestProcessCleanup:
    """Tests for subprocess process group cleanup."""

    def test_kill_process_group_sigterm_sufficient(self):
        """Process exits after SIGTERM — no SIGKILL needed."""
        mock_proc = MagicMock()
        mock_proc.pid = 12345
        mock_proc.wait.return_value = 0

        with patch("artisan.utils.external_tools.os.getpgid", return_value=12345):
            with patch("artisan.utils.external_tools.os.killpg") as mock_killpg:
                _kill_process_group(mock_proc)

        mock_killpg.assert_called_once_with(12345, signal.SIGTERM)

    def test_kill_process_group_escalates_to_sigkill(self):
        """SIGKILL sent when process doesn't exit after SIGTERM."""
        mock_proc = MagicMock()
        mock_proc.pid = 12345
        mock_proc.wait.side_effect = [
            subprocess.TimeoutExpired(cmd=[], timeout=3),
            0,
        ]

        with patch("artisan.utils.external_tools.os.getpgid", return_value=12345):
            with patch("artisan.utils.external_tools.os.killpg") as mock_killpg:
                _kill_process_group(mock_proc)

        assert mock_killpg.call_count == 2
        mock_killpg.assert_any_call(12345, signal.SIGTERM)
        mock_killpg.assert_any_call(12345, signal.SIGKILL)

    def test_kill_process_group_already_dead(self):
        """ProcessLookupError is swallowed when process already exited."""
        mock_proc = MagicMock()
        mock_proc.pid = 12345

        with patch(
            "artisan.utils.external_tools.os.getpgid",
            side_effect=ProcessLookupError,
        ):
            _kill_process_group(mock_proc)  # should not raise


class TestRunCommand:
    """Tests for run_command with EnvironmentSpec types."""

    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_success_local(self, mock_popen):
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = ("output", "")
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc

        env = LocalEnvironmentSpec()
        result = run_command(env, ["python", "run.py"])
        assert result.returncode == 0
        assert result.stdout == "output"

    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_failure_raises_error(self, mock_popen):
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = ("", "error")
        mock_proc.returncode = 1
        mock_popen.return_value = mock_proc

        env = LocalEnvironmentSpec()
        with pytest.raises(ExternalToolError) as exc_info:
            run_command(env, ["python", "run.py"])
        assert exc_info.value.return_code == 1

    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_docker_wrapping(self, mock_popen):
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = ("ok", "")
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc

        env = DockerEnvironmentSpec(image="img:latest")
        run_command(env, ["samtools", "sort"])

        call_args = mock_popen.call_args[0][0]
        assert call_args[:3] == ["docker", "run", "--rm"]
        assert "img:latest" in call_args

    @patch("artisan.utils.external_tools._kill_process_group")
    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_timeout_raises_error(self, mock_popen, mock_kill):
        mock_proc = MagicMock()
        mock_proc.communicate.side_effect = subprocess.TimeoutExpired(
            cmd=["test"], timeout=5
        )
        mock_popen.return_value = mock_proc

        env = EnvironmentSpec()
        with pytest.raises(ExternalToolError) as exc_info:
            run_command(env, ["cmd"], timeout=5)
        assert exc_info.value.return_code == -1
        mock_kill.assert_called_once_with(mock_proc)

    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_env_vars_passed(self, mock_popen):
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = ("", "")
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc

        env = EnvironmentSpec(env={"FOO": "bar"})
        run_command(env, ["cmd"])

        _, kwargs = mock_popen.call_args
        assert kwargs["env"]["FOO"] == "bar"

    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_streaming_popen_uses_process_group(self, mock_popen):
        """Popen is called with process_group=0 in streaming mode."""
        mock_proc = MagicMock()
        mock_proc.stdout = iter([])
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        env = LocalEnvironmentSpec()
        run_command(env, ["python", "run.py"], stream_output=True)

        _, kwargs = mock_popen.call_args
        assert kwargs["process_group"] == 0

    @patch("artisan.utils.external_tools._kill_process_group")
    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_streaming_interrupt_kills_group(self, mock_popen, mock_kill):
        """KeyboardInterrupt during streaming triggers process group cleanup."""
        mock_proc = MagicMock()
        mock_proc.stdout = MagicMock()
        mock_proc.stdout.__iter__ = MagicMock(side_effect=KeyboardInterrupt)
        mock_popen.return_value = mock_proc

        env = LocalEnvironmentSpec()
        with pytest.raises(KeyboardInterrupt):
            run_command(env, ["python", "run.py"], stream_output=True)

        mock_kill.assert_called_with(mock_proc)

    @patch("artisan.utils.external_tools._kill_process_group")
    @patch("artisan.utils.external_tools.time.monotonic")
    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_streaming_timeout_kills_group(self, mock_popen, mock_monotonic, mock_kill):
        """Timeout during streaming triggers _kill_process_group."""
        mock_proc = MagicMock()
        mock_proc.stdout = iter(["line\n"])
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc
        mock_monotonic.side_effect = [0.0, 100.0]

        env = LocalEnvironmentSpec()
        with pytest.raises(ExternalToolError):
            run_command(env, ["python", "run.py"], stream_output=True, timeout=5)

        mock_kill.assert_called()

    @patch("artisan.utils.external_tools._kill_process_group")
    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_captured_interrupt_kills_group(self, mock_popen, mock_kill):
        """KeyboardInterrupt during captured run triggers process group cleanup."""
        mock_proc = MagicMock()
        mock_proc.communicate.side_effect = KeyboardInterrupt
        mock_popen.return_value = mock_proc

        env = LocalEnvironmentSpec()
        with pytest.raises(KeyboardInterrupt):
            run_command(env, ["python", "run.py"])

        mock_kill.assert_called_once_with(mock_proc)
