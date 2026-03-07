"""Tests for external tool execution utilities."""

from __future__ import annotations

import signal
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from artisan.schemas.operation_config.command_spec import (
    ApptainerCommandSpec,
    DockerCommandSpec,
    LocalCommandSpec,
    PixiCommandSpec,
)
from artisan.utils.external_tools import (
    ArgStyle,
    ExternalToolError,
    _kill_process_group,
    build_command_from_spec,
    format_args,
    run_external_command,
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

    def test_hydra_simple(self):
        result = format_args({"batch_size": 16}, ArgStyle.HYDRA)
        assert result == ["batch_size=16"]

    def test_hydra_with_override_prefix(self):
        result = format_args({"++sampler.steps": 200}, ArgStyle.HYDRA)
        assert result == ["++sampler.steps=200"]

    def test_hydra_skips_none(self):
        result = format_args({"a": 1, "b": None, "c": 2}, ArgStyle.HYDRA)
        assert result == ["a=1", "c=2"]

    def test_argparse_simple(self):
        result = format_args({"batch-size": 16}, ArgStyle.ARGPARSE)
        assert result == ["--batch-size", "16"]

    def test_argparse_skips_none(self):
        result = format_args({"a": 1, "b": None}, ArgStyle.ARGPARSE)
        assert result == ["--a", "1"]

    def test_argparse_with_path(self):
        result = format_args({"output": Path("/tmp/out")}, ArgStyle.ARGPARSE)
        assert result == ["--output", "/tmp/out"]


class TestBuildCommandFromSpec:
    """Tests for build_command_from_spec with CommandSpec types."""

    def test_apptainer_minimal(self):
        spec = ApptainerCommandSpec(
            script=Path("/run.py"),
            arg_style=ArgStyle.HYDRA,
            image=Path("/app.sif"),
            interpreter="python",
        )
        cmd = build_command_from_spec(spec, ["arg1"])
        assert cmd.parts == [
            "apptainer",
            "exec",
            "/app.sif",
            "python",
            "/run.py",
            "arg1",
        ]

    def test_apptainer_with_gpu(self):
        spec = ApptainerCommandSpec(
            script=Path("/run.py"),
            arg_style=ArgStyle.HYDRA,
            image=Path("/app.sif"),
            gpu=True,
            interpreter="python",
        )
        cmd = build_command_from_spec(spec, [])
        assert "--nv" in cmd.parts

    def test_apptainer_with_subcommand(self):
        spec = ApptainerCommandSpec(
            script=Path("/run.py"),
            arg_style=ArgStyle.HYDRA,
            image=Path("/app.sif"),
            interpreter="python",
            subcommand="design",
        )
        cmd = build_command_from_spec(spec, ["key=val"])
        # subcommand should come after script_parts, before args
        idx_script = cmd.parts.index("/run.py")
        idx_sub = cmd.parts.index("design")
        idx_arg = cmd.parts.index("key=val")
        assert idx_script < idx_sub < idx_arg

    def test_apptainer_with_binds_and_env(self):
        spec = ApptainerCommandSpec(
            script=Path("/run.py"),
            arg_style=ArgStyle.HYDRA,
            image=Path("/app.sif"),
            binds=[(Path("/host"), Path("/container"))],
            env={"KEY": "val"},
        )
        cmd = build_command_from_spec(spec, [])
        assert "--bind" in cmd.parts
        assert "--env" in cmd.parts

    def test_docker_minimal(self):
        spec = DockerCommandSpec(
            script=Path("/run.py"),
            arg_style=ArgStyle.ARGPARSE,
            image=Path("img:latest"),
            interpreter="python",
        )
        cmd = build_command_from_spec(spec, [])
        assert cmd.parts[:3] == ["docker", "run", "--rm"]

    def test_docker_with_gpu(self):
        spec = DockerCommandSpec(
            script=Path("/run.py"),
            arg_style=ArgStyle.ARGPARSE,
            image=Path("img:latest"),
            gpu=True,
        )
        cmd = build_command_from_spec(spec, [])
        assert "--gpus" in cmd.parts

    def test_pixi_minimal(self):
        spec = PixiCommandSpec(
            script=Path("./run.py"),
            arg_style=ArgStyle.HYDRA,
            environment="dev",
            interpreter="python",
        )
        cmd = build_command_from_spec(spec, [])
        assert cmd.parts[:4] == ["pixi", "run", "-e", "dev"]

    def test_pixi_with_manifest(self):
        spec = PixiCommandSpec(
            script=Path("./run.py"),
            arg_style=ArgStyle.HYDRA,
            manifest_path=Path("/pixi.toml"),
        )
        cmd = build_command_from_spec(spec, [])
        assert "--manifest-path" in cmd.parts

    def test_local_minimal(self):
        spec = LocalCommandSpec(
            script=Path("/app/run.py"),
            arg_style=ArgStyle.ARGPARSE,
            interpreter="python",
        )
        cmd = build_command_from_spec(spec, ["arg1"])
        assert cmd.parts == ["python", "/app/run.py", "arg1"]

    def test_local_with_subcommand(self):
        spec = LocalCommandSpec(
            script=Path("/app/run.py"),
            arg_style=ArgStyle.ARGPARSE,
            interpreter="python",
            subcommand="serve",
        )
        cmd = build_command_from_spec(spec, ["--port", "8080"])
        assert cmd.parts == ["python", "/app/run.py", "serve", "--port", "8080"]

    def test_command_string_shell_quoted(self):
        spec = LocalCommandSpec(
            script=Path("/path with spaces/run.py"),
            arg_style=ArgStyle.ARGPARSE,
            interpreter="python",
        )
        cmd = build_command_from_spec(spec, [])
        assert "'/path with spaces/run.py'" in cmd.string


class TestRunExternalCommand:
    """Tests for run_external_command with CommandSpec types."""

    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_success(self, mock_popen):
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = ("output", "")
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc

        spec = LocalCommandSpec(
            script=Path("./run.py"),
            arg_style=ArgStyle.ARGPARSE,
            interpreter="python",
        )
        result = run_external_command(spec, [])
        assert result.returncode == 0
        assert result.stdout == "output"

    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_failure_raises_error(self, mock_popen):
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = ("", "error")
        mock_proc.returncode = 1
        mock_popen.return_value = mock_proc

        spec = LocalCommandSpec(
            script=Path("./run.py"),
            arg_style=ArgStyle.ARGPARSE,
            interpreter="python",
        )
        with pytest.raises(ExternalToolError) as exc_info:
            run_external_command(spec, [])
        assert exc_info.value.return_code == 1

    @patch("artisan.utils.external_tools._kill_process_group")
    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_timeout_raises_error(self, mock_popen, mock_kill):
        mock_proc = MagicMock()
        mock_proc.communicate.side_effect = subprocess.TimeoutExpired(
            cmd=["test"], timeout=5
        )
        mock_popen.return_value = mock_proc

        spec = LocalCommandSpec(
            script=Path("./run.py"),
            arg_style=ArgStyle.ARGPARSE,
            interpreter="python",
        )
        with pytest.raises(ExternalToolError) as exc_info:
            run_external_command(spec, [], timeout=5)
        assert exc_info.value.return_code == -1
        mock_kill.assert_called_once_with(mock_proc)


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

    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_streaming_popen_uses_process_group(self, mock_popen):
        """Popen is called with process_group=0 in streaming mode."""
        mock_proc = MagicMock()
        mock_proc.stdout = iter([])
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        spec = LocalCommandSpec(
            script=Path("./run.py"),
            arg_style=ArgStyle.ARGPARSE,
            interpreter="python",
        )
        run_external_command(spec, [], stream_output=True)

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

        spec = LocalCommandSpec(
            script=Path("./run.py"),
            arg_style=ArgStyle.ARGPARSE,
            interpreter="python",
        )
        with pytest.raises(KeyboardInterrupt):
            run_external_command(spec, [], stream_output=True)

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
        # First call is start_time (0), second call during loop (100) exceeds timeout
        mock_monotonic.side_effect = [0.0, 100.0]

        spec = LocalCommandSpec(
            script=Path("./run.py"),
            arg_style=ArgStyle.ARGPARSE,
            interpreter="python",
        )
        with pytest.raises(ExternalToolError):
            run_external_command(spec, [], stream_output=True, timeout=5)

        mock_kill.assert_called()

    @patch("artisan.utils.external_tools._kill_process_group")
    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_captured_interrupt_kills_group(self, mock_popen, mock_kill):
        """KeyboardInterrupt during captured run triggers process group cleanup."""
        mock_proc = MagicMock()
        mock_proc.communicate.side_effect = KeyboardInterrupt
        mock_popen.return_value = mock_proc

        spec = LocalCommandSpec(
            script=Path("./run.py"),
            arg_style=ArgStyle.ARGPARSE,
            interpreter="python",
        )
        with pytest.raises(KeyboardInterrupt):
            run_external_command(spec, [])

        mock_kill.assert_called_once_with(mock_proc)
