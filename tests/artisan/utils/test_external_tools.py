"""Tests for external tool execution utilities."""

from __future__ import annotations

import io
import signal
import subprocess
import sys
from typing import Any
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

    def test_string_passes_through(self):
        assert to_cli_value("/tmp/out") == "/tmp/out"

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

    def test_with_string_path(self):
        result = format_args({"output": "/tmp/out"})
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


class TestExternalToolErrorEnvelope:
    """Verify compute error envelopes and recovery hints for tool failures."""

    def test_is_artisan_error(self):
        from artisan.errors import ArtisanError

        err = ExternalToolError(
            message="boom",
            command=["tool"],
            return_code=1,
            stdout="",
            stderr="",
            runtime=None,
        )
        assert isinstance(err, ArtisanError)

    def test_envelope_code_and_type(self):
        err = ExternalToolError(
            message="boom",
            command=["tool"],
            return_code=2,
            stdout="",
            stderr="",
            runtime=None,
        )
        assert err.envelope.code == "op_execute_failed"
        assert err.envelope.error_type == "compute"

    def test_nonzero_exit_reports_to_user(self):
        err = ExternalToolError(
            message="boom",
            command=["tool"],
            return_code=3,
            stdout="",
            stderr="",
            runtime=None,
        )
        assert err.envelope.recovery_hint == "REPORT_TO_USER"

    def test_timeout_sentinel_retries_later(self):
        err = ExternalToolError(
            message="timed out",
            command=["tool"],
            return_code=-1,
            stdout="",
            stderr="",
            runtime=None,
        )
        assert err.envelope.recovery_hint == "RETRY_LATER"


class TestProcessCleanup:
    """Tests for subprocess process group cleanup."""

    def test_kill_process_group_sigterm_sufficient(self):
        """Process exits after SIGTERM — no SIGKILL needed."""
        mock_proc = MagicMock()
        mock_proc.pid = 12345
        mock_proc.wait.return_value = 0

        with (
            patch("artisan.utils.external_tools.os.getpgid", return_value=12345),
            patch("artisan.utils.external_tools.os.killpg") as mock_killpg,
        ):
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

        with (
            patch("artisan.utils.external_tools.os.getpgid", return_value=12345),
            patch("artisan.utils.external_tools.os.killpg") as mock_killpg,
        ):
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
            _kill_process_group(mock_proc)


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
        mock_proc.stdout = io.StringIO()
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        env = LocalEnvironmentSpec()
        run_command(env, ["python", "run.py"], stream_output=True)

        _, kwargs = mock_popen.call_args
        assert kwargs["process_group"] == 0
        assert mock_proc.stdout.closed

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
        mock_proc.stdout.close.assert_called_once()

    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_streaming_writes_each_line_to_stdout(self, mock_popen, capsys):
        """Each child stdout line is written to ``sys.stdout`` for live emission."""
        mock_proc = MagicMock()
        mock_proc.stdout = io.StringIO("hello\nworld\n")
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        env = LocalEnvironmentSpec()
        run_command(env, ["python", "run.py"], stream_output=True)

        captured = capsys.readouterr().out
        assert "hello\n" in captured
        assert "world\n" in captured

    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_streaming_writes_log_path_and_stdout(self, mock_popen, capsys, tmp_path):
        """Streaming writes each line to both ``log_path`` and ``sys.stdout``."""
        mock_proc = MagicMock()
        mock_proc.stdout = io.StringIO("one\ntwo\n")
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        log_path = tmp_path / "out.log"
        env = LocalEnvironmentSpec()
        run_command(
            env, ["python", "run.py"], stream_output=True, log_path=str(log_path)
        )

        captured = capsys.readouterr().out
        assert "one\n" in captured
        assert "two\n" in captured
        assert log_path.read_text() == "one\ntwo\n"

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

    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_captured_writes_log_path_on_success(self, mock_popen, tmp_path):
        """``_run_captured`` writes captured stdout to ``log_path`` after success."""
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = ("captured output\n", "")
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc

        log_path = tmp_path / "out.log"
        env = LocalEnvironmentSpec()
        run_command(env, ["python", "run.py"], log_path=str(log_path))

        assert log_path.read_text() == "captured output\n"

    @patch("artisan.utils.external_tools.subprocess.Popen")
    def test_captured_skips_log_path_when_unset(self, mock_popen, tmp_path):
        """Without ``log_path`` no file is created in captured mode."""
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = ("captured output\n", "")
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc

        env = LocalEnvironmentSpec()
        run_command(env, ["python", "run.py"])

        assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize(
    "outcome", ["succeeded", "failed", "launch_failed", "interrupted"]
)
def test_launch_timing_brackets_only_popen(monkeypatch, tmp_path, streaming, outcome):
    """Output waits and later failures cannot change a successful launch duration."""
    from artisan.execution.recording.commands import capture_commands, invocation_scope
    from artisan.utils import external_tools

    clock = [100.0]
    events = []

    def now():
        events.append("clock")
        return clock[0]

    process = MagicMock()
    process.returncode = 7 if outcome == "failed" else 0

    def output():
        events.append("output")
        clock[0] += 1000.0
        if outcome == "interrupted":
            raise KeyboardInterrupt
        return "tool output\n", ""

    def stream():
        stdout, _ = output()
        yield stdout

    process.communicate.side_effect = output
    process.stdout = stream()
    process.wait.return_value = process.returncode

    def popen(*args, **kwargs):
        events.append("popen")
        clock[0] += 0.123456789
        if outcome == "launch_failed":
            msg = "missing executable"
            raise FileNotFoundError(msg)
        return process

    monkeypatch.setattr(external_tools, "perf_counter", now)
    monkeypatch.setattr(external_tools.subprocess, "Popen", popen)
    kill = MagicMock()
    monkeypatch.setattr(external_tools, "_kill_process_group", kill)
    exception = {
        "failed": ExternalToolError,
        "launch_failed": FileNotFoundError,
        "interrupted": KeyboardInterrupt,
    }.get(outcome)
    with capture_commands() as recorder, invocation_scope():
        kwargs = {"stream_output": streaming, "log_path": str(tmp_path / "tool.log")}
        if exception is None:
            run_command(LocalEnvironmentSpec(), ["tool"], **kwargs)
        else:
            with pytest.raises(exception):
                run_command(LocalEnvironmentSpec(), ["tool"], **kwargs)
    command = recorder.snapshot().commands[0]
    assert command.outcome == outcome
    if outcome == "launch_failed":
        assert command.launch_seconds is None
        assert events == ["clock", "popen"]
    else:
        assert command.launch_seconds == pytest.approx(0.123456789, abs=1e-12)
        assert events == ["clock", "popen", "clock", "output"]
    assert kill.call_count == (outcome == "interrupted")


@pytest.mark.parametrize("returncode", [0, 4])
def test_streaming_closes_real_subprocess_pipe(
    monkeypatch: pytest.MonkeyPatch, returncode: int
) -> None:
    processes: list[subprocess.Popen[str]] = []
    original_popen = subprocess.Popen

    def launch(*args: Any, **kwargs: Any) -> subprocess.Popen[str]:
        process = original_popen(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr("artisan.utils.external_tools.subprocess.Popen", launch)
    command = [sys.executable, "-c", f"print('output'); raise SystemExit({returncode})"]
    if returncode:
        with pytest.raises(ExternalToolError) as failure:
            run_command(LocalEnvironmentSpec(), command, stream_output=True)
        assert failure.value.return_code == returncode
    else:
        result = run_command(LocalEnvironmentSpec(), command, stream_output=True)
        assert result.stdout == "output\n"
    assert len(processes) == 1
    assert processes[0].stdout is not None
    assert processes[0].stdout.closed
    assert processes[0].returncode == returncode


@pytest.mark.parametrize("outcome", ["succeeded", "failed", "interrupted"])
def test_streaming_close_failure_preserves_primary_outcome(outcome: str) -> None:
    from artisan.execution.recording.commands import capture_commands

    pipe = io.StringIO("output\n")
    original_close = pipe.close
    process = MagicMock(stdout=pipe)
    process.returncode = {"succeeded": 0, "failed": 4, "interrupted": -15}[outcome]
    process.wait.return_value = process.returncode
    interruption = KeyboardInterrupt("cancelled")
    if outcome == "interrupted":
        process.wait.side_effect = interruption

    def close() -> None:
        original_close()
        message = "read pipe close failed"
        raise OSError(message)

    with (
        capture_commands() as recorder,
        patch("artisan.utils.external_tools.subprocess.Popen", return_value=process),
        patch("artisan.utils.external_tools._kill_process_group") as kill,
        patch.object(pipe, "close", side_effect=close) as close_pipe,
    ):
        if outcome == "succeeded":
            result = run_command(LocalEnvironmentSpec(), ["tool"], stream_output=True)
            assert result.returncode == 0
        elif outcome == "failed":
            with pytest.raises(ExternalToolError) as failure:
                run_command(LocalEnvironmentSpec(), ["tool"], stream_output=True)
            assert failure.value.return_code == 4
        else:
            with pytest.raises(KeyboardInterrupt) as failure:
                run_command(LocalEnvironmentSpec(), ["tool"], stream_output=True)
            assert failure.value is interruption
    assert pipe.closed
    close_pipe.assert_called_once()
    assert kill.call_count == (outcome == "interrupted")
    command = recorder.snapshot().commands[0]
    assert command.outcome == outcome
    assert command.returncode == process.returncode
    assert command.launch_seconds is not None
