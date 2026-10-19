"""Tests for external tool execution utilities."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from artisan.schemas.operation_config.environment_spec import (
    DockerEnvironmentSpec,
    LocalEnvironmentSpec,
)
from artisan.utils.external_tools import (
    ExternalToolError,
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


@pytest.mark.parametrize("streaming", [False, True])
def test_real_command_output_and_nonzero_status(streaming, capsys):
    command = [
        sys.executable,
        "-c",
        "import sys; print('output'); print('problem',file=sys.stderr); sys.exit(4)",
    ]
    with pytest.raises(ExternalToolError) as failure:
        run_command(LocalEnvironmentSpec(), command, stream_output=streaming)
    assert failure.value.return_code == 4
    assert "output" in failure.value.stdout
    assert "problem" in (failure.value.stdout if streaming else failure.value.stderr)
    assert ("output" in capsys.readouterr().out) is streaming


@pytest.mark.parametrize("streaming", [False, True])
def test_real_command_environment_cwd_and_log_modes(streaming, tmp_path):
    log = tmp_path / "tool.log"
    command = [
        sys.executable,
        "-c",
        "import os; print(os.getcwd()); print(os.environ['CHECK_VALUE'])",
    ]
    for mode in ("w", "a"):
        result = run_command(
            LocalEnvironmentSpec(env={"CHECK_VALUE": "value"}),
            command,
            cwd=str(tmp_path),
            stream_output=streaming,
            log_path=str(log),
            log_mode=mode,
        )
        assert result.stdout == f"{tmp_path}\nvalue\n"
    assert log.read_text() == result.stdout * 2


@pytest.mark.parametrize("streaming", [False, True])
def test_real_command_universal_newlines(streaming):
    result = run_command(
        LocalEnvironmentSpec(),
        [sys.executable, "-c", "import os; os.write(1,b'a\\r\\nb\\rc')"],
        stream_output=streaming,
    )
    assert result.stdout == "a\nb\nc"


@pytest.mark.parametrize("streaming", [False, True])
def test_real_command_missing_executable(streaming):
    with pytest.raises(FileNotFoundError, match="could not be launched"):
        run_command(
            LocalEnvironmentSpec(),
            ["/missing-artisan-test-executable"],
            stream_output=streaming,
        )


def test_wrapped_command_reaches_owner_with_prepared_environment():
    command = MagicMock(returncode=0, launch_seconds=0.01)
    command.communicate.return_value = ("ok", "")
    with patch(
        "artisan.utils.external_tools.ManagedCommand", return_value=command
    ) as launch:
        run_command(
            DockerEnvironmentSpec(image="example:1", env={"FOO": "bar"}), ["tool"]
        )
    assert launch.call_args.args[1][:3] == ["docker", "run", "--rm"]
    assert "example:1" in launch.call_args.args[1]
    assert launch.call_args.kwargs["external"] is True
    command.dispose.assert_called_once()


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize(
    "outcome", ["succeeded", "failed", "launch_failed", "interrupted"]
)
def test_owner_launch_duration_and_single_final_outcome(streaming, outcome, tmp_path):
    from artisan.execution.recording.commands import capture_commands

    command = MagicMock(
        returncode=4 if outcome == "failed" else 0, launch_seconds=0.123
    )
    if outcome == "interrupted":
        command.communicate.side_effect = KeyboardInterrupt
    else:
        command.communicate.return_value = ("output", "")
    exception = {
        "failed": ExternalToolError,
        "launch_failed": FileNotFoundError,
        "interrupted": KeyboardInterrupt,
    }.get(outcome)
    with (
        capture_commands() as recorder,
        patch(
            "artisan.utils.external_tools.ManagedCommand", return_value=command
        ) as launch,
    ):
        if outcome == "launch_failed":
            launch.side_effect = FileNotFoundError("missing executable")
        if exception:
            with pytest.raises(exception):
                run_command(LocalEnvironmentSpec(), ["tool"], stream_output=streaming)
        else:
            run_command(LocalEnvironmentSpec(), ["tool"], stream_output=streaming)
    records = recorder.snapshot().commands
    assert len(records) == 1
    assert records[0].outcome == outcome
    assert records[0].launch_seconds == (
        None if outcome == "launch_failed" else pytest.approx(0.123)
    )
    assert command.interrupt.call_count == (outcome == "interrupted")
    assert command.dispose.call_count == (outcome != "launch_failed")


@pytest.mark.parametrize("streaming", [False, True])
def test_primary_interruption_preserved_when_cleanup_is_unknown(streaming):
    interruption = KeyboardInterrupt("cancelled")
    command = MagicMock(returncode=None, launch_seconds=0.1)
    command.communicate.side_effect = interruption
    command.interrupt.side_effect = lambda error: error.add_note("cleanup unconfirmed")
    with (
        patch("artisan.utils.external_tools.ManagedCommand", return_value=command),
        pytest.raises(KeyboardInterrupt) as caught,
    ):
        run_command(LocalEnvironmentSpec(), ["tool"], stream_output=streaming)
    assert caught.value is interruption
    assert "cleanup unconfirmed" in interruption.__notes__
    command.dispose.assert_called_once()
