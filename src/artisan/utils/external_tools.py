"""External tool execution utilities.

Runtime-agnostic helpers for building and running CLI commands from within
pipeline operations.

Key exports: :func:`format_args`, :func:`run_command`.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import signal
import subprocess
import time
from dataclasses import dataclass
from typing import Any

_tool_logger = logging.getLogger("artisan.tools")


# =============================================================================
# COMMAND DATACLASS
# =============================================================================


@dataclass
class Command:
    """Command in both list and string formats.

    Attributes:
        parts: List of command parts for subprocess.run(..., shell=False).
        string: Shell-quoted string for logging/display.
    """

    parts: list[str]
    string: str

    def __str__(self) -> str:
        return self.string


# =============================================================================
# EXCEPTION
# =============================================================================


@dataclass
class ExternalToolError(Exception):
    """Structured error for external tool failures.

    Raised when an external tool exits with non-zero status or times out.
    Contains all context needed to diagnose the failure.

    Attributes:
        message: Human-readable error description.
        command: The command that was executed (as list of parts).
        return_code: Exit code (-1 for timeout).
        stdout: Captured standard output.
        stderr: Captured standard error.
        runtime: The EnvironmentSpec or context that was executed.
    """

    message: str
    command: list[str]
    return_code: int
    stdout: str
    stderr: str
    runtime: Any

    def __str__(self) -> str:
        parts = [f"{self.message} (exit code {self.return_code})"]
        parts.append(f"Command: {' '.join(self.command)}")
        if self.stderr:
            tail = "\n".join(self.stderr.splitlines()[-20:])
            parts.append(f"--- stderr (last 20 lines) ---\n{tail}")
        if self.stdout:
            tail = "\n".join(self.stdout.splitlines()[-20:])
            parts.append(f"--- stdout (last 20 lines) ---\n{tail}")
        return "\n".join(parts)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def to_cli_value(value: Any) -> str:
    """Convert a Python value to its CLI string representation.

    Args:
        value: The value to convert. Supports None, bool, Path, list,
            tuple, dict, and any stringifiable type.

    Returns:
        String representation suitable for command-line usage.

    Examples:
        >>> to_cli_value(None)
        ''
        >>> to_cli_value(True)
        'true'
        >>> to_cli_value(False)
        'false'
        >>> to_cli_value([1, 2, 3])
        '[1, 2, 3]'
        >>> to_cli_value({"key": "value"})
        '{"key": "value"}'
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list | tuple | dict):
        return json.dumps(value)
    return str(value)


def format_args(params: dict[str, Any]) -> list[str]:
    """Format parameters as CLI arguments.

    Produces ``--key value`` format with booleans as flags
    (``--verbose`` for True, omitted for False).

    Args:
        params: Key-value pairs to format.

    Returns:
        List of formatted argument strings.

    Examples:
        >>> format_args({"batch-size": 16, "verbose": True})
        ['--batch-size', '16', '--verbose']
    """
    result: list[str] = []
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, bool):
            if value:
                result.append(f"--{key}")
            continue
        result.extend([f"--{key}", to_cli_value(value)])
    return result


# =============================================================================
# PROCESS CLEANUP
# =============================================================================


def _kill_process_group(process: subprocess.Popen, timeout: float = 3.0) -> None:
    """Kill a subprocess and its entire process group.

    Sends SIGTERM first for graceful shutdown, then escalates to SIGKILL
    if the process doesn't exit within the timeout.

    Args:
        process: Subprocess to kill (must have been started with process_group=0).
        timeout: Seconds to wait after SIGTERM before escalating to SIGKILL.
    """
    try:
        pgid = os.getpgid(process.pid)
        os.killpg(pgid, signal.SIGTERM)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(pgid, signal.SIGKILL)
            process.wait()
    except ProcessLookupError:
        pass


# =============================================================================
# COMMAND EXECUTION
# =============================================================================


def run_command(
    environment: Any,
    cmd: list[str],
    cwd: str | None = None,
    timeout: float | None = None,
    stream_output: bool = False,
    log_path: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Execute a command in the given environment.

    The operation builds the command; the environment wraps it. Works with
    the EnvironmentSpec hierarchy.

    Args:
        environment: An EnvironmentSpec instance that wraps the command.
        cmd: Pre-built command list (e.g. from ToolSpec.parts() + args).
        cwd: Working directory for subprocess.
        timeout: Timeout in seconds.
        stream_output: If True, print output lines in real-time.
        log_path: If provided, write output to this file.

    Returns:
        CompletedProcess with captured stdout/stderr.

    Raises:
        ExternalToolError: On non-zero exit or timeout.
    """
    wrapped = environment.wrap_command(cmd, cwd)
    full_cmd = Command(parts=wrapped, string=shlex.join(wrapped))
    env = environment.prepare_env()

    try:
        if stream_output:
            result = _run_with_streaming(full_cmd, cwd, timeout, log_path, env)
        else:
            result = _run_captured(full_cmd, cwd, timeout, env)
    except subprocess.TimeoutExpired as e:
        raise ExternalToolError(
            message=f"Command timed out after {timeout}s",
            command=full_cmd.parts,
            return_code=-1,
            stdout=e.stdout or "",
            stderr=e.stderr or "",
            runtime=environment,
        ) from e

    if result.returncode != 0:
        raise ExternalToolError(
            message=f"Command failed with exit code {result.returncode}",
            command=full_cmd.parts,
            return_code=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            runtime=environment,
        )
    return result


def _run_with_streaming(
    cmd: Command,
    cwd: str | None,
    timeout: float | None,
    log_path: str | None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run command with real-time output streaming.

    Args:
        cmd: Command to execute.
        cwd: Working directory.
        timeout: Approximate timeout (checked between lines, not during).
        log_path: Optional file to write output.
        env: Environment variables.

    Returns:
        CompletedProcess with accumulated stdout.
    """
    from contextlib import nullcontext

    log_context = open(log_path, "w") if log_path else nullcontext()

    with log_context as log_file:
        process = subprocess.Popen(
            cmd.parts,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
            process_group=0,
        )

        stdout_lines: list[str] = []
        start_time = time.monotonic()

        try:
            for line in process.stdout:
                if timeout and (time.monotonic() - start_time) > timeout:
                    _kill_process_group(process)
                    raise subprocess.TimeoutExpired(cmd.parts, timeout)

                if log_file:
                    log_file.write(line)
                    log_file.flush()

                _tool_logger.debug("%s", line.rstrip("\n"))

                stdout_lines.append(line)

            returncode = process.wait()
        except BaseException:
            _kill_process_group(process)
            raise

        return subprocess.CompletedProcess(
            args=cmd.parts,
            returncode=returncode,
            stdout="".join(stdout_lines),
            stderr="",
        )


def _run_captured(
    cmd: Command,
    cwd: str | None,
    timeout: float | None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run command with captured output and process group cleanup.

    Args:
        cmd: Command to execute.
        cwd: Working directory.
        timeout: Timeout in seconds.
        env: Environment variables.

    Returns:
        CompletedProcess with captured stdout and stderr.
    """
    process = subprocess.Popen(
        cmd.parts,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        process_group=0,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except BaseException:
        _kill_process_group(process)
        raise
    return subprocess.CompletedProcess(
        args=cmd.parts,
        returncode=process.returncode,
        stdout=stdout,
        stderr=stderr,
    )
