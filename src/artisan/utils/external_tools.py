"""External tool execution utilities.

Runtime-agnostic helpers for building and running CLI commands from within
pipeline operations.

Key exports: :func:`format_args`, :func:`run_command`.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import signal
import subprocess
import sys
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Literal, Protocol

from artisan.errors import ArtisanError, ErrorCode


def redact_values(text: str, sensitive_values: tuple[str, ...]) -> str:
    """Replace known nonempty values in a diagnostic copy, longest first."""
    values = sorted(set(sensitive_values) - {""}, key=len, reverse=True)
    if not values:
        return text
    return re.sub("|".join(re.escape(value) for value in values), "<redacted>", text)


@dataclass
class CommandAttempt:
    """One subprocess boundary shared by command evidence and launch timing."""

    command: list[str]
    sanitize: Callable[[str], str]
    on_finish: Callable[[str, int | None, float | None], None] | None = None
    launch_seconds: float | None = None

    def finish(self, outcome: str, returncode: int | None) -> None:
        """Publish a final outcome before any post-exit output I/O."""
        if self.on_finish is not None:
            self.on_finish(outcome, returncode, self.launch_seconds)
            self.on_finish = None


class CommandObserver(Protocol):
    """Utility-owned interface; implementations may live in higher layers."""

    def prepare(
        self,
        environment: Any,
        requested: list[str],
        sensitive_values: tuple[str, ...],
    ) -> None:
        """Register diagnostic redactions without claiming a launch attempt."""
        ...

    def begin(
        self,
        environment: Any,
        requested: list[str],
        argv: list[str],
        cwd: str | None,
        sensitive_values: tuple[str, ...],
    ) -> CommandAttempt:
        """Observe immediately before attempting process creation."""
        ...


command_observer: ContextVar[CommandObserver | None] = ContextVar(
    "artisan_command_observer", default=None
)


@dataclass
class _CommandObservation:
    environment: Any
    requested: list[str]
    argv: list[str]
    cwd: str | None
    sensitive_values: tuple[str, ...]
    attempt: CommandAttempt | None = None

    def begin(self) -> CommandAttempt:
        observer = command_observer.get()
        if observer is not None:
            self.attempt = observer.begin(
                self.environment,
                self.requested,
                self.argv,
                self.cwd,
                self.sensitive_values,
            )
        else:

            def sanitize(text: str) -> str:
                return redact_values(text, self.sensitive_values)

            self.attempt = CommandAttempt(
                command=[sanitize(arg) for arg in self.argv],
                sanitize=sanitize,
            )
        return self.attempt


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


class ExternalToolError(ArtisanError):
    """Structured error for external tool failures.

    Raised when an external tool exits with non-zero status.
    Contains all context needed to diagnose the failure. Reuses
    ``OP_EXECUTE_FAILED`` (``error_type="compute"``) — a local tool crash
    and a remote one read the same code — and keeps the full multi-line
    composition in ``__str__`` while the envelope ``message`` stays a short
    one-line summary.

    Attributes:
        message: Human-readable error description.
        command: The command that was executed (as list of parts).
        return_code: Exit code from the subprocess.
        stdout: Captured standard output.
        stderr: Captured standard error.
        runtime: The EnvironmentSpec or context that was executed.
    """

    def __init__(
        self,
        message: str,
        *,
        command: list[str],
        return_code: int,
        stdout: str,
        stderr: str,
        runtime: Any,
    ) -> None:
        super().__init__(
            code=ErrorCode.OP_EXECUTE_FAILED,
            error_type="compute",
            message=message,
            recovery_hint="RETRY_LATER" if return_code == -1 else "REPORT_TO_USER",
        )
        self.message = message
        self.command = command
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr
        self.runtime = runtime

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


def _kill_process_group(process: subprocess.Popen[str], timeout: float = 3.0) -> None:
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
    stream_output: bool = False,
    log_path: str | None = None,
    log_mode: Literal["w", "a"] = "w",
    *,
    sensitive_values: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    """Execute a command in the given environment.

    The operation builds the command; the environment wraps it. Works with
    the EnvironmentSpec hierarchy.

    Args:
        environment: An EnvironmentSpec instance that wraps the command.
        cmd: Pre-built command list (e.g. from ToolSpec.parts() + args).
        cwd: Working directory for subprocess.
        stream_output: If True, print output lines in real-time.
        log_path: If provided, write output to this file.
        log_mode: Open mode for ``log_path`` — ``"w"`` truncates (default),
            ``"a"`` appends so sequential calls sharing one log accumulate.
        sensitive_values: Opaque values to remove from diagnostic copies only.

    Returns:
        CompletedProcess with captured stdout/stderr.

    Raises:
        ExternalToolError: On non-zero exit.
    """
    observer = command_observer.get()
    if observer is not None:
        observer.prepare(environment, cmd, sensitive_values)
    wrapped = environment.wrap_command(cmd, cwd)
    full_cmd = Command(parts=wrapped, string=shlex.join(wrapped))
    env = environment.prepare_env()

    observation = _CommandObservation(environment, cmd, wrapped, cwd, sensitive_values)
    if stream_output:
        result = _run_with_streaming(
            full_cmd, cwd, log_path, log_mode, env, observation
        )
    else:
        result = _run_captured(full_cmd, cwd, log_path, log_mode, env, observation)

    if result.returncode != 0:
        attempt = observation.attempt
        assert attempt is not None
        raise ExternalToolError(
            message=f"Command failed with exit code {result.returncode}",
            command=attempt.command,
            return_code=result.returncode,
            stdout=attempt.sanitize(result.stdout),
            stderr=attempt.sanitize(result.stderr),
            runtime=environment,
        )
    return result


def _run_with_streaming(
    cmd: Command,
    cwd: str | None,
    log_path: str | None,
    log_mode: Literal["w", "a"] = "w",
    env: dict[str, str] | None = None,
    observation: _CommandObservation | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run command with real-time output streaming.

    Each child stdout line is written to three sinks:

    - ``log_path`` (when set): the recoverable file. The Modal compute
      router ferries this back post-execute and the recorder reads it
      into the parquet ``tool_output`` column.
    - ``sys.stdout``: live emission. Visible on the operator's terminal
      locally, on Modal's dashboard remotely (the parent process's
      stdout *is* the container's stdout, which Modal captures), and
      in the Jupyter cell in notebooks. Note: child-side buffering is
      the child's concern — set ``PYTHONUNBUFFERED=1`` (or equivalent)
      on Python tools that block-buffer stdout when piped.
    - Accumulator: returned in ``CompletedProcess.stdout``.

    Args:
        cmd: Command to execute.
        cwd: Working directory.
        log_path: Optional file to write output.
        log_mode: Open mode for ``log_path`` (``"w"`` or ``"a"``).
        env: Environment variables.

    Returns:
        CompletedProcess with accumulated stdout.
    """
    from contextlib import nullcontext

    log_context = open(log_path, log_mode) if log_path else nullcontext()  # noqa: SIM115 — conditional; held via `with log_context` below

    with log_context as log_file:
        attempt = observation.begin() if observation else None
        try:
            launch_start = perf_counter()
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
            launch_seconds = perf_counter() - launch_start
            if attempt:
                attempt.launch_seconds = launch_seconds
        except BaseException:
            if attempt:
                attempt.finish("launch_failed", None)
            raise

        stdout_lines: list[str] = []

        # stdout=PIPE guarantees process.stdout is not None
        assert process.stdout is not None
        try:
            for line in process.stdout:
                if log_file:
                    log_file.write(line)
                    log_file.flush()

                sys.stdout.write(line)
                sys.stdout.flush()

                stdout_lines.append(line)

            returncode = process.wait()
            if attempt:
                attempt.finish("succeeded" if returncode == 0 else "failed", returncode)
        except BaseException:
            _kill_process_group(process)
            if attempt:
                attempt.finish("interrupted", process.returncode)
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
    log_path: str | None,
    log_mode: Literal["w", "a"] = "w",
    env: dict[str, str] | None = None,
    observation: _CommandObservation | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run command with captured output and process group cleanup.

    Parameter order mirrors :func:`_run_with_streaming` so
    :func:`run_command` can dispatch positionally to either path.

    When ``log_path`` is set, captured stdout is written to it after
    the process completes. Stderr stays on the returned
    ``CompletedProcess`` and surfaces via
    :attr:`ExternalToolError.stderr` on non-zero exit; it is not written
    to ``log_path`` (asymmetric with streaming mode, which merges via
    ``stderr=subprocess.STDOUT``).

    Args:
        cmd: Command to execute.
        cwd: Working directory.
        log_path: Optional file to write captured stdout.
        log_mode: Open mode for ``log_path`` (``"w"`` or ``"a"``).
        env: Environment variables.

    Returns:
        CompletedProcess with captured stdout and stderr.
    """
    attempt = observation.begin() if observation else None
    try:
        launch_start = perf_counter()
        process = subprocess.Popen(
            cmd.parts,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            process_group=0,
        )
        launch_seconds = perf_counter() - launch_start
        if attempt:
            attempt.launch_seconds = launch_seconds
    except BaseException:
        if attempt:
            attempt.finish("launch_failed", None)
        raise
    try:
        stdout, stderr = process.communicate()
        if attempt:
            attempt.finish(
                "succeeded" if process.returncode == 0 else "failed",
                process.returncode,
            )
    except BaseException:
        _kill_process_group(process)
        if attempt:
            attempt.finish("interrupted", process.returncode)
        raise

    if log_path:
        with open(log_path, log_mode) as f:
            f.write(stdout)

    return subprocess.CompletedProcess(
        args=cmd.parts,
        returncode=process.returncode,
        stdout=stdout,
        stderr=stderr,
    )
