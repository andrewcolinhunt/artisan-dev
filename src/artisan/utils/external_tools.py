"""External tool execution utilities.

Runtime-agnostic helpers for building and running CLI commands from within
pipeline operations. Supports Apptainer, Docker, Pixi, and local execution
environments via the CommandSpec hierarchy.

Key exports: :class:`ArgStyle`, :func:`format_args`,
:func:`build_command_from_spec`, :func:`run_external_command`.
"""

from __future__ import annotations

import json
import os
import shlex
import signal
import subprocess
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from artisan.schemas.operation_config.command_spec import (
        ApptainerCommandSpec,
        CommandSpec,
        DockerCommandSpec,
        LocalCommandSpec,
        PixiCommandSpec,
    )


# =============================================================================
# ARGUMENT STYLE
# =============================================================================


class ArgStyle(Enum):
    """Argument formatting styles for external tools.

    Different tools expect different CLI argument formats:
    - HYDRA: key=value format (example_tool, etc.)
    - ARGPARSE: --key value format (standard Python argparse)

    Examples:
        HYDRA:    batch_size=16 ++sampler.steps=200
        ARGPARSE: --batch-size 16 --temperature 0.1 (keys used as-is)
    """

    HYDRA = "hydra"
    ARGPARSE = "argparse"


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
        runtime: The CommandSpec that was executed.
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
        >>> to_cli_value(Path("/tmp/out"))
        '/tmp/out'
        >>> to_cli_value([1, 2, 3])
        '[1, 2, 3]'
        >>> to_cli_value({"key": "value"})
        '{"key": "value"}'
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, list | tuple | dict):
        return json.dumps(value)
    return str(value)


def format_args(params: dict[str, Any], style: ArgStyle) -> list[str]:
    """Format parameters according to argument style.

    Args:
        params: Key-value pairs to format. For HYDRA style, keys are used
            as-is (include '++' prefix for Hydra overrides). For ARGPARSE
            style, keys are used as-is (include hyphens if the target CLI
            expects them).
        style: The argument formatting style to use.

    Returns:
        List of formatted argument strings.

    Examples:
        >>> format_args({"batch_size": 16}, ArgStyle.HYDRA)
        ['batch_size=16']
        >>> format_args({"++sampler.steps": 200}, ArgStyle.HYDRA)
        ['++sampler.steps=200']
        >>> format_args({"batch-size": 16}, ArgStyle.ARGPARSE)
        ['--batch-size', '16']
        >>> format_args({"verbose": None}, ArgStyle.ARGPARSE)
        []
    """
    result: list[str] = []
    for key, value in params.items():
        if value is None:
            continue
        cli_value = to_cli_value(value)
        if style == ArgStyle.HYDRA:
            result.append(f"{key}={cli_value}")
        else:  # ARGPARSE
            result.extend([f"--{key}", cli_value])
    return result


# =============================================================================
# COMMAND BUILDING
# =============================================================================


def build_command_from_spec(
    command: CommandSpec,
    args: list[str],
    cwd: Path | None = None,
) -> Command:
    """Build command for subprocess execution from a CommandSpec.

    Args:
        command: CommandSpec instance (ApptainerCommandSpec, DockerCommandSpec, etc.)
        args: Pre-formatted CLI arguments from format_args().
        cwd: Working directory. For container runtimes, the parent directory
            is automatically bind-mounted so sibling directories (e.g.
            materialized_inputs/) are accessible inside the container.

    Returns:
        Command with parts list and shell-quoted string.
    """
    from artisan.schemas.operation_config.command_spec import (
        ApptainerCommandSpec,
        DockerCommandSpec,
        LocalCommandSpec,
        PixiCommandSpec,
    )

    match command:
        case ApptainerCommandSpec():
            parts = _build_apptainer_from_spec(command, args, cwd)
        case DockerCommandSpec():
            parts = _build_docker_from_spec(command, args, cwd)
        case PixiCommandSpec():
            parts = _build_pixi_from_spec(command, args)
        case LocalCommandSpec():
            parts = _build_local_from_spec(command, args)
        case _:
            msg = f"Unknown CommandSpec type: {type(command).__name__}"
            raise TypeError(msg)

    return Command(parts=parts, string=shlex.join(parts))


def _build_apptainer_from_spec(
    command: ApptainerCommandSpec,
    args: list[str],
    cwd: Path | None = None,
) -> list[str]:
    """Build an Apptainer ``exec`` invocation with binds, env, and GPU flags."""
    parts: list[str] = ["apptainer", "exec"]
    if command.gpu:
        parts.append("--nv")
    # Auto-bind sandbox root so sibling dirs (materialized_inputs/) are accessible
    if cwd is not None:
        parts.extend(["--bind", f"{cwd.parent}:{cwd.parent}"])
    for host_path, container_path in command.binds:
        parts.extend(["--bind", f"{host_path}:{container_path}"])
    for key, value in command.env.items():
        parts.extend(["--env", f"{key}={value}"])
    parts.append(str(command.image))
    parts.extend(command.script_parts())
    if command.subcommand:
        parts.append(command.subcommand)
    parts.extend(args)
    return parts


def _build_docker_from_spec(
    command: DockerCommandSpec,
    args: list[str],
    cwd: Path | None = None,
) -> list[str]:
    """Build a Docker ``run --rm`` invocation with volumes, env, and GPU flags."""
    parts: list[str] = ["docker", "run", "--rm"]
    if command.gpu:
        parts.extend(["--gpus", "all"])
    # Auto-bind sandbox root so sibling dirs (materialized_inputs/) are accessible
    if cwd is not None:
        parts.extend(["--volume", f"{cwd.parent}:{cwd.parent}"])
    for host_path, container_path in command.binds:
        parts.extend(["--volume", f"{host_path}:{container_path}"])
    for key, value in command.env.items():
        parts.extend(["--env", f"{key}={value}"])
    parts.append(str(command.image))
    parts.extend(command.script_parts())
    if command.subcommand:
        parts.append(command.subcommand)
    parts.extend(args)
    return parts


def _build_pixi_from_spec(command: PixiCommandSpec, args: list[str]) -> list[str]:
    """Build a Pixi ``run -e`` invocation with optional manifest path."""
    parts: list[str] = ["pixi", "run", "-e", command.environment]
    if command.manifest_path:
        parts.extend(["--manifest-path", str(command.manifest_path)])
    parts.extend(command.script_parts())
    if command.subcommand:
        parts.append(command.subcommand)
    parts.extend(args)
    return parts


def _build_local_from_spec(command: LocalCommandSpec, args: list[str]) -> list[str]:
    """Build a bare local command with optional subcommand."""
    parts = command.script_parts()
    if command.subcommand:
        parts.append(command.subcommand)
    parts.extend(args)
    return parts


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


def run_external_command(
    command: CommandSpec,
    args: list[str],
    cwd: Path | None = None,
    timeout: float | None = None,
    stream_output: bool = False,
    log_path: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Execute external command from CommandSpec, raising ExternalToolError on failure.

    Args:
        command: CommandSpec instance describing the command.
        args: Formatted CLI arguments (from format_args()).
        cwd: Working directory for subprocess.
        timeout: Timeout in seconds.
        stream_output: If True, print output lines in real-time.
        log_path: If provided, write output to this file.

    Returns:
        CompletedProcess with captured stdout/stderr.

    Raises:
        ExternalToolError: On non-zero exit or timeout.
    """
    from artisan.schemas.operation_config.command_spec import LocalCommandSpec

    cmd = build_command_from_spec(command, args, cwd=cwd)

    # Prepare environment
    env = None
    if isinstance(command, LocalCommandSpec) and command.venv_path:
        import os

        env = os.environ.copy()
        venv_bin = command.venv_path / "bin"
        env["PATH"] = f"{venv_bin}:{env.get('PATH', '')}"
        env["VIRTUAL_ENV"] = str(command.venv_path)
        env.update(command.env)
    elif hasattr(command, "env") and command.env:
        import os

        env = os.environ.copy()
        env.update(command.env)

    try:
        if stream_output:
            result = _run_with_streaming(cmd, cwd, timeout, log_path, env)
        else:
            result = _run_captured(cmd, cwd, timeout, env)
    except subprocess.TimeoutExpired as e:
        raise ExternalToolError(
            message=f"Command timed out after {timeout}s",
            command=cmd.parts,
            return_code=-1,
            stdout=e.stdout or "",
            stderr=e.stderr or "",
            runtime=command,
        ) from e

    if result.returncode != 0:
        raise ExternalToolError(
            message=f"Command failed with exit code {result.returncode}",
            command=cmd.parts,
            return_code=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            runtime=command,
        )

    return result


def _run_with_streaming(
    cmd: Command,
    cwd: Path | None,
    timeout: float | None,
    log_path: Path | None,
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

    log_context = log_path.open("w") if log_path else nullcontext()

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

                print(line.rstrip("\n"))  # noqa: T201

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
    cwd: Path | None,
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
