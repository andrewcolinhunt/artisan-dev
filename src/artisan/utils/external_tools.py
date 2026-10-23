"""External tool execution utilities.

Runtime-agnostic helpers for building and running CLI commands from within
pipeline operations.

Key exports: :func:`format_args`, :func:`run_command`.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from artisan.errors import ArtisanError, ErrorCode
from artisan.utils.command_supervisor import (
    ManagedCommand,
    attachment_sensitive_values,
    command_attachment,
    scope_command_observer,
)
from artisan.utils.configuration_secrets import sensitive_environment


def redact_values(text: str, sensitive_values: tuple[str, ...]) -> str:
    """Replace known nonempty values in a diagnostic copy, longest first."""
    values = sorted(set(sensitive_values) - {""}, key=len, reverse=True)
    if not values:
        return text
    # Repeated persistence/transport boundaries must preserve prior markers.
    values = sorted({*values, "<redacted>"}, key=len, reverse=True)
    pattern = re.compile("|".join(re.escape(value) for value in values))
    clean = pattern.sub("<redacted>", text)
    # A replacement can compose another credential across a marker boundary.
    # Drop that diagnostic instead of repeatedly rescanning adversarial text.
    return "<redacted>" if pattern.sub("<redacted>", clean) != clean else clean


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
        observer = command_observer.get() or scope_command_observer()
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
    with command_attachment() as attachment:
        sensitive_values = (
            *sensitive_values,
            *attachment_sensitive_values(),
            attachment["secret"],
            *sensitive_environment(
                getattr(environment, "env", {}), getattr(environment, "public_env", ())
            ).values(),
        )
        observer = command_observer.get() or scope_command_observer()
        if observer is not None:
            observer.prepare(environment, cmd, sensitive_values)
        wrapped = environment.wrap_command(cmd, cwd)
        from artisan.schemas.operation_config.environment_spec import (
            DockerEnvironmentSpec,
        )

        docker_create = (
            environment._managed_container_command(wrapped)
            if type(environment) is DockerEnvironmentSpec
            else None
        )
        env = environment.prepare_env()
        observation = _CommandObservation(
            environment, cmd, wrapped, cwd, sensitive_values
        )
        result = _run_managed(
            attachment,
            wrapped,
            cwd,
            log_path,
            log_mode,
            dict(os.environ if env is None else env),
            observation,
            streaming=stream_output,
            external=getattr(environment, "cancellation_scope", "external")
            != "process_group",
            docker_create=docker_create,
        )
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


def _run_managed(
    attachment: dict[str, str],
    argv: list[str],
    cwd: str | None,
    log_path: str | None,
    log_mode: Literal["w", "a"],
    env: dict[str, str],
    observation: _CommandObservation,
    *,
    streaming: bool,
    external: bool,
    docker_create: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Drain worker-owned pipes while the dedicated owner retains command identity."""
    from contextlib import nullcontext

    log_context = open(log_path, log_mode) if streaming and log_path else nullcontext()  # noqa: SIM115
    with log_context as log_file:
        attempt = observation.begin()
        try:
            command = ManagedCommand(
                attachment,
                argv,
                env,
                cwd,
                merged=streaming,
                external=external,
                docker_create=docker_create,
            )
            attempt.launch_seconds = command.launch_seconds
        except BaseException:
            attempt.finish("launch_failed", None)
            raise

        def emit(line: str) -> None:
            if log_file is not None:
                log_file.write(line)
                log_file.flush()
            sys.stdout.write(line)
            sys.stdout.flush()

        try:
            stdout, stderr = command.communicate(emit if streaming else None)
            assert command.returncode is not None
            attempt.finish(
                "succeeded" if command.returncode == 0 else "failed", command.returncode
            )
        except BaseException as error:
            command.interrupt(error)
            attempt.finish("interrupted", command.returncode)
            raise
        finally:
            command.dispose()
    if log_path and not streaming:
        with open(log_path, log_mode) as handle:
            handle.write(stdout)
    return subprocess.CompletedProcess(argv, command.returncode, stdout, stderr)
