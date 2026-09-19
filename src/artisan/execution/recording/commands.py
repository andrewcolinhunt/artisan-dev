"""Scoped subprocess observation, credential redaction, and bounded evidence."""

from __future__ import annotations

import json
import os
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, cast
from urllib.parse import urlsplit

from pydantic_core import to_json

from artisan.schemas.execution.command_record import (
    MAX_COMMAND_RECORDING_BYTES,
    CommandRecord,
    CommandRecording,
    MissingInvocation,
    MissingReason,
)
from artisan.schemas.operation_config.endpoint_policy import safe_uri_display
from artisan.utils.env_file import env_or_dotenv
from artisan.utils.external_tools import CommandAttempt, command_observer, redact_values

_CREDENTIAL = re.compile(r"TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|AUTH|API_KEY", re.I)
_FLAGS = {
    "--token",
    "--secret",
    "--password",
    "--passwd",
    "--api-key",
    "--api_key",
    "--credential",
    "--credentials",
    "--authorization",
}
_URI = re.compile(r"(?<![a-zA-Z0-9+.-])[a-zA-Z][a-zA-Z0-9+.-]*://[^\s\"'<>]+")
_current: ContextVar[CommandRecorder | None] = ContextVar(
    "command_recorder", default=None
)
_invocation: ContextVar[int | None] = ContextVar("command_invocation", default=None)
_tool: ContextVar[Any] = ContextVar("command_tool", default=None)


def credential_name(name: str) -> bool:
    """Recognize the documented credential variable/key names."""
    return (
        bool(_CREDENTIAL.search(name.replace("-", "_")))
        or name.upper() == "AWS_ACCESS_KEY_ID"
    )


class CommandRecorder:
    """Retain only a redacted logical prefix, independent of completion order."""

    def __init__(self, operation: Any = None, *, location: str = "local") -> None:
        self.location = location
        self._lock = threading.RLock()
        self._next_invocation = 0
        self._sequences: dict[int, int] = {}
        self._commands: dict[tuple[int, int], CommandRecord] = {}
        self._missing: dict[int, MissingInvocation] = {}
        self._cutoff: tuple[int, float] | None = None
        self._entry_bytes = 0
        self._omitted_commands = 0
        self._omitted_missing = 0
        self._values: set[str] = set()
        self._environment: dict[str, str] = {}
        self.add_environment(
            {k: v for k, v in os.environ.items() if credential_name(k)}
        )
        self.add_operation(operation)

    def add_environment(self, values: dict[str, str]) -> None:
        """Remember known credential values for diagnostic replacement only."""
        with self._lock:
            for name, value in values.items():
                if value:
                    self._values.add(value)
                    self._environment[name] = value

    def add_sensitive_data(self, value: Any) -> None:
        """Register decoded replacement leaves without creating environment hints."""
        if isinstance(value, dict):
            for item in value.values():
                self.add_sensitive_data(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                self.add_sensitive_data(item)
        elif value is not None:
            with self._lock:
                self._values.add(value if isinstance(value, str) else json.dumps(value))

    @property
    def sensitive_values(self) -> tuple[str, ...]:
        """Return a transient copy for propagation to this execution's worker."""
        with self._lock:
            return tuple(self._values)

    def add_operation(self, operation: Any) -> None:
        """Register explicit selected environment and endpoint credentials."""
        if operation is None:
            return
        environments = getattr(operation, "environments", None)
        environment = getattr(environments, getattr(environments, "active", ""), None)
        self.add_environment(getattr(environment, "env", {}))
        provider = getattr(operation, "compute_provider", None)
        selected = (
            "modal" if self.location == "endpoint" else getattr(provider, "active", "")
        )
        config = getattr(provider, selected, None)
        self.add_environment(getattr(config, "env", {}))
        if not hasattr(config, "auth_secret"):
            return
        prefix = getattr(config, "auth_secret", None) or "MODAL_PROXY"
        self.add_environment(
            {
                name: value
                for suffix in ("TOKEN_ID", "TOKEN_SECRET")
                if (value := env_or_dotenv(name := f"{prefix}_{suffix}"))
            }
        )

    def reserve(self, count: int = 1) -> list[int]:
        """Allocate invocation slots in dispatch order."""
        with self._lock:
            first = self._next_invocation
            self._next_invocation += count
            return list(range(first, first + count))

    def sanitize(self, text: str) -> str:
        """Remove URI capabilities and known values from a diagnostic string."""

        def safe(match: re.Match[str]) -> str:
            uri = match.group()
            try:
                parts = urlsplit(uri)
            except ValueError:
                return "<redacted>"
            if parts.username or parts.query or parts.fragment:
                return (
                    safe_uri_display(uri)
                    if match.span() == (0, len(text))
                    else "<redacted>"
                )
            return uri

        if "://" in text:
            text = _URI.sub(safe, text)
        with self._lock:
            return redact_values(text, tuple(self._values))

    def sanitize_data(self, value: Any) -> Any:
        """Sanitize string leaves without changing structured diagnostic shape."""
        if isinstance(value, str):
            return self.sanitize(value)
        if isinstance(value, (bool, int, float)):
            with self._lock:
                if json.dumps(value) in self._values:
                    return "<redacted>"
        if isinstance(value, list):
            return [self.sanitize_data(item) for item in value]
        if isinstance(value, dict):
            return {key: self.sanitize_data(item) for key, item in value.items()}
        return value

    def _argument(self, value: str) -> str:
        prefix, separator, tail = value.partition("=")
        if value.lstrip().startswith(("{", "[")):
            prefix, separator, tail = "", "", value
        candidate = tail if separator else value
        if _URI.fullmatch(candidate):
            return self.sanitize(prefix + separator + safe_uri_display(candidate))
        try:
            parsed = json.loads(candidate)
        except (ValueError, TypeError):
            return self.sanitize(value)
        if isinstance(parsed, (dict, list)):

            def redact_json(item: Any) -> Any:
                if isinstance(item, dict):
                    return {
                        self.sanitize(key): "<redacted>"
                        if credential_name(key)
                        else redact_json(val)
                        for key, val in item.items()
                    }
                if isinstance(item, list):
                    return [redact_json(val) for val in item]
                return self.sanitize_data(item)

            sanitized = redact_json(parsed)
            if sanitized == parsed:
                return self.sanitize(value)
            clean = json.dumps(sanitized, ensure_ascii=False, separators=(",", ":"))
            return (prefix + separator if separator else "") + clean
        return self.sanitize(value)

    def _argv(self, argv: list[str]) -> list[str]:
        clean = []
        secret_next = False
        for arg in argv:
            flag, separator, _ = arg.partition("=")
            if secret_next:
                clean.append("<redacted>")
            elif separator and flag.lower() in _FLAGS:
                clean.append(self.sanitize(flag) + "=<redacted>")
            else:
                clean.append(self._argument(arg))
            secret_next = not separator and arg.lower() in _FLAGS
        return clean

    def _learn_arguments(self, argv: list[str]) -> None:
        secret_next = False
        for arg in argv:
            flag, separator, value = arg.partition("=")
            if secret_next and arg:
                self._values.add(arg)
            elif separator and flag.lower() in _FLAGS and value:
                self._values.add(value)
            secret_next = not separator and arg.lower() in _FLAGS
            candidate = (
                arg
                if arg.lstrip().startswith(("{", "["))
                else value
                if separator
                else arg
            )
            try:
                parsed = json.loads(candidate)
            except (ValueError, TypeError):
                continue
            self._learn_json(parsed)

    def _learn_json(self, item: Any, *, credential: bool = False) -> None:
        if isinstance(item, dict):
            for name, value in item.items():
                self._learn_json(value, credential=credential or credential_name(name))
        elif isinstance(item, list):
            for value in item:
                self._learn_json(value, credential=credential)
        elif credential and isinstance(item, str) and item:
            self._values.add(item)

    def prepare(
        self,
        environment: Any,
        requested: list[str],
        sensitive_values: tuple[str, ...],
    ) -> None:
        """Register values before wrapper preparation, without allocating evidence."""
        with self._lock:
            self.add_environment(getattr(environment, "env", {}))
            self._values.update(value for value in sensitive_values if value)
            self._learn_arguments(requested)

    def begin(
        self,
        environment: Any,
        requested: list[str],
        argv: list[str],
        cwd: str | None,
        sensitive_values: tuple[str, ...],
    ) -> CommandAttempt:
        """Reserve one attempt and immediately discard its raw diagnostic inputs."""
        with self._lock:
            self.prepare(environment, requested, sensitive_values)
            self._learn_arguments(argv)
            invocation = _invocation.get()
            if invocation is None:
                invocation = self.reserve()[0]
            sequence = self._sequences.get(invocation, 0)
            if _invocation.get() is not None:
                self._sequences[invocation] = sequence + 1
            tool = _tool.get()
            raw = {
                "requested_argv": requested,
                "argv": argv,
                "cwd": os.path.realpath(cwd or os.getcwd()),
                "tool": (
                    {
                        key: getattr(tool, key)
                        for key in ("executable", "interpreter", "subcommand")
                    }
                    if tool
                    else None
                ),
                "environment": {
                    "type": type(environment).__name__,
                    "identity": {
                        key: val
                        for key in (
                            "venv_path",
                            "image",
                            "pixi_environment",
                            "manifest_path",
                        )
                        if isinstance(val := getattr(environment, key, None), str)
                    },
                    "variable_names": sorted(getattr(environment, "env", {})),
                },
            }
            clean = self.sanitize_data(raw)
            clean["requested_argv"] = self._argv(requested)
            clean["argv"] = self._argv(argv)
            paths = _changed_paths(raw, clean)
            clean.update(
                invocation=invocation,
                sequence=sequence,
                location=self.location,
                outcome="interrupted",
                returncode=None,
                launch_seconds=None,
                redacted_fields=paths,
                required_environment=sorted(
                    {
                        self.sanitize(k)
                        for k, v in self._environment.items()
                        if _contains_value(raw, v)
                    }
                ),
            )
            record = CommandRecord.model_validate(clean)

        def finish(
            outcome: str, returncode: int | None, launch_seconds: float | None
        ) -> None:
            with self._lock:
                self._add(
                    CommandRecord.model_validate(
                        {
                            **record.model_dump(),
                            "outcome": outcome,
                            "returncode": returncode,
                            "launch_seconds": launch_seconds,
                        }
                    )
                )

        return CommandAttempt(
            command=clean["argv"], sanitize=self.sanitize, on_finish=finish
        )

    def _add(self, record: CommandRecord | MissingInvocation) -> None:
        is_command = isinstance(record, CommandRecord)
        key = (
            record.invocation,
            record.sequence if isinstance(record, CommandRecord) else float("inf"),
        )
        if self._cutoff is not None and key >= self._cutoff:
            self._omitted_commands += int(is_command)
            self._omitted_missing += int(not is_command)
        elif isinstance(record, CommandRecord):
            previous = self._commands.get((record.invocation, record.sequence))
            self._entry_bytes += len(record.model_dump_json().encode())
            if previous is not None:
                self._entry_bytes -= len(previous.model_dump_json().encode())
            self._commands[(record.invocation, record.sequence)] = record
        else:
            previous_missing = self._missing.get(record.invocation)
            self._entry_bytes += len(record.model_dump_json().encode())
            if previous_missing is not None:
                self._entry_bytes -= len(previous_missing.model_dump_json().encode())
            self._missing[record.invocation] = record
        self._trim()

    def _metadata(self) -> dict[str, Any]:
        status = "complete"
        if self._omitted_commands or self._omitted_missing:
            status = "partial"
        elif self._missing:
            status = "partial" if self._commands else "unavailable"
        return {
            "status": status,
            "commands": [],
            "missing_invocations": [],
            "omitted_commands": self._omitted_commands,
            "omitted_missing_invocations": self._omitted_missing,
            "unavailable_reason": None,
        }

    def _data(self) -> dict[str, Any]:
        return {
            **self._metadata(),
            "commands": [
                c.model_dump(mode="json") for _, c in sorted(self._commands.items())
            ],
            "missing_invocations": [
                m.model_dump(mode="json") for _, m in sorted(self._missing.items())
            ],
        }

    def _drop_suffix(self, cutoff: tuple[int, float]) -> None:
        self._cutoff = min(self._cutoff, cutoff) if self._cutoff is not None else cutoff
        for key in list(self._commands):
            if key >= self._cutoff:
                self._entry_bytes -= len(
                    self._commands.pop(key).model_dump_json().encode()
                )
                self._omitted_commands += 1
        for invocation in list(self._missing):
            if (invocation, float("inf")) >= self._cutoff:
                self._entry_bytes -= len(
                    self._missing.pop(invocation).model_dump_json().encode()
                )
                self._omitted_missing += 1

    def _trim(self) -> None:
        while True:
            size = (
                len(to_json(self._metadata()))
                + self._entry_bytes
                + max(0, len(self._commands) - 1)
                + max(0, len(self._missing) - 1)
            )
            if size <= MAX_COMMAND_RECORDING_BYTES:
                return
            last = max([*self._commands, *((i, float("inf")) for i in self._missing)])
            self._drop_suffix(last)

    def missing(self, invocation: int, reason: MissingReason) -> None:
        """Mark a dispatched invocation with no trustworthy returned recording."""
        with self._lock:
            self._add(MissingInvocation(invocation=invocation, reason=reason))

    def merge(self, recording: CommandRecording, invocation: int) -> None:
        """Redact and remap worker evidence before the client handles its result."""
        with self._lock:
            if recording.unavailable_reason is not None:
                msg = "endpoint must supply observed worker evidence"
                raise ValueError(msg)
            remote_invocations = {c.invocation for c in recording.commands} | {
                m.invocation for m in recording.missing_invocations
            }
            if len(remote_invocations) > 1:
                msg = "endpoint request must use one invocation"
                raise ValueError(msg)
            for command in recording.commands:
                original = command.model_dump(mode="json")
                clean = dict(original)
                for name in (
                    "requested_argv",
                    "argv",
                    "cwd",
                    "tool",
                    "environment",
                    "redacted_fields",
                    "required_environment",
                ):
                    clean[name] = self.sanitize_data(original[name])
                clean["requested_argv"] = self._argv(original["requested_argv"])
                clean["argv"] = self._argv(original["argv"])
                redacted_paths = _changed_paths(original, clean)
                clean["required_environment"] = sorted(
                    set(clean["required_environment"])
                    | {
                        self.sanitize(name)
                        for name, value in self._environment.items()
                        if _contains_value(original, value)
                    }
                )
                clean["redacted_fields"] = sorted(
                    set(clean["redacted_fields"] + redacted_paths)
                )
                clean.update(invocation=invocation, location="endpoint")
                self._add(CommandRecord.model_validate(clean))
            for marker in recording.missing_invocations:
                self._add(
                    MissingInvocation(invocation=invocation, reason=marker.reason)
                )
            self._omitted_commands += recording.omitted_commands
            self._omitted_missing += recording.omitted_missing_invocations
            if recording.omitted_commands or recording.omitted_missing_invocations:
                last_sequence = (
                    recording.commands[-1].sequence if recording.commands else -1
                )
                cutoff = (
                    (invocation, last_sequence + 1)
                    if recording.omitted_commands
                    else (invocation, float("inf"))
                )
                self._drop_suffix(cutoff)
            self._trim()

    def snapshot(self) -> CommandRecording:
        """Return a validated compact recording, including explicit omissions."""
        with self._lock:
            self._trim()
            return CommandRecording.model_validate(self._data())


def _contains_value(value: Any, needle: str) -> bool:
    if isinstance(value, str):
        return needle in value
    if isinstance(value, list):
        return any(_contains_value(item, needle) for item in value)
    if isinstance(value, dict):
        return any(_contains_value(item, needle) for item in value.values())
    return False


def _changed_paths(original: Any, clean: Any, path: str = "$") -> list[str]:
    if isinstance(original, dict):
        return [
            p
            for key in original
            for p in _changed_paths(original[key], clean.get(key), f"{path}.{key}")
        ]
    if isinstance(original, list) and isinstance(clean, list):
        return [
            p
            for i, value in enumerate(original)
            for p in _changed_paths(value, clean[i], f"{path}[{i}]")
        ]
    return [path] if original != clean else []


def current_recorder() -> CommandRecorder | None:
    """Return the observer owned by the current execution or endpoint request."""
    return _current.get()


def command_snapshot() -> CommandRecording:
    """Read evidence inside a framework-owned capture scope."""
    recorder = current_recorder()
    if recorder is None:
        msg = "command evidence requires an active capture scope"
        raise RuntimeError(msg)
    return recorder.snapshot()


def sanitize_diagnostic[Diagnostic](value: Diagnostic) -> Diagnostic:
    """Sanitize a diagnostic copy with the current execution's known values."""
    recorder = current_recorder()
    return (
        cast(Diagnostic, recorder.sanitize_data(value))
        if recorder is not None
        else value
    )


@contextmanager
def capture_commands(
    operation: Any = None, *, location: str = "local"
) -> Iterator[CommandRecorder]:
    """Install a fresh execution collector and always reset all context tokens."""
    recorder = CommandRecorder(operation, location=location)
    tokens = (
        _current.set(recorder),
        command_observer.set(recorder),
        _invocation.set(None),
        _tool.set(None),
    )
    try:
        yield recorder
    finally:
        _tool.reset(tokens[3])
        _invocation.reset(tokens[2])
        command_observer.reset(tokens[1])
        _current.reset(tokens[0])


@contextmanager
def invocation_scope(
    operation: Any = None, invocation: int | None = None
) -> Iterator[int | None]:
    """Associate helper calls with one invocation and its declared tool."""
    recorder = current_recorder()
    if recorder is not None:
        recorder.add_operation(operation)
        if invocation is None:
            invocation = _invocation.get()
        if invocation is None:
            invocation = recorder.reserve()[0]
    previous_invocation = _invocation.get()
    token = _invocation.set(invocation)
    tool_token = _tool.set(
        getattr(operation, "tool", None) if operation is not None else _tool.get()
    )
    try:
        yield invocation
    finally:
        _tool.reset(tool_token)
        _invocation.reset(token)
        if (
            recorder is not None
            and invocation is not None
            and invocation != previous_invocation
        ):
            with recorder._lock:
                recorder._sequences.pop(invocation, None)


def reserve_invocations(count: int) -> list[int | None]:
    """Reserve dispatch slots when a framework capture scope is installed."""
    recorder = current_recorder()
    if recorder is None:
        return [None] * count
    return list(recorder.reserve(count))
