"""Strict, bounded diagnostic evidence for observed subprocess attempts."""

from __future__ import annotations

import math
from collections.abc import Iterator
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_core import to_json

MAX_COMMAND_RECORDING_BYTES = 1024 * 1024
NonnegativeInt = Annotated[int, Field(strict=True, ge=0)]
MissingReason = Literal[
    "transport_failure", "cancelled", "missing_recording", "invalid_recording"
]


def _string_chunks(value: str) -> Iterator[bytes]:
    """Encode bounded pieces so a single untrusted string cannot bypass the cap."""
    yield b'"'
    for start in range(0, len(value), 4096):
        yield to_json(value[start : start + 4096])[1:-1]
    yield b'"'


def _compact_chunks(value: Any) -> Iterator[bytes]:
    """Use the model serializer's scalar encoding without buffering the payload."""
    if isinstance(value, dict):
        yield b"{"
        for index, (key, item) in enumerate(value.items()):
            if not isinstance(key, str):
                msg = "command recording object keys must be strings"
                raise ValueError(msg)
            if index:
                yield b","
            yield from _string_chunks(key)
            yield b":"
            yield from _compact_chunks(item)
        yield b"}"
    elif isinstance(value, list):
        yield b"["
        for index, item in enumerate(value):
            if index:
                yield b","
            yield from _compact_chunks(item)
        yield b"]"
    elif isinstance(value, str):
        yield from _string_chunks(value)
    elif value is None or isinstance(value, (bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            msg = "command recording numbers must be finite"
            raise ValueError(msg)
        yield to_json(value)
    else:
        msg = "command recording must contain JSON values"
        raise ValueError(msg)


def check_recording_size(
    value: Any, *, limit: int = MAX_COMMAND_RECORDING_BYTES
) -> None:
    """Reject oversized compact JSON before constructing nested wire models."""
    size = 0
    for chunk in _compact_chunks(value):
        size += len(chunk)
        if size > limit:
            msg = "command recording exceeds its byte limit"
            raise ValueError(msg)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class CommandTool(_StrictModel):
    """Declared tool identity, without probing the executable."""

    executable: str
    interpreter: str | None
    subcommand: str | None


class CommandEnvironment(_StrictModel):
    """Environment identity and explicitly supplied variable names only."""

    type: str
    identity: dict[
        Literal["venv_path", "image", "pixi_environment", "manifest_path"], str
    ]
    variable_names: list[str]


class CommandRecord(_StrictModel):
    """One observed launch boundary, with redacted diagnostic strings."""

    invocation: NonnegativeInt
    sequence: NonnegativeInt
    location: Literal["local", "endpoint"]
    requested_argv: list[str]
    argv: list[str]
    cwd: str
    tool: CommandTool | None
    environment: CommandEnvironment
    outcome: Literal["succeeded", "failed", "launch_failed", "interrupted"]
    returncode: Annotated[int, Field(strict=True)] | None
    redacted_fields: list[str]
    required_environment: list[str]
    launch_seconds: (
        Annotated[float, Field(strict=True, ge=0, allow_inf_nan=False)] | None
    )

    @model_validator(mode="after")
    def _consistent_outcome(self) -> Self:
        valid = (
            self.outcome == "interrupted"
            or (self.outcome == "succeeded" and self.returncode == 0)
            or (self.outcome == "failed" and self.returncode not in (None, 0))
            or (self.outcome == "launch_failed" and self.returncode is None)
        )
        if not valid:
            msg = "command outcome and returncode disagree"
            raise ValueError(msg)
        return self


class MissingInvocation(_StrictModel):
    """A dispatched endpoint invocation whose evidence could not be recovered."""

    invocation: NonnegativeInt
    reason: MissingReason


class CommandRecording(_StrictModel):
    """Canonical execution evidence; every field must be supplied explicitly."""

    status: Literal["complete", "partial", "unavailable"]
    commands: list[CommandRecord]
    missing_invocations: list[MissingInvocation]
    omitted_commands: NonnegativeInt
    omitted_missing_invocations: NonnegativeInt
    unavailable_reason: Literal["worker_evidence_unavailable"] | None

    @classmethod
    def empty(cls) -> Self:
        """Construct explicit complete evidence for an observed command-free flow."""
        return cls(
            status="complete",
            commands=[],
            missing_invocations=[],
            omitted_commands=0,
            omitted_missing_invocations=0,
            unavailable_reason=None,
        )

    @classmethod
    def unavailable(cls) -> Self:
        """Construct explicit absence of worker evidence for synthetic failures."""
        return cls(
            status="unavailable",
            commands=[],
            missing_invocations=[],
            omitted_commands=0,
            omitted_missing_invocations=0,
            unavailable_reason="worker_evidence_unavailable",
        )

    @model_validator(mode="after")
    def _canonical(self) -> Self:
        keys = [(c.invocation, c.sequence) for c in self.commands]
        missing = [m.invocation for m in self.missing_invocations]
        if keys != sorted(set(keys)) or missing != sorted(set(missing)):
            msg = "command evidence keys must be unique and sorted"
            raise ValueError(msg)
        omitted = self.omitted_commands or self.omitted_missing_invocations
        if self.unavailable_reason is not None:
            if self.commands or self.missing_invocations or omitted:
                msg = "unit-level unavailable evidence must be empty"
                raise ValueError(msg)
            expected = "unavailable"
        elif omitted:
            expected = "partial"
        elif missing:
            expected = "partial" if self.commands else "unavailable"
        else:
            expected = "complete"
        if self.status != expected:
            msg = "command recording status disagrees with evidence"
            raise ValueError(msg)
        check_recording_size(self.model_dump(mode="json"))
        return self
