"""Verify the runtime and payload before loading a trusted live worker call."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, field_validator

from artisan.execution.application_bootstrap import (
    bootstrap_worker_applications,
    validate_worker_applications,
    worker_application_scope,
)
from artisan.execution.runtime_compatibility import verify_runtime_compatibility
from artisan.schemas.execution.worker_transport import (
    RuntimeCompatibility,
    WorkerTransportHeader,
)
from artisan.utils.application_modules import validate_application_modules

_ENVELOPE_PREFIX = b"artisan.worker-call.v1\0"


class _CallBootstrap(BaseModel):
    """Inert caller selection covered by the complete live payload hash."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    application_modules: tuple[str, ...]

    @field_validator("application_modules", mode="before")
    @classmethod
    def _modules(cls, value: object) -> tuple[str, ...]:
        return validate_application_modules(value)


@dataclass(frozen=True)
class PreparedProcessCall[ResultT]:
    """A trusted live call paired with inert transport qualification evidence."""

    header: WorkerTransportHeader
    payload: bytes


def serialize_worker_call[ResultT](
    function: Callable[..., ResultT],
    *,
    compatibility: RuntimeCompatibility,
    args: tuple[Any, ...] = (),
    kwargs: Mapping[str, Any] | None = None,
    application_modules: tuple[str, ...] = (),
) -> PreparedProcessCall[ResultT]:
    """Verify controller compatibility, then capture its authorized live call."""
    verify_runtime_compatibility(compatibility)
    bootstrap = _CallBootstrap(application_modules=application_modules)
    validate_worker_applications(
        bootstrap.application_modules,
        qualified_modules=bootstrap.application_modules,
    )
    import cloudpickle  # type: ignore[import-untyped]

    encoded = bootstrap.model_dump_json().encode()
    payload = (
        _ENVELOPE_PREFIX
        + len(encoded).to_bytes(4, "big")
        + encoded
        + cloudpickle.dumps((function, args, dict(kwargs or {})))
    )
    return PreparedProcessCall(
        WorkerTransportHeader(
            compatibility=compatibility,
            payload_sha256=hashlib.sha256(payload).hexdigest(),
        ),
        payload,
    )


def execute_worker_call[ResultT](
    call: PreparedProcessCall[ResultT],
) -> ResultT:
    """Check runtime metadata and payload bytes before unpickling.

    Software identity is selected during admission. Durable replay data never
    enters this live-object transport path without controller admission.
    """
    from artisan.utils.command_supervisor import validate_worker_attachment

    validate_worker_attachment()
    if hashlib.sha256(call.payload).hexdigest() != call.header.payload_sha256:
        msg = "Submitted worker payload does not match its transport header"
        raise ValueError(msg)
    verify_runtime_compatibility(call.header.compatibility)
    bootstrap, payload = _decode_envelope(call.payload)
    bootstrap_worker_applications(
        bootstrap.application_modules,
        qualified_modules=bootstrap.application_modules,
        compatibility=call.header.compatibility,
    )
    import cloudpickle

    with worker_application_scope(bootstrap.application_modules):
        try:
            function, args, kwargs = cast(
                tuple[Callable[..., ResultT], tuple[Any, ...], dict[str, Any]],
                cloudpickle.loads(payload),
            )
        except Exception as error:
            msg = "Worker live-call deserialization failed"
            raise RuntimeError(msg) from error
        return function(*args, **kwargs)


def _decode_envelope(payload: bytes) -> tuple[_CallBootstrap, bytes]:
    start = len(_ENVELOPE_PREFIX) + 4
    if not payload.startswith(_ENVELOPE_PREFIX) or len(payload) < start:
        msg = "Invalid live worker-call envelope"
        raise ValueError(msg)
    end = start + int.from_bytes(payload[start - 4 : start], "big")
    if end >= len(payload):
        msg = "Truncated live worker-call envelope"
        raise ValueError(msg)
    return _CallBootstrap.model_validate_json(payload[start:end]), payload[end:]
