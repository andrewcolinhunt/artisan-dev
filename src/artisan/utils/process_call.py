"""Cloudpickle-backed calls for spawned worker processes."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

import cloudpickle  # type: ignore[import-untyped]


@dataclass(frozen=True)
class SerializedProcessCall[ResultT]:
    """A process call whose callable and arguments are cloudpickled."""

    payload: bytes


def serialize_process_call[**P, ResultT](
    function: Callable[P, ResultT],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> SerializedProcessCall[ResultT]:
    """Serialize a callable invocation for a spawned worker process."""
    return SerializedProcessCall(cloudpickle.dumps((function, args, kwargs)))


def execute_process_call[ResultT](call: SerializedProcessCall[ResultT]) -> ResultT:
    """Deserialize and execute a trusted worker-process call."""
    function, args, kwargs = cast(
        tuple[Callable[..., ResultT], tuple[Any, ...], dict[str, Any]],
        cloudpickle.loads(call.payload),
    )
    return function(*args, **kwargs)
