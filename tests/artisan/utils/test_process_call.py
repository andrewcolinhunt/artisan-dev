"""Tests for cloudpickle-backed process calls."""

from __future__ import annotations

from artisan.utils.process_call import execute_process_call, serialize_process_call


def test_process_call_round_trips_local_callable_and_payload() -> None:
    class LocalPayload:
        def __init__(self, value: int) -> None:
            self.value = value

    def _read_payload(payload: LocalPayload, increment: int) -> int:
        return payload.value + increment

    call = serialize_process_call(_read_payload, LocalPayload(40), increment=2)

    assert execute_process_call(call) == 42
