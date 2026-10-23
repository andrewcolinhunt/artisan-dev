"""Local execute router — direct passthrough."""

from __future__ import annotations

from typing import Any

from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.compute.invoke import invoke_op_work
from artisan.execution.recording.commands import invocation_scope, reserve_invocations
from artisan.execution.recording.replay_snapshot import current_replay_builder
from artisan.schemas.specs.input_models import ExecuteInput


class LocalExecuteRouter(ExecuteRouter):
    """Run the execute phase here: in-process Python or a local subprocess."""

    def route_execute(
        self,
        operation: Any,
        execute_inputs: list[ExecuteInput],
        sandbox_root: str,
    ) -> list[Any]:
        slots = reserve_invocations(len(execute_inputs))
        results = []
        for slot, execute_input in zip(slots, execute_inputs, strict=True):
            with invocation_scope(operation, slot):
                builder = current_replay_builder()
                results.append(
                    invoke_op_work(
                        operation,
                        execute_input,
                        stream_output=builder is not None
                        and builder.snapshot.diagnostic is not None,
                    )
                )
        return results
