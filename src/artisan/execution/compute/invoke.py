"""Run one op's work where this code is running.

The operation describes the work (``execute_function()`` body or
``execute_command()`` argv); this module performs it. Shared by the local
execute router and the tool-endpoint worker so the two sides cannot drift.
"""

from __future__ import annotations

from typing import Any

from artisan.schemas.specs.input_models import ExecuteInput
from artisan.utils.external_tools import run_command


def tool_command_inputs(prepared: dict[str, Any]) -> dict[str, Any]:
    """Normalize prepared inputs for ``execute_command``.

    Per-artifact dispatch delivers each sliced role as a one-element list
    (the list interface ``execute_function()`` implementations expect); an
    external tool's argv addresses one artifact's files, so the framework
    unwraps single-element lists before ``execute_command`` — identically
    under the local subprocess and the endpoint client. ``execute_as_tool``
    ops skip the unwrap: their generated argv embeds the dict as JSON, and
    the runner must re-deliver the exact list shape preprocess produced.

    Args:
        prepared: ``ExecuteInput.inputs`` for one artifact.

    Returns:
        The dict with one-element list values unwrapped to their item.
    """
    return {
        key: value[0] if isinstance(value, list) and len(value) == 1 else value
        for key, value in prepared.items()
    }


def invoke_op_work(
    operation: Any,
    execute_input: ExecuteInput,
    *,
    environment: Any | None = None,
    stream_output: bool = False,
) -> Any:
    """Run one artifact's work at the current location.

    Command ops: build argv via ``execute_command()`` and run it as a
    subprocess wrapped by the environment. Function ops: call
    ``execute_function()`` in-process.

    The log is opened in append mode: a unit's per-artifact ExecuteInputs
    share one unit-level ``log_path``, so sequential per-artifact runs
    accumulate rather than truncate (sandboxes are fresh per run, so the
    log never carries over between runs).

    Args:
        operation: The operation instance.
        execute_input: Prepared inputs and the execute directory.
        environment: EnvironmentSpec wrapping the subprocess. Defaults to
            the operation's active environment. The endpoint worker passes
            ``LocalEnvironmentSpec()`` — the container is the environment.
        stream_output: Stream tool output line-by-line. The worker streams
            so container stdout (the Modal dashboard log) shows progress.

    Returns:
        The raw execute result. ``None`` for command ops — their products
        are the files written to ``execute_input.execute_dir`` plus the
        tool log.
    """
    if operation.is_command_op():
        prepared = (
            execute_input.inputs
            if operation.execute_as_tool
            else tool_command_inputs(execute_input.inputs)
        )
        run_command(
            environment or operation.environments.current(),
            operation.execute_command(prepared),
            cwd=execute_input.execute_dir,
            log_path=execute_input.log_path,
            log_mode="a",
            stream_output=stream_output,
        )
        return None
    return operation.execute_function(execute_input)
