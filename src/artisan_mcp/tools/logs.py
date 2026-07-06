"""Log tool: get_step_logs (failure logs only, v1 scope).

Only failed executions have discoverable log paths — ``inspect_failures``
emits a relative fragment under ``<runs_dir>/logs/failures/``. This tool
tails those files for a named step. Success-step logs are out of scope
(deferred to the full log-reading design). Failure logs are always local
files, matching the standard layout where ``runs_dir`` is the parent of
``delta_root``.
"""

from __future__ import annotations

import os

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import boundary, require_delta_root

_READ_ONLY = {"readOnlyHint": True, "idempotentHint": True}


def register(mcp: FastMCP) -> None:
    """Attach the log tool to ``mcp``."""

    @mcp.tool(annotations=_READ_ONLY)
    async def artisan_get_step_logs(
        ctx: Context,
        pipeline_run_id: str,
        step_name: str,
        tail_lines: int = 200,
    ) -> dict:
        """Tail the failure logs of a failed step to read its error output.

        Failure logs only (v1): this reads the human failure log files a
        failed execution writes, not success-step logs. Resolves the step
        name to its number, finds that step's failed-execution log paths via
        the failures report, reads them, and returns the last tail_lines
        lines with a truncated flag. A step that did not fail — or a store
        with no failures — yields an empty lines list. Use it after
        artisan_get_run_status flags a failed step.
        """
        config = ctx.lifespan_context["config"]

        def payload() -> dict:
            import polars as pl

            from artisan.orchestration.run_status import resolve_step_number
            from artisan.schemas.execution.storage_config import StorageConfig
            from artisan.visualization.inspect import inspect_failures

            root = require_delta_root(config)
            storage = StorageConfig()
            opts = storage.delta_storage_options()
            fs = storage.filesystem()

            number = resolve_step_number(
                root, pipeline_run_id, step_name, storage_options=opts, fs=fs
            )
            if number is None:
                return {"lines": [], "truncated": False}

            failures = inspect_failures(
                root, pipeline_run_id=pipeline_run_id, storage_options=opts, fs=fs
            )
            logs_root = os.path.join(
                os.path.dirname(root.rstrip("/")), "logs", "failures"
            )
            lines: list[str] = []
            for row in failures.filter(pl.col("step") == number).iter_rows(named=True):
                path = os.path.join(logs_root, row["log"])
                if os.path.exists(path):
                    with open(path) as handle:
                        lines.extend(handle.read().splitlines())

            truncated = len(lines) > tail_lines
            return {"lines": lines[-tail_lines:], "truncated": truncated}

        return boundary(payload)
