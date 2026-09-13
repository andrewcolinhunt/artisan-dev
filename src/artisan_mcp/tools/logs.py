"""Log tool: get_step_logs (failure logs only, v1 scope).

Only failed executions have discoverable log paths — ``inspect_failures``
emits a relative fragment under ``<runs_dir>/logs/failures/``. This tool
tails those files for a named step. Success-step logs are out of scope
(deferred to the full log-reading design). Failure logs are always local
files, matching the standard layout where ``runs_dir`` is the parent of
``delta_root``.
"""

from __future__ import annotations

import logging
import os

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import boundary, require_delta_root, validate_int_range
from artisan_mcp._common import READ_ONLY

MAX_TAIL_LINES = 1_000
MAX_LOG_BYTES = 256 * 1024
MAX_LOG_FILES = 100

logger = logging.getLogger(__name__)


def register(mcp: FastMCP) -> None:
    """Attach the log tool to ``mcp``."""

    @mcp.tool(annotations=READ_ONLY)
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
        lines with a truncated flag. The request is capped at 1,000 lines and
        256 KiB. A step that did not fail — or a store with no failures —
        yields an empty lines list. Use it after artisan_get_run_status flags
        a failed step.
        """
        config = ctx.lifespan_context["config"]

        def payload() -> dict:
            import polars as pl

            from artisan.orchestration.run_status import resolve_step_number
            from artisan.schemas.execution.storage_config import StorageConfig
            from artisan.visualization.inspect import inspect_failures

            validate_int_range(
                tail_lines,
                field="tail_lines",
                minimum=1,
                maximum=MAX_TAIL_LINES,
            )
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
            paths: list[str] = []
            for row in failures.filter(pl.col("step") == number).iter_rows(named=True):
                path = _contained_log_path(logs_root, row["log"])
                if os.path.exists(path):
                    paths.append(path)

            return _tail_logs(paths, tail_lines)

        return boundary(payload)


def _contained_log_path(logs_root: str, relative_path: str) -> str:
    """Resolve a persisted log reference without allowing path traversal."""
    resolved_root = os.path.realpath(logs_root)
    resolved_path = os.path.realpath(os.path.join(resolved_root, relative_path))
    if os.path.commonpath([resolved_root, resolved_path]) != resolved_root:
        logger.warning("Ignoring failure-log path outside the configured log root")
        return ""
    return resolved_path


def _tail_logs(paths: list[str], tail_lines: int) -> dict:
    """Tail several files within aggregate file, byte, and line bounds."""
    selected = paths[-MAX_LOG_FILES:]
    chunks: list[bytes] = []
    remaining = MAX_LOG_BYTES
    line_breaks = 0
    truncated = len(selected) < len(paths)

    for path in reversed(selected):
        if remaining == 0 or line_breaks >= tail_lines:
            truncated = True
            break
        data, file_truncated = _tail_bytes(path, remaining)
        chunks.append(data)
        remaining -= len(data)
        line_breaks += data.count(b"\n")
        truncated = truncated or file_truncated

    combined = b"\n".join(reversed(chunks))
    if len(combined) > MAX_LOG_BYTES:
        combined = combined[-MAX_LOG_BYTES:]
        truncated = True
    lines = combined.decode("utf-8", errors="replace").splitlines()
    if len(lines) > tail_lines:
        lines = lines[-tail_lines:]
        truncated = True
    return {"lines": lines, "truncated": truncated}


def _tail_bytes(path: str, byte_limit: int) -> tuple[bytes, bool]:
    """Read at most ``byte_limit`` bytes from the end of one file."""
    with open(path, "rb") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        start = max(0, size - byte_limit)
        handle.seek(start)
        return handle.read(byte_limit), start > 0
