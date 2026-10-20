"""Failure-log tail tool for failed step executions.

Only failed executions have discoverable log paths — ``inspect_failures``
emits a relative fragment under ``<runs_dir>/logs/failures/``. This tool
tails the newest source execution starts for a named step, with execution-ID
ties broken deterministically. It does not expose success-step logs.
Failure logs are always local files, matching the standard layout where
``runs_dir`` is the parent of ``delta_root``.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from fastmcp import Context, FastMCP
from fastmcp.tools import ToolResult
from pydantic import StrictInt

from artisan_mcp._boundary import render_tool, require_delta_root, validate_int_range
from artisan_mcp._common import READ_ONLY
from artisan_mcp._evidence import MAX_PAYLOAD_BYTES, encode_json
from artisan_mcp.models import LogsResponse, output_schema

MAX_TAIL_LINES = 1_000
MAX_LOG_BYTES = 256 * 1024
MAX_LOG_FILES = 100

logger = logging.getLogger(__name__)


def register(mcp: FastMCP) -> None:
    """Attach the log tool to ``mcp``."""

    @mcp.tool(annotations=READ_ONLY, output_schema=output_schema(LogsResponse))
    def artisan_get_step_logs(
        ctx: Context,
        pipeline_run_id: str,
        step_name: str,
        tail_lines: StrictInt = 200,
    ) -> ToolResult:
        """Tail the failure logs of a failed step to read its error output.

        This reads the human failure log files a failed execution writes, not
        success-step logs. It resolves the step name to its number, finds that
        step's failed-execution log paths via the failures report, reads them,
        and returns the last tail_lines lines with a truncated flag. The
        request is capped at 1,000 lines and 256 KiB. A step that did not fail
        — or a store with no failures — yields an empty lines list. Returned bytes are separately bounded;
        truncation_reasons and partial_first_line describe the excerpt. Use it
        after artisan_get_run_status flags a failed step.
        """
        config = ctx.lifespan_context["config"]

        def payload() -> dict[str, Any]:
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
                return _excerpt([], set(), False)

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

        return render_tool(payload, LogsResponse)


def _contained_log_path(logs_root: str, relative_path: str) -> str:
    """Resolve a persisted log reference without allowing path traversal."""
    resolved_root = os.path.realpath(logs_root)
    resolved_path = os.path.realpath(os.path.join(resolved_root, relative_path))
    if os.path.commonpath([resolved_root, resolved_path]) != resolved_root:
        logger.warning("Ignoring failure-log path outside the configured log root")
        return ""
    return resolved_path


def _excerpt(lines: list[str], reasons: set[str], partial: bool) -> dict[str, Any]:
    """Build the complete excerpt envelope before measuring its encoded bytes."""
    return {
        "projection_version": "1",
        "follow_up": None,
        "lines": lines,
        "truncated": bool(reasons),
        "truncation_reasons": sorted(reasons),
        "partial_first_line": partial,
    }


def _tail_logs(paths: list[str], tail_lines: StrictInt) -> dict[str, Any]:
    """Tail newest files within source-read, line and serialized-response bounds."""
    selected = paths[-MAX_LOG_FILES:]
    chunks: list[bytes] = []
    remaining, line_breaks = MAX_LOG_BYTES, 0
    reasons = {"files"} if len(selected) < len(paths) else set()
    partial = False
    for path in reversed(selected):
        if remaining == 0 or line_breaks >= tail_lines:
            reasons.add("bytes" if remaining == 0 else "lines")
            break
        data, read_count, source_cut, partial = _tail_bytes(path, remaining)
        chunks.append(data)
        remaining -= read_count
        line_breaks += data.count(b"\n")
        if source_cut:
            reasons.add("bytes")
    combined = b"\n".join(reversed(chunks))
    if len(combined) > MAX_LOG_BYTES:
        start = len(combined) - MAX_LOG_BYTES
        partial = combined[start - 1 : start] != b"\n"
        combined = combined[start:]
        reasons.add("bytes")
    lines = combined.decode("utf-8", errors="replace").splitlines()
    if len(lines) > tail_lines:
        lines = lines[-tail_lines:]
        partial = False
        reasons.add("lines")
    return _fit_excerpt(lines, reasons, partial)


def _fit_excerpt(lines: list[str], reasons: set[str], partial: bool) -> dict[str, Any]:
    """Drop oldest whole lines, then shorten only an indivisible final line."""
    while (
        len(lines) > 1
        and len(encode_json(_excerpt(lines, reasons, partial))) > MAX_PAYLOAD_BYTES
    ):
        lines = lines[1:]
        partial = False
        reasons.add("bytes")
    if (
        lines
        and len(encode_json(_excerpt(lines, reasons, partial))) > MAX_PAYLOAD_BYTES
    ):
        reasons.add("bytes")
        partial = True
        line = lines[0]
        low, high = 0, len(line)
        while low < high:
            middle = (low + high) // 2
            if (
                len(encode_json(_excerpt([line[middle:]], reasons, partial)))
                <= MAX_PAYLOAD_BYTES
            ):
                high = middle
            else:
                low = middle + 1
        lines = [line[low:]]
    return _excerpt(lines, reasons, partial)


def _tail_bytes(path: str, byte_limit: StrictInt) -> tuple[bytes, int, bool, bool]:
    """Read within the source budget, retaining one byte to prove a line boundary."""
    with open(path, "rb") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        start = max(0, size - byte_limit)
        handle.seek(start)
        data = handle.read(byte_limit)
    if start == 0:
        return data, len(data), False, False
    # The first read byte is the predecessor of the returned excerpt; it is
    # included in the source-read budget instead of doing an uncounted peek.
    return data[1:], len(data), True, data[:1] != b"\n"
