"""Canonical local failure-log names shared by writers and readers."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path


def _validate_execution_id(execution_run_id: str) -> None:
    """Reject IDs that could become paths or glob patterns."""
    if (
        not execution_run_id
        or execution_run_id in {".", ".."}
        or any(char in execution_run_id for char in "/\\\x00*?[]")
    ):
        msg = "Execution run ID must be a nonempty literal path component"
        raise ValueError(msg)


def failure_log_relative_path(execution_run_id: str, timestamp_start: datetime) -> str:
    """Return the dated failure filename for an immutable execution attempt.

    Args:
        execution_run_id: Literal execution ID, without path or glob characters.
        timestamp_start: Timezone-aware source execution start time.

    Returns:
        Relative path with UTC date directory and microsecond timestamp prefix.

    Raises:
        ValueError: The ID is unsafe or the timestamp is timezone-naive.
    """
    _validate_execution_id(execution_run_id)
    if timestamp_start.tzinfo is None or timestamp_start.utcoffset() is None:
        msg = "Execution start timestamp must be timezone-aware"
        raise ValueError(msg)
    start = timestamp_start.astimezone(UTC)
    return f"{start:%Y%m%d}/{start:%Y%m%dT%H%M%S%fZ}_{execution_run_id}.log"


def find_failure_log(failure_logs_root: str, execution_run_id: str) -> str | None:
    """Find one exact ID in canonical date buckets; reject ambiguous matches.

    Args:
        failure_logs_root: Local root containing dated failure-log directories.
        execution_run_id: Literal execution attempt ID to locate.

    Returns:
        Matching local filename, or None if the log is absent.

    Raises:
        ValueError: The ID is unsafe or more than one canonical log matches.
    """
    _validate_execution_id(execution_run_id)
    matches = []
    pattern = re.compile(r"(\d{8}T\d{12}Z)_" + re.escape(execution_run_id) + r"\.log")
    for path in Path(failure_logs_root).glob(f"????????/*_{execution_run_id}.log"):
        match = pattern.fullmatch(path.name)
        if match is None or not path.is_file():
            continue
        try:
            start = datetime.strptime(match[1], "%Y%m%dT%H%M%S%fZ").replace(tzinfo=UTC)
        except ValueError:
            continue
        if path.parent.name == start.strftime("%Y%m%d"):
            matches.append(str(path))
    if len(matches) > 1:
        msg = f"Ambiguous failure logs for execution {execution_run_id!r}"
        raise ValueError(msg)
    return matches[0] if matches else None
