"""Canonical failure names use source time and literal execution identity."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from artisan.utils.log_paths import failure_log_relative_path, find_failure_log

START = datetime(2026, 9, 19, 1, 2, 3, 456789, tzinfo=UTC)


def test_failure_log_relative_path_normalizes_utc() -> None:
    local = START.astimezone(timezone(timedelta(hours=-7)))
    assert (
        failure_log_relative_path("id", local)
        == "20260919/20260919T010203456789Z_id.log"
    )


def test_failure_paths_sort_by_source_time_then_id() -> None:
    paths = [
        failure_log_relative_path("b", START),
        failure_log_relative_path("a", START),
        failure_log_relative_path("z", START - timedelta(days=1)),
    ]
    assert sorted(paths) == [paths[2], paths[1], paths[0]]


@pytest.mark.parametrize(
    "run_id", ["", ".", "..", "../id", "a/b", "a\\b", "a\0b", "x*", "x?", "[id]"]
)
def test_failure_paths_reject_unsafe_ids(tmp_path: Path, run_id: str) -> None:
    with pytest.raises(ValueError):
        failure_log_relative_path(run_id, START)
    with pytest.raises(ValueError):
        find_failure_log(str(tmp_path), run_id)


def test_failure_path_rejects_naive_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        failure_log_relative_path("id", START.replace(tzinfo=None))


def test_find_failure_log_exact_canonical_match_only(tmp_path: Path) -> None:
    for relative in [
        "step_0_op/id.log",
        "20260919/unrelated_id.log",
        "20260918/20260919T010203456789Z_id.log",
        "20260919/20260919T250203456789Z_id.log",
        failure_log_relative_path("other_id", START),
    ]:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    assert find_failure_log(str(tmp_path), "id") is None
    path = tmp_path / failure_log_relative_path("id", START)
    path.touch()
    assert find_failure_log(str(tmp_path), "id") == str(path)
    other = tmp_path / failure_log_relative_path("id", START + timedelta(seconds=1))
    other.touch()
    with pytest.raises(ValueError, match="Ambiguous"):
        find_failure_log(str(tmp_path), "id")
