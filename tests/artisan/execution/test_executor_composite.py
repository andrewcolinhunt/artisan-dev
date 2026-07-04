"""Tests for run_composite failure handling."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import polars as pl

from artisan.composites.base.composite_context import CompositeContext
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.execution.executors.composite import run_composite
from artisan.execution.models.execution_composite import ExecutionComposite
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment


class _RaisingComposite(CompositeDefinition):
    """Composite whose compose() always raises."""

    name: ClassVar[str] = "raising_composite_test"

    def compose(self, ctx: CompositeContext) -> None:
        msg = "Intentional composite failure"
        raise RuntimeError(msg)


def _transport() -> ExecutionComposite:
    return ExecutionComposite(
        composite=_RaisingComposite(),
        inputs={},
        step_number=1,
        execution_spec_id="spec_comp" + "0" * 23,
    )


def test_run_composite_stages_failure_record_when_compose_raises(tmp_path):
    """A failing compose() stages an executions row, not just a bare result."""
    for name in ("delta", "working", "staging"):
        (tmp_path / name).mkdir()
    runtime_env = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        working_root=str(tmp_path / "working"),
        staging_root=str(tmp_path / "staging"),
    )

    result = run_composite(_transport(), runtime_env)

    assert result.success is False
    assert result.staging_path is not None
    df = pl.read_parquet(Path(result.staging_path) / "executions.parquet")
    assert df["success"][0] is False
    assert "Intentional composite failure" in df["error"][0]


def test_run_composite_returns_bare_result_when_context_cannot_build(tmp_path):
    """With no working_root, the failure falls back to a bare StagingResult."""
    for name in ("delta", "staging"):
        (tmp_path / name).mkdir()
    runtime_env = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        working_root=None,
        staging_root=str(tmp_path / "staging"),
    )

    result = run_composite(_transport(), runtime_env)

    assert result.success is False
    assert result.staging_path is None
    assert "Intentional composite failure" in result.error
