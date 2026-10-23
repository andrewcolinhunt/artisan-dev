"""Run a complete pipeline through the published package facades."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def test_public_surface_end_to_end(pipeline_env: dict[str, str]) -> None:
    """Run and inspect a pipeline using the public operation/orchestration facades."""
    import polars as pl

    from artisan.operations.examples import DataGenerator
    from artisan.orchestration import (
        PipelineConfig,
        PipelineManager,
        Runner,
        RunnerBase,
        list_runs,
    )

    pipeline = PipelineManager.create(
        name="public_smoke",
        default_step_runner=Runner.LOCAL,
        **pipeline_env,
    )
    assert isinstance(pipeline.config, PipelineConfig)
    assert isinstance(Runner.LOCAL, RunnerBase)

    pipeline.run(DataGenerator, params={"count": 1, "seed": 0})
    summary = pipeline.finalize()
    assert summary["overall_success"] is True

    df = list_runs(pipeline_env["delta_root"])
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 1
    assert "pipeline_run_id" in df.columns
