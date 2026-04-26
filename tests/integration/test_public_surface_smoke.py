"""Public surface smoke — round-trip using only the package-level imports.

Existing integration tests reach into internal modules
(``from artisan.orchestration.runners import Runner``,
``from artisan.orchestration.pipeline_manager import PipelineManager``)
because they were written that way historically. That makes them
behavior tests but NOT public-surface tests — a forgotten re-export
on ``artisan.orchestration.__init__`` would slip through.

This file imports ONLY from ``artisan.orchestration`` (the public
package) and runs a complete pipeline. If ``__all__`` advertises a
name that isn't actually accessible, or a name in ``__all__`` resolves
to None, this file fails.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def test_public_surface_end_to_end(pipeline_env: dict[str, str]) -> None:
    """Round-trip a small pipeline using only artisan.orchestration imports."""
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
        default_compute_provider="local",
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


def test_public_all_resolves() -> None:
    """Every name in artisan.orchestration.__all__ must be importable and not None.

    Cheap structural check: catches a stale ``__all__`` entry where the
    re-export was removed but the listing was forgotten — or vice versa,
    a re-export that fails to import (would error on ``import
    artisan.orchestration`` itself, but this asserts the names).
    """
    import artisan.orchestration as orch

    assert orch.__all__, "artisan.orchestration.__all__ is empty"
    for name in orch.__all__:
        attr = getattr(orch, name, None)
        assert attr is not None, (
            f"{name!r} is listed in artisan.orchestration.__all__ "
            f"but resolves to None or is missing"
        )


def test_public_all_does_not_leak_internals() -> None:
    """``__all__`` must not include private names (single-underscore prefix).

    Guards against accidental re-export of private helpers if someone
    adds a name to ``__all__`` without checking.
    """
    import artisan.orchestration as orch

    private = [n for n in orch.__all__ if n.startswith("_")]
    assert not private, f"private name(s) in artisan.orchestration.__all__: {private}"
