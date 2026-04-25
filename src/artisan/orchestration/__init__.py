"""Public orchestration API for artisan pipelines.

This package-level module exposes the stable entry points used by pipeline
definitions:

- PipelineManager: Main interface for defining and executing pipeline steps.
- PipelineConfig: Frozen configuration model returned by ``pipeline.config``.
- Runner: Namespace of pre-built step runner instances (LOCAL, SLURM, SLURM_INTRA).
- RunnerBase: ABC for custom runners.
- list_runs: Module-level function that lists pipeline runs in a Delta root.

Example:
    from artisan.orchestration import PipelineManager, Runner, list_runs

    pipeline = PipelineManager.create(
        name="my_pipeline",
        delta_root="/data/delta",
        staging_root="/data/staging",
        default_step_runner=Runner.SLURM,
    )
    output = pipeline.output

    pipeline.run(operation=IngestData, name="ingest", inputs=files)
    pipeline.run(operation=ScoreOp, name="score", inputs={"data": output("ingest", "data")})
    result = pipeline.finalize()

    df = list_runs("/data/delta")
"""

from __future__ import annotations

import os as _os

# Suppress Prefect's verbose flow-run logging before any Prefect import
# triggers its dict-config. Must happen here because step_runner modules
# import Prefect at class-definition time (ProcessPoolTaskRunner).
# Users override via PREFECT_LOGGING_LEVEL=INFO in their environment;
# configure_logging(suppress_noise=False) also undoes this.
_os.environ.setdefault("PREFECT_LOGGING_LEVEL", "CRITICAL")

from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.orchestration.run_history import list_runs
from artisan.orchestration.runners import Runner, RunnerBase
from artisan.schemas.orchestration.pipeline_config import PipelineConfig

__all__ = [
    "PipelineConfig",
    "PipelineManager",
    "Runner",
    "RunnerBase",
    "list_runs",
]
