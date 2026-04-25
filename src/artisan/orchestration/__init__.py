"""Public orchestration API for artisan pipelines.

This package-level module exposes the stable entry points used by pipeline
definitions:

- PipelineManager: Main interface for defining and executing pipeline steps.
- Runner: Namespace of pre-built step runner instances (LOCAL, SLURM, SLURM_INTRA).
- RunnerBase: ABC for custom runners.
- FailurePolicy: Enum controlling behavior after step failures.
- CachePolicy: Enum controlling when completed steps qualify as cache hits.

Example:
    from artisan.orchestration import PipelineManager, Runner

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
from artisan.orchestration.runners import Runner, RunnerBase

__all__ = [
    "PipelineManager",
    "Runner",
    "RunnerBase",
]
