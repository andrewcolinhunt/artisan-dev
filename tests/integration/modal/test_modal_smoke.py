"""Real-Modal smoke tests for the compute routing layer.

These tests genuinely submit work to Modal (no mocks). They are gated by
the ``modal`` pytest marker plus the ``modal_credentials`` fixture, so
they skip cleanly in environments without ``MODAL_TOKEN_ID`` /
``MODAL_TOKEN_SECRET`` (or ``~/.modal.toml``).
"""

from __future__ import annotations

import pytest

from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager

pytestmark = pytest.mark.modal


def test_single_dispatch_to_modal(pipeline_env, modal_credentials):
    """A pipeline whose transform step runs on Modal completes successfully."""
    pipeline = PipelineManager.create(
        name="modal_smoke_single",
        **pipeline_env,
    )
    pipeline.run(
        operation=DataGenerator,
        name="generate",
        params={"count": 1, "seed": 0},
    )
    pipeline.run(
        operation=DataTransformer,
        name="transform",
        inputs={"dataset": pipeline.output("generate", "datasets")},
        compute_provider="modal",
    )
    summary = pipeline.finalize()
    assert summary["overall_success"] is True


def test_batch_dispatch_to_modal(pipeline_env, modal_credentials):
    """Per-artifact batch dispatch via experimental_spawn_map runs on Modal."""
    pipeline = PipelineManager.create(
        name="modal_smoke_batch",
        **pipeline_env,
    )
    pipeline.run(
        operation=DataGenerator,
        name="generate",
        params={"count": 3, "seed": 1},
    )
    pipeline.run(
        operation=DataTransformer,
        name="transform",
        inputs={"dataset": pipeline.output("generate", "datasets")},
        params={"variants": 1},
        compute_provider={"active": "modal", "modal": {"min_containers": 2}},
    )
    summary = pipeline.finalize()
    assert summary["overall_success"] is True
