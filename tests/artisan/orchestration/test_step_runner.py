"""Tests for step_runner abstraction in the orchestration layer.

Tests that the Runner namespace and the step_runner= parameter work
correctly across PipelineManager and execute_step. Resolution-only
tests live in runners/test_resolve.py.
"""

from __future__ import annotations

import inspect

from artisan.orchestration.runners import Runner
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.runner_resources import RunnerResources


class TestRunnerRouting:
    """Tests for built-in local lifecycle-router construction."""

    def test_local_runner_returns_lifecycle_router(self):
        from artisan.orchestration.engine.lifecycle_router import LifecycleRouter

        runner_resources = RunnerResources()
        batch_strategy = BatchStrategy()
        handle = Runner.LOCAL.create_lifecycle_router(
            runner_resources, batch_strategy, step_number=0, job_name="test_op"
        )
        assert isinstance(handle, LifecycleRouter)


class TestPipelineManagerStepRunnerParam:
    """Tests for step_runner parameter in PipelineManager.run()."""

    def test_run_signature_has_step_runner(self):
        from artisan.orchestration.pipeline_manager import PipelineManager

        sig = inspect.signature(PipelineManager.run)
        params = list(sig.parameters.keys())
        assert "step_runner" in params
        assert "compute_backend" not in params

    def test_run_step_runner_default_is_none(self):
        from artisan.orchestration.pipeline_manager import PipelineManager

        sig = inspect.signature(PipelineManager.run)
        default = sig.parameters["step_runner"].default
        assert default is None


class TestExecuteStepStepRunnerParam:
    """Tests for step_runner in execute_step function."""

    def test_execute_step_signature_has_step_runner(self):
        from artisan.orchestration.engine.step_executor import execute_step

        sig = inspect.signature(execute_step)
        params = list(sig.parameters.keys())
        assert "step_runner" in params
        assert "compute_backend" not in params
