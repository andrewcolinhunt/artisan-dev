"""Real local manager sessions and persisted command launch evidence."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_state import StepState
from artisan.schemas.specs.input_models import ExecuteInput
from artisan.utils.external_tools import run_command
from artisan.utils.logging import configure_logging
from artisan.visualization import PipelineTimings

pytestmark = pytest.mark.integration


class _CommandGenerator(DataGenerator):
    """Exercise both actual Popen paths inside an ordinary creator lifecycle."""

    name = "logging_command_generator"

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        for streaming in (False, True):
            run_command(
                LocalEnvironmentSpec(),
                [sys.executable, "-c", "print('observed launch')"],
                cwd=inputs.execute_dir,
                stream_output=streaming,
            )
        return super().execute_function(inputs)


@pytest.fixture
def configured_logger():
    logger = logging.getLogger("artisan")
    original = logger.handlers[:], logger.level, logger.propagate
    logger.handlers.clear()
    configure_logging("DEBUG")
    yield logger
    for handler in logger.handlers[:]:
        handler.close()
    logger.handlers[:] = original[0]
    logger.setLevel(original[1])
    logger.propagate = original[2]


def test_local_sessions_isolate_orchestration_and_persist_real_launches(
    tmp_path, configured_logger
):
    first = PipelineManager.create(
        "first-session",
        str(tmp_path / "first-delta"),
        str(tmp_path / "first-staging"),
        default_step_runner=LocalRunner(default_max_workers=1),
    )
    second = PipelineManager.create(
        "second-session",
        str(tmp_path / "second-delta"),
        str(tmp_path / "second-staging"),
        default_step_runner=LocalRunner(default_max_workers=1),
    )
    try:
        one = first.submit(
            _CommandGenerator,
            params={"seed": 11, "rows_per_file": 2},
            name="first-work",
            compact=False,
        )
        two = second.submit(
            DataGenerator,
            params={"seed": 12, "rows_per_file": 2},
            name="second-work",
            compact=False,
        )
        assert one.result().status == StepStatus.SUCCEEDED
        assert two.result().status == StepStatus.SUCCEEDED
        logging.getLogger("artisan.test").info("unbound integration marker")
    finally:
        first.finalize()
        second.finalize()
    assert first.log_path != second.log_path
    assert Path(first.log_path).parent.parent == Path(second.log_path).parent.parent
    first_text, second_text = (
        Path(first.log_path).read_text(),
        Path(second.log_path).read_text(),
    )
    assert "first-work" in first_text
    assert "first-session' complete" in first_text
    assert "second-work" in second_text
    assert "second-session' complete" in second_text
    assert "second-work" not in first_text
    assert "first-work" not in second_text
    assert "unbound integration marker" not in first_text + second_text
    assert "[step_runner=" not in first_text + second_text
    assert "Step 0 runner: local" in first_text
    timings = PipelineTimings.from_delta(
        first.config.delta_root, pipeline_run_id=first.config.pipeline_run_id
    )
    commands = timings.command_timings(0)
    assert commands.height == 2
    assert commands["recording_status"].to_list() == ["complete", "complete"]
    assert commands["launch_seconds"].null_count() == 0
    assert (commands["launch_seconds"] >= 0).all()
    assert "launch_seconds" not in timings.execution_timings(0).columns


def test_resumed_sessions_are_distinct_and_restore_failure_closes_sink(
    tmp_path, configured_logger
):
    kwargs = {
        "delta_root": str(tmp_path / "delta"),
        "staging_root": str(tmp_path / "staging"),
    }
    with PipelineManager.create(
        "source", **kwargs, default_step_runner=LocalRunner(default_max_workers=1)
    ) as source:
        assert (
            source.run(
                DataGenerator, params={"seed": 9, "rows_per_file": 2}, compact=False
            ).status
            == StepStatus.SUCCEEDED
        )
    first = PipelineManager.resume(
        **kwargs, pipeline_run_id=source.config.pipeline_run_id
    )
    second = PipelineManager.resume(
        **kwargs, pipeline_run_id=source.config.pipeline_run_id
    )
    try:
        assert (
            first.config.pipeline_run_id
            == second.config.pipeline_run_id
            == source.config.pipeline_run_id
        )
        assert len({source.log_path, first.log_path, second.log_path}) == 3
        handlers = configured_logger.handlers[:]
        with (
            patch.object(
                StepState, "to_step_result", side_effect=RuntimeError("restore failed")
            ),
            pytest.raises(RuntimeError, match="restore failed"),
        ):
            PipelineManager.resume(
                **kwargs, pipeline_run_id=source.config.pipeline_run_id
            )
        assert configured_logger.handlers == handlers
    finally:
        first.finalize()
        second.finalize()
