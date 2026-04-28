"""End-to-end Modal tests for tool-output capture.

Verifies that ``<sandbox_root>/tool_output.log`` survives the round
trip from a Modal container back to the local filesystem on success,
that batch dispatch concatenates per-artifact bytes with separators,
and that partial logs from failed containers reach the parquet
``tool_output`` column on the failure record.

Gated by ``@pytest.mark.modal`` plus the ``modal_credentials`` fixture
— skips cleanly without ``MODAL_TOKEN_ID`` / ``MODAL_TOKEN_SECRET``.
"""

from __future__ import annotations

import os
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.compute import (
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.environment_spec import (
    LocalEnvironmentSpec,
)
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.external_tools import run_command

pytestmark = pytest.mark.modal

# Token that future assertions look for inside the captured log.
_LOG_TOKEN = "ARTISAN_TOOL_OUTPUT_TEST_TOKEN"


class _LoggingEcho(OperationDefinition):
    """Run ``echo`` per dataset via run_command(stream_output=True).

    The point is to drive the same ``run_command`` path real ops use
    so the unit log gets populated by ``_run_with_streaming``. Output
    artifact mirrors the input so postprocess always succeeds.
    """

    name: ClassVar[str] = "logging_echo"
    description: ClassVar[str] = "Echo a token via run_command for log capture"

    class InputRole(StrEnum):
        DATASET = "dataset"

    class OutputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(
            artifact_type="data",
            materialize=True,
            required=True,
        ),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["dataset"]},
        ),
    }

    tool: ToolSpec = ToolSpec(executable="echo", interpreter=None)
    environments: Environments = Environments(local=LocalEnvironmentSpec())
    runner_resources: RunnerResources = RunnerResources(time_limit="00:05:00")
    batch_strategy: BatchStrategy = BatchStrategy(job_name="logging_echo")
    compute_provider: ComputeProvider = ComputeProvider(modal=ModalComputeConfig())

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        artifacts = inputs.input_artifacts.get("dataset", [])
        return {
            "paths": [str(a.materialized_path) for a in artifacts],
            "names": [a.original_name for a in artifacts],
        }

    def execute(self, inputs: ExecuteInput) -> Any:
        env = self.environments.current()
        for name in inputs.inputs["names"]:
            run_command(
                env,
                [*self.tool.parts(), f"{_LOG_TOKEN}:{name}"],
                cwd=inputs.execute_dir,
                stream_output=True,
                log_path=inputs.log_path,
            )
        return None

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        # Mirror inputs as outputs so postprocess succeeds without
        # producing new files (this op exists only to drive run_command).
        drafts = []
        for artifacts in inputs.input_artifacts.values():
            for a in artifacts:
                with open(a.materialized_path, "rb") as fh:
                    content = fh.read()
                drafts.append(
                    DataArtifact.draft(
                        content=content,
                        original_name=a.original_name,
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"dataset": drafts})


class _FailingLogger(OperationDefinition):
    """Write known bytes to log_path, then raise.

    Drives the partial-log capture path: bytes must reach the parquet
    ``tool_output`` column on the failure record.
    """

    name: ClassVar[str] = "failing_logger"
    description: ClassVar[str] = "Write to log_path then raise"

    class InputRole(StrEnum):
        DATASET = "dataset"

    class OutputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(
            artifact_type="data",
            materialize=True,
            required=True,
        ),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["dataset"]},
        ),
    }

    environments: Environments = Environments(local=LocalEnvironmentSpec())
    runner_resources: RunnerResources = RunnerResources(time_limit="00:05:00")
    batch_strategy: BatchStrategy = BatchStrategy(job_name="failing_logger")
    compute_provider: ComputeProvider = ComputeProvider(modal=ModalComputeConfig())

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {}

    def execute(self, inputs: ExecuteInput) -> Any:
        with open(inputs.log_path, "w") as f:
            for i in range(10):
                f.write(f"{_LOG_TOKEN}: line {i}\n")
            f.flush()
        msg = "intentional failure for partial-log capture test"
        raise ValueError(msg)

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        return ArtifactResult(success=True, artifacts={"dataset": []})


def _find_unit_log(working_root: str) -> Path:
    """Locate the single ``tool_output.log`` under a sandbox tree."""
    matches = list(Path(working_root).rglob("tool_output.log"))
    assert matches, f"no tool_output.log under {working_root}"
    assert len(matches) == 1, (
        f"expected one tool_output.log, found {len(matches)}: {matches}"
    )
    return matches[0]


def test_single_dispatch_captures_tool_output(pipeline_env, modal_credentials):
    """Bytes written via run_command(stream_output=True) appear at sandbox-root."""
    pipeline = PipelineManager.create(
        name="modal_tool_output_single",
        preserve_working=True,  # keep sandbox so we can inspect the log
        **pipeline_env,
    )
    pipeline.run(
        operation=DataGenerator,
        name="generate",
        params={"count": 1, "seed": 0},
    )
    pipeline.run(
        operation=_LoggingEcho,
        name="echo",
        inputs={"dataset": pipeline.output("generate", "datasets")},
        compute_provider="modal",
    )
    summary = pipeline.finalize()
    assert summary["overall_success"] is True

    log_file = _find_unit_log(pipeline_env["working_root"])
    contents = log_file.read_bytes()
    assert _LOG_TOKEN.encode() in contents


def test_batch_dispatch_concatenates_per_artifact(pipeline_env, modal_credentials):
    """Per-artifact bytes are concatenated with `=== artifact i ===` separators."""
    pipeline = PipelineManager.create(
        name="modal_tool_output_batch",
        preserve_working=True,
        **pipeline_env,
    )
    pipeline.run(
        operation=DataGenerator,
        name="generate",
        params={"count": 3, "seed": 1},
    )
    pipeline.run(
        operation=_LoggingEcho,
        name="echo",
        inputs={"dataset": pipeline.output("generate", "datasets")},
        compute_provider={"active": "modal", "modal": {"min_containers": 2}},
    )
    summary = pipeline.finalize()
    assert summary["overall_success"] is True

    log_file = _find_unit_log(pipeline_env["working_root"])
    contents = log_file.read_bytes()
    # Three artifacts, three separators in input order.
    assert b"=== artifact 0 ===" in contents
    assert b"=== artifact 1 ===" in contents
    assert b"=== artifact 2 ===" in contents
    assert contents.index(b"artifact 0") < contents.index(b"artifact 1")
    assert contents.index(b"artifact 1") < contents.index(b"artifact 2")
    assert _LOG_TOKEN.encode() in contents


def test_failure_captures_partial_log(pipeline_env, modal_credentials):
    """Partial log bytes from a failing container land in parquet tool_output."""
    pipeline = PipelineManager.create(
        name="modal_tool_output_failure",
        **pipeline_env,
    )
    pipeline.run(
        operation=DataGenerator,
        name="generate",
        params={"count": 1, "seed": 2},
    )
    pipeline.run(
        operation=_FailingLogger,
        name="fail",
        inputs={"dataset": pipeline.output("generate", "datasets")},
        compute_provider="modal",
    )
    summary = pipeline.finalize()
    # Pipeline-level success is False because step "fail" failed.
    assert summary["overall_success"] is False

    # Read the executions Delta table and find the failure row.
    df = pl.read_delta(
        os.path.join(pipeline_env["delta_root"], "orchestration/executions"),
    )
    failed = df.filter(pl.col("success") == False)  # noqa: E712
    assert failed.height >= 1
    tool_outputs = failed["tool_output"].to_list()
    captured = [s for s in tool_outputs if s and _LOG_TOKEN in s]
    assert captured, (
        f"expected a failure row whose tool_output contains {_LOG_TOKEN!r}; "
        f"got {tool_outputs!r}"
    )
    # All ten lines should have made it into the partial log.
    assert "line 0" in captured[0]
    assert "line 9" in captured[0]
