"""Tests for creator lifecycle execution and staged outcomes."""

from __future__ import annotations

import json
import os
import shutil
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
from fixtures.logical_commit_store import commit_test_inputs
from pydantic import BaseModel, Field

from artisan.errors import ArtisanError, ArtisanErrorEnvelope, ErrorCode
from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.executors.creator import (
    LifecycleResult,
    run_creator_flow,
    run_creator_lifecycle,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.utils import generate_execution_run_id
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


def _setup_delta_tables(
    base_path: Path,
    metrics: list[dict] | None = None,
    index_entries: list[dict] | None = None,
):
    """Commit test input tables through the logical-commit boundary."""
    from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA

    tables: dict[str, pl.DataFrame] = {}
    if metrics:
        tables["artifacts/metrics"] = pl.DataFrame(
            metrics, schema=MetricArtifact.POLARS_SCHEMA
        )

    if index_entries:
        tables["artifacts/index"] = pl.DataFrame(
            index_entries, schema=ARTIFACT_INDEX_SCHEMA
        )
    commit_test_inputs(
        base_path,
        base_path.parent / "fixture-staging",
        tables,
    )


class MetricCopyTestOp(OperationDefinition):
    """Test operation that copies input metric with a modification.

    Consume materialized input paths and return draft artifacts.
    """

    class InputRole(StrEnum):
        source = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "metric_copy_test"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.source: InputSpec(artifact_type=ArtifactTypes.METRIC, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"inputs": ["source"]},
        ),
    }

    class Params(BaseModel):
        suffix: str = Field(default="_copy", description="Output file suffix.")

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Extract materialized paths from input artifacts."""
        return {
            role: PerArtifact([a.materialized_path for a in artifacts])
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute_function(self, inputs: ExecuteInput) -> dict:
        source_paths = inputs.inputs["source"]
        source_path = (
            source_paths[0] if isinstance(source_paths, list) else source_paths
        )

        # Read content and write with suffix
        with open(source_path) as fh:
            content = json.loads(fh.read())
        content["copied"] = True
        stem = os.path.splitext(os.path.basename(source_path))[0]
        output_path = os.path.join(
            inputs.execute_dir, f"{stem}{self.params.suffix}.json"
        )
        with open(output_path, "w") as fh:
            fh.write(json.dumps(content))

        return {"copied": True}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Create draft MetricArtifacts from output files."""
        drafts: list[MetricArtifact] = []
        for file_path in inputs.file_outputs:
            if file_path.endswith(".json"):
                with open(file_path) as fh:
                    content = json.loads(fh.read())
                drafts.append(
                    MetricArtifact.draft(
                        content=content,
                        original_name=os.path.basename(file_path),
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"output": drafts})


class GenerativeTestOp(OperationDefinition):
    """Test operation that generates output without inputs.

    Return draft artifacts declared as orphan outputs.
    """

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "generative_test"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"inputs": []},  # Orphan - no lineage required
        ),
    }

    class Params(BaseModel):
        count: int = Field(default=1, description="Number of outputs to generate.")

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Generative operation - no inputs to preprocess."""
        return {}

    def execute_function(self, inputs: ExecuteInput) -> dict:
        for i in range(self.params.count):
            content = json.dumps({"value": i})
            output_path = os.path.join(inputs.execute_dir, f"generated_{i:03d}.json")
            with open(output_path, "w") as fh:
                fh.write(content)

        return {"generated": self.params.count}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Create draft MetricArtifacts from output files."""
        drafts: list[MetricArtifact] = []
        for file_path in inputs.file_outputs:
            if file_path.endswith(".json"):
                with open(file_path) as fh:
                    content = json.loads(fh.read())
                drafts.append(
                    MetricArtifact.draft(
                        content=content,
                        original_name=os.path.basename(file_path),
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"output": drafts})


class EchoToolTestOp(OperationDefinition):
    """Command op whose tool prints to stdout and writes a metric file."""

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "echo_tool_executor_test"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"inputs": []},
        ),
    }

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [
            *self.tool.parts(),
            "-c",
            'echo "tool stdout line"; echo \'{"value": 1}\' > generated.json',
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts: list[MetricArtifact] = []
        for file_path in inputs.file_outputs:
            if file_path.endswith(".json"):
                with open(file_path) as fh:
                    content = json.loads(fh.read())
                drafts.append(
                    MetricArtifact.draft(
                        content=content,
                        original_name=os.path.basename(file_path),
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"output": drafts})


class FailingTestOp(OperationDefinition):
    """Test operation that always fails."""

    name: ClassVar[str] = "failing_test"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {}

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """No inputs to preprocess."""
        return {}

    def execute_function(self, inputs: ExecuteInput) -> dict:
        return {"failed": True}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        return ArtifactResult(success=False, error="Intentional failure")


class ExceptionTestOp(OperationDefinition):
    """Test operation that raises an exception."""

    name: ClassVar[str] = "exception_test"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {}

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """No inputs to preprocess."""
        return {}

    def execute_function(self, inputs: ExecuteInput) -> Any:
        msg = "Intentional exception"
        raise RuntimeError(msg)

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        # This should never be called since execute raises
        return ArtifactResult(success=True)


class MetricOutputTestOp(OperationDefinition):
    """Test operation that produces metric outputs.

    Create MetricArtifact drafts in postprocess.
    """

    class OutputRole(StrEnum):
        scores = auto()

    name: ClassVar[str] = "metric_output_test"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.scores: OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"inputs": []},  # Orphan - no lineage required
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """No inputs to preprocess."""
        return {}

    def execute_function(self, inputs: ExecuteInput) -> dict:
        return {"score": 0.95, "confidence": 0.87}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Create draft MetricArtifacts from in-memory results."""
        raw = inputs.memory_outputs
        drafts = [
            MetricArtifact.draft(
                content={"value": raw["score"]},
                original_name="score",
                step_number=inputs.step_number,
            ),
            MetricArtifact.draft(
                content={"value": raw["confidence"]},
                original_name="confidence",
                step_number=inputs.step_number,
            ),
        ]
        return ArtifactResult(success=True, artifacts={"scores": drafts})


@pytest.fixture
def staging_root(tmp_path):
    """Create staging root directory."""
    staging = tmp_path / "staging"
    staging.mkdir()
    return staging


@pytest.fixture
def working_root(tmp_path):
    """Create working directory for execution sandboxes."""
    work = tmp_path / "working"
    work.mkdir()
    return work


@pytest.fixture
def delta_root_with_input(tmp_path):
    """Create delta root with a single metric artifact."""
    base_path = tmp_path / "delta"
    artifact = MetricArtifact.draft({"score": 0.5}, "input.json", 0)
    artifact.finalize()
    artifact_id = artifact.artifact_id

    _setup_delta_tables(
        base_path,
        metrics=[artifact.to_row()],
        index_entries=[
            {
                "artifact_id": artifact_id,
                "artifact_type": "metric",
                "origin_step_number": 0,
                "metadata": "{}",
            }
        ],
    )

    return base_path, artifact_id


@pytest.fixture
def runtime_env(delta_root_with_input, working_root, staging_root):
    """Create RuntimeEnvironment with configured dependencies."""
    delta_path, _ = delta_root_with_input
    return RuntimeEnvironment(
        delta_root=str(delta_path),
        working_root=str(working_root),
        staging_root=str(staging_root),
    )


class TestRunExecutionFullLifecycle:
    """Tests for the full creator execution lifecycle."""

    def test_execute_full_lifecycle(
        self, delta_root_with_input, working_root, staging_root
    ):
        """The creator lifecycle stages its artifacts and execution evidence."""
        delta_path, input_artifact_id = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )

        unit = ExecutionUnit(
            operation=MetricCopyTestOp(params=MetricCopyTestOp.Params(suffix="_copy")),
            inputs={"source": [input_artifact_id]},  # Batch format: list
            execution_spec_id="spec_123" + "0" * 24,
            step_number=1,
        )

        result = run_creator_flow(unit, config)

        assert result.success is True
        assert result.staging_path is not None
        assert Path(result.staging_path).exists()
        assert (Path(result.staging_path) / "metrics.parquet").exists()
        assert (Path(result.staging_path) / "executions.parquet").exists()
        # Verify execution_run_id was generated
        assert len(result.execution_run_id) == 32

    def test_execute_generative_operation(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Generative operation (no inputs) executes successfully."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )

        unit = ExecutionUnit(
            operation=GenerativeTestOp(params=GenerativeTestOp.Params(count=3)),
            inputs={},  # Empty inputs for generative ops
            execution_spec_id="spec_gen" + "0" * 24,
            step_number=0,
        )

        result = run_creator_flow(unit, config)

        assert result.success is True
        assert result.staging_path is not None
        metrics_path = Path(result.staging_path) / "metrics.parquet"
        assert metrics_path.is_file()
        df = pl.read_parquet(metrics_path)
        assert len(df) == 3
        assert len(result.artifact_ids) == 3

    @pytest.mark.parametrize("fails", [False, True])
    def test_execute_with_worker_id(
        self, delta_root_with_input, working_root, staging_root, fails, monkeypatch
    ):
        """Run ID generation and success/failure recording use runtime identity."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
            worker_id=42,
            worker_id_env_var="WORKER_ID",
        )
        monkeypatch.setenv("WORKER_ID", "7")

        unit = ExecutionUnit(
            operation=(
                FailingTestOp()
                if fails
                else GenerativeTestOp(params=GenerativeTestOp.Params(count=1))
            ),
            inputs={},
            execution_spec_id="spec_worker" + "0" * 21,
            step_number=0,
        )

        result = run_creator_flow(unit, config)

        assert result.success is not fails
        row = pl.read_parquet(Path(result.staging_path) / "executions.parquet").row(
            0, named=True
        )
        assert row["source_worker"] == 42
        assert row["execution_run_id"] == result.execution_run_id
        assert result.execution_run_id == generate_execution_run_id(
            unit.execution_spec_id, row["timestamp_start"], 42
        )
        assert row["success"] is not fails


class TestToolOutputRecording:
    """Successful runs persist the unit log to the executions row."""

    @pytest.mark.skipif(shutil.which("bash") is None, reason="bash not on PATH")
    def test_command_op_success_records_tool_output(
        self, delta_root_with_input, working_root, staging_root
    ):
        """The tool's stdout survives sandbox cleanup into executions.parquet."""
        delta_path, _ = delta_root_with_input
        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )
        unit = ExecutionUnit(
            operation=EchoToolTestOp(),
            inputs={},
            execution_spec_id="spec_echo" + "0" * 23,
            step_number=0,
        )

        result = run_creator_flow(unit, config)

        assert result.success is True
        df = pl.read_parquet(Path(result.staging_path) / "executions.parquet")
        assert "tool stdout line" in df["tool_output"][0]

    def test_function_op_success_records_null_tool_output(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Function ops write no unit log — the column stays null."""
        delta_path, _ = delta_root_with_input
        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )
        unit = ExecutionUnit(
            operation=GenerativeTestOp(params=GenerativeTestOp.Params(count=1)),
            inputs={},
            execution_spec_id="spec_fn00" + "0" * 23,
            step_number=0,
        )

        result = run_creator_flow(unit, config)

        assert result.success is True
        df = pl.read_parquet(Path(result.staging_path) / "executions.parquet")
        assert df["tool_output"][0] is None


class TestRunExecutionFailureHandling:
    """Tests for failure handling in run_creator_flow."""

    def test_execute_failed_operation(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Failed operation stages error record."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )

        unit = ExecutionUnit(
            operation=FailingTestOp(),
            inputs={},
            execution_spec_id="spec_fail" + "0" * 23,
            step_number=1,
        )

        result = run_creator_flow(unit, config)

        # Staging still happens, but with error
        assert result.staging_path is not None
        df = pl.read_parquet(Path(result.staging_path) / "executions.parquet")
        assert df["success"][0] is False
        assert df["error"][0] == "Intentional failure"

    def test_execute_exception_staged_as_error(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Exceptions caught and staged as error records."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )

        unit = ExecutionUnit(
            operation=ExceptionTestOp(),
            inputs={},
            execution_spec_id="spec_exc" + "0" * 24,
            step_number=1,
        )

        result = run_creator_flow(unit, config)

        df = pl.read_parquet(Path(result.staging_path) / "executions.parquet")
        assert df["success"][0] is False
        assert "Intentional exception" in df["error"][0]

    def test_error_message_includes_exception_type(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Error messages include the exception type name."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )

        unit = ExecutionUnit(
            operation=ExceptionTestOp(),
            inputs={},
            execution_spec_id="spec_f4t" + "0" * 24,
            step_number=1,
        )

        result = run_creator_flow(unit, config)

        assert result.success is False
        assert result.error is not None
        assert "RuntimeError" in result.error

    def test_setup_failure_returns_staging_result(
        self, delta_root_with_input, staging_root
    ):
        """Setup failure (no working_root) returns StagingResult(success=False)."""
        delta_path, _ = delta_root_with_input

        # No working_root — forces setup to fail
        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=None,
            staging_root=str(staging_root),
        )

        unit = ExecutionUnit(
            operation=GenerativeTestOp(params=GenerativeTestOp.Params(count=1)),
            inputs={},
            execution_spec_id="spec_setup_fail" + "0" * 17,
            step_number=0,
        )

        result = run_creator_flow(unit, config)

        assert result.success is False
        assert result.error is not None
        assert "working_root" in result.error


class _EnvelopeReturningRouter(ExecuteRouter):
    """Router that returns an ArtisanError as a raw result.

    Mirrors the endpoint batch contract: ``EndpointExecuteRouter._call_one``
    catches the client's re-raised ``ArtisanError`` and returns it as a
    ``raw_results`` entry rather than raising.
    """

    def __init__(self, error: ArtisanError) -> None:
        self._error = error

    def route_execute(self, operation, execute_inputs, sandbox_root):
        return [self._error]


class TestEndpointDoubleHop:
    """Regression: a returned ArtisanError survives to executions.error_envelope.

    Pins the ``creator.py`` ``from failures[0]`` chaining — without it the
    batch-path ``_ExecuteFailure`` buries the client envelope and this
    column persists NULL.
    """

    def test_returned_artisan_error_persists_envelope(
        self, delta_root_with_input, working_root, staging_root
    ):
        delta_path, _ = delta_root_with_input
        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )
        worker_error = ArtisanError(
            code=ErrorCode.OP_EXECUTE_FAILED,
            message="worker tool exploded",
            error_type="compute",
            operation_name="generative_test",
            recovery_hint="REPORT_TO_USER",
        )
        unit = ExecutionUnit(
            operation=GenerativeTestOp(params=GenerativeTestOp.Params(count=1)),
            inputs={},
            execution_spec_id="spec_dhop" + "0" * 23,
            step_number=1,
        )

        result = run_creator_flow(
            unit, config, execute_router=_EnvelopeReturningRouter(worker_error)
        )

        assert result.success is False
        df = pl.read_parquet(Path(result.staging_path) / "executions.parquet")
        raw = df["error_envelope"][0]
        assert raw is not None
        env = ArtisanErrorEnvelope.model_validate_json(raw)
        assert env.code == "op_execute_failed"
        assert env.recovery_hint == "REPORT_TO_USER"


class TestRunExecutionMetricOutputs:
    """Tests for metric output handling."""

    def test_execute_metric_outputs(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Metric outputs are captured and staged."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )

        unit = ExecutionUnit(
            operation=MetricOutputTestOp(),
            inputs={},
            execution_spec_id="spec_metrics" + "0" * 20,
            step_number=1,
        )

        result = run_creator_flow(unit, config)

        assert result.success is True
        assert (Path(result.staging_path) / "metrics.parquet").exists()

        df = pl.read_parquet(Path(result.staging_path) / "metrics.parquet")
        # Each memory output key creates a separate MetricArtifact (score, confidence)
        assert len(df) == 2


class TestRunExecutionStagedOutput:
    """Tests for staged output content."""

    def test_staged_execution_edges_has_input_output_rows(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Execution edges contain inputs/outputs with correct mappings."""
        delta_path, input_artifact_id = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )

        unit = ExecutionUnit(
            operation=MetricCopyTestOp(params=MetricCopyTestOp.Params(suffix="_copy")),
            inputs={"source": [input_artifact_id]},  # Batch format: list
            execution_spec_id="spec_io" + "0" * 25,
            step_number=1,
        )

        result = run_creator_flow(unit, config)

        # Check execution_edges.parquet has the input/output rows
        df = pl.read_parquet(Path(result.staging_path) / "execution_edges.parquet")

        inputs = df.filter(pl.col("direction") == "input")
        assert len(inputs) >= 1
        # Each entry should have role and artifact_id
        assert "role" in inputs.columns
        assert "artifact_id" in inputs.columns

        outputs = df.filter(pl.col("direction") == "output")
        assert len(outputs) >= 1
        assert "role" in outputs.columns
        assert "artifact_id" in outputs.columns

    def test_staged_artifact_index_has_entries(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Artifact index contains entries for all staged artifacts."""
        delta_path, input_artifact_id = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )

        unit = ExecutionUnit(
            operation=MetricCopyTestOp(params=MetricCopyTestOp.Params(suffix="_copy")),
            inputs={"source": [input_artifact_id]},  # Batch format: list
            execution_spec_id="spec_idx" + "0" * 24,
            step_number=1,
        )

        result = run_creator_flow(unit, config)

        df = pl.read_parquet(Path(result.staging_path) / "index.parquet")

        # Should have one entry for the copied metric
        assert len(df) == 1
        assert df["artifact_type"][0] == "metric"
        assert df["origin_step_number"][0] == 1


class TestRunCreatorLifecycle:
    """Tests for the extracted run_creator_lifecycle function."""

    def test_lifecycle_returns_lifecycle_result(
        self, delta_root_with_input, working_root, staging_root
    ):
        """run_creator_lifecycle returns a LifecycleResult with correct structure."""
        delta_path, input_artifact_id = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )

        unit = ExecutionUnit(
            operation=MetricCopyTestOp(params=MetricCopyTestOp.Params(suffix="_copy")),
            inputs={"source": [input_artifact_id]},
            execution_spec_id="spec_lc1" + "0" * 24,
            step_number=1,
        )

        result = run_creator_lifecycle(unit, config)

        assert isinstance(result, LifecycleResult)
        assert "source" in result.input_artifacts
        assert len(result.input_artifacts["source"]) == 1
        assert "output" in result.artifacts
        assert len(result.artifacts["output"]) == 1
        assert len(result.edges) >= 1
        assert "setup" in result.timings
        assert "preprocess" in result.timings
        assert "execute" in result.timings
        assert "postprocess" in result.timings
        assert "lineage" in result.timings

    def test_lifecycle_generative_returns_artifacts(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Generative operation returns output artifacts with no input artifacts."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )

        unit = ExecutionUnit(
            operation=GenerativeTestOp(params=GenerativeTestOp.Params(count=3)),
            inputs={},
            execution_spec_id="spec_lc2" + "0" * 24,
            step_number=0,
        )

        result = run_creator_lifecycle(unit, config)

        assert isinstance(result, LifecycleResult)
        assert result.input_artifacts == {}
        assert "output" in result.artifacts
        assert len(result.artifacts["output"]) == 3

    def test_lifecycle_raises_on_failure(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Lifecycle raises when postprocess reports failure."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )

        unit = ExecutionUnit(
            operation=FailingTestOp(),
            inputs={},
            execution_spec_id="spec_lc3" + "0" * 24,
            step_number=1,
        )

        with pytest.raises(Exception, match="Intentional failure"):
            run_creator_lifecycle(unit, config)

    def test_lifecycle_raises_on_execute_exception(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Lifecycle raises when execute() throws."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
        )

        unit = ExecutionUnit(
            operation=ExceptionTestOp(),
            inputs={},
            execution_spec_id="spec_lc4" + "0" * 24,
            step_number=1,
        )

        with pytest.raises(Exception, match="Intentional exception"):
            run_creator_lifecycle(unit, config)


class TestSandboxPathComputation:
    """Tests for sandbox path layout selection in run_creator_lifecycle."""

    def test_sandbox_path_flat_for_temp(
        self, delta_root_with_input, staging_root, tmp_path, monkeypatch
    ):
        """When working_root matches gettempdir(), sandbox is flat."""
        import tempfile as tempfile_mod

        delta_path, _ = delta_root_with_input
        fake_tmp = tmp_path / "fake_tmp"
        fake_tmp.mkdir()
        monkeypatch.setattr(tempfile_mod, "tempdir", str(fake_tmp))

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=tempfile_mod.gettempdir(),
            staging_root=str(staging_root),
            preserve_working=True,
        )

        unit = ExecutionUnit(
            operation=GenerativeTestOp(params=GenerativeTestOp.Params(count=1)),
            inputs={},
            execution_spec_id="spec_flat" + "0" * 24,
            step_number=1,
        )

        run_creator_lifecycle(unit, config)

        # Sandbox should be directly under fake_tmp (flat, no step/shard dirs)
        sandbox_dirs = [d for d in fake_tmp.iterdir() if d.is_dir()]
        assert len(sandbox_dirs) == 1
        assert sandbox_dirs[0].parent == fake_tmp

    def test_sandbox_path_sharded_for_custom_root(
        self, delta_root_with_input, working_root, staging_root
    ):
        """When working_root is user-specified, sandbox uses sharded layout."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working_root),
            staging_root=str(staging_root),
            preserve_working=True,
        )

        unit = ExecutionUnit(
            operation=GenerativeTestOp(params=GenerativeTestOp.Params(count=1)),
            inputs={},
            execution_spec_id="spec_shard" + "0" * 23,
            step_number=1,
        )

        run_creator_lifecycle(unit, config)

        # Sandbox should be sharded: working_root / step_dir / xx / yy / run_id
        step_dirs = [d for d in working_root.iterdir() if d.is_dir()]
        assert len(step_dirs) == 1
        assert step_dirs[0].name.startswith("1_")


@pytest.mark.parametrize("failure", [None, "execute", "postprocess"])
def test_creator_records_commands_before_later_failures(
    runtime_env, monkeypatch, failure
):
    import sys

    from artisan.schemas.execution.command_record import CommandRecording
    from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
    from artisan.utils.external_tools import run_command

    def execute(self, inputs):
        for _ in range(2):
            run_command(LocalEnvironmentSpec(), [sys.executable, "-c", "pass"])
        if failure == "execute":
            msg = "later execution failed"
            raise RuntimeError(msg)
        return {"score": 0.95, "confidence": 0.87}

    monkeypatch.setattr(MetricOutputTestOp, "execute_function", execute)
    if failure == "postprocess":
        monkeypatch.setattr(
            MetricOutputTestOp,
            "postprocess",
            lambda *args: (_ for _ in ()).throw(RuntimeError("postprocess failed")),
        )
    result = run_creator_flow(
        ExecutionUnit(operation=MetricOutputTestOp(), inputs={}, step_number=0),
        runtime_env,
    )
    assert result.success is (failure is None)
    row = pl.read_parquet(Path(result.staging_path) / "executions.parquet").row(
        0, named=True
    )
    recording = CommandRecording.model_validate_json(row["command_recording"])
    assert [(c.invocation, c.sequence) for c in recording.commands] == [(0, 0), (0, 1)]
    assert all(c.outcome == "succeeded" for c in recording.commands)


def test_failed_launch_error_and_chained_traceback_are_redacted(
    runtime_env, monkeypatch
):
    from artisan.schemas.execution.command_record import CommandRecording
    from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
    from artisan.utils.external_tools import run_command

    def execute(self, inputs):
        run_command(
            LocalEnvironmentSpec(),
            ["/missing/opaque-credential"],
            sensitive_values=("opaque-credential",),
        )

    monkeypatch.setattr(MetricOutputTestOp, "execute_function", execute)
    result = run_creator_flow(
        ExecutionUnit(operation=MetricOutputTestOp(), inputs={}, step_number=0),
        runtime_env,
    )
    assert result.success is False
    row = pl.read_parquet(Path(result.staging_path) / "executions.parquet").row(
        0, named=True
    )
    assert "opaque-credential" not in row["error"]
    assert "opaque-credential" not in result.error
    recording = CommandRecording.model_validate_json(row["command_recording"])
    assert recording.commands[0].outcome == "launch_failed"
    assert recording.commands[0].argv == ["/missing/<redacted>"]
