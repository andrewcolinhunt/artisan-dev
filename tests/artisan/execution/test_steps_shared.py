"""Tests for shared atomic step functions.

Reference: design_unified-execution-steps.md

These tests verify each shared step function independently.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
from fsspec.implementations.local import LocalFileSystem

from artisan.execution.staging.recorder import (
    build_execution_edges,
    record_execution_failure,
    record_execution_success,
)
from artisan.execution.utils import generate_execution_run_id
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


def _local_fs() -> LocalFileSystem:
    """Create a local filesystem instance for test construction."""
    return LocalFileSystem()


# =============================================================================
# Test Fixtures: Mock Operations
# =============================================================================


class MockCreatorOp(OperationDefinition):
    """Mock creator operation for testing."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_creator"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.DATA, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    tolerance: float = 0.1
    max_steps: int = 100

    def preprocess(self, inputs: Any) -> dict[str, Any]:
        return {}

    def execute(self, inputs):
        return {"status": "ok"}


class MockFileRefInputOp(OperationDefinition):
    """Mock operation that accepts FILE_REF type inputs."""

    class InputRole(StrEnum):
        files = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_file_ref_input"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.files: InputSpec(artifact_type=ArtifactTypes.FILE_REF, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": ["files"]},
        ),
    }

    def preprocess(self, inputs: Any) -> dict[str, Any]:
        return {}

    def execute(self, inputs):
        return {"status": "ok"}


class MockNoDefaultsOp(OperationDefinition):
    """Mock operation with no default parameters."""

    name: ClassVar[str] = "mock_no_defaults"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {}

    def execute(self, inputs):
        return {}


# =============================================================================
# Test Classes
# =============================================================================


class TestGenerateExecutionRunId:
    """Tests for generate_execution_run_id()."""

    def test_returns_32_char_hex(self):
        """Returns 32-character hexadecimal string."""
        run_id = generate_execution_run_id(
            spec_id="a" * 32,
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            worker_id=0,
        )

        assert len(run_id) == 32
        assert all(c in "0123456789abcdef" for c in run_id)

    def test_deterministic(self):
        """Same inputs produce same output."""
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        run_id_1 = generate_execution_run_id("a" * 32, ts, 0)
        run_id_2 = generate_execution_run_id("a" * 32, ts, 0)

        assert run_id_1 == run_id_2

    def test_different_spec_id_different_hash(self):
        """Different spec_id produces different hash."""
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        run_id_1 = generate_execution_run_id("a" * 32, ts, 0)
        run_id_2 = generate_execution_run_id("b" * 32, ts, 0)

        assert run_id_1 != run_id_2

    def test_different_timestamp_different_hash(self):
        """Different timestamp produces different hash."""
        ts_1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        ts_2 = datetime(2024, 1, 1, 12, 0, 1, tzinfo=UTC)

        run_id_1 = generate_execution_run_id("a" * 32, ts_1, 0)
        run_id_2 = generate_execution_run_id("a" * 32, ts_2, 0)

        assert run_id_1 != run_id_2

    def test_different_worker_id_different_hash(self):
        """Different worker_id produces different hash."""
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        run_id_1 = generate_execution_run_id("a" * 32, ts, 0)
        run_id_2 = generate_execution_run_id("a" * 32, ts, 1)

        assert run_id_1 != run_id_2


class TestBuildExecutionEdges:
    """Tests for build_execution_edges() — returns pl.DataFrame."""

    def test_builds_input_rows_from_single_item_batch(self):
        """Builds input provenance rows from single-item batch."""
        inputs = {"data": ["a" * 32]}
        df = build_execution_edges("run123", inputs, outputs={})

        assert len(df) == 1
        row = df.row(0, named=True)
        assert row == {
            "execution_run_id": "run123",
            "direction": "input",
            "role": "data",
            "artifact_id": "a" * 32,
        }

    def test_builds_input_rows_from_multi_item_batch(self):
        """Builds input provenance rows from multi-item batch."""
        inputs = {"data": ["a" * 32, "b" * 32]}
        df = build_execution_edges("run123", inputs, outputs={})

        assert len(df) == 2
        assert df["artifact_id"][0] == "a" * 32
        assert df["artifact_id"][1] == "b" * 32

    def test_builds_output_rows(self):
        """Builds output provenance rows from outputs dict."""
        outputs = {"output": ["c" * 32, "d" * 32]}
        df = build_execution_edges("run123", inputs={}, outputs=outputs)

        assert len(df) == 2
        row = df.row(0, named=True)
        assert row == {
            "execution_run_id": "run123",
            "direction": "output",
            "role": "output",
            "artifact_id": "c" * 32,
        }
        assert df["artifact_id"][1] == "d" * 32

    def test_combined_input_and_output(self):
        """Builds both input and output rows together."""
        inputs = {"data": ["a" * 32]}
        outputs = {"output": ["b" * 32]}
        df = build_execution_edges("run123", inputs, outputs)

        input_df = df.filter(pl.col("direction") == "input")
        output_df = df.filter(pl.col("direction") == "output")

        assert len(input_df) == 1
        assert len(output_df) == 1

    def test_multiple_output_roles(self):
        """Multiple output roles are recorded with correct role names."""
        outputs = {
            "passthrough": ["p" * 32, "q" * 32],
            "metric": ["m" * 32],
        }
        df = build_execution_edges("run123", inputs={}, outputs=outputs)

        assert len(df) == 3
        passthrough_df = df.filter(pl.col("role") == "passthrough")
        assert len(passthrough_df) == 2
        assert set(passthrough_df["artifact_id"].to_list()) == {"p" * 32, "q" * 32}

    def test_outputs_combined_with_inputs(self):
        """Outputs are added alongside inputs."""
        inputs = {"data": ["a" * 32]}
        outputs = {"metric": ["m" * 32], "passthrough": ["p" * 32]}
        df = build_execution_edges("run123", inputs, outputs)

        assert len(df) == 3
        input_df = df.filter(pl.col("direction") == "input")
        output_df = df.filter(pl.col("direction") == "output")

        assert len(input_df) == 1
        assert len(output_df) == 2

    def test_empty_outputs_adds_no_rows(self):
        """Empty outputs dict adds no rows."""
        df = build_execution_edges("run123", inputs={}, outputs={})
        assert df.is_empty()

    def test_returns_dataframe_type(self):
        """Return type is pl.DataFrame with correct columns."""
        df = build_execution_edges("run123", inputs={}, outputs={})
        assert isinstance(df, pl.DataFrame)
        assert df.columns == [
            "execution_run_id",
            "direction",
            "role",
            "artifact_id",
        ]


class TestRecordExecutionSuccess:
    """Tests for record_execution_success()."""

    def test_records_success_with_artifacts(self, tmp_path):
        """Records successful execution with artifacts."""

        from artisan.schemas.execution.execution_context import ExecutionContext
        from artisan.storage.core.artifact_store import ArtifactStore

        staging_root = tmp_path / "staging"
        staging_root.mkdir(parents=True, exist_ok=True)
        delta_root = tmp_path / "delta"
        delta_root.mkdir(parents=True, exist_ok=True)

        execution_context = ExecutionContext(
            execution_run_id="run" + "0" * 29,
            execution_spec_id="spec" + "0" * 28,
            step_number=1,
            timestamp_start=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            worker_id=0,
            artifact_store=ArtifactStore(str(delta_root), fs=_local_fs()),
            staging_root=str(staging_root),
            fs=_local_fs(),
            operation_name="test_op",
            operation=MockCreatorOp(),
            sandbox_path=None,
            compute_backend="local",
        )

        result = record_execution_success(
            execution_context=execution_context,
            artifacts={},
            lineage_edges=[],
            inputs={},
            timestamp_end=datetime(2024, 1, 1, 12, 0, 1, tzinfo=UTC),
            params={},
        )

        assert result.success is True
        assert result.staging_path is not None
        assert Path(result.staging_path).exists()
        assert (Path(result.staging_path) / "executions.parquet").exists()

    def test_records_input_provenance(self, tmp_path):
        """Records input provenance for debugging."""
        import polars as pl

        from artisan.schemas.execution.execution_context import ExecutionContext
        from artisan.storage.core.artifact_store import ArtifactStore

        staging_root = tmp_path / "staging"
        staging_root.mkdir(parents=True, exist_ok=True)
        delta_root = tmp_path / "delta"
        delta_root.mkdir(parents=True, exist_ok=True)

        execution_context = ExecutionContext(
            execution_run_id="run" + "0" * 29,
            execution_spec_id="spec" + "0" * 28,
            step_number=1,
            timestamp_start=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            worker_id=0,
            artifact_store=ArtifactStore(str(delta_root), fs=_local_fs()),
            staging_root=str(staging_root),
            fs=_local_fs(),
            operation_name="test_op",
            operation=MockCreatorOp(),
            sandbox_path=None,
            compute_backend="local",
        )

        result = record_execution_success(
            execution_context=execution_context,
            artifacts={},
            lineage_edges=[],
            inputs={"data": ["input_id_" + "0" * 23]},
            timestamp_end=datetime(2024, 1, 1, 12, 0, 1, tzinfo=UTC),
        )

        # Verify input provenance was recorded
        prov_path = Path(result.staging_path) / "execution_edges.parquet"
        assert prov_path.exists()
        df = pl.read_parquet(prov_path)
        assert len(df) == 1
        assert df["direction"][0] == "input"


class TestRecordExecutionFailure:
    """Tests for record_execution_failure()."""

    def test_records_failure_with_error(self, tmp_path):
        """Records failed execution with error message."""
        import polars as pl

        from artisan.schemas.execution.execution_context import ExecutionContext
        from artisan.storage.core.artifact_store import ArtifactStore

        staging_root = tmp_path / "staging"
        staging_root.mkdir(parents=True, exist_ok=True)
        delta_root = tmp_path / "delta"
        delta_root.mkdir(parents=True, exist_ok=True)

        execution_context = ExecutionContext(
            execution_run_id="run" + "0" * 29,
            execution_spec_id="spec" + "0" * 28,
            step_number=1,
            timestamp_start=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            worker_id=0,
            artifact_store=ArtifactStore(str(delta_root), fs=_local_fs()),
            staging_root=str(staging_root),
            fs=_local_fs(),
            operation_name="test_op",
            operation=MockCreatorOp(),
            sandbox_path=None,
            compute_backend="local",
        )

        result = record_execution_failure(
            execution_context=execution_context,
            error="Test error message",
            inputs={"data": ["input_id_" + "0" * 23]},
            timestamp_end=datetime(2024, 1, 1, 12, 0, 1, tzinfo=UTC),
        )

        # StagingResult.success reflects execution success (False for failures)
        assert result.success is False
        assert result.error == "Test error message"
        assert result.artifact_ids == []

        # Verify error was recorded
        exec_path = Path(result.staging_path) / "executions.parquet"
        df = pl.read_parquet(exec_path)
        assert df["success"][0] is False
        assert df["error"][0] == "Test error message"

    def test_records_input_provenance_on_failure(self, tmp_path):
        """Records input provenance even on failure (aids debugging)."""
        import polars as pl

        from artisan.schemas.execution.execution_context import ExecutionContext
        from artisan.storage.core.artifact_store import ArtifactStore

        staging_root = tmp_path / "staging"
        staging_root.mkdir(parents=True, exist_ok=True)
        delta_root = tmp_path / "delta"
        delta_root.mkdir(parents=True, exist_ok=True)

        execution_context = ExecutionContext(
            execution_run_id="run" + "0" * 29,
            execution_spec_id="spec" + "0" * 28,
            step_number=1,
            timestamp_start=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            worker_id=0,
            artifact_store=ArtifactStore(str(delta_root), fs=_local_fs()),
            staging_root=str(staging_root),
            fs=_local_fs(),
            operation_name="test_op",
            operation=MockCreatorOp(),
            sandbox_path=None,
            compute_backend="local",
        )

        result = record_execution_failure(
            execution_context=execution_context,
            error="Test error",
            inputs={"data": ["input_a" + "0" * 24, "input_b" + "0" * 24]},
            timestamp_end=datetime(2024, 1, 1, 12, 0, 1, tzinfo=UTC),
        )

        # Verify input provenance was recorded
        prov_path = Path(result.staging_path) / "execution_edges.parquet"
        assert prov_path.exists()
        df = pl.read_parquet(prov_path)
        assert len(df) == 2  # Both inputs recorded
        assert df["direction"].to_list() == ["input", "input"]
