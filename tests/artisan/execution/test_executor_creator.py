"""Tests for run_creator_flow function.

Reference: Phase 4c - Executor (v2)
Reference: design_execution_api_refactor_v2.md
Reference: design_execution_unit_refactor.md
Reference: design_provenance_phase3_execution.md
Reference: v4 design - artifact-centric execution model
"""

from __future__ import annotations

import json
from datetime import datetime
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock

import polars as pl
import pytest
import xxhash

from artisan.execution.executors.creator import (
    LifecycleResult,
    run_creator_flow,
    run_creator_lifecycle,
)
from artisan.execution.lineage.enrich import (
    build_artifact_edges_from_store,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.utils import generate_execution_run_id
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.provenance.source_target_pair import SourceTargetPair
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.path import shard_path


def compute_artifact_id(content: bytes) -> str:
    """Compute xxh3_128 hash for content-addressed ID."""
    return xxhash.xxh3_128(content).hexdigest()


def _setup_delta_tables(
    base_path: Path,
    metrics: list[dict] | None = None,
    index_entries: list[dict] | None = None,
):
    """Minimal helper to set up Delta Lake tables for testing."""
    from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA

    if metrics:
        metrics_path = base_path / "artifacts/metrics"
        df = pl.DataFrame(metrics, schema=MetricArtifact.POLARS_SCHEMA)
        df.write_delta(str(metrics_path))

    if index_entries:
        index_path = base_path / "artifacts/index"
        df = pl.DataFrame(index_entries, schema=ARTIFACT_INDEX_SCHEMA)
        df.write_delta(str(index_path))


# =============================================================================
# Test Operations (v4 API - artifact-centric)
# =============================================================================


class MetricCopyTestOp(OperationDefinition):
    """Test operation that copies input metric with a modification.

    v4 API: Uses input_artifacts with materialized_path, returns draft Artifacts.
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

    suffix: str = "_copy"

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Extract materialized paths from input artifacts."""
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict:
        source_paths = inputs.inputs["source"]
        source_path = (
            source_paths[0] if isinstance(source_paths, list) else source_paths
        )

        # Read content and write with suffix
        content = json.loads(source_path.read_text())
        content["copied"] = True
        output_path = inputs.execute_dir / f"{source_path.stem}{self.suffix}.json"
        output_path.write_text(json.dumps(content))

        return {"copied": True}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """v4: Create draft MetricArtifacts from file_outputs."""
        drafts: list[MetricArtifact] = []
        for file_path in inputs.file_outputs:
            if file_path.suffix == ".json":
                drafts.append(
                    MetricArtifact.draft(
                        content=json.loads(file_path.read_text()),
                        original_name=file_path.name,
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"output": drafts})


class GenerativeTestOp(OperationDefinition):
    """Test operation that generates output without inputs.

    v4 API: Returns draft Artifacts with infer_lineage_from={"inputs": []} (orphan).
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

    count: int = 1

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Generative operation - no inputs to preprocess."""
        return {}

    def execute(self, inputs: ExecuteInput) -> dict:
        for i in range(self.count):
            content = json.dumps({"value": i})
            output_path = inputs.execute_dir / f"generated_{i:03d}.json"
            output_path.write_text(content)

        return {"generated": self.count}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """v4: Create draft MetricArtifacts from file_outputs."""
        drafts: list[MetricArtifact] = []
        for file_path in inputs.file_outputs:
            if file_path.suffix == ".json":
                drafts.append(
                    MetricArtifact.draft(
                        content=json.loads(file_path.read_text()),
                        original_name=file_path.name,
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

    def execute(self, inputs: ExecuteInput) -> dict:
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

    def execute(self, inputs: ExecuteInput) -> Any:
        raise RuntimeError("Intentional exception")

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        # This should never be called since execute raises
        return ArtifactResult(success=True)


class MetricOutputTestOp(OperationDefinition):
    """Test operation that produces metric outputs.

    v4 API: Creates MetricArtifact drafts in postprocess.
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

    def execute(self, inputs: ExecuteInput) -> dict:
        return {"score": 0.95, "confidence": 0.87}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """v4: Create draft MetricArtifacts from memory_outputs."""
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


# =============================================================================
# Fixtures
# =============================================================================


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
    content = json.dumps({"score": 0.5}).encode("utf-8")
    artifact_id = compute_artifact_id(content)

    _setup_delta_tables(
        base_path,
        metrics=[
            {
                "artifact_id": artifact_id,
                "origin_step_number": 0,
                "content": content,
                "original_name": "input",
                "extension": ".json",
                "metadata": "{}",
                "external_path": None,
            }
        ],
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
        delta_root_path=delta_path,
        working_root_path=working_root,
        staging_root_path=staging_root,
    )


# =============================================================================
# Tests
# =============================================================================


class TestGenerateExecutionRunId:
    """Tests for execution_run_id generation."""

    def test_generates_32_char_hex(self):
        """Run ID should be 32 hex characters."""
        run_id = generate_execution_run_id("spec_123", datetime.now())
        assert len(run_id) == 32
        assert all(c in "0123456789abcdef" for c in run_id)

    def test_different_timestamps_different_ids(self):
        """Different timestamps produce different IDs."""
        t1 = datetime(2024, 1, 1, 12, 0, 0)
        t2 = datetime(2024, 1, 1, 12, 0, 1)

        id1 = generate_execution_run_id("spec_123", t1)
        id2 = generate_execution_run_id("spec_123", t2)

        assert id1 != id2

    def test_different_specs_different_ids(self):
        """Different spec IDs produce different IDs."""
        t = datetime.now()
        id1 = generate_execution_run_id("spec_a", t)
        id2 = generate_execution_run_id("spec_b", t)

        assert id1 != id2

    def test_includes_worker_id(self):
        """Worker ID affects the run ID."""
        t = datetime.now()
        id1 = generate_execution_run_id("spec_123", t, worker_id=0)
        id2 = generate_execution_run_id("spec_123", t, worker_id=1)

        assert id1 != id2


class TestShardPath:
    """Tests for shard_path helper function."""

    def test_creates_two_level_sharding(self):
        """shard_path creates two-level sharding."""
        root = Path("/tmp/staging")
        run_id = "abcdef1234567890abcdef1234567890"

        result = shard_path(root, run_id)

        assert result == root / "ab" / "cd" / run_id

    def test_handles_short_hash(self):
        """shard_path works with shorter hashes."""
        root = Path("/tmp")
        run_id = "abc"  # Short hash

        result = shard_path(root, run_id)

        # Uses first 4 chars: ab/c/abc
        assert result == root / "ab" / "c" / run_id


class TestRunExecutionFullLifecycle:
    """Tests for full 12-step execution lifecycle."""

    def test_execute_full_lifecycle(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Full 12-step lifecycle produces staged output."""
        delta_path, input_artifact_id = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root_path=delta_path,
            working_root_path=working_root,
            staging_root_path=staging_root,
        )

        unit = ExecutionUnit(
            operation=MetricCopyTestOp(suffix="_copy"),
            inputs={"source": [input_artifact_id]},  # Batch format: list
            execution_spec_id="spec_123" + "0" * 24,
            step_number=1,
        )

        result = run_creator_flow(unit, config)

        assert result.success is True
        assert result.staging_path is not None
        assert result.staging_path.exists()
        assert (result.staging_path / "metrics.parquet").exists()
        assert (result.staging_path / "executions.parquet").exists()
        # Verify execution_run_id was generated
        assert len(result.execution_run_id) == 32

    def test_execute_generative_operation(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Generative operation (no inputs) executes successfully."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root_path=delta_path,
            working_root_path=working_root,
            staging_root_path=staging_root,
        )

        unit = ExecutionUnit(
            operation=GenerativeTestOp(count=3),
            inputs={},  # Empty inputs for generative ops
            execution_spec_id="spec_gen" + "0" * 24,
            step_number=0,
        )

        result = run_creator_flow(unit, config)

        assert result.success is True
        # Check that metrics were staged
        if result.staging_path and (result.staging_path / "metrics.parquet").exists():
            df = pl.read_parquet(result.staging_path / "metrics.parquet")
            assert len(df) == 3  # 3 generated metrics
            assert len(result.artifact_ids) == 3

    def test_execute_with_worker_id(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Worker ID is included in execution."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root_path=delta_path,
            working_root_path=working_root,
            staging_root_path=staging_root,
        )

        unit = ExecutionUnit(
            operation=GenerativeTestOp(count=1),
            inputs={},
            execution_spec_id="spec_worker" + "0" * 21,
            step_number=0,
        )

        result = run_creator_flow(unit, config, worker_id=42)

        assert result.success is True


class TestRunExecutionFailureHandling:
    """Tests for failure handling in run_creator_flow."""

    def test_execute_failed_operation(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Failed operation stages error record."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root_path=delta_path,
            working_root_path=working_root,
            staging_root_path=staging_root,
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
        df = pl.read_parquet(result.staging_path / "executions.parquet")
        assert df["success"][0] is False
        assert df["error"][0] == "Intentional failure"

    def test_execute_exception_staged_as_error(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Exceptions caught and staged as error records."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root_path=delta_path,
            working_root_path=working_root,
            staging_root_path=staging_root,
        )

        unit = ExecutionUnit(
            operation=ExceptionTestOp(),
            inputs={},
            execution_spec_id="spec_exc" + "0" * 24,
            step_number=1,
        )

        result = run_creator_flow(unit, config)

        df = pl.read_parquet(result.staging_path / "executions.parquet")
        assert df["success"][0] is False
        assert "Intentional exception" in df["error"][0]

    def test_error_message_includes_exception_type(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Error messages include the exception type name (F4)."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root_path=delta_path,
            working_root_path=working_root,
            staging_root_path=staging_root,
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

        # No working_root_path — forces setup to fail
        config = RuntimeEnvironment(
            delta_root_path=delta_path,
            working_root_path=None,
            staging_root_path=staging_root,
        )

        unit = ExecutionUnit(
            operation=GenerativeTestOp(count=1),
            inputs={},
            execution_spec_id="spec_setup_fail" + "0" * 17,
            step_number=0,
        )

        result = run_creator_flow(unit, config)

        assert result.success is False
        assert result.error is not None
        assert "working_root_path" in result.error


class TestRunExecutionMetricOutputs:
    """Tests for metric output handling."""

    def test_execute_metric_outputs(
        self, delta_root_with_input, working_root, staging_root
    ):
        """Metric outputs are captured and staged."""
        delta_path, _ = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root_path=delta_path,
            working_root_path=working_root,
            staging_root_path=staging_root,
        )

        unit = ExecutionUnit(
            operation=MetricOutputTestOp(),
            inputs={},
            execution_spec_id="spec_metrics" + "0" * 20,
            step_number=1,
        )

        result = run_creator_flow(unit, config)

        assert result.success is True
        assert (result.staging_path / "metrics.parquet").exists()

        df = pl.read_parquet(result.staging_path / "metrics.parquet")
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
            delta_root_path=delta_path,
            working_root_path=working_root,
            staging_root_path=staging_root,
        )

        unit = ExecutionUnit(
            operation=MetricCopyTestOp(suffix="_copy"),
            inputs={"source": [input_artifact_id]},  # Batch format: list
            execution_spec_id="spec_io" + "0" * 25,
            step_number=1,
        )

        result = run_creator_flow(unit, config)

        # Check execution_edges.parquet has the input/output rows
        df = pl.read_parquet(result.staging_path / "execution_edges.parquet")

        # Check inputs
        inputs = df.filter(pl.col("direction") == "input")
        assert len(inputs) >= 1
        # Each entry should have role and artifact_id
        assert "role" in inputs.columns
        assert "artifact_id" in inputs.columns

        # Check outputs
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
            delta_root_path=delta_path,
            working_root_path=working_root,
            staging_root_path=staging_root,
        )

        unit = ExecutionUnit(
            operation=MetricCopyTestOp(suffix="_copy"),
            inputs={"source": [input_artifact_id]},  # Batch format: list
            execution_spec_id="spec_idx" + "0" * 24,
            step_number=1,
        )

        result = run_creator_flow(unit, config)

        df = pl.read_parquet(result.staging_path / "index.parquet")

        # Should have one entry for the copied metric
        assert len(df) == 1
        assert df["artifact_type"][0] == "metric"
        assert df["origin_step_number"][0] == 1


class TestRuntimeEnvironment:
    """Tests for RuntimeEnvironment model."""

    def test_config_is_frozen(self, tmp_path):
        """RuntimeEnvironment should be immutable."""
        env = RuntimeEnvironment(
            delta_root_path=tmp_path / "delta",
            working_root_path=tmp_path / "working",
            staging_root_path=tmp_path / "staging",
        )

        # Should raise ValidationError when trying to modify
        with pytest.raises(Exception):  # pydantic.ValidationError
            env.delta_root_path = tmp_path / "other"

    def test_config_requires_all_paths(self, tmp_path):
        """RuntimeEnvironment requires all three paths."""
        with pytest.raises(Exception):  # pydantic.ValidationError
            RuntimeEnvironment(
                delta_root_path=tmp_path / "delta",
                working_root_path=tmp_path / "working",
                # missing staging_root_path
            )


class TestBuildArtifactEdgesFromStore:
    """Tests for build_artifact_edges_from_store helper."""

    def test_enriches_source_target_pairs(self):
        """Test that SourceTargetPairs are enriched to ArtifactProvenance."""
        mock_store = MagicMock()
        mock_store.provenance.load_type_map.return_value = {
            "s" * 32: ArtifactTypes.METRIC,
            "t" * 32: ArtifactTypes.METRIC,
        }

        pairs = [
            SourceTargetPair(
                source="s" * 32,
                target="t" * 32,
                source_role="input",
                target_role="energy",
            )
        ]

        result = build_artifact_edges_from_store(
            source_target_pairs=pairs,
            execution_run_id="e" * 32,
            artifact_store=mock_store,
        )

        assert len(result) == 1
        prov = result[0]
        assert prov.execution_run_id == "e" * 32
        assert prov.source_artifact_id == "s" * 32
        assert prov.target_artifact_id == "t" * 32
        assert prov.source_artifact_type == "metric"
        assert prov.target_artifact_type == "metric"
        assert prov.source_role == "input"
        assert prov.target_role == "energy"

    def test_enriches_multiple_pairs(self):
        """Test that multiple SourceTargetPairs are enriched."""
        mock_store = MagicMock()
        mock_store.provenance.load_type_map.return_value = {
            "a" * 32: ArtifactTypes.METRIC,
            "b" * 32: ArtifactTypes.METRIC,
            "c" * 32: ArtifactTypes.METRIC,
        }

        pairs = [
            SourceTargetPair(
                source="a" * 32,
                target="b" * 32,
                source_role="input",
                target_role="output",
            ),
            SourceTargetPair(
                source="b" * 32,
                target="c" * 32,
                source_role="input",
                target_role="energy",
            ),
        ]

        result = build_artifact_edges_from_store(
            source_target_pairs=pairs,
            execution_run_id="e" * 32,
            artifact_store=mock_store,
        )

        assert len(result) == 2
        assert result[0].source_artifact_id == "a" * 32
        assert result[1].target_artifact_type == "metric"

    def test_empty_pairs_returns_empty_list(self):
        """Test that empty pairs returns empty list."""
        mock_store = MagicMock()

        result = build_artifact_edges_from_store(
            source_target_pairs=[],
            execution_run_id="e" * 32,
            artifact_store=mock_store,
        )

        assert result == []
        mock_store.provenance.load_type_map.assert_not_called()


class TestRunCreatorLifecycle:
    """Tests for the extracted run_creator_lifecycle function."""

    def test_lifecycle_returns_lifecycle_result(
        self, delta_root_with_input, working_root, staging_root
    ):
        """run_creator_lifecycle returns a LifecycleResult with correct structure."""
        delta_path, input_artifact_id = delta_root_with_input

        config = RuntimeEnvironment(
            delta_root_path=delta_path,
            working_root_path=working_root,
            staging_root_path=staging_root,
        )

        unit = ExecutionUnit(
            operation=MetricCopyTestOp(suffix="_copy"),
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
            delta_root_path=delta_path,
            working_root_path=working_root,
            staging_root_path=staging_root,
        )

        unit = ExecutionUnit(
            operation=GenerativeTestOp(count=3),
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
            delta_root_path=delta_path,
            working_root_path=working_root,
            staging_root_path=staging_root,
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
            delta_root_path=delta_path,
            working_root_path=working_root,
            staging_root_path=staging_root,
        )

        unit = ExecutionUnit(
            operation=ExceptionTestOp(),
            inputs={},
            execution_spec_id="spec_lc4" + "0" * 24,
            step_number=1,
        )

        with pytest.raises(Exception, match="Intentional exception"):
            run_creator_lifecycle(unit, config)
