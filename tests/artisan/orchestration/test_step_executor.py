"""Tests for step_executor module.

Reference: design_orchestration_internals_v2.md
Reference: design_utility_operations.md

These tests verify the step executor behavior for:
- File-path promotion via _promote_file_paths_to_store() (in pipeline_manager)
- Orchestrator-level input pairing (group_inputs integration)
"""

from __future__ import annotations

import resource
import signal
from contextlib import contextmanager
from enum import StrEnum, auto
from typing import ClassVar
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from pydantic import BaseModel, Field

from artisan.errors import ArtifactIntegrityError, PersistenceIntegrityError
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.inputs import PreparedInputs
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import FailurePolicy, GroupByStrategy
from artisan.schemas.execution.curator_result import (
    ArtifactResult,
    PassthroughResult,
)
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_overrides import StepOverrides
from artisan.schemas.specs.input_models import PreprocessInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.hashing import CacheInputIdentity

# =============================================================================
# Mock Operations
# =============================================================================


class MockIngestOp(OperationDefinition):
    """Mock ingest operation for testing file path promotion."""

    class InputRole(StrEnum):
        file = auto()

    class OutputRole(StrEnum):
        data = auto()

    name: ClassVar[str] = "mock_ingest"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.file: InputSpec(artifact_type=ArtifactTypes.FILE_REF, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.data: OutputSpec(
            artifact_type=ArtifactTypes.FILE_REF, is_memory_output=True
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict:
        """No inputs to preprocess for curator op."""
        return {}

    def execute_curator(self, inputs, step_number, artifact_store) -> ArtifactResult:
        """Mock curator execution."""
        return ArtifactResult(
            success=True,
        )


class MockCreatorOp(OperationDefinition):
    """Mock creator operation for testing."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_creator"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.FILE_REF, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.FILE_REF,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict:
        """Extract materialized paths from input artifacts."""
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute_function(self, inputs, output_dir):
        """Mock creator execution."""
        return ArtifactResult(success=True)


# =============================================================================
# Tests for File Path Detection
# =============================================================================


class TestFilePathDetection:
    """Tests for _is_file_path_input detection in pipeline_manager."""

    def test_detects_file_paths(self):
        """File path strings are detected correctly."""
        from artisan.orchestration.pipeline_manager import _is_file_path_input

        assert _is_file_path_input(["/path/to/file.csv"])
        assert _is_file_path_input(["relative/path.csv", "another.csv"])

    def test_rejects_non_file_inputs(self):
        """Non-file inputs are not detected as file paths."""
        from artisan.orchestration.pipeline_manager import _is_file_path_input
        from artisan.schemas.orchestration.output_reference import OutputReference

        assert not _is_file_path_input(None)
        assert not _is_file_path_input({})
        assert not _is_file_path_input([])
        assert not _is_file_path_input(
            [OutputReference(source_step=0, role="file", artifact_type="file_ref")]
        )


class TestCreatorRejectsFilePaths:
    """Tests for creator operation rejection of raw file paths via pipeline_manager."""

    def test_creator_rejects_raw_file_paths(self, tmp_path):
        """Creator operations should reject raw file paths at the PipelineManager level."""
        from artisan.orchestration.pipeline_manager import (
            _is_file_path_input,
        )

        # Create a test file
        test_file = tmp_path / "test.csv"
        test_file.write_text("ATOM content")

        inputs = [str(test_file)]
        assert _is_file_path_input(inputs)

        # Creator operations should raise ValueError (not call _promote)
        # The actual raise happens in submit(), so we test the detection
        from artisan.execution.executors.curator import is_curator_operation

        assert not is_curator_operation(MockCreatorOp())


class TestFilePathPromotion:
    """Tests for _promote_file_paths_to_store in pipeline_manager."""

    def test_missing_file_fails_closed(self, tmp_path):
        """A missing raw input aborts before promotion."""
        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test_pipeline",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        (tmp_path / "delta").mkdir(parents=True, exist_ok=True)
        (tmp_path / "staging").mkdir(parents=True, exist_ok=True)

        non_existent = str(tmp_path / "does_not_exist.csv")
        with pytest.raises(ArtifactIntegrityError, match="Not found"):
            _promote_file_paths_to_store(
                [non_existent], config, 1, "mock_ingest", "a" * 32
            )

    def test_directory_path_fails_closed(self, tmp_path):
        """A raw directory input is rejected rather than skipped."""
        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test_pipeline",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        (tmp_path / "delta").mkdir(parents=True, exist_ok=True)
        (tmp_path / "staging").mkdir(parents=True, exist_ok=True)

        test_dir = tmp_path / "test_directory"
        test_dir.mkdir()

        with pytest.raises(ArtifactIntegrityError, match="Not a file"):
            _promote_file_paths_to_store(
                [str(test_dir)], config, 1, "mock_ingest", "a" * 32
            )

    def test_valid_files_promoted(self, tmp_path):
        """Valid file paths should be promoted to artifact IDs."""
        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test_pipeline",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        (tmp_path / "delta").mkdir(parents=True, exist_ok=True)
        (tmp_path / "staging").mkdir(parents=True, exist_ok=True)

        test_file = tmp_path / "test.csv"
        test_file.write_bytes(b"ATOM content")

        result, count, _verified = _promote_file_paths_to_store(
            [str(test_file)], config, 0, "mock_ingest", "a" * 32
        )

        assert result is not None
        assert "file" in result
        assert len(result["file"]) == 1
        assert count == 1


# =============================================================================
# Mock Operations with group_by for pairing tests
# =============================================================================

# Use 32-char hex artifact IDs for tests
_ID_S1 = "a" * 32
_ID_S2 = "b" * 32
_ID_C1 = "c" * 32
_ID_C2 = "d" * 32


def _prepared(
    inputs: dict[str, list[str]] | None,
    group_ids: list[str] | None = None,
) -> PreparedInputs:
    """Build the resolved identity snapshot accepted by step executors."""
    resolved = inputs or {}
    artifact_types = {
        artifact_id: (
            ArtifactTypes.CONFIG if role == "config" else ArtifactTypes.FILE_REF
        )
        for role, artifact_ids in resolved.items()
        for artifact_id in artifact_ids
    }
    cache_inputs = {
        role: [
            CacheInputIdentity(
                role=role,
                group_id=group_ids[position] if group_ids is not None else None,
                position=position,
                artifact_type=artifact_types[artifact_id],
                artifact_id=artifact_id,
            )
            for position, artifact_id in enumerate(artifact_ids)
        ]
        for role, artifact_ids in resolved.items()
    }
    return PreparedInputs(resolved, artifact_types, group_ids, cache_inputs)


class MockMultiInputCreatorOp(OperationDefinition):
    """Mock multi-input creator op with group_by=ZIP."""

    class InputRole(StrEnum):
        data = auto()
        config = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_multi_creator"
    group_by: GroupByStrategy | None = GroupByStrategy.ZIP
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.FILE_REF, required=True),
        InputRole.config: InputSpec(artifact_type=ArtifactTypes.CONFIG, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.FILE_REF,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict:
        return {}

    def execute_function(self, inputs, output_dir):
        return ArtifactResult(success=True)


class MockNoGroupByCreatorOp(OperationDefinition):
    """Mock single-input creator op without group_by."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_no_groupby_creator"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.FILE_REF, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.FILE_REF,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict:
        return {}

    def execute_function(self, inputs, output_dir):
        return ArtifactResult(success=True)


class MockMultiInputCuratorOp(OperationDefinition):
    """Mock multi-input curator op with group_by=ZIP."""

    class InputRole(StrEnum):
        data = auto()
        config = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_multi_curator"
    group_by: GroupByStrategy | None = GroupByStrategy.ZIP
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.FILE_REF, required=True),
        InputRole.config: InputSpec(artifact_type=ArtifactTypes.CONFIG, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.FILE_REF,
            is_memory_output=True,
        ),
    }

    def execute_curator(self, inputs, step_number, artifact_store) -> ArtifactResult:
        return ArtifactResult(success=True)


class MockNoGroupByCuratorOp(OperationDefinition):
    """Mock curator op without group_by."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_no_groupby_curator"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.FILE_REF, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.FILE_REF,
            is_memory_output=True,
        ),
    }

    def execute_curator(self, inputs, step_number, artifact_store) -> ArtifactResult:
        return ArtifactResult(success=True)


class MockFilterOp(OperationDefinition):
    """Mock filter operation (name='filter') for testing filter log diagnostics."""

    class InputRole(StrEnum):
        passthrough = auto()

    class OutputRole(StrEnum):
        passthrough = auto()

    name: ClassVar[str] = "filter"
    runtime_defined_inputs: ClassVar[bool] = True
    independent_input_streams: ClassVar[bool] = True
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.passthrough: InputSpec(
            artifact_type=ArtifactTypes.FILE_REF, required=True
        ),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.passthrough: OutputSpec(
            artifact_type=ArtifactTypes.FILE_REF,
        ),
    }

    def execute_curator(self, inputs, step_number, artifact_store) -> PassthroughResult:
        return PassthroughResult(success=True, passthrough={})


# =============================================================================
# Helpers
# =============================================================================


def _make_mock_backend(
    flow_return_value=None, flow_side_effect=None, needs_staging_verification=False
):
    """Create a mock step_runner for step executor tests.

    Returns a mock step_runner with a mock lifecycle router.  The handle's
    ``run()`` returns *flow_return_value* (or raises *flow_side_effect*).
    """
    from unittest.mock import MagicMock

    mock_backend = MagicMock()
    mock_backend.name = "local"
    mock_backend.worker_traits.worker_id_env_var = None
    mock_backend.worker_traits.shared_filesystem = False
    mock_backend.orchestrator_traits.needs_staging_verification = (
        needs_staging_verification
    )
    mock_backend.orchestrator_traits.staging_verification_timeout = 60.0

    mock_handle = MagicMock()
    mock_handle._captured_units = None
    return_value = flow_return_value if flow_return_value is not None else []

    def _capture_and_run(units, runtime_env, **kwargs):
        mock_handle._captured_units = units
        if flow_side_effect is not None:
            raise flow_side_effect
        return return_value

    mock_handle.run.side_effect = _capture_and_run

    mock_backend.create_lifecycle_router.return_value = mock_handle
    return mock_backend, mock_handle


# =============================================================================
# Tests for Creator Step Pairing Phase
# =============================================================================


class TestCreatorStepPairing:
    """Tests for prepared grouping in _execute_creator_step()."""

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_creator_with_group_by_uses_prepared_groups(
        self,
        mock_cache,
        tmp_path,
    ):
        """Creator dispatch preserves groups prepared before cache hashing."""
        from artisan.orchestration.engine.step_executor import (
            _execute_creator_step,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_backend, mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(success=True, error=None, item_count=2, execution_run_ids=[])
            ],
        )

        paired = {
            "data": [_ID_S1, _ID_S2],
            "config": [_ID_C1, _ID_C2],
        }
        gids = ["gid1", "gid2"]

        mock_cache.return_value = None  # No cache hits

        _execute_creator_step(
            operation=MockMultiInputCreatorOp(),
            inputs=_prepared(paired, gids),
            step_runner=mock_backend,
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        # step_runner flow receives units_path; verify captured units
        dispatched_units = mock_handle._captured_units
        assert len(dispatched_units) > 0
        # With batch size 1 (default), 2 items -> 2 units
        for unit in dispatched_units:
            assert unit.group_ids is not None

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_creator_without_group_by_preserves_no_groups(
        self,
        mock_cache,
        tmp_path,
    ):
        """Creator dispatch preserves an ungrouped prepared snapshot."""
        from artisan.orchestration.engine.step_executor import (
            _execute_creator_step,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_backend, mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(success=True, error=None, item_count=2, execution_run_ids=[])
            ],
        )

        resolved = {"data": [_ID_S1, _ID_S2]}
        mock_cache.return_value = None

        _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs=_prepared(resolved),
            step_runner=mock_backend,
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        # step_runner flow receives units_path; verify captured units
        dispatched_units = mock_handle._captured_units
        for unit in dispatched_units:
            assert unit.group_ids is None

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_creator_group_ids_sliced_across_batches(
        self,
        mock_cache,
        tmp_path,
    ):
        """Group_ids should be sliced by batching in sync with inputs."""
        from artisan.orchestration.engine.step_executor import (
            _execute_creator_step,
        )
        from artisan.schemas.execution.batch_strategy import BatchStrategy
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_backend, mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(success=True, error=None, item_count=4, execution_run_ids=[])
            ],
        )

        # 4 artifacts, batch size 2 -> 2 ExecutionUnits
        id_s3 = "e" * 32
        id_s4 = "f" * 32
        id_c3 = "1" * 32
        id_c4 = "2" * 32

        resolved = {
            "data": [_ID_S1, _ID_S2, id_s3, id_s4],
            "config": [_ID_C1, _ID_C2, id_c3, id_c4],
        }
        mock_cache.return_value = None

        # Create operation with artifacts_per_unit=2
        op = MockMultiInputCreatorOp()
        op = op.model_copy(
            update={"batch_strategy": BatchStrategy(artifacts_per_unit=2)}
        )

        _execute_creator_step(
            operation=op,
            inputs=_prepared(resolved, ["g1", "g2", "g3", "g4"]),
            step_runner=mock_backend,
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        dispatched_units = mock_handle._captured_units
        assert len(dispatched_units) == 2

        # First batch: group_ids[0:2]
        assert dispatched_units[0].group_ids == ["g1", "g2"]
        assert dispatched_units[0].inputs["data"] == [_ID_S1, _ID_S2]
        assert dispatched_units[0].inputs["config"] == [_ID_C1, _ID_C2]

        # Second batch: group_ids[2:4]
        assert dispatched_units[1].group_ids == ["g3", "g4"]
        assert dispatched_units[1].inputs["data"] == [id_s3, id_s4]
        assert dispatched_units[1].inputs["config"] == [id_c3, id_c4]


# =============================================================================
# Tests for Curator Step Pairing Phase
# =============================================================================


class TestCuratorStepPairing:
    """Tests for prepared grouping in _execute_curator_step()."""

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_curator_with_group_by_uses_prepared_groups(
        self,
        mock_cache,
        mock_curator_flow,
        tmp_path,
    ):
        """Curator dispatch preserves groups prepared before cache hashing."""
        from artisan.execution.recording.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        paired = {
            "data": [_ID_S1, _ID_S2],
            "config": [_ID_C1, _ID_C2],
        }
        gids = ["gid1", "gid2"]

        mock_cache.return_value = None
        mock_curator_flow.return_value = StagingResult(
            success=True, artifact_ids=[_ID_S1, _ID_S2], execution_run_id="run1"
        )

        _execute_curator_step(
            operation=MockMultiInputCuratorOp(),
            inputs=_prepared(paired, gids),
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        # _run_curator_in_subprocess should receive a unit with group_ids set
        unit = mock_curator_flow.call_args[0][0]
        assert unit.group_ids == gids

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_curator_without_group_by_preserves_no_groups(
        self,
        mock_cache,
        mock_curator_flow,
        tmp_path,
    ):
        """Curator dispatch preserves an ungrouped prepared snapshot."""
        from artisan.execution.recording.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        resolved = {"data": [_ID_S1, _ID_S2]}
        mock_cache.return_value = None
        mock_curator_flow.return_value = StagingResult(
            success=True, artifact_ids=[_ID_S1, _ID_S2], execution_run_id="run1"
        )

        _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs=_prepared(resolved),
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        # _run_curator_in_subprocess should receive a unit with group_ids=None
        unit = mock_curator_flow.call_args[0][0]
        assert unit.group_ids is None

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_curator_group_ids_set_on_execution_unit(
        self,
        mock_cache,
        mock_curator_flow,
        tmp_path,
    ):
        """Curator step group_ids should be attached to the ExecutionUnit."""
        from artisan.execution.recording.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        paired = {
            "data": [_ID_S2, _ID_S1],  # Reordered by pairing
            "config": [_ID_C2, _ID_C1],
        }
        gids = ["gid_x", "gid_y"]

        mock_cache.return_value = None
        mock_curator_flow.return_value = StagingResult(
            success=True, artifact_ids=[_ID_S1, _ID_S2], execution_run_id="run1"
        )

        _execute_curator_step(
            operation=MockMultiInputCuratorOp(),
            inputs=_prepared(paired, gids),
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        unit = mock_curator_flow.call_args[0][0]
        # Verify the paired inputs (reordered) are used
        assert unit.inputs == paired
        assert unit.group_ids == gids


# =============================================================================
# Tests for Step Result Metadata and Phase Timing
# =============================================================================


class TestStepResultMetadata:
    """Tests for metadata field on StepResult."""

    def test_step_result_default_empty_metadata(self):
        """StepResult should have empty metadata by default."""
        from artisan.schemas.orchestration.step_result import StepResult

        result = StepResult(
            step_name="test",
            step_number=1,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
        )
        assert result.metadata == {}

    def test_step_result_with_timings_metadata(self):
        """StepResult should accept timings in metadata."""
        from artisan.schemas.orchestration.step_result import StepResult

        timings = {"resolve_inputs": 0.1, "execute": 1.5, "total": 1.6}
        result = StepResult(
            step_name="test",
            step_number=1,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            metadata={"timings": timings},
        )
        assert result.metadata["timings"]["total"] == 1.6

    def test_build_step_result_passes_metadata(self):
        """build_step_result should pass metadata to StepResult."""
        from artisan.orchestration.engine.step_executor import build_step_result

        metadata = {"timings": {"total": 2.5}}
        result = build_step_result(
            operation=MockNoGroupByCreatorOp(),
            step_number=1,
            succeeded_count=5,
            failed_count=0,
            failure_policy=FailurePolicy.CONTINUE,
            metadata=metadata,
        )
        assert result.metadata == metadata

    def test_build_step_result_default_no_metadata(self):
        """build_step_result without metadata should have empty dict."""
        from artisan.orchestration.engine.step_executor import build_step_result

        result = build_step_result(
            operation=MockNoGroupByCreatorOp(),
            step_number=1,
            succeeded_count=5,
            failed_count=0,
            failure_policy=FailurePolicy.CONTINUE,
        )
        assert result.metadata == {}


class TestStepTimingIntegration:
    """Tests that step execution produces timing metadata."""

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_creator_step_returns_timings(
        self,
        mock_cache,
        tmp_path,
    ):
        """Creator step should include timing metadata in result."""
        from artisan.orchestration.engine.step_executor import (
            _execute_creator_step,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_backend, _mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(success=True, error=None, item_count=2, execution_run_ids=[])
            ],
        )

        mock_cache.return_value = None

        result = _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs=_prepared({"data": [_ID_S1]}),
            step_runner=mock_backend,
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        assert "timings" in result.metadata
        timings = result.metadata["timings"]
        assert "resolve_inputs" in timings
        assert "batch_and_cache" in timings
        assert "execute" in timings
        assert "verify_staging" in timings
        assert "commit" not in timings
        assert "compact" not in timings
        assert "total" in timings
        # All values should be non-negative floats
        for key, value in timings.items():
            assert isinstance(value, float), f"{key} should be float"
            assert value >= 0, f"{key} should be non-negative"
        # total is independently measured, so it should be >= sum of phases
        phase_sum = sum(
            v for k, v in timings.items() if k != "total" and isinstance(v, float)
        )
        assert timings["total"] >= phase_sum - 0.001  # small tolerance for rounding

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_curator_step_returns_timings(
        self,
        mock_cache,
        mock_curator_flow,
        tmp_path,
    ):
        """Curator step should include timing metadata in result."""
        from artisan.execution.recording.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_cache.return_value = None
        mock_curator_flow.return_value = StagingResult(
            success=True, artifact_ids=[_ID_S1, _ID_S2], execution_run_id="run1"
        )

        result = _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs=_prepared({"data": [_ID_S1]}),
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        assert "timings" in result.metadata
        timings = result.metadata["timings"]
        assert "resolve_inputs" in timings
        assert "batch_and_cache" in timings
        assert "execute" in timings
        assert "verify_staging" in timings
        assert "commit" not in timings
        assert "compact" not in timings
        assert "total" in timings
        for key, value in timings.items():
            assert isinstance(value, float), f"{key} should be float"
            assert value >= 0, f"{key} should be non-negative"
        # total is independently measured, so it should be >= sum of phases
        phase_sum = sum(
            v for k, v in timings.items() if k != "total" and isinstance(v, float)
        )
        assert timings["total"] >= phase_sum - 0.001  # small tolerance for rounding


# =============================================================================
# Tests for Empty Input Handling (graceful skip on filtered-out inputs)
# =============================================================================


class TestEmptyInputHandling:
    """Tests for graceful skipping when upstream filter removes all artifacts."""

    def test_creator_step_skips_on_empty_inputs(
        self,
        tmp_path,
    ):
        """Creator step should skip execution when all input roles are empty."""
        from artisan.orchestration.engine.step_executor import (
            _execute_creator_step,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_backend, _mock_handle = _make_mock_backend()

        result = _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs=_prepared({"data": []}),
            step_runner=mock_backend,
            config_overrides=None,
            step_number=2,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        mock_backend.create_lifecycle_router.assert_not_called()
        assert result.status == StepStatus.SKIPPED
        assert result.metadata["skip_reason"] == "empty_inputs"
        assert result.succeeded_count == 0
        assert result.failed_count == 0

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    def test_curator_step_skips_on_empty_inputs(
        self,
        mock_curator_flow,
        tmp_path,
    ):
        """Curator step should skip execution when all input roles are empty."""
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        result = _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs=_prepared({"data": []}),
            config_overrides=None,
            step_number=2,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        mock_curator_flow.assert_not_called()
        assert result.status == StepStatus.SKIPPED
        assert result.metadata["skip_reason"] == "empty_inputs"
        assert result.succeeded_count == 0
        assert result.failed_count == 0

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_generative_op_not_skipped(
        self,
        mock_cache,
        tmp_path,
    ):
        """Generative ops (empty dict inputs) should NOT be skipped."""
        from artisan.orchestration.engine.step_executor import (
            _execute_creator_step,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_backend, mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(success=True, error=None, item_count=1, execution_run_ids=[])
            ],
        )

        mock_cache.return_value = None

        result = _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs=_prepared(None),
            step_runner=mock_backend,
            config_overrides=None,
            step_number=0,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        mock_handle.run.assert_called_once()
        assert result.status == StepStatus.SUCCEEDED

    def test_all_inputs_empty_with_partial_roles(self):
        """_all_inputs_empty returns False when some roles have artifacts."""
        from artisan.orchestration.engine.step_executor import _all_inputs_empty

        assert _all_inputs_empty({"data": [_ID_S1], "config": []}) is False

    def test_all_inputs_empty_with_all_empty(self):
        """_all_inputs_empty returns True when every role is empty."""
        from artisan.orchestration.engine.step_executor import _all_inputs_empty

        assert _all_inputs_empty({"data": [], "config": []}) is True

    def test_all_inputs_empty_with_generative(self):
        """_all_inputs_empty returns False for empty dict (generative ops)."""
        from artisan.orchestration.engine.step_executor import _all_inputs_empty

        assert _all_inputs_empty({}) is False


# =============================================================================
# Tests for Failure Handling (Phase 3 hardening)
# =============================================================================


class TestDispatchFailureHandling:
    """Tests for F14: dispatch failure resilience."""

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_creator_dispatch_failure_returns_step_result(
        self,
        mock_cache,
        tmp_path,
    ):
        """Creator step returns StepResult (not raises) on dispatch failure."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_backend, _mock_handle = _make_mock_backend(
            flow_side_effect=ConnectionError("Network down"),
        )

        mock_cache.return_value = None

        result = _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs=_prepared({"data": [_ID_S1]}),
            step_runner=mock_backend,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        assert result.succeeded_count == 0
        assert result.failed_count == 1  # 1 unit dispatched
        assert result.status == StepStatus.FAILED
        assert result.error is not None
        assert "ConnectionError" in result.error
        assert "Network down" in result.error

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch(
        "artisan.orchestration.engine.step_executor._run_curator_in_subprocess",
        side_effect=ConnectionError("Network down"),
    )
    def test_curator_dispatch_failure_returns_step_result(
        self,
        mock_curator_flow,
        mock_cache,
        tmp_path,
    ):
        """Curator step returns StepResult (not raises) on execution failure."""
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_cache.return_value = None

        result = _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs=_prepared({"data": [_ID_S1]}),
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        assert result.succeeded_count == 0
        assert result.failed_count == 1
        assert result.status == StepStatus.FAILED
        assert result.error is not None
        assert "ConnectionError" in result.error
        assert "Network down" in result.error

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_dispatch_fail_fast_returns_failed_terminal_result(
        self,
        mock_cache,
        tmp_path,
    ):
        """Fail-fast remains an explicit failed result after durable work."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_backend, _mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(
                    success=False, error="boom", item_count=1, execution_run_ids=[]
                )
            ],
        )

        mock_cache.return_value = None

        result = _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs=_prepared({"data": [_ID_S1]}),
            step_runner=mock_backend,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.FAIL_FAST,
        )

        assert result.status == StepStatus.FAILED
        assert result.error == "boom"

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_dispatch_runtimeerror_becomes_failed_result(
        self,
        mock_cache,
        tmp_path,
    ):
        """A plain RuntimeError from dispatch becomes a failed result."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_backend, _mock_handle = _make_mock_backend(
            flow_side_effect=RuntimeError("dispatch machinery exploded"),
        )

        mock_cache.return_value = None

        result = _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs=_prepared({"data": [_ID_S1, _ID_S2]}),
            step_runner=mock_backend,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        assert result.succeeded_count == 0
        assert result.failed_count == 2
        assert result.status == StepStatus.FAILED
        assert result.error is not None
        assert "RuntimeError" in result.error
        assert "dispatch machinery exploded" in result.error

        staged = list((tmp_path / "staging").rglob("executions.parquet"))
        assert len(staged) == 2
        failures = pl.concat([pl.read_parquet(path) for path in staged])
        assert failures.height == 2
        assert set(failures["operation_name"]) == {MockNoGroupByCreatorOp.name}
        assert all(
            "dispatch machinery exploded" in error for error in failures["error"]
        )
        assert len(list((tmp_path / "logs" / "failures").rglob("*.log"))) == 2


class TestCreatorCancellationCleanup:
    """Cancelled creator work must never survive into staging recovery."""

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_cancelled_pending_failure_record_is_discarded(
        self,
        mock_cache,
        tmp_path,
    ):
        """A synthesized pending-future failure is removed before returning."""
        import threading

        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        cancel_event = threading.Event()
        mock_backend, mock_handle = _make_mock_backend()

        def _cancel_with_failure(units, runtime_env, **kwargs):
            cancel_event.set()
            return [
                UnitResult(
                    success=False,
                    error="CancelledError: pending work cancelled",
                    item_count=1,
                    execution_run_ids=[],
                )
                for _ in units
            ]

        mock_handle.run.side_effect = _cancel_with_failure
        mock_cache.return_value = None

        result = _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs=_prepared({"data": [_ID_S1]}),
            step_runner=mock_backend,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            cancel_event=cancel_event,
            step_run_id="cancelled-step",
        )

        assert result.status == StepStatus.CANCELLED
        assert result.cancellation_status == CancellationStatus.CONFIRMED
        assert not list((tmp_path / "staging").rglob("*.parquet"))

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_cleanup_removes_only_current_cancel_sentinel(
        self,
        mock_cache,
        tmp_path,
    ):
        """Finishing one step cannot erase another step's cancellation signal."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig
        from artisan.utils.path import cancel_sentinel_path

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        own_sentinel = cancel_sentinel_path(config.staging_root, "current-step")
        other_sentinel = cancel_sentinel_path(config.staging_root, "other-step")
        fs = config.storage.filesystem()
        fs.makedirs(str(tmp_path / "staging" / "_dispatch"), exist_ok=True)
        fs.touch(own_sentinel)
        fs.touch(other_sentinel)

        mock_backend, _ = _make_mock_backend(
            flow_return_value=[
                UnitResult(
                    success=True,
                    error=None,
                    item_count=1,
                    execution_run_ids=[],
                )
            ]
        )
        mock_cache.return_value = None

        _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs=_prepared({"data": [_ID_S1]}),
            step_runner=mock_backend,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            step_run_id="current-step",
        )

        assert not fs.exists(own_sentinel)
        assert fs.exists(other_sentinel)


class TestCommitFailureHandling:
    """Tests for F16: commit phase failure resilience."""

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_creator_commit_failure_propagates(
        self,
        mock_cache,
        tmp_path,
    ):
        """Creator commit errors propagate for manager terminalization."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_backend, _mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(
                    success=True, error=None, item_count=1, execution_run_ids=["a"]
                )
            ],
        )

        mock_cache.return_value = None

        def fail_persistence(*_args):
            msg = "Disk full"
            raise OSError(msg)

        with pytest.raises(OSError, match="Disk full"):
            _execute_creator_step(
                operation=MockNoGroupByCreatorOp(),
                inputs=_prepared({"data": [_ID_S1]}),
                step_runner=mock_backend,
                step_number=1,
                config=config,
                failure_policy=FailurePolicy.CONTINUE,
                persist_result=fail_persistence,
            )


class TestLogicalPersistenceBoundary:
    """Worker seals and curator callbacks are mandatory persistence inputs."""

    @pytest.mark.parametrize(
        "execution_run_ids",
        [[], ["a" * 32, "a" * 32]],
    )
    def test_invalid_worker_seals_fail_before_orchestrator_staging(
        self,
        tmp_path,
        execution_run_ids,
    ):
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        runner, _handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(
                    success=True,
                    error=None,
                    item_count=1,
                    execution_run_ids=execution_run_ids,
                )
            ]
        )
        persist = MagicMock()

        with (
            patch(
                "artisan.orchestration.engine.step_executor.check_cache_for_batch",
                return_value=None,
            ),
            patch(
                "artisan.orchestration.engine.step_executor._stage_cache_reuse"
            ) as stage,
            pytest.raises(PersistenceIntegrityError, match="staging identities"),
        ):
            _execute_creator_step(
                operation=MockNoGroupByCreatorOp(),
                inputs=_prepared({"data": [_ID_S1]}),
                step_runner=runner,
                step_number=1,
                config=config,
                step_run_id="b" * 32,
                persist_result=persist,
            )

        stage.assert_not_called()
        persist.assert_not_called()

    def test_execute_step_forwards_curator_persistence_callback(self, tmp_path):
        from artisan.orchestration.engine.step_executor import execute_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        persist = MagicMock()
        expected = MagicMock()

        with patch(
            "artisan.orchestration.engine.step_executor._execute_curator_step",
            return_value=expected,
        ) as curator:
            result = execute_step(
                MockNoGroupByCuratorOp(),
                _prepared({"data": [_ID_S1]}),
                StepOverrides.from_user(),
                MagicMock(),
                config=config,
                persist_result=persist,
            )

        assert result is expected
        assert curator.call_args.kwargs["persist_result"] is persist


class TestStagingTimeoutHandling:
    """Tests for F15: staging verification timeout resilience."""

    @patch("artisan.orchestration.engine.step_executor.await_staging_files")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_staging_timeout_propagates(
        self,
        mock_cache,
        mock_await,
        tmp_path,
    ):
        """Staging verification timeouts propagate for terminalization."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_backend, _mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(
                    success=True, error=None, item_count=1, execution_run_ids=["a"]
                )
            ],
            needs_staging_verification=True,
        )

        mock_cache.return_value = None
        mock_await.side_effect = TimeoutError("NFS cache timeout")

        with pytest.raises(TimeoutError, match="NFS cache timeout"):
            _execute_creator_step(
                operation=MockNoGroupByCreatorOp(),
                inputs=_prepared({"data": [_ID_S1]}),
                step_runner=mock_backend,
                step_number=1,
                config=config,
                failure_policy=FailurePolicy.CONTINUE,
            )


class TestFileValidationBatch:
    """Tests for batch file validation in _promote_file_paths_to_store."""

    def test_mixed_valid_invalid_files_fail_without_partial_promotion(self, tmp_path):
        """One invalid raw input rejects the full ordered input occurrence list."""
        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        (tmp_path / "delta").mkdir(parents=True, exist_ok=True)
        (tmp_path / "staging").mkdir(parents=True, exist_ok=True)

        # One valid file, one non-existent
        valid_file = tmp_path / "valid.csv"
        valid_file.write_bytes(b"ATOM content")
        non_existent = str(tmp_path / "missing.csv")

        with pytest.raises(ArtifactIntegrityError, match="missing.csv"):
            _promote_file_paths_to_store(
                [str(valid_file), non_existent],
                config,
                1,
                "mock_ingest",
                "a" * 32,
            )

        assert not list((tmp_path / "staging").rglob("*.parquet"))


# =============================================================================
# Tests for Filter Step Logging
# =============================================================================


class TestFilterStepLogging:
    """Verify filter step logs correct pass/total counts."""

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_filter_log_counts_only_passthrough_role(
        self,
        mock_cache,
        mock_curator_flow,
        tmp_path,
        caplog,
    ):
        """Filter log should count only passthrough role, not metric inputs."""
        from artisan.execution.recording.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        # 3 passthrough artifacts, 5 metric artifacts
        passthrough_ids = [_ID_S1, _ID_S2, "c" * 32]
        metric_ids = ["d" * 32, "e" * 32, "f" * 32, "g" * 32, "h" * 32]
        prepared = _prepared(
            {
                "passthrough": passthrough_ids,
                "quality_metrics": metric_ids,
            }
        )
        mock_cache.return_value = None

        # 2 of 3 passthrough artifacts pass the filter
        mock_curator_flow.return_value = StagingResult(
            success=True,
            artifact_ids=[_ID_S1, _ID_S2],
            execution_run_id="run1",
        )

        import logging

        # Ensure caplog can capture via propagation
        artisan_logger = logging.getLogger("artisan")
        artisan_logger.propagate = True

        with caplog.at_level(logging.INFO):
            result = _execute_curator_step(
                operation=MockFilterOp(),
                inputs=prepared,
                config_overrides=None,
                step_number=11,
                config=config,
                failure_policy=FailurePolicy.CONTINUE,
            )

        assert result.succeeded_count == 2

        # Find the filter diagnostic log line
        filter_logs = [
            r.getMessage()
            for r in caplog.records
            if "artifacts passed" in r.getMessage()
        ]
        assert len(filter_logs) == 1, f"Expected 1 filter log, got: {filter_logs}"

        log_msg = filter_logs[0]
        # Should say "2/3 artifacts passed (1 filtered out)" not "2/8"
        assert "2/3 artifacts passed" in log_msg
        assert "1 filtered out" in log_msg

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_filter_log_zero_pass(
        self,
        mock_cache,
        mock_curator_flow,
        tmp_path,
        caplog,
    ):
        """Filter log should show 0/N when nothing passes."""
        from artisan.execution.recording.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        passthrough_ids = [_ID_S1, _ID_S2]
        mock_cache.return_value = None

        # Nothing passes — artifact_ids is empty but success=True
        mock_curator_flow.return_value = StagingResult(
            success=True,
            artifact_ids=[],
            execution_run_id="run1",
        )

        import logging

        # Ensure caplog can capture via propagation
        artisan_logger = logging.getLogger("artisan")
        artisan_logger.propagate = True

        with caplog.at_level(logging.INFO):
            result = _execute_curator_step(
                operation=MockFilterOp(),
                inputs=_prepared({"passthrough": passthrough_ids}),
                config_overrides=None,
                step_number=5,
                config=config,
                failure_policy=FailurePolicy.CONTINUE,
            )

        assert result.succeeded_count == 0

        filter_logs = [
            r.getMessage()
            for r in caplog.records
            if "artifacts passed" in r.getMessage()
        ]
        assert len(filter_logs) == 1
        assert "0/2 artifacts passed" in filter_logs[0]
        assert "2 filtered out" in filter_logs[0]


# =============================================================================
# Tests for curator execution cache identity
# =============================================================================


class TestExecutionCacheReuseCapture:
    """Cache hits become durable membership only for their current attempt."""

    def test_curator_cache_hit_stages_validated_relation(self, tmp_path):
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.execution.cache_result import CacheHit
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        current = "a" * 32
        cached = "b" * 32
        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        with (
            patch(
                "artisan.orchestration.engine.step_executor.check_cache_for_batch",
                return_value=CacheHit(cached, "spec"),
            ),
            patch(
                "artisan.orchestration.engine.step_executor._validate_cache_reuse",
                return_value=[cached],
            ) as validate,
            patch(
                "artisan.orchestration.engine.step_executor._stage_cache_reuse",
                return_value=True,
            ) as stage,
            patch(
                "artisan.orchestration.engine.step_executor._run_curator_in_subprocess"
            ) as execute,
        ):
            persist = MagicMock(side_effect=lambda result, _ids: result)
            result = _execute_curator_step(
                operation=MockNoGroupByCuratorOp(),
                inputs=_prepared({"data": [_ID_S1]}),
                step_number=4,
                config=config,
                failure_policy=FailurePolicy.CONTINUE,
                step_run_id=current,
                persist_result=persist,
            )

        validate.assert_called_once_with(config, current, {cached})
        stage.assert_called_once_with(
            config,
            current,
            [cached],
            step_number=4,
            operation_name=MockNoGroupByCuratorOp.name,
        )
        assert persist.call_args.args[1] == ()
        execute.assert_not_called()
        assert result.step_run_id == current

    def test_all_cached_creator_commits_relation_without_dispatch(self, tmp_path):
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.execution.cache_result import CacheHit
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        current = "a" * 32
        cached = "b" * 32
        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        runner, _handle = _make_mock_backend()

        with (
            patch(
                "artisan.orchestration.engine.step_executor.check_cache_for_batch",
                return_value=CacheHit(cached, "spec"),
            ),
            patch(
                "artisan.orchestration.engine.step_executor._validate_cache_reuse",
                return_value=[cached],
            ) as validate,
            patch(
                "artisan.orchestration.engine.step_executor._stage_cache_reuse",
                return_value=True,
            ) as stage,
        ):
            persist = MagicMock(side_effect=lambda result, _ids: result)
            result = _execute_creator_step(
                operation=MockNoGroupByCreatorOp(),
                inputs=_prepared({"data": [_ID_S1]}),
                step_runner=runner,
                step_number=4,
                config=config,
                failure_policy=FailurePolicy.CONTINUE,
                step_run_id=current,
                persist_result=persist,
            )

        validate.assert_called_once_with(config, current, {cached})
        stage.assert_called_once_with(
            config,
            current,
            [cached],
            step_number=4,
            operation_name=MockNoGroupByCreatorOp.name,
        )
        assert persist.call_args.args[1] == ()
        runner.create_lifecycle_router.assert_not_called()
        assert result.succeeded_count == 1

    def test_mixed_creator_stages_only_cache_hits(self, tmp_path):
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.execution.batch_strategy import BatchStrategy
        from artisan.schemas.execution.cache_result import CacheHit
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        current = "a" * 32
        cached = "b" * 32
        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        operation = MockNoGroupByCreatorOp().model_copy(
            update={"batch_strategy": BatchStrategy(artifacts_per_unit=1)}
        )
        runner, handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(
                    success=True,
                    error=None,
                    item_count=1,
                    execution_run_ids=["c" * 32],
                )
            ]
        )

        with (
            patch(
                "artisan.orchestration.engine.step_executor.check_cache_for_batch",
                side_effect=[CacheHit(cached, "spec"), None],
            ),
            patch(
                "artisan.orchestration.engine.step_executor._validate_cache_reuse",
                return_value=[cached],
            ) as validate,
            patch(
                "artisan.orchestration.engine.step_executor._stage_cache_reuse",
                return_value=True,
            ) as stage,
        ):
            persist = MagicMock(side_effect=lambda result, _ids: result)
            result = _execute_creator_step(
                operation=operation,
                inputs=_prepared({"data": [_ID_S1, _ID_S2]}),
                step_runner=runner,
                step_number=4,
                config=config,
                failure_policy=FailurePolicy.CONTINUE,
                step_run_id=current,
                persist_result=persist,
            )

        validate.assert_called_once_with(config, current, {cached})
        assert stage.call_args.args[2] == [cached]
        assert len(handle._captured_units) == 1
        assert persist.call_args.args[1] == ("c" * 32,)
        assert result.succeeded_count == 2

    def test_cancelled_cache_selection_is_never_staged(self, tmp_path):
        import threading

        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.execution.cache_result import CacheHit
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        current = "a" * 32
        cached = "b" * 32
        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        runner, _handle = _make_mock_backend()
        cancelled = threading.Event()
        cancelled.set()

        with (
            patch(
                "artisan.orchestration.engine.step_executor.check_cache_for_batch",
                return_value=CacheHit(cached, "spec"),
            ),
            patch(
                "artisan.orchestration.engine.step_executor._validate_cache_reuse",
                return_value=[cached],
            ) as validate,
            patch(
                "artisan.orchestration.engine.step_executor._stage_cache_reuse"
            ) as stage,
        ):
            persist = MagicMock(side_effect=lambda result, _ids: result)
            result = _execute_creator_step(
                operation=MockNoGroupByCreatorOp(),
                inputs=_prepared({"data": [_ID_S1]}),
                step_runner=runner,
                step_number=4,
                config=config,
                failure_policy=FailurePolicy.CONTINUE,
                cancel_event=cancelled,
                step_run_id=current,
                persist_result=persist,
            )

        validate.assert_called_once_with(config, current, {cached})
        stage.assert_not_called()
        persist.assert_not_called()
        assert result.status == StepStatus.CANCELLED
        assert result.cancellation_status == CancellationStatus.CONFIRMED
        assert result.step_run_id == current

    def test_cache_relation_commit_failure_blocks_success(self, tmp_path):
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.execution.cache_result import CacheHit
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        current = "a" * 32
        cached = "b" * 32
        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        runner, _handle = _make_mock_backend()

        with (
            patch(
                "artisan.orchestration.engine.step_executor.check_cache_for_batch",
                return_value=CacheHit(cached, "spec"),
            ),
            patch(
                "artisan.orchestration.engine.step_executor._validate_cache_reuse",
                return_value=[cached],
            ),
            patch(
                "artisan.orchestration.engine.step_executor._stage_cache_reuse",
                return_value=True,
            ),
            pytest.raises(OSError, match="commit unavailable"),
        ):
            _execute_creator_step(
                operation=MockNoGroupByCreatorOp(),
                inputs=_prepared({"data": [_ID_S1]}),
                step_runner=runner,
                step_number=4,
                config=config,
                failure_policy=FailurePolicy.CONTINUE,
                step_run_id=current,
                persist_result=MagicMock(side_effect=OSError("commit unavailable")),
            )


class TestCuratorExecutionCacheIdentity:
    """Tests for concrete execution-cache lookup in curator execution."""

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_concrete_inputs_drive_cache_lookup(
        self,
        mock_cache,
        mock_curator_flow,
        tmp_path,
    ):
        """Curator execution always checks its concrete execution identity."""
        from artisan.execution.recording.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_cache.return_value = None
        mock_curator_flow.return_value = StagingResult(
            success=True, artifact_ids=[_ID_S1], execution_run_id="run1"
        )

        result = _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs=_prepared({"data": [_ID_S1]}),
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        mock_cache.assert_called_once()
        assert result.status == StepStatus.SUCCEEDED


# =============================================================================
# Tests for Curator Subprocess Isolation
# =============================================================================


class _DeadlineExceeded(Exception):
    """Raised by _deadline when the guarded block overruns.

    Deliberately not a TimeoutError so a spinning poll loop that swallows
    TimeoutError cannot also swallow the deadline signal.
    """


@contextmanager
def _deadline(seconds: int):
    """Fail the wrapped block if it runs longer than *seconds* (SIGALRM).

    Guards against a regression where the poll loop spins forever instead of
    surfacing a task-raised exception.
    """

    def _handler(signum, frame):
        msg = f"deadline exceeded after {seconds}s"
        raise _DeadlineExceeded(msg)

    old = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


class TestCuratorSubprocessIsolation:
    """Tests for subprocess isolation of curator operations."""

    def test_curator_runs_in_subprocess(self) -> None:
        """_run_curator_in_subprocess should delegate to ProcessPoolExecutor."""
        from unittest.mock import MagicMock

        from artisan.execution.recording.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import (
            _run_curator_in_subprocess,
        )
        from artisan.utils.process_call import (
            SerializedProcessCall,
            execute_process_call,
        )

        unit = MagicMock()
        runtime_env = MagicMock()
        expected = StagingResult(
            success=True, artifact_ids=["a1", "a2"], execution_run_id="run1"
        )

        with patch(
            "artisan.orchestration.engine.step_executor.ProcessPoolExecutor"
        ) as mock_pool_cls:
            mock_pool = MagicMock()
            mock_pool_cls.return_value.__enter__ = MagicMock(return_value=mock_pool)
            mock_pool_cls.return_value.__exit__ = MagicMock(return_value=False)
            mock_pool.submit.return_value.result.return_value = expected

            result = _run_curator_in_subprocess(unit, runtime_env)

        assert result is expected
        # Verify spawn context is used (avoids fork deadlocks with threaded parents)
        call_kwargs = mock_pool_cls.call_args[1]
        assert call_kwargs["max_workers"] == 1
        assert call_kwargs["mp_context"].get_start_method() == "spawn"
        mock_pool.submit.assert_called_once()
        submit_args = mock_pool.submit.call_args.args
        assert submit_args[0] is execute_process_call
        assert isinstance(submit_args[1], SerializedProcessCall)

    def test_cloudpickles_locally_defined_curator_for_spawn(self, tmp_path) -> None:
        from artisan.execution.models.execution_unit import ExecutionUnit
        from artisan.execution.recording.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import (
            _run_curator_in_subprocess,
        )
        from artisan.schemas.execution.runtime_environment import RuntimeEnvironment

        class NotebookCurator(OperationDefinition):
            name: ClassVar[str] = "notebook_curator"
            inputs: ClassVar[dict[str, InputSpec]] = {}
            outputs: ClassVar[dict[str, OutputSpec]] = {}

            class Params(BaseModel):
                marker: str = Field(description="Artifact marker.")

            params: Params

            def execute_curator(self, inputs, step_number, artifact_store):
                raise NotImplementedError

        def _run_notebook_curator(
            child_unit: ExecutionUnit,
            child_runtime_env: RuntimeEnvironment,
            worker_id: int,
        ) -> StagingResult:
            del child_runtime_env
            return StagingResult(
                success=True,
                execution_run_id=f"run-{worker_id}",
                artifact_ids=[child_unit.operation.params.marker],
            )

        unit = ExecutionUnit.model_construct(
            operation=NotebookCurator(
                params=NotebookCurator.Params(marker="notebook-artifact")
            ),
            inputs={},
            execution_spec_id="notebook-spec",
            step_number=0,
            group_ids=None,
            user_overrides=None,
            step_run_id=None,
        )
        runtime_env = RuntimeEnvironment(
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
        )

        with patch(
            "artisan.orchestration.engine.step_executor.run_curator_flow",
            _run_notebook_curator,
        ):
            result = _run_curator_in_subprocess(unit, runtime_env)

        assert result.success is True
        assert result.execution_run_id == "run-0"
        assert result.artifact_ids == ["notebook-artifact"]

    def test_curator_task_timeouterror_surfaces_as_failure(self) -> None:
        """A task raising TimeoutError surfaces, not an infinite poll loop.

        On Python 3.12 concurrent.futures.TimeoutError IS
        builtins.TimeoutError; the poll loop must call result() exactly once
        after done() and let the task-raised TimeoutError propagate rather
        than eating it as a poll timeout.
        """
        from artisan.orchestration.engine import step_executor as se

        unit = MagicMock()
        runtime_env = MagicMock()

        with patch(
            "artisan.orchestration.engine.step_executor.ProcessPoolExecutor"
        ) as mock_pool_cls:
            mock_pool = MagicMock()
            mock_pool_cls.return_value.__enter__ = MagicMock(return_value=mock_pool)
            mock_pool_cls.return_value.__exit__ = MagicMock(return_value=False)
            future = mock_pool.submit.return_value
            future.done.return_value = True
            future.result.side_effect = TimeoutError("task self-timeout")

            # Deadline guard: a regression would spin forever on result(timeout=).
            with (
                _deadline(10),
                pytest.raises(TimeoutError, match="task self-timeout"),
            ):
                se._run_curator_in_subprocess(unit, runtime_env)

    @patch("artisan.orchestration.engine.step_executor.record_execution_failure")
    @patch("artisan.orchestration.engine.step_executor.build_execution_context")
    @patch("artisan.orchestration.engine.step_executor._format_subprocess_kill_error")
    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_curator_subprocess_death_records_failure(
        self,
        mock_cache,
        mock_subprocess,
        mock_format_error,
        mock_build_ctx,
        mock_record_failure,
        tmp_path,
    ) -> None:
        """BrokenProcessPool should record failure and return failed StepResult."""
        from concurrent.futures.process import BrokenProcessPool

        from artisan.execution.recording.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_cache.return_value = None
        mock_subprocess.side_effect = BrokenProcessPool(
            "A process in the process pool was terminated abruptly"
        )
        mock_format_error.return_value = (
            "Curator subprocess killed (likely OOM). Child peak RSS: 4096 MB."
        )
        mock_build_ctx.return_value = MagicMock()
        mock_record_failure.return_value = StagingResult(
            success=False,
            error="Curator subprocess killed (likely OOM).",
            execution_run_id="killed-abc",
        )

        result = _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs=_prepared({"data": [_ID_S1, _ID_S2]}),
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        assert result.failed_count == 2
        assert result.succeeded_count == 0
        mock_record_failure.assert_called_once()

    def test_subprocess_kill_error_message_format(self) -> None:
        """_format_subprocess_kill_error should include RSS and input count."""
        from unittest.mock import MagicMock, mock_open

        from artisan.orchestration.engine.step_executor import (
            _format_subprocess_kill_error,
        )

        unit = MagicMock()
        unit.inputs = {"data": ["a" * 32, "b" * 32, "c" * 32]}

        meminfo_content = (
            "MemTotal:       16384000 kB\n"
            "MemFree:         1000000 kB\n"
            "MemAvailable:    3355443 kB\n"
        )

        mock_rusage = MagicMock()
        mock_rusage.ru_maxrss = 8634368  # KB → 8432 MB

        with (
            patch("artisan.orchestration.engine.step_executor.resource") as mock_res,
            patch("builtins.open", mock_open(read_data=meminfo_content)),
        ):
            mock_res.getrusage.return_value = mock_rusage
            mock_res.RUSAGE_CHILDREN = resource.RUSAGE_CHILDREN

            msg = _format_subprocess_kill_error(unit)

        assert "Curator subprocess killed (likely OOM)" in msg
        assert "8432 MB" in msg
        assert "Input artifacts: 3" in msg
        assert "System memory:" in msg
        assert "Consider reducing input size" in msg

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_regular_exception_not_caught_by_broken_executor(
        self,
        mock_cache,
        mock_subprocess,
        tmp_path,
    ) -> None:
        """ValueError should be caught by except Exception, not BrokenProcessPool."""
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        mock_cache.return_value = None
        mock_subprocess.side_effect = ValueError("bad input data")

        result = _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs=_prepared({"data": [_ID_S1]}),
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
        )

        assert result.failed_count == 1
        assert result.succeeded_count == 0
        assert result.status == StepStatus.FAILED
        assert result.error is not None
        assert "ValueError" in result.error


class TestCreateRuntimeEnvironmentFailureLogsRoot:
    """failure_logs_root must always be a local path regardless of delta_root."""

    def test_local_delta_root_uses_sibling_layout(self, tmp_path):
        from artisan.orchestration.engine.step_executor import (
            _create_runtime_environment,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        env = _create_runtime_environment(config, MockIngestOp)
        assert env.failure_logs_root == str(tmp_path / "logs" / "failures")

    def test_cloud_delta_root_derives_from_working_root(self, tmp_path):
        from artisan.orchestration.engine.step_executor import (
            _create_runtime_environment,
        )
        from artisan.schemas.execution.storage_config import StorageConfig
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root="s3://bucket/delta",
            staging_root="s3://bucket/staging",
            working_root=str(tmp_path / "working"),
            files_root="s3://bucket/files",
            storage=StorageConfig(protocol="s3"),
        )
        env = _create_runtime_environment(config, MockIngestOp)

        # Must be local (no s3:// prefix) per the runtime_environment.py:76
        # invariant.
        assert env.failure_logs_root is not None
        assert not env.failure_logs_root.startswith("s3://")
        assert env.failure_logs_root == str(tmp_path / "working" / "logs" / "failures")


# =============================================================================
# Per-step group_by override
# =============================================================================


class TestInstantiateOperationGroupByOverride:
    """``instantiate_operation`` applies a per-step ``group_by`` override
    via ``model_copy``, mirroring every other per-step knob."""

    def test_override_replaces_class_default(self):
        """An op declaring ``group_by=ZIP`` is overridden to CROSS_PRODUCT."""
        from artisan.orchestration.engine.step_executor import instantiate_operation

        # MockMultiInputCreatorOp declares group_by = ZIP at class level.
        instance = instantiate_operation(
            MockMultiInputCreatorOp,
            StepOverrides.from_user(group_by=GroupByStrategy.CROSS_PRODUCT),
        )
        assert instance.group_by is GroupByStrategy.CROSS_PRODUCT

    def test_override_none_preserves_class_default(self):
        """Without an override, the class-declared default is preserved."""
        from artisan.orchestration.engine.step_executor import instantiate_operation

        instance = instantiate_operation(
            MockMultiInputCreatorOp,
            StepOverrides.from_user(),
        )
        assert instance.group_by is GroupByStrategy.ZIP

    def test_override_applies_to_op_with_no_class_default(self):
        """An op declaring no class-level ``group_by`` (default ``None``)
        still accepts the override — symmetric with every other knob."""
        from artisan.orchestration.engine.step_executor import instantiate_operation

        # MockNoGroupByCreatorOp declares no class-level group_by.
        instance = instantiate_operation(
            MockNoGroupByCreatorOp,
            StepOverrides.from_user(group_by=GroupByStrategy.CROSS_PRODUCT),
        )
        assert instance.group_by is GroupByStrategy.CROSS_PRODUCT


class TestGroupByEffectiveConfigHashing:
    """``group_by`` reaches the cache key through ``effective_config_payload``
    off the instantiated op — class default or per-step override alike — so
    distinct strategies produce distinct spec ids."""

    def test_group_by_none_serializes_as_none(self):
        """An op with no class default and no override emits ``group_by=None``."""
        from artisan.orchestration.engine.step_executor import instantiate_operation
        from artisan.utils.hashing import effective_config_payload

        instance = instantiate_operation(
            MockNoGroupByCreatorOp, StepOverrides.from_user()
        )
        assert effective_config_payload(instance)["group_by"] is None

    def test_class_default_group_by_appears_without_override(self):
        """A class-level ``group_by=ZIP`` reaches the payload with no override —
        the effective-config behavior the old typed-override path missed."""
        from artisan.orchestration.engine.step_executor import instantiate_operation
        from artisan.utils.hashing import effective_config_payload

        instance = instantiate_operation(
            MockMultiInputCreatorOp, StepOverrides.from_user()
        )
        assert effective_config_payload(instance)["group_by"] == "zip"

    def test_override_group_by_emits_value_as_string(self):
        from artisan.orchestration.engine.step_executor import instantiate_operation
        from artisan.utils.hashing import effective_config_payload

        instance = instantiate_operation(
            MockNoGroupByCreatorOp,
            StepOverrides.from_user(group_by=GroupByStrategy.CROSS_PRODUCT),
        )
        assert effective_config_payload(instance)["group_by"] == "cross_product"

    def test_distinct_strategies_produce_distinct_step_spec_ids(self):
        """Two strategies → two ``step_spec_id`` values. Locks in the
        Design Criterion: the cache key reflects the effective ``group_by``."""
        from artisan.orchestration.engine.step_executor import instantiate_operation
        from artisan.utils.hashing import compute_step_spec_id, effective_config_payload

        common = {
            "operation_name": "x",
            "step_number": 0,
            "params": {"a": 1},
            "inputs": _prepared({"data": [_ID_S1]}).cache_inputs,
        }

        def spec_for(strategy: GroupByStrategy | None) -> str:
            instance = instantiate_operation(
                MockNoGroupByCreatorOp,
                StepOverrides.from_user(group_by=strategy),
            )
            return compute_step_spec_id(
                **common, config_overrides=effective_config_payload(instance)
            )

        spec_lineage = spec_for(GroupByStrategy.LINEAGE)
        spec_cross = spec_for(GroupByStrategy.CROSS_PRODUCT)
        spec_none = spec_for(None)
        assert spec_lineage != spec_cross
        assert spec_none not in {spec_lineage, spec_cross}


class TestFailureRecordSynthesis:
    """Seam tests for orchestrator-side failure-record synthesis (Fix 2)."""

    def _config(self, tmp_path):
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        return PipelineConfig(
            name="synth",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

    def test_backfills_empty_run_id_with_a_readable_worker_seal(self, tmp_path):
        """A failed UnitResult with no run id receives a sealed staging record.

        Covers the pre-try / unimportable-op path (Mechanism B) that cannot be
        built importably: a worker returns success=False with empty
        execution_run_ids, the orchestrator synthesizes the record, and the
        logical committer can use its execution ID as exact staging evidence.
        """
        from datetime import UTC, datetime

        from artisan.execution.models.execution_unit import ExecutionUnit
        from artisan.orchestration.engine.step_executor import (
            _create_runtime_environment,
            _require_recorded_execution_ids,
            _synthesize_missing_failure_records,
        )
        from artisan.utils.path import shard_uri

        config = self._config(tmp_path)
        op = MockNoGroupByCreatorOp()
        runtime_env = _create_runtime_environment(config, op)
        unit = ExecutionUnit(
            operation=op,
            inputs={},
            execution_spec_id="a" * 32,
            step_number=0,
        )
        result = UnitResult(
            success=False,
            error="pre-try boom",
            item_count=1,
            execution_run_ids=[],
        )

        patched = _synthesize_missing_failure_records(
            [unit],
            [result],
            runtime_env,
            datetime.now(UTC),
            None,
            step_run_id=None,
        )
        assert patched[0].execution_run_ids == ["killed-" + "a" * 24]
        assert _require_recorded_execution_ids(patched) == ["killed-" + "a" * 24]

        shard = shard_uri(
            config.staging_root,
            patched[0].execution_run_ids[0],
            step_number=0,
            operation_name=op.name,
        )
        rows = pl.read_parquet(f"{shard}/executions.parquet").to_dicts()
        assert len(rows) == 1
        assert rows[0]["origin_step_number"] == 0
        assert rows[0]["success"] is False
        assert "pre-try boom" in rows[0]["error"]

    def test_skips_units_that_already_recorded(self, tmp_path):
        """A failed result that already carries a run id is left untouched."""
        from datetime import UTC, datetime

        from artisan.execution.models.execution_unit import ExecutionUnit
        from artisan.orchestration.engine.step_executor import (
            _create_runtime_environment,
            _synthesize_missing_failure_records,
        )

        config = self._config(tmp_path)
        op = MockNoGroupByCreatorOp()
        runtime_env = _create_runtime_environment(config, op)
        unit = ExecutionUnit(operation=op, inputs={}, step_number=0)
        recorded = UnitResult(
            success=False,
            error="worker already recorded this",
            item_count=1,
            execution_run_ids=["real_run_id"],
        )

        patched = _synthesize_missing_failure_records(
            [unit],
            [recorded],
            runtime_env,
            datetime.now(UTC),
            None,
            step_run_id=None,
        )

        # Unchanged, and nothing was staged (no delta/staging writes).
        assert patched == [recorded]
        assert not (tmp_path / "staging").exists() or not any(
            (tmp_path / "staging").rglob("executions.parquet")
        )
