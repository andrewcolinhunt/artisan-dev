"""Tests for step_executor module.

Reference: design_orchestration_internals_v2.md
Reference: design_utility_operations.md

These tests verify the step executor behavior for:
- File-path promotion via _promote_file_paths_to_store() (in pipeline_manager)
- Orchestrator-level input pairing (group_inputs integration)
"""

from __future__ import annotations

import resource
from enum import StrEnum, auto
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import FailurePolicy, GroupByStrategy
from artisan.schemas.execution.curator_result import ArtifactResult, PassthroughResult
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.specs.input_models import PreprocessInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

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

    def execute(self, inputs, output_dir):
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

    def test_all_invalid_files_returns_none(self, tmp_path):
        """All invalid file paths should return None."""
        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test_pipeline",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )
        (tmp_path / "delta").mkdir(parents=True)
        (tmp_path / "staging").mkdir(parents=True)

        non_existent = str(tmp_path / "does_not_exist.csv")
        result, count = _promote_file_paths_to_store(
            [non_existent], config, 1, "mock_ingest"
        )

        assert result is None
        assert count == 0

    def test_directory_path_filtered_out(self, tmp_path):
        """Directory paths should be filtered out."""
        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test_pipeline",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )
        (tmp_path / "delta").mkdir(parents=True)
        (tmp_path / "staging").mkdir(parents=True)

        test_dir = tmp_path / "test_directory"
        test_dir.mkdir()

        result, count = _promote_file_paths_to_store(
            [str(test_dir)], config, 1, "mock_ingest"
        )

        assert result is None
        assert count == 0

    def test_valid_files_promoted(self, tmp_path):
        """Valid file paths should be promoted to artifact IDs."""
        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test_pipeline",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )
        (tmp_path / "delta").mkdir(parents=True)
        (tmp_path / "staging").mkdir(parents=True)

        test_file = tmp_path / "test.csv"
        test_file.write_bytes(b"ATOM content")

        result, count = _promote_file_paths_to_store(
            [str(test_file)], config, 0, "mock_ingest"
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


class MockMultiInputCreatorOp(OperationDefinition):
    """Mock multi-input creator op with group_by=ZIP."""

    class InputRole(StrEnum):
        data = auto()
        config = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_multi_creator"
    group_by: ClassVar[GroupByStrategy | None] = GroupByStrategy.ZIP
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

    def execute(self, inputs, output_dir):
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

    def execute(self, inputs, output_dir):
        return ArtifactResult(success=True)


class MockMultiInputCuratorOp(OperationDefinition):
    """Mock multi-input curator op with group_by=ZIP."""

    class InputRole(StrEnum):
        data = auto()
        config = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_multi_curator"
    group_by: ClassVar[GroupByStrategy | None] = GroupByStrategy.ZIP
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
    """Create a mock backend for step executor tests.

    Returns a mock backend with a mock dispatch handle.  The handle's
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

    mock_backend.create_dispatch_handle.return_value = mock_handle
    return mock_backend, mock_handle


# =============================================================================
# Tests for Creator Step Pairing Phase
# =============================================================================


class TestCreatorStepPairing:
    """Tests for group_inputs integration in _execute_creator_step()."""

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    @patch("artisan.orchestration.engine.step_executor.group_inputs")
    def test_creator_with_group_by_calls_group_inputs(
        self,
        mock_group_inputs,
        mock_resolve,
        mock_cache,
        tmp_path,
    ):
        """Creator step with group_by should call group_inputs()."""
        from artisan.orchestration.engine.step_executor import (
            _execute_creator_step,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_backend, mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(success=True, error=None, item_count=2, execution_run_ids=[])
            ],
        )

        resolved = {
            "data": [_ID_S1, _ID_S2],
            "config": [_ID_C1, _ID_C2],
        }
        paired = {
            "data": [_ID_S1, _ID_S2],
            "config": [_ID_C1, _ID_C2],
        }
        gids = ["gid1", "gid2"]

        mock_resolve.return_value = resolved
        mock_group_inputs.return_value = (paired, gids)
        mock_cache.return_value = None  # No cache hits

        _execute_creator_step(
            operation=MockMultiInputCreatorOp(),
            inputs={"data": [_ID_S1, _ID_S2], "config": [_ID_C1, _ID_C2]},
            backend=mock_backend,
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        # group_inputs should be called with resolved inputs and ZIP strategy
        mock_group_inputs.assert_called_once()
        call_args = mock_group_inputs.call_args
        assert call_args[0][0] == resolved
        assert call_args[0][1] == GroupByStrategy.ZIP

        # backend flow receives units_path; verify captured units
        dispatched_units = mock_handle._captured_units
        assert len(dispatched_units) > 0
        # With batch size 1 (default), 2 items -> 2 units
        for unit in dispatched_units:
            assert unit.group_ids is not None

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    @patch("artisan.orchestration.engine.step_executor.group_inputs")
    def test_creator_without_group_by_skips_pairing(
        self,
        mock_group_inputs,
        mock_resolve,
        mock_cache,
        tmp_path,
    ):
        """Creator step without group_by should NOT call group_inputs()."""
        from artisan.orchestration.engine.step_executor import (
            _execute_creator_step,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_backend, mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(success=True, error=None, item_count=2, execution_run_ids=[])
            ],
        )

        resolved = {"data": [_ID_S1, _ID_S2]}
        mock_resolve.return_value = resolved
        mock_cache.return_value = None

        _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs={"data": [_ID_S1, _ID_S2]},
            backend=mock_backend,
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        # group_inputs should NOT be called
        mock_group_inputs.assert_not_called()

        # backend flow receives units_path; verify captured units
        dispatched_units = mock_handle._captured_units
        for unit in dispatched_units:
            assert unit.group_ids is None

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    @patch("artisan.orchestration.engine.step_executor.group_inputs")
    def test_creator_group_ids_sliced_across_batches(
        self,
        mock_group_inputs,
        mock_resolve,
        mock_cache,
        tmp_path,
    ):
        """Group_ids should be sliced by batching in sync with inputs."""
        from artisan.orchestration.engine.step_executor import (
            _execute_creator_step,
        )
        from artisan.schemas.execution.execution_config import ExecutionConfig
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
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
        mock_resolve.return_value = resolved
        mock_group_inputs.return_value = (resolved, ["g1", "g2", "g3", "g4"])
        mock_cache.return_value = None

        # Create operation with artifacts_per_unit=2
        op = MockMultiInputCreatorOp()
        op = op.model_copy(update={"execution": ExecutionConfig(artifacts_per_unit=2)})

        _execute_creator_step(
            operation=op,
            inputs=resolved,
            backend=mock_backend,
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
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
    """Tests for group_inputs integration in _execute_curator_step()."""

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    @patch("artisan.orchestration.engine.step_executor.group_inputs")
    def test_curator_with_group_by_calls_group_inputs(
        self,
        mock_group_inputs,
        mock_resolve,
        mock_cache,
        mock_curator_flow,
        tmp_path,
    ):
        """Curator step with group_by should call group_inputs()."""
        from artisan.execution.staging.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        resolved = {
            "data": [_ID_S1, _ID_S2],
            "config": [_ID_C1, _ID_C2],
        }
        paired = {
            "data": [_ID_S1, _ID_S2],
            "config": [_ID_C1, _ID_C2],
        }
        gids = ["gid1", "gid2"]

        mock_resolve.return_value = resolved
        mock_group_inputs.return_value = (paired, gids)
        mock_cache.return_value = None
        mock_curator_flow.return_value = StagingResult(
            success=True, artifact_ids=[_ID_S1, _ID_S2], execution_run_id="run1"
        )

        _execute_curator_step(
            operation=MockMultiInputCuratorOp(),
            inputs={"data": [_ID_S1, _ID_S2], "config": [_ID_C1, _ID_C2]},
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        # group_inputs should be called
        mock_group_inputs.assert_called_once()
        call_args = mock_group_inputs.call_args
        assert call_args[0][0] == resolved
        assert call_args[0][1] == GroupByStrategy.ZIP

        # _run_curator_in_subprocess should receive a unit with group_ids set
        unit = mock_curator_flow.call_args[0][0]
        assert unit.group_ids == gids

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    @patch("artisan.orchestration.engine.step_executor.group_inputs")
    def test_curator_without_group_by_skips_pairing(
        self,
        mock_group_inputs,
        mock_resolve,
        mock_cache,
        mock_curator_flow,
        tmp_path,
    ):
        """Curator step without group_by should NOT call group_inputs()."""
        from artisan.execution.staging.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        resolved = {"data": [_ID_S1, _ID_S2]}
        mock_resolve.return_value = resolved
        mock_cache.return_value = None
        mock_curator_flow.return_value = StagingResult(
            success=True, artifact_ids=[_ID_S1, _ID_S2], execution_run_id="run1"
        )

        _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs={"data": [_ID_S1, _ID_S2]},
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        # group_inputs should NOT be called
        mock_group_inputs.assert_not_called()

        # _run_curator_in_subprocess should receive a unit with group_ids=None
        unit = mock_curator_flow.call_args[0][0]
        assert unit.group_ids is None

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    @patch("artisan.orchestration.engine.step_executor.group_inputs")
    def test_curator_group_ids_set_on_execution_unit(
        self,
        mock_group_inputs,
        mock_resolve,
        mock_cache,
        mock_curator_flow,
        tmp_path,
    ):
        """Curator step group_ids should be attached to the ExecutionUnit."""
        from artisan.execution.staging.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        resolved = {
            "data": [_ID_S1, _ID_S2],
            "config": [_ID_C1, _ID_C2],
        }
        paired = {
            "data": [_ID_S2, _ID_S1],  # Reordered by pairing
            "config": [_ID_C2, _ID_C1],
        }
        gids = ["gid_x", "gid_y"]

        mock_resolve.return_value = resolved
        mock_group_inputs.return_value = (paired, gids)
        mock_cache.return_value = None
        mock_curator_flow.return_value = StagingResult(
            success=True, artifact_ids=[_ID_S1, _ID_S2], execution_run_id="run1"
        )

        _execute_curator_step(
            operation=MockMultiInputCuratorOp(),
            inputs={"data": [_ID_S1, _ID_S2], "config": [_ID_C1, _ID_C2]},
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
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
            success=True,
        )
        assert result.metadata == {}

    def test_step_result_with_timings_metadata(self):
        """StepResult should accept timings in metadata."""
        from artisan.schemas.orchestration.step_result import StepResult

        timings = {"resolve_inputs": 0.1, "execute": 1.5, "total": 1.6}
        result = StepResult(
            step_name="test",
            step_number=1,
            success=True,
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
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_creator_step_returns_timings(
        self,
        mock_resolve,
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
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_backend, mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(success=True, error=None, item_count=2, execution_run_ids=[])
            ],
        )

        mock_resolve.return_value = {"data": [_ID_S1]}
        mock_cache.return_value = None

        result = _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs={"data": [_ID_S1]},
            backend=mock_backend,
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        assert "timings" in result.metadata
        timings = result.metadata["timings"]
        assert "resolve_inputs" in timings
        assert "batch_and_cache" in timings
        assert "execute" in timings
        assert "verify_staging" in timings
        assert "commit" in timings
        assert "compact" in timings
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
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_curator_step_returns_timings(
        self,
        mock_resolve,
        mock_cache,
        mock_curator_flow,
        tmp_path,
    ):
        """Curator step should include timing metadata in result."""
        from artisan.execution.staging.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_resolve.return_value = {"data": [_ID_S1]}
        mock_cache.return_value = None
        mock_curator_flow.return_value = StagingResult(
            success=True, artifact_ids=[_ID_S1, _ID_S2], execution_run_id="run1"
        )

        result = _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs={"data": [_ID_S1]},
            config_overrides=None,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        assert "timings" in result.metadata
        timings = result.metadata["timings"]
        assert "resolve_inputs" in timings
        assert "batch_and_cache" in timings
        assert "execute" in timings
        assert "verify_staging" in timings
        assert "commit" in timings
        assert "compact" in timings
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

    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_creator_step_skips_on_empty_inputs(
        self,
        mock_resolve,
        tmp_path,
    ):
        """Creator step should skip execution when all input roles are empty."""
        from artisan.orchestration.engine.step_executor import (
            _execute_creator_step,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_backend, mock_handle = _make_mock_backend()

        mock_resolve.return_value = {"data": []}

        result = _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs={"data": []},
            backend=mock_backend,
            config_overrides=None,
            step_number=2,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        mock_backend.create_dispatch_handle.assert_not_called()
        assert result.metadata["skipped"] is True
        assert result.metadata["skip_reason"] == "empty_inputs"
        assert result.succeeded_count == 0
        assert result.failed_count == 0

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_curator_step_skips_on_empty_inputs(
        self,
        mock_resolve,
        mock_curator_flow,
        tmp_path,
    ):
        """Curator step should skip execution when all input roles are empty."""
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_resolve.return_value = {"data": []}

        result = _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs={"data": []},
            config_overrides=None,
            step_number=2,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        mock_curator_flow.assert_not_called()
        assert result.metadata["skipped"] is True
        assert result.metadata["skip_reason"] == "empty_inputs"
        assert result.succeeded_count == 0
        assert result.failed_count == 0

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_generative_op_not_skipped(
        self,
        mock_resolve,
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
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_backend, mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(success=True, error=None, item_count=1, execution_run_ids=[])
            ],
        )

        mock_resolve.return_value = {}
        mock_cache.return_value = None

        result = _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs=None,
            backend=mock_backend,
            config_overrides=None,
            step_number=0,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        mock_handle.run.assert_called_once()
        assert result.metadata.get("skipped") is not True

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
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_creator_dispatch_failure_returns_step_result(
        self,
        mock_resolve,
        mock_cache,
        tmp_path,
    ):
        """Creator step returns StepResult (not raises) on dispatch failure."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_backend, mock_handle = _make_mock_backend(
            flow_side_effect=ConnectionError("Network down"),
        )

        mock_resolve.return_value = {"data": [_ID_S1]}
        mock_cache.return_value = None

        result = _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs={"data": [_ID_S1]},
            backend=mock_backend,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        assert result.succeeded_count == 0
        assert result.failed_count == 1  # 1 unit dispatched
        assert "dispatch_error" in result.metadata
        assert "ConnectionError" in result.metadata["dispatch_error"]
        assert "Network down" in result.metadata["dispatch_error"]

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    @patch(
        "artisan.orchestration.engine.step_executor._run_curator_in_subprocess",
        side_effect=ConnectionError("Network down"),
    )
    def test_curator_dispatch_failure_returns_step_result(
        self,
        mock_curator_flow,
        mock_resolve,
        mock_cache,
        tmp_path,
    ):
        """Curator step returns StepResult (not raises) on execution failure."""
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_resolve.return_value = {"data": [_ID_S1]}
        mock_cache.return_value = None

        result = _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs={"data": [_ID_S1]},
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        assert result.succeeded_count == 0
        assert result.failed_count == 1
        assert "dispatch_error" in result.metadata
        assert "ConnectionError" in result.metadata["dispatch_error"]
        assert "Network down" in result.metadata["dispatch_error"]

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_dispatch_fail_fast_still_raises(
        self,
        mock_resolve,
        mock_cache,
        tmp_path,
    ):
        """RuntimeError (from fail_fast) should still propagate."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_backend, mock_handle = _make_mock_backend(
            flow_side_effect=RuntimeError("fail_fast: step failed"),
        )

        mock_resolve.return_value = {"data": [_ID_S1]}
        mock_cache.return_value = None

        with pytest.raises(RuntimeError, match="fail_fast"):
            _execute_creator_step(
                operation=MockNoGroupByCreatorOp(),
                inputs={"data": [_ID_S1]},
                backend=mock_backend,
                step_number=1,
                config=config,
                failure_policy=FailurePolicy.CONTINUE,
                compact=False,
            )


class TestCommitFailureHandling:
    """Tests for F16: commit phase failure resilience."""

    @patch("artisan.storage.io.commit.DeltaCommitter.commit_all_tables")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_creator_commit_failure_returns_step_result_with_error(
        self,
        mock_resolve,
        mock_cache,
        mock_commit,
        tmp_path,
    ):
        """Creator step captures commit error in metadata, doesn't raise."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_backend, mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(
                    success=True, error=None, item_count=1, execution_run_ids=["a"]
                )
            ],
        )

        mock_resolve.return_value = {"data": [_ID_S1]}
        mock_cache.return_value = None
        mock_commit.side_effect = OSError("Disk full")

        result = _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs={"data": [_ID_S1]},
            backend=mock_backend,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        assert "commit_error" in result.metadata
        assert "Disk full" in result.metadata["commit_error"]
        # succeeded/failed counts come from dispatch, not commit
        assert result.succeeded_count == 1


class TestStagingTimeoutHandling:
    """Tests for F15: staging verification timeout resilience."""

    @patch("artisan.orchestration.engine.step_executor.await_staging_files")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_staging_timeout_continues_to_commit(
        self,
        mock_resolve,
        mock_cache,
        mock_await,
        tmp_path,
    ):
        """Staging timeout should log warning and continue, not raise."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_backend, mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(
                    success=True, error=None, item_count=1, execution_run_ids=["a"]
                )
            ],
            needs_staging_verification=True,
        )

        mock_resolve.return_value = {"data": [_ID_S1]}
        mock_cache.return_value = None
        mock_await.side_effect = TimeoutError("NFS cache timeout")

        result = _execute_creator_step(
            operation=MockNoGroupByCreatorOp(),
            inputs={"data": [_ID_S1]},
            backend=mock_backend,
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        # Should not raise, should return result
        assert result.succeeded_count == 1


class TestFileValidationBatch:
    """Tests for batch file validation in _promote_file_paths_to_store."""

    def test_mixed_valid_invalid_files_processes_valid_only(self, tmp_path):
        """Valid files should be promoted even when some are invalid."""
        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )
        (tmp_path / "delta").mkdir(parents=True)
        (tmp_path / "staging").mkdir(parents=True)

        # One valid file, one non-existent
        valid_file = tmp_path / "valid.csv"
        valid_file.write_bytes(b"ATOM content")
        non_existent = str(tmp_path / "missing.csv")

        result, count = _promote_file_paths_to_store(
            [str(valid_file), non_existent],
            config,
            1,
            "mock_ingest",
        )

        # Should promote the valid file, skip the invalid one
        assert result is not None
        assert "file" in result
        assert count == 1


# =============================================================================
# Tests for Filter Step Logging
# =============================================================================


class TestFilterStepLogging:
    """Verify filter step logs correct pass/total counts."""

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_filter_log_counts_only_passthrough_role(
        self,
        mock_resolve,
        mock_cache,
        mock_curator_flow,
        tmp_path,
        caplog,
    ):
        """Filter log should count only passthrough role, not metric inputs."""
        from artisan.execution.staging.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        # 3 passthrough artifacts, 5 metric artifacts
        passthrough_ids = [_ID_S1, _ID_S2, "c" * 32]
        metric_ids = ["d" * 32, "e" * 32, "f" * 32, "g" * 32, "h" * 32]
        mock_resolve.return_value = {
            "passthrough": passthrough_ids,
            "quality_metrics": metric_ids,
        }
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
                inputs={
                    "passthrough": passthrough_ids,
                    "quality_metrics": metric_ids,
                },
                config_overrides=None,
                step_number=11,
                config=config,
                failure_policy=FailurePolicy.CONTINUE,
                compact=False,
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
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_filter_log_zero_pass(
        self,
        mock_resolve,
        mock_cache,
        mock_curator_flow,
        tmp_path,
        caplog,
    ):
        """Filter log should show 0/N when nothing passes."""
        from artisan.execution.staging.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        passthrough_ids = [_ID_S1, _ID_S2]
        mock_resolve.return_value = {"passthrough": passthrough_ids}
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
                inputs={"passthrough": passthrough_ids},
                config_overrides=None,
                step_number=5,
                config=config,
                failure_policy=FailurePolicy.CONTINUE,
                compact=False,
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
# Tests for step_spec_id bypass in curator execution
# =============================================================================


class TestCuratorStepSpecId:
    """Tests for step_spec_id fast path in _execute_curator_step."""

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_step_spec_id_skips_cache_check(
        self,
        mock_resolve,
        mock_cache,
        mock_curator_flow,
        tmp_path,
    ):
        """When step_spec_id is provided, check_cache_for_batch is not called."""
        from artisan.execution.staging.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_resolve.return_value = {"data": [_ID_S1]}
        mock_curator_flow.return_value = StagingResult(
            success=True, artifact_ids=[_ID_S1], execution_run_id="run1"
        )

        result = _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs={"data": [_ID_S1]},
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
            step_spec_id="precomputed_spec_abc123",
        )

        # Cache check should NOT be called — step_spec_id fast path
        mock_cache.assert_not_called()
        assert result.success

        # The ExecutionUnit should use the step_spec_id as its spec_id
        call_args = mock_curator_flow.call_args
        unit = call_args[0][0]  # first positional arg
        assert unit.execution_spec_id == "precomputed_spec_abc123"

    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_no_step_spec_id_uses_fallback(
        self,
        mock_resolve,
        mock_cache,
        mock_curator_flow,
        tmp_path,
    ):
        """When step_spec_id is None, check_cache_for_batch is still called."""
        from artisan.execution.staging.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_resolve.return_value = {"data": [_ID_S1]}
        mock_cache.return_value = None
        mock_curator_flow.return_value = StagingResult(
            success=True, artifact_ids=[_ID_S1], execution_run_id="run1"
        )

        result = _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs={"data": [_ID_S1]},
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        # Fallback path: cache check SHOULD be called
        mock_cache.assert_called_once()
        assert result.success


# =============================================================================
# Tests for Curator Subprocess Isolation
# =============================================================================


class TestCuratorSubprocessIsolation:
    """Tests for subprocess isolation of curator operations."""

    def test_curator_runs_in_subprocess(self) -> None:
        """_run_curator_in_subprocess should delegate to ProcessPoolExecutor."""
        from unittest.mock import MagicMock

        from artisan.execution.staging.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import (
            _run_curator_in_subprocess,
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

    @patch("artisan.orchestration.engine.step_executor.record_execution_failure")
    @patch("artisan.orchestration.engine.step_executor.build_curator_execution_context")
    @patch("artisan.orchestration.engine.step_executor._format_subprocess_kill_error")
    @patch("artisan.orchestration.engine.step_executor._run_curator_in_subprocess")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_curator_subprocess_death_records_failure(
        self,
        mock_resolve,
        mock_cache,
        mock_subprocess,
        mock_format_error,
        mock_build_ctx,
        mock_record_failure,
        tmp_path,
    ) -> None:
        """BrokenProcessPool should record failure and return failed StepResult."""
        from concurrent.futures.process import BrokenProcessPool

        from artisan.execution.staging.parquet_writer import StagingResult
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_resolve.return_value = {"data": [_ID_S1, _ID_S2]}
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
            inputs={"data": [_ID_S1, _ID_S2]},
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        assert result.failed_count == 1
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
            patch("pathlib.Path.open", mock_open(read_data=meminfo_content)),
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
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_regular_exception_not_caught_by_broken_executor(
        self,
        mock_resolve,
        mock_cache,
        mock_subprocess,
        tmp_path,
    ) -> None:
        """ValueError should be caught by except Exception, not BrokenProcessPool."""
        from artisan.orchestration.engine.step_executor import _execute_curator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
            working_root=tmp_path / "working",
        )

        mock_resolve.return_value = {"data": [_ID_S1]}
        mock_cache.return_value = None
        mock_subprocess.side_effect = ValueError("bad input data")

        result = _execute_curator_step(
            operation=MockNoGroupByCuratorOp(),
            inputs={"data": [_ID_S1]},
            step_number=1,
            config=config,
            failure_policy=FailurePolicy.CONTINUE,
            compact=False,
        )

        assert result.failed_count == 1
        assert result.succeeded_count == 0
        assert "dispatch_error" in result.metadata
        assert "ValueError" in result.metadata["dispatch_error"]
