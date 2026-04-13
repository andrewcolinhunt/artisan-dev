"""Tests for cancel_event handling and dispatch routing in step_executor."""

from __future__ import annotations

import threading
from enum import StrEnum, auto
from typing import ClassVar
from unittest.mock import MagicMock, patch

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.step_executor import (
    _cancelled_result,
    execute_step,
)
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import FailurePolicy
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.compute import (
    Compute,
    LocalComputeConfig,
    ModalComputeConfig,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class TestExecuteStepPassesCancelEvent:
    """execute_step should forward cancel_event to creator/curator paths."""

    @patch("artisan.orchestration.engine.step_executor._execute_creator_step")
    @patch(
        "artisan.orchestration.engine.step_executor.is_curator_operation",
        return_value=False,
    )
    @patch("artisan.orchestration.engine.step_executor.instantiate_operation")
    def test_passes_cancel_event_to_creator(
        self, mock_instantiate, mock_is_curator, mock_creator
    ):
        mock_op = MagicMock()
        mock_op.name = "test"
        mock_instantiate.return_value = mock_op
        mock_creator.return_value = MagicMock()

        event = threading.Event()
        execute_step(
            operation_class=MagicMock(),
            inputs=None,
            params=None,
            backend=MagicMock(),
            cancel_event=event,
        )

        _, kwargs = mock_creator.call_args
        assert kwargs["cancel_event"] is event

    @patch("artisan.orchestration.engine.step_executor._execute_curator_step")
    @patch(
        "artisan.orchestration.engine.step_executor.is_curator_operation",
        return_value=True,
    )
    @patch("artisan.orchestration.engine.step_executor.instantiate_operation")
    def test_passes_cancel_event_to_curator(
        self, mock_instantiate, mock_is_curator, mock_curator
    ):
        mock_op = MagicMock()
        mock_op.name = "test"
        mock_instantiate.return_value = mock_op
        mock_curator.return_value = MagicMock()

        event = threading.Event()
        execute_step(
            operation_class=MagicMock(),
            inputs=None,
            params=None,
            backend=MagicMock(),
            cancel_event=event,
        )

        _, kwargs = mock_curator.call_args
        assert kwargs["cancel_event"] is event


class TestCreatorCancelChecks:
    """_execute_creator_step returns cancelled result when event is set."""

    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    @patch("artisan.orchestration.engine.step_executor.get_batch_config")
    @patch(
        "artisan.orchestration.engine.step_executor.generate_execution_unit_batches",
        return_value=[],
    )
    def test_cancel_before_execute_phase(
        self, mock_batches, mock_batch_config, mock_resolve
    ):
        """Cancel event set before PHASE 2 should return cancelled result."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step

        mock_op = MagicMock()
        mock_op.name = "test_op"
        mock_op.outputs = {}
        mock_op.group_by = None
        mock_resolve.return_value = {"data": ["id1"]}

        event = threading.Event()
        event.set()  # Pre-set = cancelled

        config = MagicMock()
        config.delta_root = MagicMock()
        config.staging_root = MagicMock()

        result = _execute_creator_step(
            operation=mock_op,
            inputs={"data": ["id1"]},
            backend=MagicMock(),
            step_number=1,
            config=config,
            cancel_event=event,
        )

        assert result.metadata.get("cancelled") is True

    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_cancel_before_execute_phase_curator(self, mock_resolve):
        """Cancel event set before execute should return cancelled result for curator."""
        from artisan.orchestration.engine.step_executor import _execute_curator_step

        mock_op = MagicMock()
        mock_op.name = "filter"
        mock_op.outputs = {}
        mock_op.group_by = None
        mock_resolve.return_value = {"data": ["id1"]}

        event = threading.Event()
        event.set()

        config = MagicMock()
        config.delta_root = MagicMock()

        result = _execute_curator_step(
            operation=mock_op,
            inputs={"data": ["id1"]},
            step_number=1,
            config=config,
            cancel_event=event,
            step_spec_id="test-spec-id",
        )

        assert result.metadata.get("cancelled") is True


class TestCancelledResult:
    """Tests for the _cancelled_result helper."""

    def test_cancelled_result_has_metadata(self):
        mock_op = MagicMock()
        mock_op.name = "test"
        mock_op.outputs = {}

        result = _cancelled_result(mock_op, 1, FailurePolicy.CONTINUE)
        assert result.metadata["cancelled"] is True
        assert result.succeeded_count == 0
        assert result.failed_count == 0


_ID = "a" * 32


class _SimpleCreatorOp(OperationDefinition):
    """Minimal creator op for dispatch routing tests."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "routing_test_op"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.FILE_REF, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.FILE_REF,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    def preprocess(self, inputs):
        return {}

    def execute(self, inputs):
        return {}


def _make_mock_backend(flow_return_value=None):
    """Create a mock backend whose dispatch handle captures dispatched units."""
    mock_backend = MagicMock()
    mock_backend.name = "local"
    mock_backend.worker_traits.worker_id_env_var = None
    mock_backend.worker_traits.shared_filesystem = False
    mock_backend.orchestrator_traits.needs_staging_verification = False
    mock_backend.orchestrator_traits.staging_verification_timeout = 60.0

    mock_handle = MagicMock()
    return_value = flow_return_value if flow_return_value is not None else []
    mock_handle.run.return_value = return_value
    mock_backend.create_dispatch_handle.return_value = mock_handle
    return mock_backend, mock_handle


class TestComputeRoutingSelection:
    """_execute_creator_step routes to ComputeRoutingDispatchHandle for Modal."""

    @patch(
        "artisan.orchestration.engine.compute_routing_handle"
        ".ComputeRoutingDispatchHandle"
    )
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_modal_compute_uses_routing_handle(
        self,
        mock_resolve,
        mock_cache,
        mock_handle_cls,
        tmp_path,
    ):
        """ModalComputeConfig triggers ComputeRoutingDispatchHandle."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        op = _SimpleCreatorOp(
            compute=Compute(active="modal", modal=ModalComputeConfig()),
        )

        mock_resolve.return_value = {"data": [_ID]}
        mock_cache.return_value = None

        mock_handle = MagicMock()
        mock_handle.run.return_value = [
            UnitResult(success=True, error=None, item_count=1, execution_run_ids=[]),
        ]
        mock_handle_cls.return_value = mock_handle

        mock_backend, _ = _make_mock_backend()

        _execute_creator_step(
            operation=op,
            inputs={"data": [_ID]},
            backend=mock_backend,
            step_number=1,
            config=config,
            compact=False,
        )

        mock_handle_cls.assert_called_once()
        call_kwargs = mock_handle_cls.call_args.kwargs
        assert isinstance(call_kwargs["compute_config"], ModalComputeConfig)
        mock_backend.create_dispatch_handle.assert_not_called()

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_local_compute_uses_backend_dispatch(
        self,
        mock_resolve,
        mock_cache,
        tmp_path,
    ):
        """LocalComputeConfig uses the standard backend dispatch path."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        op = _SimpleCreatorOp()  # default: compute.active="local"

        mock_resolve.return_value = {"data": [_ID]}
        mock_cache.return_value = None

        mock_backend, mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(success=True, error=None, item_count=1, execution_run_ids=[]),
            ],
        )

        _execute_creator_step(
            operation=op,
            inputs={"data": [_ID]},
            backend=mock_backend,
            step_number=1,
            config=config,
            compact=False,
        )

        mock_backend.create_dispatch_handle.assert_called_once()
        mock_handle.run.assert_called_once()
