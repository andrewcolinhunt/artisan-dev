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
    instantiate_operation,
)
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import FailurePolicy
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.compute import (
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.environment_spec import DockerEnvironmentSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.orchestration.step_overrides import StepOverrides
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class TestExecuteStepPassesCancelEvent:
    """execute_step should forward cancel_event to creator/curator paths."""

    @patch(
        "artisan.orchestration.engine.step_executor.effective_config_payload",
        return_value={},
    )
    @patch("artisan.orchestration.engine.step_executor._execute_creator_step")
    @patch(
        "artisan.orchestration.engine.step_executor.is_curator_operation",
        return_value=False,
    )
    @patch("artisan.orchestration.engine.step_executor.instantiate_operation")
    def test_passes_cancel_event_to_creator(
        self, mock_instantiate, mock_is_curator, mock_creator, mock_config_payload
    ):
        mock_op = MagicMock()
        mock_op.name = "test"
        mock_instantiate.return_value = mock_op
        mock_creator.return_value = MagicMock()

        event = threading.Event()
        execute_step(
            operation_class=MagicMock(),
            inputs=None,
            ov=StepOverrides(),
            step_runner=MagicMock(),
            cancel_event=event,
        )

        _, kwargs = mock_creator.call_args
        assert kwargs["cancel_event"] is event

    @patch(
        "artisan.orchestration.engine.step_executor.effective_config_payload",
        return_value={},
    )
    @patch("artisan.orchestration.engine.step_executor._execute_curator_step")
    @patch(
        "artisan.orchestration.engine.step_executor.is_curator_operation",
        return_value=True,
    )
    @patch("artisan.orchestration.engine.step_executor.instantiate_operation")
    def test_passes_cancel_event_to_curator(
        self, mock_instantiate, mock_is_curator, mock_curator, mock_config_payload
    ):
        mock_op = MagicMock()
        mock_op.name = "test"
        mock_instantiate.return_value = mock_op
        mock_curator.return_value = MagicMock()

        event = threading.Event()
        execute_step(
            operation_class=MagicMock(),
            inputs=None,
            ov=StepOverrides(),
            step_runner=MagicMock(),
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
            step_runner=MagicMock(),
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

    def preprocess(self, _inputs):
        return {}

    def execute_function(self, _inputs):
        return {}


class _SimpleToolOp(OperationDefinition):
    """Minimal tool op for endpoint dispatch routing tests."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "routing_tool_op"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.FILE_REF, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.FILE_REF,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    def preprocess(self, _inputs):
        return {}

    def execute_command(self, inputs):
        return [*self.tool.parts(), "-c", "true"]


def _make_mock_backend(flow_return_value=None):
    """Create a mock step_runner whose lifecycle router captures dispatched units."""
    mock_backend = MagicMock()
    mock_backend.name = "local"
    mock_backend.worker_traits.worker_id_env_var = None
    mock_backend.worker_traits.shared_filesystem = False
    mock_backend.orchestrator_traits.needs_staging_verification = False
    mock_backend.orchestrator_traits.staging_verification_timeout = 60.0

    mock_handle = MagicMock()
    return_value = flow_return_value if flow_return_value is not None else []
    mock_handle.run.return_value = return_value
    mock_backend.create_lifecycle_router.return_value = mock_handle
    return mock_backend, mock_handle


class TestComputeRoutingSelection:
    """_execute_creator_step routes every creator step through the runner.

    The compute provider (axis 2) is consulted inside the lifecycle, in
    create_execute_router — modal misconfiguration coverage lives in
    tests/artisan/execution/test_compute_routing.py.
    """

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_modal_tool_op_uses_runner_dispatch(
        self,
        mock_resolve,
        mock_cache,
        tmp_path,
    ):
        """Modal ops ride the same lifecycle-router path as local ops."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        op = _SimpleToolOp(
            compute_provider=ComputeProvider(
                active="modal", modal=ModalComputeConfig()
            ),
        )

        mock_resolve.return_value = {"data": [_ID]}
        mock_cache.return_value = None

        mock_backend, mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(
                    success=True, error=None, item_count=1, execution_run_ids=[]
                ),
            ]
        )

        _execute_creator_step(
            operation=op,
            inputs={"data": [_ID]},
            step_runner=mock_backend,
            step_number=1,
            config=config,
            compact=False,
        )

        mock_backend.validate_operation.assert_called_once_with(op)
        mock_backend.create_lifecycle_router.assert_called_once()
        mock_handle.run.assert_called_once()

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @patch("artisan.orchestration.engine.step_executor.resolve_inputs")
    def test_local_compute_uses_backend_dispatch(
        self,
        mock_resolve,
        mock_cache,
        tmp_path,
    ):
        """LocalComputeConfig uses the standard step_runner dispatch path."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )

        op = _SimpleCreatorOp()  # default: compute_provider.active="local"

        mock_resolve.return_value = {"data": [_ID]}
        mock_cache.return_value = None

        mock_backend, mock_handle = _make_mock_backend(
            flow_return_value=[
                UnitResult(
                    success=True, error=None, item_count=1, execution_run_ids=[]
                ),
            ],
        )

        _execute_creator_step(
            operation=op,
            inputs={"data": [_ID]},
            step_runner=mock_backend,
            step_number=1,
            config=config,
            compact=False,
        )

        mock_backend.create_lifecycle_router.assert_called_once()
        mock_handle.run.assert_called_once()


class TestInstantiateOperationComputeOverrides:
    """Dict-form compute_provider overrides must coerce into proper Pydantic models."""

    def test_instantiate_operation_compute_dict_coerces_modal_config(self):
        """Dict override creates ModalComputeConfig when field starts as None."""
        op = instantiate_operation(
            _SimpleCreatorOp,
            StepOverrides.from_user(
                compute_provider={"active": "modal", "modal": {"min_containers": 8}}
            ),
        )
        assert isinstance(op.compute_provider.modal, ModalComputeConfig)
        assert op.compute_provider.modal.min_containers == 8
        assert op.compute_provider.active == "modal"

    def test_instantiate_operation_compute_dict_merges_existing_modal(self):
        """Partial dict preserves existing fields on the nested config.

        Hardware fields (gpu, memory_gb, timeout) live on
        ``ComputeResources``; ``ModalComputeConfig`` carries Modal-specific
        non-hardware fields like ``retries`` and ``min_containers``.
        """

        # active stays "local": a class-level modal *default* now requires a
        # tool op (ToolSpec + execute_command); the dict-merge under test only
        # needs an existing nested modal config.
        class _ModalOp(_SimpleCreatorOp):
            compute_provider: ComputeProvider = ComputeProvider(
                modal=ModalComputeConfig(retries=5, min_containers=2)
            )

        result = instantiate_operation(
            _ModalOp,
            StepOverrides.from_user(compute_provider={"modal": {"min_containers": 4}}),
        )
        assert result.compute_provider.modal.retries == 5
        assert result.compute_provider.modal.min_containers == 4

    def test_instantiate_operation_compute_string_override(self):
        """String compute_provider override selects the active provider."""
        op = instantiate_operation(
            _SimpleCreatorOp,
            StepOverrides.from_user(compute_provider="modal"),
        )
        assert op.compute_provider.active == "modal"

    def test_instantiate_operation_compute_dict_passes_isinstance_check(self):
        """Reproduces the bug: compute_provider.current() must return a ModalComputeConfig."""
        op = instantiate_operation(
            _SimpleCreatorOp,
            StepOverrides.from_user(
                compute_provider={"active": "modal", "modal": {"min_containers": 8}}
            ),
        )
        assert isinstance(op.compute_provider.current(), ModalComputeConfig)


class TestInstantiateOperationEnvironmentOverrides:
    """Dict-form environment overrides must coerce into proper Pydantic models."""

    def test_instantiate_operation_env_dict_coerces_docker_from_none(self):
        """Dict override creates DockerEnvironmentSpec when field starts as None."""
        op = instantiate_operation(
            _SimpleCreatorOp,
            StepOverrides.from_user(
                environment={"active": "docker", "docker": {"image": "my-image:v2"}}
            ),
        )
        assert isinstance(op.environments.docker, DockerEnvironmentSpec)
        assert op.environments.docker.image == "my-image:v2"
        assert op.environments.active == "docker"

    def test_instantiate_operation_env_dict_merges_existing_docker(self):
        """Partial dict preserves existing fields on the nested spec."""

        class _DockerOp(_SimpleCreatorOp):
            environments: Environments = Environments(
                active="docker",
                docker=DockerEnvironmentSpec(image="old:v1", gpu=True),
            )

        result = instantiate_operation(
            _DockerOp,
            StepOverrides.from_user(environment={"docker": {"image": "new:v2"}}),
        )
        assert result.environments.docker.image == "new:v2"
        assert result.environments.docker.gpu is True
