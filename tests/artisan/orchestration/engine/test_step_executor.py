"""Tests for step execution, operation preparation, dispatch, and cancellation."""

from __future__ import annotations

import resource
import signal
import threading
from contextlib import contextmanager
from enum import StrEnum, auto
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from pydantic import BaseModel, Field, ValidationError

from artisan.errors import PersistenceIntegrityError
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.inputs import PreparedInputs
from artisan.orchestration.engine.step_executor import (
    _cancelled_result,
    execute_step,
    instantiate_operation,
)
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import FailurePolicy, GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.curator_result import (
    ArtifactResult,
    PassthroughResult,
)
from artisan.schemas.execution.unit_result import UnitResult
from artisan.schemas.operation_config.compute import (
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.environment_spec import DockerEnvironmentSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_overrides import StepOverrides
from artisan.schemas.specs.input_models import PreprocessInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.hashing import CacheInputIdentity, compute_step_spec_id


class TestExecuteStepPassesCancelEvent:
    """execute_step uses and forwards the already-prepared operation."""

    @patch(
        "artisan.orchestration.engine.step_executor.effective_config_payload",
        return_value={},
    )
    @patch("artisan.orchestration.engine.step_executor._execute_creator_step")
    @patch(
        "artisan.orchestration.engine.step_executor.is_curator_operation",
        return_value=False,
    )
    def test_passes_cancel_event_to_creator(
        self, mock_is_curator, mock_creator, mock_config_payload
    ):
        mock_op = MagicMock()
        mock_op.name = "test"
        mock_creator.return_value = MagicMock()
        config = MagicMock(failure_policy=FailurePolicy.CONTINUE, skip_cache=False)

        event = threading.Event()
        execute_step(
            operation=mock_op,
            inputs=_prepared({}),
            ov=StepOverrides(),
            step_runner=MagicMock(),
            config=config,
            cancel_event=event,
        )

        _, kwargs = mock_creator.call_args
        assert kwargs["cancel_event"] is event
        assert kwargs["operation"] is mock_op

    @patch(
        "artisan.orchestration.engine.step_executor.effective_config_payload",
        return_value={},
    )
    @patch("artisan.orchestration.engine.step_executor._execute_curator_step")
    @patch(
        "artisan.orchestration.engine.step_executor.is_curator_operation",
        return_value=True,
    )
    def test_passes_cancel_event_to_curator(
        self, mock_is_curator, mock_curator, mock_config_payload
    ):
        mock_op = MagicMock()
        mock_op.name = "test"
        mock_curator.return_value = MagicMock()
        config = MagicMock(failure_policy=FailurePolicy.CONTINUE, skip_cache=False)

        event = threading.Event()
        execute_step(
            operation=mock_op,
            inputs=_prepared({}),
            ov=StepOverrides(),
            step_runner=MagicMock(),
            config=config,
            cancel_event=event,
        )

        _, kwargs = mock_curator.call_args
        assert kwargs["cancel_event"] is event
        assert kwargs["operation"] is mock_op


class TestCreatorCancelChecks:
    """_execute_creator_step returns cancelled result when event is set."""

    @patch("artisan.orchestration.engine.step_executor.get_batch_config")
    @patch(
        "artisan.orchestration.engine.step_executor.generate_execution_unit_batches",
        return_value=[],
    )
    def test_cancel_before_execute_phase(self, mock_batches, mock_batch_config):
        """Cancel event set before PHASE 2 should return cancelled result."""
        from artisan.orchestration.engine.step_executor import _execute_creator_step

        mock_op = MagicMock()
        mock_op.name = "test_op"
        mock_op.outputs = {}
        mock_op.group_by = None

        event = threading.Event()
        event.set()  # Pre-set = cancelled

        config = MagicMock()
        config.delta_root = MagicMock()
        config.staging_root = MagicMock()

        result = _execute_creator_step(
            operation=mock_op,
            inputs=_prepared({"data": ["id1"]}),
            step_runner=MagicMock(),
            step_number=1,
            config=config,
            cancel_event=event,
        )

        assert result.status == StepStatus.CANCELLED
        assert result.cancellation_status == CancellationStatus.CONFIRMED

    def test_cancel_before_execute_phase_curator(self):
        """Cancel event set before execute should return cancelled result for curator."""
        from artisan.orchestration.engine.step_executor import _execute_curator_step

        mock_op = MagicMock()
        mock_op.name = "filter"
        mock_op.outputs = {}
        mock_op.group_by = None
        mock_op.params = None

        event = threading.Event()
        event.set()

        config = MagicMock()
        config.delta_root = MagicMock()

        result = _execute_curator_step(
            operation=mock_op,
            inputs=_prepared({"data": ["id1"]}),
            step_number=1,
            config=config,
            cancel_event=event,
            skip_cache=True,
        )

        assert result.status == StepStatus.CANCELLED
        assert result.cancellation_status == CancellationStatus.CONFIRMED


class TestCancelledResult:
    """Tests for the _cancelled_result helper."""

    def test_cancelled_result_has_explicit_state(self):
        mock_op = MagicMock()
        mock_op.name = "test"
        mock_op.outputs = {}

        result = _cancelled_result(mock_op, 1, FailurePolicy.CONTINUE)
        assert result.status == StepStatus.CANCELLED
        assert result.cancellation_status == CancellationStatus.CONFIRMED
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
            derives_from={"inputs": ["data"]},
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
            derives_from={"inputs": ["data"]},
        ),
    }

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    def preprocess(self, _inputs):
        return {}

    def execute_command(self, inputs):
        return [*self.tool.parts(), "-c", "true"]


class _ConfiguredToolOp(_SimpleToolOp):
    """Tool op with non-schema defaults for recursive patch tests."""

    runner_resources: RunnerResources = RunnerResources(
        cpus=8,
        memory_gb=32,
        extra={"scheduler": {"queue": "cpu", "account": "research"}},
    )
    batch_strategy: BatchStrategy = BatchStrategy(
        artifacts_per_unit=4,
        max_workers=8,
    )
    environments: Environments = Environments(
        active="docker",
        docker=DockerEnvironmentSpec(
            image="old:v1",
            gpu=True,
            binds=[("/host", "/container")],
            env={"KEEP": "yes", "CHANGE": "old"},
        ),
    )
    tool: ToolSpec = ToolSpec(
        executable="bash",
        interpreter="env",
        subcommand="old",
    )
    compute_provider: ComputeProvider = ComputeProvider(
        active="modal",
        modal=ModalComputeConfig(
            retries=5,
            secrets=["old"],
            env={"KEEP": "yes", "CHANGE": "old"},
        ),
    )
    compute_resources: ComputeResources = ComputeResources(
        gpu="A100",
        memory_gb=32,
    )


class _DefaultParamsOp(_SimpleCreatorOp):
    """Operation with defaulted nested parameters."""

    name: ClassVar[str] = "default_params_instantiation_test"

    class Params(BaseModel):
        count: int = Field(default=3, description="Number of items.")

    params: Params = Params()


class _RequiredParamsOp(_SimpleCreatorOp):
    """Operation with required nested parameters."""

    name: ClassVar[str] = "required_params_instantiation_test"

    class Params(BaseModel):
        count: int = Field(description="Number of items.")

    params: Params


class TestComputeRoutingSelection:
    """_execute_creator_step routes every creator step through the runner.

    The compute provider (axis 2) is consulted inside the lifecycle, in
    create_execute_router — modal misconfiguration coverage lives in
    tests/artisan/execution/test_compute_routing.py.
    """

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_modal_tool_op_uses_runner_dispatch(
        self,
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
            inputs=_prepared({"data": [_ID]}),
            step_runner=mock_backend,
            step_number=1,
            config=config,
        )

        mock_backend.validate_operation.assert_called_once_with(op)
        mock_backend.create_lifecycle_router.assert_called_once()
        mock_handle.run.assert_called_once()

    @patch("artisan.orchestration.engine.step_executor.persist_worker_logs")
    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    def test_local_compute_uses_backend_dispatch(
        self,
        mock_cache,
        mock_persist_worker_logs,
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
            inputs=_prepared({"data": [_ID]}),
            step_runner=mock_backend,
            step_number=1,
            config=config,
        )

        mock_backend.create_lifecycle_router.assert_called_once()
        router_kwargs = mock_backend.create_lifecycle_router.call_args.kwargs
        assert "log_folder" not in router_kwargs
        mock_handle.run.assert_called_once()
        mock_persist_worker_logs.assert_called_once()
        assert "fs" in mock_persist_worker_logs.call_args.kwargs
        mock_backend.capture_logs.assert_not_called()


class TestInstantiateOperationParams:
    def test_default_params_apply_when_override_is_none(self) -> None:
        operation = instantiate_operation(_DefaultParamsOp, StepOverrides(params=None))

        assert operation.params == _DefaultParamsOp.Params(count=3)

    def test_empty_params_mapping_uses_nested_defaults(self) -> None:
        operation = instantiate_operation(_DefaultParamsOp, StepOverrides(params={}))

        assert operation.params == _DefaultParamsOp.Params(count=3)

    def test_user_params_are_nested_and_validated(self) -> None:
        operation = instantiate_operation(
            _DefaultParamsOp,
            StepOverrides(params={"count": 7}),
        )

        assert operation.params == _DefaultParamsOp.Params(count=7)

    @pytest.mark.parametrize("params", [None, {}])
    def test_required_params_reject_missing_value(
        self, params: dict[str, object] | None
    ) -> None:
        with pytest.raises(ValidationError):
            instantiate_operation(_RequiredParamsOp, StepOverrides(params=params))

    def test_parameterless_operation_rejects_nonempty_params(self) -> None:
        with pytest.raises(ValueError, match="declares no Params"):
            instantiate_operation(
                _SimpleCreatorOp,
                StepOverrides(params={"count": 1}),
            )


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
        """String selector uses the configured target on the operation."""

        class _ConfiguredModalOp(_SimpleToolOp):
            compute_provider: ComputeProvider = ComputeProvider(
                modal=ModalComputeConfig()
            )

        op = instantiate_operation(
            _ConfiguredModalOp,
            StepOverrides.from_user(compute_provider="modal"),
        )
        assert op.compute_provider.active == "modal"

    def test_unknown_string_selector_is_validated(self):
        with pytest.raises(ValidationError, match="Unknown compute provider"):
            instantiate_operation(
                _SimpleCreatorOp,
                StepOverrides.from_user(compute_provider="slurm"),
            )

    def test_unconfigured_string_selector_is_rejected(self):
        with pytest.raises(ValueError, match="not configured"):
            instantiate_operation(
                _SimpleCreatorOp,
                StepOverrides.from_user(compute_provider="modal"),
            )

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

    def test_instantiate_operation_environment_string_override(self):
        class _DockerOp(_SimpleCreatorOp):
            environments: Environments = Environments(
                docker=DockerEnvironmentSpec(image="image:v1")
            )

        result = instantiate_operation(
            _DockerOp,
            StepOverrides.from_user(environment="docker"),
        )

        assert result.environments.active == "docker"
        assert result.environments.current().image == "image:v1"

    def test_unconfigured_environment_selector_is_rejected(self):
        with pytest.raises(ValueError, match="not configured"):
            instantiate_operation(
                _SimpleCreatorOp,
                StepOverrides.from_user(environment={"active": "docker"}),
            )


class TestInstantiateOperationRecursivePatches:
    """Every target model follows the same recursive replacement rule."""

    def test_recursive_updates_preserve_untouched_siblings(self) -> None:
        operation = instantiate_operation(
            _ConfiguredToolOp,
            StepOverrides.from_user(
                runner_resources={
                    "cpus": 2,
                    "extra": {"scheduler": {"queue": "gpu"}},
                },
                batch_strategy={"max_workers": 3},
                environment={"docker": {"env": {"CHANGE": "new"}}},
                tool={"subcommand": "new"},
                compute_provider={"modal": {"env": {"CHANGE": "new"}}},
                compute_resources={"memory_gb": 64},
            ),
        )

        assert operation.runner_resources.cpus == 2
        assert operation.runner_resources.memory_gb == 32
        assert operation.runner_resources.extra == {
            "scheduler": {"queue": "gpu", "account": "research"}
        }
        assert operation.batch_strategy.artifacts_per_unit == 4
        assert operation.batch_strategy.max_workers == 3
        assert operation.environments.docker.env == {
            "KEEP": "yes",
            "CHANGE": "new",
        }
        assert operation.environments.docker.image == "old:v1"
        assert operation.tool == ToolSpec(
            executable="bash", interpreter="env", subcommand="new"
        )
        assert operation.compute_provider.modal.env == {
            "KEEP": "yes",
            "CHANGE": "new",
        }
        assert operation.compute_provider.modal.retries == 5
        assert operation.compute_resources == ComputeResources(gpu="A100", memory_gb=64)

    def test_nested_empty_mappings_replace_inherited_mappings(self) -> None:
        operation = instantiate_operation(
            _ConfiguredToolOp,
            StepOverrides.from_user(
                runner_resources={"extra": {}},
                environment={"docker": {"env": {}}},
                compute_provider={"modal": {"env": {}}},
            ),
        )

        assert operation.runner_resources.extra == {}
        assert operation.environments.docker.env == {}
        assert operation.compute_provider.modal.env == {}

    def test_scalars_lists_and_none_replace_inherited_values(self) -> None:
        operation = instantiate_operation(
            _ConfiguredToolOp,
            StepOverrides.from_user(
                batch_strategy={"max_workers": None},
                environment={"docker": {"binds": [], "gpu": False}},
                tool={"subcommand": None},
                compute_provider={"modal": {"secrets": []}},
                compute_resources={"gpu": None},
            ),
        )

        assert operation.batch_strategy.max_workers is None
        assert operation.environments.docker.binds == []
        assert operation.environments.docker.gpu is False
        assert operation.tool.subcommand is None
        assert operation.compute_provider.modal.secrets == []
        assert operation.compute_resources.gpu is None
        assert operation.compute_resources.memory_gb == 32

    def test_empty_root_tool_patch_is_noop_without_declared_tool(self) -> None:
        operation = instantiate_operation(
            _SimpleCreatorOp,
            StepOverrides.from_user(tool={}),
        )

        assert operation.tool is None


class TestInstantiateOperationValidatedMappingOverrides:
    @pytest.mark.parametrize(
        ("operation_class", "overrides"),
        [
            (_SimpleCreatorOp, {"runner_resources": {"cpus": 0}}),
            (_SimpleCreatorOp, {"batch_strategy": {"artifacts_per_unit": 0}}),
            (
                _SimpleCreatorOp,
                {"environment": {"local": {"unknown": True}}},
            ),
            (
                _SimpleCreatorOp,
                {"compute_provider": {"modal": {"min_container": 2}}},
            ),
            (_SimpleCreatorOp, {"compute_resources": {"cpu": 0}}),
            (_SimpleToolOp, {"tool": {"executable": None}}),
        ],
    )
    def test_invalid_mapping_patch_is_revalidated(
        self,
        operation_class: type[OperationDefinition],
        overrides: dict[str, object],
    ) -> None:
        with pytest.raises(ValidationError):
            instantiate_operation(
                operation_class,
                StepOverrides.from_user(**overrides),  # type: ignore[arg-type]
            )

    def test_valid_mapping_patches_preserve_unset_defaults(self) -> None:
        operation = instantiate_operation(
            _SimpleCreatorOp,
            StepOverrides.from_user(
                runner_resources={"cpus": 2},
                batch_strategy={"max_workers": 3},
            ),
        )

        assert operation.runner_resources == RunnerResources(cpus=2)
        assert operation.batch_strategy == BatchStrategy(max_workers=3)

    def test_valid_tool_patch_preserves_executable(self) -> None:
        operation = instantiate_operation(
            _SimpleToolOp,
            StepOverrides.from_user(tool={"subcommand": "run"}),
        )

        assert operation.tool == ToolSpec(
            executable="bash",
            interpreter=None,
            subcommand="run",
        )


@pytest.mark.parametrize("curator", [False, True])
@pytest.mark.parametrize("cacheable", [False, True])
def test_execute_step_cacheability_controls_both_dispatch_routes(
    tmp_path,
    monkeypatch,
    curator: bool,
    cacheable: bool,
) -> None:
    """An explicit reuse policy and skip_cache=False cannot override the class."""
    from artisan.operations.curator import IngestPipelineStep
    from artisan.schemas.enums import CachePolicy
    from artisan.schemas.orchestration.pipeline_config import PipelineConfig

    operation = (
        IngestPipelineStep(
            params={
                "source_delta_root": str(tmp_path / "source"),
                "source_run_id": "source",
                "source_step": 0,
            }
        )
        if curator
        else _SimpleCreatorOp()
    )
    monkeypatch.setattr(type(operation), "cacheable", cacheable)
    route = "_execute_curator_step" if curator else "_execute_creator_step"
    with patch(f"artisan.orchestration.engine.step_executor.{route}") as dispatch:
        execute_step(
            operation,
            _prepared({}),
            StepOverrides(skip_cache=False, cache_policy=CachePolicy.STEP_COMPLETED),
            MagicMock(),
            config=PipelineConfig(
                name="test",
                delta_root=str(tmp_path / "delta"),
                staging_root=str(tmp_path / "staging"),
                skip_cache=False,
            ),
        )
    assert dispatch.call_args.kwargs["skip_cache"] is (not cacheable)


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
        OutputRole.data: OutputSpec(artifact_type=ArtifactTypes.FILE_REF),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict:
        """No inputs to preprocess for curator op."""
        return {}

    def execute_curator(self, inputs, step_number, artifact_store) -> ArtifactResult:
        """Mock curator execution."""
        return ArtifactResult(
            success=True,
        )


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
            derives_from={"inputs": ["data"]},
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
            derives_from={"inputs": ["data"]},
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

        # Inspect the ordered units received by the lifecycle router.
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

        # Inspect the ordered units received by the lifecycle router.
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


class TestDispatchFailureHandling:
    """Tests for dispatch failure resilience."""

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


class TestCreatorCancellationRetention:
    """Cancellation retains evidence while control-sentinel cleanup stays exact."""

    @patch("artisan.orchestration.engine.step_executor.check_cache_for_batch")
    @pytest.mark.parametrize("preserve_staging", [False, True])
    def test_cancelled_pending_failure_record_is_retained(
        self,
        mock_cache,
        tmp_path,
        preserve_staging,
    ):
        """A synthesized failure remains available with either preservation flag."""
        import threading

        from artisan.orchestration.engine.step_executor import _execute_creator_step
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
            preserve_staging=preserve_staging,
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
        seals = list((tmp_path / "staging").rglob("executions.parquet"))
        assert len(seals) == 1
        assert pl.read_parquet(seals[0]).item(0, "success") is False

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


@pytest.mark.parametrize("kind", ["creator", "curator"])
@pytest.mark.parametrize("preserve_staging", [False, True])
@pytest.mark.parametrize(
    "outcome", [CancellationStatus.CONFIRMED, CancellationStatus.UNKNOWN]
)
def test_cancellation_outcomes_preserve_existing_shards(
    tmp_path,
    monkeypatch,
    kind,
    preserve_staging,
    outcome,
):
    from pathlib import Path

    from artisan.execution.recording.parquet_writer import StagingResult
    from artisan.orchestration.engine import step_executor
    from artisan.schemas.orchestration.pipeline_config import PipelineConfig
    from artisan.schemas.orchestration.step_lifecycle import CancellationAcknowledgement
    from artisan.utils.path import shard_uri

    config = PipelineConfig(
        name="retention",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "working"),
        preserve_staging=preserve_staging,
    )
    event = threading.Event()
    operation = (
        MockNoGroupByCreatorOp() if kind == "creator" else MockNoGroupByCuratorOp()
    )
    run_id = "a" * 32
    directory = Path(
        shard_uri(
            config.staging_root, run_id, step_number=1, operation_name=operation.name
        )
    )
    directory.mkdir(parents=True)
    (directory / "executions.parquet").write_bytes(b"worker evidence")
    (directory / "artifact_index.parquet").write_bytes(b"artifact evidence")
    before = {path: path.read_bytes() for path in directory.iterdir()}
    acknowledgement = CancellationAcknowledgement(outcome, "cancellation evidence")
    monkeypatch.setattr(
        step_executor, "check_cache_for_batch", lambda *_args, **_kwargs: None
    )

    def finish_creator(*_args, **_kwargs):
        event.set()
        return [
            UnitResult(
                success=True,
                error=None,
                item_count=1,
                execution_run_ids=[run_id],
                cancellation_acknowledgement=acknowledgement,
            )
        ]

    def finish_curator(*_args, **_kwargs):
        event.set()
        return StagingResult(
            success=True,
            execution_run_id=run_id,
            artifact_ids=[_ID_S1],
            cancellation_acknowledgement=acknowledgement,
        )

    kwargs = {
        "operation": operation,
        "inputs": _prepared({"data": [_ID_S1]}),
        "config": config,
        "step_number": 1,
        "cancel_event": event,
        "step_run_id": "b" * 32,
    }
    if kind == "creator":
        backend, router = _make_mock_backend()
        router.run.side_effect = finish_creator
        result = step_executor._execute_creator_step(step_runner=backend, **kwargs)
    else:
        monkeypatch.setattr(step_executor, "_run_curator_in_subprocess", finish_curator)
        result = step_executor._execute_curator_step(**kwargs)
    assert result.cancellation_status is outcome
    assert result.status is (
        StepStatus.CANCELLED
        if outcome is CancellationStatus.CONFIRMED
        else StepStatus.FAILED
    )
    assert {path: path.read_bytes() for path in directory.iterdir()} == before


class TestCommitFailureHandling:
    """Tests for commit phase failure resilience."""

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
    """Tests for staging verification timeout resilience."""

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
        metric_ids = ["d" * 32, "e" * 32, "f" * 32, "0" * 32, "1" * 32]
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
            mock_pool_cls.return_value = mock_pool
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
        ) -> StagingResult:
            return StagingResult(
                success=True,
                execution_run_id=f"run-{child_runtime_env.worker_id}",
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
            mock_pool_cls.return_value = mock_pool
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
    """Seam tests for orchestrator-side failure-record synthesis."""

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

        Covers the pre-try / unimportable-op path that cannot be
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
        runtime_env = _create_runtime_environment(config, op).model_copy(
            update={"worker_id": 42}
        )
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
        assert len(patched[0].execution_run_ids[0]) == 32
        int(patched[0].execution_run_ids[0], 16)
        assert _require_recorded_execution_ids(patched) == patched[0].execution_run_ids
        repeated = _synthesize_missing_failure_records(
            [unit], [result], runtime_env, datetime.now(UTC), None, step_run_id=None
        )
        assert repeated[0].execution_run_ids != patched[0].execution_run_ids

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
        assert rows[0]["source_worker"] == 0
        assert runtime_env.worker_id == 42
        assert "pre-try boom" in rows[0]["error"]
        from artisan.schemas.execution.command_record import CommandRecording

        assert (
            CommandRecording.model_validate_json(rows[0]["command_recording"])
            == CommandRecording.unavailable()
        )

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


class _DefaultHashOpV1(OperationDefinition):
    name = "test_op"
    description = "Test operation v1"
    inputs: ClassVar[dict[str, InputSpec]] = {}

    class OutputRole(StrEnum):
        result = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "result": OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            derives_from={"inputs": []},
        ),
    }

    class Params(BaseModel):
        """Mock parameters.

        Attributes:
            temperature: Mock temperature.
            max_steps: Mock maximum step count.
        """

        temperature: float = Field(default=0.5)
        max_steps: int = Field(default=100)

    params: Params = Params()

    def execute_function(self, inputs: Any) -> dict[str, Any]:
        return {}

    def postprocess(self, inputs: Any) -> Any:
        return None


class _DefaultHashOpV2(OperationDefinition):
    name = "test_op"
    description = "Test operation v2"
    inputs: ClassVar[dict[str, InputSpec]] = {}

    class OutputRole(StrEnum):
        result = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "result": OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            derives_from={"inputs": []},
        ),
    }

    class Params(BaseModel):
        """Mock parameters with a changed default.

        Attributes:
            temperature: Mock temperature.
            max_steps: Mock maximum step count.
        """

        temperature: float = Field(default=0.8)
        max_steps: int = Field(default=100)

    params: Params = Params()

    def execute_function(self, inputs: Any) -> dict[str, Any]:
        return {}

    def postprocess(self, inputs: Any) -> Any:
        return None


class TestStepSpecIdWithDefaults:
    """Tests that step_spec_id correctly reflects operation defaults."""

    def test_different_defaults_produce_different_spec_id(self):
        """step_spec_id changes when operation defaults change, even with params=None.

        The step identity uses full instantiated
        params (defaults + overrides), not just user overrides.
        """
        instance_v1 = instantiate_operation(_DefaultHashOpV1, StepOverrides.from_user())
        instance_v2 = instantiate_operation(_DefaultHashOpV2, StepOverrides.from_user())

        full_params_v1 = instance_v1.params.model_dump(mode="json")
        full_params_v2 = instance_v2.params.model_dump(mode="json")

        assert full_params_v1 != full_params_v2

        spec1 = compute_step_spec_id(
            operation_name="test_op",
            step_number=0,
            params=full_params_v1,
            inputs={},
        )
        spec2 = compute_step_spec_id(
            operation_name="test_op",
            step_number=0,
            params=full_params_v2,
            inputs={},
        )
        assert spec1 != spec2

    def test_same_defaults_produce_same_spec_id(self):
        """step_spec_id is stable when defaults are unchanged."""
        instance1 = instantiate_operation(_DefaultHashOpV1, StepOverrides.from_user())
        instance2 = instantiate_operation(_DefaultHashOpV1, StepOverrides.from_user())

        full_params1 = instance1.params.model_dump(mode="json")
        full_params2 = instance2.params.model_dump(mode="json")

        spec1 = compute_step_spec_id(
            operation_name="test_op",
            step_number=0,
            params=full_params1,
            inputs={},
        )
        spec2 = compute_step_spec_id(
            operation_name="test_op",
            step_number=0,
            params=full_params2,
            inputs={},
        )
        assert spec1 == spec2

    def test_user_override_matches_same_default(self):
        """Explicit user override equal to default produces same spec_id as no override."""
        instance_no_override = instantiate_operation(
            _DefaultHashOpV1, StepOverrides.from_user()
        )
        instance_with_override = instantiate_operation(
            _DefaultHashOpV1, StepOverrides.from_user(params={"temperature": 0.5})
        )

        params_no = instance_no_override.params.model_dump(mode="json")
        params_with = instance_with_override.params.model_dump(mode="json")

        assert params_no == params_with

        spec_no = compute_step_spec_id(
            operation_name="test_op",
            step_number=0,
            params=params_no,
            inputs={},
        )
        spec_with = compute_step_spec_id(
            operation_name="test_op",
            step_number=0,
            params=params_with,
            inputs={},
        )
        assert spec_no == spec_with
