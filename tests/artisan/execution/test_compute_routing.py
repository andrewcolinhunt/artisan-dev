"""Tests for the create_execute_router factory."""

from __future__ import annotations

from enum import StrEnum, auto
from typing import Any, ClassVar

import pytest

from artisan.errors import ArtisanError, ErrorCode
from artisan.execution.compute.endpoint import EndpointExecuteRouter
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.compute.routing import create_execute_router
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.operation_config.compute import (
    ComputeConfig,
    LocalComputeConfig,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class _RoutingFunctionOp(OperationDefinition):
    """Function op — not routable to modal."""

    class OutputRole(StrEnum):
        result = auto()

    name: ClassVar[str] = "routing_function_op_test"
    description: ClassVar[str] = "Function op for router factory tests"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": []},
        ),
    }

    def execute_function(self, inputs):
        return None


class _RoutingCommandOp(OperationDefinition):
    """Command op — routable to modal."""

    class OutputRole(StrEnum):
        result = auto()

    name: ClassVar[str] = "routing_command_op_test"
    description: ClassVar[str] = "Command op for router factory tests"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": []},
        ),
    }

    tool: ToolSpec = ToolSpec(executable="bash", interpreter=None)

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [*self.tool.parts(), "-c", "true"]


class TestCreateExecuteRouter:
    def test_local_config_creates_local_router(self):
        config = LocalComputeConfig()
        router = create_execute_router(config, _RoutingFunctionOp())
        assert isinstance(router, LocalExecuteRouter)

    def test_modal_config_creates_endpoint_router(self):
        """Modal + command op routes the execute phase to the endpoint."""
        config = ModalComputeConfig(image="test-image", max_concurrent_calls=7)
        router = create_execute_router(config, _RoutingCommandOp())
        assert isinstance(router, EndpointExecuteRouter)
        assert router._max_concurrent_calls == 7

    def test_modal_threads_cancel_check_through(self):
        def probe() -> bool:
            return False

        config = ModalComputeConfig(image="test-image")
        router = create_execute_router(config, _RoutingCommandOp(), cancel_check=probe)
        assert isinstance(router, EndpointExecuteRouter)
        assert router._cancel_check is probe

    def test_modal_function_op_fails_fast(self):
        """Modal requires a command op — function ops are misconfigured."""
        config = ModalComputeConfig(image="test-image")
        with pytest.raises(ArtisanError) as exc_info:
            create_execute_router(config, _RoutingFunctionOp())
        assert exc_info.value.code == ErrorCode.TOOL_ENDPOINT_MISCONFIGURED
        assert "command op" in str(exc_info.value)

    def test_unknown_config_raises(self):
        config = ComputeConfig()
        with pytest.raises(ValueError, match="Unknown compute provider config"):
            create_execute_router(config, _RoutingFunctionOp())
