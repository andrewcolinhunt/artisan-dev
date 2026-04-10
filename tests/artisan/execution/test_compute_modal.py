"""Tests for ModalComputeRouter."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import cloudpickle

from artisan.execution.compute.modal import ModalComputeRouter
from artisan.schemas.operation_config.compute import ModalComputeConfig
from artisan.schemas.operation_config.environment_spec import (
    DockerEnvironmentSpec,
)
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.specs.input_models import ExecuteInput


def _make_mock_modal():
    """Build a mock modal module with App and Image."""
    mock_modal = MagicMock()

    # modal.Image.from_registry returns an image object
    mock_image = MagicMock()
    mock_modal.Image.from_registry.return_value = mock_image

    # modal.App() returns an app with a .function() decorator
    mock_app = MagicMock()
    mock_modal.App.return_value = mock_app

    # app.function() returns a decorator that wraps the function
    # and gives it a .remote() method
    def function_decorator(**kwargs):
        def decorator(fn):
            wrapped = MagicMock()
            wrapped._original_fn = fn
            wrapped.remote = MagicMock(side_effect=lambda **kw: fn(**kw))
            return wrapped

        return decorator

    mock_app.function = function_decorator
    return mock_modal


class TestModalComputeRouter:
    def test_route_execute_serializes_and_calls_remote(self):
        """route_execute cloudpickles args and calls fn.remote."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.environments = Environments()
        operation.execute.return_value = {"result": 42}
        # Make operation picklable by providing model_copy
        operation.model_copy = MagicMock(return_value=operation)

        execute_input = ExecuteInput(
            inputs={}, execute_dir="/tmp/test", log_path="/tmp/log"
        )

        with patch.dict("sys.modules", {"modal": mock_modal}):
            result = router.route_execute(operation, execute_input, "/tmp/sandbox")

        assert result == {"result": 42}

    def test_route_execute_returns_none(self):
        """route_execute passes through None returns."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.environments = Environments()
        operation.execute.return_value = None

        execute_input = ExecuteInput(
            inputs={}, execute_dir="/tmp/test", log_path="/tmp/log"
        )

        with patch.dict("sys.modules", {"modal": mock_modal}):
            result = router.route_execute(operation, execute_input, "/tmp/sandbox")

        assert result is None

    def test_get_or_create_fn_caches(self):
        """_get_or_create_fn returns the same function on repeated calls."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            fn1 = router._get_or_create_fn()
            fn2 = router._get_or_create_fn()

        assert fn1 is fn2

    def test_get_or_create_fn_passes_config(self):
        """_get_or_create_fn passes config fields to modal.App and Image."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(
            image="my-registry/gpu-image:v1",
            gpu="A100",
            memory_gb=16,
            timeout=7200,
            retries=2,
        )
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._get_or_create_fn()

        mock_modal.App.assert_called_once()
        mock_modal.Image.from_registry.assert_called_once_with(
            "my-registry/gpu-image:v1"
        )

    def test_force_local_environment_switches_docker(self):
        """_force_local_environment switches Docker environment to local."""
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        docker_envs = Environments(
            active="docker",
            docker=DockerEnvironmentSpec(image="some-image"),
        )
        operation.environments = docker_envs

        # model_copy should return a new operation with updated environments
        updated_op = MagicMock()
        operation.model_copy.return_value = updated_op

        result = router._force_local_environment(operation)

        assert result is updated_op
        operation.model_copy.assert_called_once()
        call_kwargs = operation.model_copy.call_args[1]
        updated_envs = call_kwargs["update"]["environments"]
        assert updated_envs.active == "local"

    def test_force_local_environment_noop_when_local(self):
        """_force_local_environment is a no-op when already local."""
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.environments = Environments()  # default is local

        result = router._force_local_environment(operation)

        assert result is operation  # same object, not a copy

    def test_remote_receives_cloudpickle_bytes(self):
        """fn.remote receives valid cloudpickle bytes."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.environments = Environments()
        operation.execute.return_value = "ok"

        execute_input = ExecuteInput(
            inputs={"data": ["/tmp/file"]},
            execute_dir="/tmp/exec",
            log_path="/tmp/log",
        )

        with patch.dict("sys.modules", {"modal": mock_modal}):
            fn = router._get_or_create_fn()
            # Call route_execute to trigger the remote call
            router.route_execute(operation, execute_input, "/tmp/sandbox")

        # Verify the remote was called with bytes that can be deserialized
        call_kwargs = fn.remote.call_args[1]
        deserialized_op = cloudpickle.loads(call_kwargs["operation_bytes"])
        deserialized_input = cloudpickle.loads(call_kwargs["execute_input_bytes"])
        assert deserialized_input.execute_dir == "/tmp/exec"
        assert deserialized_input.inputs == {"data": ["/tmp/file"]}
