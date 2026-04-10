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

    # app.run() returns a context manager that no-ops
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=None)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_app.run.return_value = mock_ctx

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
    def test_route_execute_serializes_and_calls_remote(self, tmp_path):
        """route_execute cloudpickles args and calls fn.remote."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.environments = Environments()
        operation.execute.return_value = {"result": 42}
        operation.tool = None

        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()

        execute_input = ExecuteInput(
            inputs={}, execute_dir=str(execute_dir), log_path="/tmp/log"
        )

        with patch.dict("sys.modules", {"modal": mock_modal}):
            result = router.route_execute(operation, execute_input, str(sandbox))

        assert result == {"result": 42}

    def test_route_execute_returns_none(self, tmp_path):
        """route_execute passes through None returns."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.environments = Environments()
        operation.execute.return_value = None
        operation.tool = None

        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()

        execute_input = ExecuteInput(
            inputs={}, execute_dir=str(execute_dir), log_path="/tmp/log"
        )

        with patch.dict("sys.modules", {"modal": mock_modal}):
            result = router.route_execute(operation, execute_input, str(sandbox))

        assert result is None

    def test_ensure_running_caches(self):
        """_ensure_running returns the same function on repeated calls."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            fn1 = router._ensure_running()
            fn2 = router._ensure_running()

        assert fn1 is fn2

    def test_ensure_running_enters_app_run(self):
        """_ensure_running enters app.run() context."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running()

        mock_app = mock_modal.App.return_value
        mock_app.run.assert_called_once()
        mock_app.run.return_value.__enter__.assert_called_once()

    def test_ensure_running_passes_config(self):
        """_ensure_running passes config fields to modal.App and Image."""
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
            router._ensure_running()

        mock_modal.App.assert_called_once()
        mock_modal.Image.from_registry.assert_called_once_with(
            "my-registry/gpu-image:v1"
        )

    def test_close_exits_app_run(self):
        """close() exits the app.run() context."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running()
            router.close()

        mock_ctx = mock_modal.App.return_value.run.return_value
        mock_ctx.__exit__.assert_called_once_with(None, None, None)
        assert router._fn is None
        assert router._app is None
        assert router._ctx is None

    def test_close_noop_when_not_running(self):
        """close() is a no-op when the router hasn't been started."""
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)
        router.close()  # should not raise

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

    def test_remote_receives_cloudpickle_bytes(self, tmp_path):
        """fn.remote receives valid cloudpickle bytes."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.environments = Environments()
        operation.execute.return_value = "ok"
        operation.tool = None

        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()

        execute_input = ExecuteInput(
            inputs={"data": ["/tmp/file"]},
            execute_dir=str(execute_dir),
            log_path="/tmp/log",
        )

        with patch.dict("sys.modules", {"modal": mock_modal}):
            fn = router._ensure_running()
            router.route_execute(operation, execute_input, str(sandbox))

        call_kwargs = fn.remote.call_args[1]
        deserialized_input = cloudpickle.loads(call_kwargs["execute_input_bytes"])
        assert deserialized_input.execute_dir == str(execute_dir)
        assert deserialized_input.inputs == {"data": ["/tmp/file"]}

    def test_sandbox_snapshot_passed_to_remote(self, tmp_path):
        """Sandbox files are snapshotted and passed to fn.remote."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.environments = Environments()
        operation.execute.return_value = "ok"
        operation.tool = None

        # Create sandbox with a file in materialized_inputs
        sandbox = tmp_path / "sandbox"
        (sandbox / "materialized_inputs").mkdir(parents=True)
        (sandbox / "materialized_inputs" / "input.txt").write_bytes(b"data")
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()

        execute_input = ExecuteInput(
            inputs={}, execute_dir=str(execute_dir), log_path="/tmp/log"
        )

        with patch.dict("sys.modules", {"modal": mock_modal}):
            fn = router._ensure_running()
            router.route_execute(operation, execute_input, str(sandbox))

        call_kwargs = fn.remote.call_args[1]
        assert call_kwargs["sandbox"] == {"materialized_inputs/input.txt": b"data"}

    def test_tool_files_passed_to_remote(self, tmp_path):
        """Tool script files are snapshotted and passed to fn.remote."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        # Create a tool script
        script = tmp_path / "tool.py"
        script.write_bytes(b"print('tool')")

        operation = MagicMock()
        operation.environments = Environments()
        operation.execute.return_value = "ok"
        operation.tool = MagicMock()
        operation.tool.executable = str(script)

        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()

        execute_input = ExecuteInput(
            inputs={}, execute_dir=str(execute_dir), log_path="/tmp/log"
        )

        with patch.dict("sys.modules", {"modal": mock_modal}):
            fn = router._ensure_running()
            router.route_execute(operation, execute_input, str(sandbox))

        call_kwargs = fn.remote.call_args[1]
        assert call_kwargs["tool_files"] == {str(script): b"print('tool')"}

    def test_output_snapshot_restored_locally(self, tmp_path):
        """Output files from remote are restored in the sandbox."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.environments = Environments()
        operation.tool = None

        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()

        # Operation writes a file during execute
        def write_output(inp):
            (execute_dir / "result.json").write_bytes(b'{"done": true}')
            return {"success": True}

        operation.execute.side_effect = write_output

        execute_input = ExecuteInput(
            inputs={}, execute_dir=str(execute_dir), log_path="/tmp/log"
        )

        with patch.dict("sys.modules", {"modal": mock_modal}):
            result = router.route_execute(operation, execute_input, str(sandbox))

        assert result == {"success": True}
        # The output file should exist (restored from remote snapshot)
        assert (execute_dir / "result.json").read_bytes() == b'{"done": true}'
