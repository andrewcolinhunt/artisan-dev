"""Tests for ModalComputeRouter."""

from __future__ import annotations

import subprocess
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
    # and gives it a .remote() method. The remote mock returns a
    # (result, output_snapshot) tuple — matching the real return
    # signature — without deserializing the cloudpickled bytes
    # (MagicMock can't survive cloudpickle round-trips).
    _captured = {}

    def function_decorator(**kwargs):
        def decorator(fn):
            wrapped = MagicMock()
            wrapped._original_fn = fn
            # Default: return (None, {}) — tests override via _captured
            wrapped.remote = MagicMock(
                side_effect=lambda **kw: _captured.get(
                    "remote_fn", lambda **k: (None, {})
                )(**kw)
            )
            return wrapped

        return decorator

    mock_app.function = function_decorator
    mock_modal._captured = _captured
    return mock_modal


class TestModalComputeRouter:
    def test_route_execute_serializes_and_calls_remote(self, tmp_path):
        """route_execute cloudpickles args and calls fn.remote."""
        mock_modal = _make_mock_modal()
        mock_modal._captured["remote_fn"] = lambda **kw: ({"result": 42}, {})
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.environments = Environments()
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
        mock_modal._captured["remote_fn"] = lambda **kw: (None, {})
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.environments = Environments()
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
            fn1 = router._ensure_running("test_op")
            fn2 = router._ensure_running("test_op")

        assert fn1 is fn2

    def test_ensure_running_enters_app_run(self):
        """_ensure_running enters app.run() context."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        mock_modal.App.assert_called_once_with("artisan-test_op")
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
            router._ensure_running("gpu_op")

        mock_modal.App.assert_called_once_with("artisan-gpu_op")
        mock_modal.Image.from_registry.assert_called_once_with(
            "my-registry/gpu-image:v1"
        )

    def test_close_exits_app_run(self):
        """close() exits the app.run() context."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")
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
            fn = router._ensure_running("test_op")
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
            fn = router._ensure_running("test_op")
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
            fn = router._ensure_running("test_op")
            router.route_execute(operation, execute_input, str(sandbox))

        call_kwargs = fn.remote.call_args[1]
        assert call_kwargs["tool_files"] == {str(script): b"print('tool')"}

    def test_init_app_no_registry_secret(self):
        """Default config does not pass a secret to from_registry."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        mock_modal.Image.from_registry.assert_called_once_with("test:latest")
        mock_modal.Secret.from_name.assert_not_called()

    def test_init_app_resolves_registry_secret(self):
        """When set, image_registry_secret resolves via Secret.from_name."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(
            image="priv:latest",
            image_registry_secret="ghcr-pat",
        )
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        mock_modal.Secret.from_name.assert_called_once_with("ghcr-pat")
        mock_modal.Image.from_registry.assert_called_once_with(
            "priv:latest",
            secret=mock_modal.Secret.from_name.return_value,
        )

    def test_output_snapshot_restored_locally(self, tmp_path):
        """Output files from remote are restored in the sandbox."""
        mock_modal = _make_mock_modal()
        # Simulate remote returning an output snapshot
        output_snapshot = {"result.json": b'{"done": true}'}
        mock_modal._captured["remote_fn"] = lambda **kw: (
            {"success": True},
            output_snapshot,
        )
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.environments = Environments()
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

        assert result == {"success": True}
        # The output file should exist in execute_dir (not sandbox_root)
        assert (execute_dir / "result.json").read_bytes() == b'{"done": true}'

    def test_route_execute_passes_empty_dirs(self, tmp_path):
        """Empty sandbox dirs are threaded to fn.remote as sandbox_dirs."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.environments = Environments()
        operation.execute.return_value = "ok"
        operation.tool = None

        # Sandbox has an empty execute/ leaf — captured by snapshot_sandbox.
        sandbox = tmp_path / "sandbox"
        (sandbox / "materialized_inputs").mkdir(parents=True)
        (sandbox / "materialized_inputs" / "input.txt").write_bytes(b"data")
        (sandbox / "execute").mkdir()  # empty shell
        execute_dir = sandbox / "execute"
        # For the outer route_execute test path, execute_dir exists locally;
        # the assertion below is on what crossed the `remote()` boundary.

        execute_input = ExecuteInput(
            inputs={}, execute_dir=str(execute_dir), log_path="/tmp/log"
        )

        with patch.dict("sys.modules", {"modal": mock_modal}):
            fn = router._ensure_running("test_op")
            router.route_execute(operation, execute_input, str(sandbox))

        call_kwargs = fn.remote.call_args[1]
        assert call_kwargs["sandbox"] == {"materialized_inputs/input.txt": b"data"}
        assert call_kwargs["sandbox_dirs"] == ["execute"]


class TestExecuteOnModalCallback:
    """End-to-end regression for the Modal container callback.

    The symptom we're fixing: cloudpickled ops that do
    `subprocess.Popen(cwd=execute_input.execute_dir)` hit ENOENT when
    the Modal container has the sandbox restored but not the
    per-artifact `execute/artifact_i/` empty dir.

    These tests extract the real ``_execute_on_modal`` function from
    the decorated mock (``wrapped._original_fn``) and invoke it
    directly against a fresh sandbox_root — mirroring what Modal does
    in the real container.
    """

    def _capture_execute_on_modal(self, config):
        """Initialize a router and return (router, _execute_on_modal)."""
        mock_modal = _make_mock_modal()
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            fn = router._ensure_running("test_op")

        return router, fn._original_fn

    def _snapshot_local_and_build_fresh_input(self, tmp_path):
        """Build a local sandbox, snapshot it, and return the snapshot
        plus an `ExecuteInput` whose `execute_dir` points at a FRESH
        sandbox_root (not yet on disk). Mirrors what happens on Modal:
        the container sees a new absolute path that must be recreated
        via restore_sandbox.
        """
        from artisan.execution.transport.sandbox_transport import (
            snapshot_sandbox_for_artifact,
        )

        # Local sandbox the way prep_unit would create it.
        local_root = tmp_path / "local_sandbox"
        (local_root / "preprocess").mkdir(parents=True)
        local_artifact_exec = local_root / "execute" / "artifact_0"
        local_artifact_exec.mkdir(parents=True)  # empty leaf

        local_input = ExecuteInput(
            execute_dir=str(local_artifact_exec),
            inputs={},
            log_path="/tmp/log",
        )
        files, empty_dirs = snapshot_sandbox_for_artifact(
            str(local_root), local_input
        )
        assert "execute/artifact_0" in empty_dirs  # precondition

        # Point the cloudpickled ExecuteInput at a path that does NOT
        # yet exist. Only restore_sandbox can materialize it.
        fresh_root = tmp_path / "fresh_sandbox"
        fresh_artifact_exec = fresh_root / "execute" / "artifact_0"
        fresh_input = ExecuteInput(
            execute_dir=str(fresh_artifact_exec),
            inputs={},
            log_path="/tmp/log",
        )
        return files, empty_dirs, fresh_root, fresh_input, fresh_artifact_exec

    def test_creates_missing_execute_dir_from_empty_dirs(self, tmp_path):
        """Fake op subprocess-cwd'd on execute_dir succeeds on a fresh root.

        Reproduces the original FoundryRFD3-on-Modal failure in a
        single-process test: local sandbox has empty
        `execute/artifact_0/`; we snapshot it, invoke
        `_execute_on_modal` against a fresh (non-existent)
        sandbox_root, and the op's `subprocess.run(cwd=execute_dir)`
        must succeed.
        """

        class _CwdOp:
            name = "cwd_op"

            def execute(self, execute_input):
                subprocess.run(
                    ["sh", "-c", "echo ok > marker.txt"],
                    cwd=execute_input.execute_dir,
                    check=True,
                )
                return "done"

        files, empty_dirs, fresh_root, fresh_input, fresh_artifact_exec = (
            self._snapshot_local_and_build_fresh_input(tmp_path)
        )
        assert not fresh_artifact_exec.exists()  # precondition

        _, execute_on_modal = self._capture_execute_on_modal(
            ModalComputeConfig(image="test:latest")
        )

        raw_result, output_files = execute_on_modal(
            operation_bytes=cloudpickle.dumps(_CwdOp()),
            execute_input_bytes=cloudpickle.dumps(fresh_input),
            sandbox=files,
            sandbox_dirs=empty_dirs,
            sandbox_root=str(fresh_root),
            tool_files={},
        )

        assert raw_result == "done"
        assert output_files == {"marker.txt": b"ok\n"}
        assert (fresh_artifact_exec / "marker.txt").read_bytes() == b"ok\n"

    def test_subprocess_cwd_fails_without_sandbox_dirs(self, tmp_path):
        """Negative control: dropping sandbox_dirs reproduces the bug.

        Same setup but we call _execute_on_modal without
        ``sandbox_dirs``. The empty execute dir never gets recreated
        on the fresh root, and the op's subprocess.run raises
        FileNotFoundError at Popen.

        This test is load-bearing: if a future refactor silently
        drops ``sandbox_dirs`` from the Modal call chain, this test
        fails — which means the positive test above would also fail —
        making the contract explicit in CI.
        """

        class _CwdOp:
            name = "cwd_op"

            def execute(self, execute_input):
                subprocess.run(
                    ["true"], cwd=execute_input.execute_dir, check=True
                )

        files, _, fresh_root, fresh_input, fresh_artifact_exec = (
            self._snapshot_local_and_build_fresh_input(tmp_path)
        )
        assert not fresh_artifact_exec.exists()  # precondition

        _, execute_on_modal = self._capture_execute_on_modal(
            ModalComputeConfig(image="test:latest")
        )

        try:
            execute_on_modal(
                operation_bytes=cloudpickle.dumps(_CwdOp()),
                execute_input_bytes=cloudpickle.dumps(fresh_input),
                sandbox=files,
                # sandbox_dirs intentionally omitted — pre-fix behavior.
                sandbox_root=str(fresh_root),
                tool_files={},
            )
        except FileNotFoundError:
            pass  # expected
        else:
            raise AssertionError(
                "Expected FileNotFoundError when sandbox_dirs is dropped; "
                "the execute/artifact_0 shell should not exist on the fresh root."
            )
