"""Tests for ModalComputeRouter."""

from __future__ import annotations

import subprocess
from typing import Any
from unittest.mock import MagicMock, patch

import cloudpickle

from artisan.execution.compute.modal import ModalComputeRouter
from artisan.schemas.operation_config.compute import ModalComputeConfig
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.environment_spec import (
    DockerEnvironmentSpec,
)
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.specs.input_models import ExecuteInput


def _make_mock_modal():
    """Build a mock modal module with App and Image.

    ``remote_fn`` and ``spawn_map_iter`` can be injected via
    ``mock_modal._captured`` to stub the container-side return values:

    - ``_captured["remote_fn"]``: callable returning the 4-tuple
      ``(raw_result, output_snapshot, container_timings,
      tool_output_bytes)``. Default returns ``(None, {}, {}, b"")``.
    - ``_captured["spawn_map_iter"]``: iterable (or callable returning
      one) of 4-tuples yielded by ``function_call.iter()`` for the
      batch path. Default yields nothing.
    """
    mock_modal = MagicMock()

    # modal.Image.from_registry returns an image object
    mock_image = MagicMock()
    mock_modal.Image.from_registry.return_value = mock_image
    # Modal's image API is chainable — ``from_registry().add_local_python_source().env()``
    # all return the same image. Mirror that so tests can assert on the
    # final ``image.env(...)`` call without walking a chain of return values.
    mock_image.add_local_python_source.return_value = mock_image
    mock_image.env.return_value = mock_image

    # modal.Volume.from_name(...) returns a unique volume mock per name so
    # tests can assert on which volume landed at which mount path.
    mock_modal.Volume.from_name.side_effect = lambda *a, **kw: MagicMock()

    # modal.App() returns an app with a .function() decorator
    mock_app = MagicMock()
    mock_modal.App.return_value = mock_app

    # app.run() returns a context manager that no-ops
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=None)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_app.run.return_value = mock_ctx

    # app.function() returns a decorator that wraps the function and
    # gives it both ``.remote()`` (single-path) and
    # ``.experimental_spawn_map()`` (batch-path) mocks. Return values
    # are 4-tuples to match the container-side wire format;
    # callers can't rely on cloudpickled args being deserialized
    # (MagicMock can't survive cloudpickle round-trips) so injection
    # happens via ``_captured``.
    _captured: dict[str, Any] = {}

    def _make_function_call():
        """Build a FunctionCall mock whose .iter() returns configured items."""
        source = _captured.get("spawn_map_iter", [])
        items = source() if callable(source) else source
        fc = MagicMock()
        fc.iter.return_value = iter(items)
        return fc

    def function_decorator(**kwargs):
        # Record the kwargs so tests can assert which fn_kwargs the router
        # passed to @app.function(...). Stored under "fn_kwargs" — last
        # decoration wins, which matches the single-_init_app-call pattern.
        _captured["fn_kwargs"] = kwargs

        def decorator(fn):
            wrapped = MagicMock()
            wrapped._original_fn = fn
            # Default: return (None, {}, {}, b"") — tests override via _captured
            wrapped.remote = MagicMock(
                side_effect=lambda **kw: _captured.get(
                    "remote_fn", lambda **k: (None, {}, {}, b"")
                )(**kw)
            )
            wrapped.experimental_spawn_map = MagicMock(
                side_effect=lambda *a, **kw: _make_function_call()
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
        mock_modal._captured["remote_fn"] = lambda **kw: (
            {"result": 42},
            {},
            {},
            b"",
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

        assert result == {"result": 42}

    def test_route_execute_returns_none(self, tmp_path):
        """route_execute passes through None returns."""
        mock_modal = _make_mock_modal()
        mock_modal._captured["remote_fn"] = lambda **kw: (None, {}, {}, b"")
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

    def test_local_python_sources_default(self):
        """Default config splats ["artisan"] into add_local_python_source."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        mock_image = mock_modal.Image.from_registry.return_value
        mock_image.add_local_python_source.assert_called_once_with("artisan")

    def test_local_python_sources_multiple(self):
        """Multiple sources splat in declaration order."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(
            image="test:latest",
            local_python_sources=["artisan", "pipelines"],
        )
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        mock_image = mock_modal.Image.from_registry.return_value
        mock_image.add_local_python_source.assert_called_once_with(
            "artisan", "pipelines"
        )

    def test_local_python_sources_opts_out_of_artisan(self):
        """Dropping 'artisan' from the list drops it from the call."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(
            image="test:latest",
            local_python_sources=["pipelines"],
        )
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        mock_image = mock_modal.Image.from_registry.return_value
        mock_image.add_local_python_source.assert_called_once_with("pipelines")

    def test_local_python_sources_empty(self):
        """Empty list splats to a zero-arg call."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(
            image="test:latest",
            local_python_sources=[],
        )
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        mock_image = mock_modal.Image.from_registry.return_value
        mock_image.add_local_python_source.assert_called_once_with()

    def test_cpu_passed_when_set(self):
        """``compute_resources.cpu`` lands in @app.function kwargs."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config, ComputeResources(cpu=2.0))

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        assert mock_modal._captured["fn_kwargs"]["cpu"] == 2.0

    def test_cpu_omitted_when_none(self):
        """``cpu=None`` (default) → no ``cpu`` kwarg passed to Modal."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        assert "cpu" not in mock_modal._captured["fn_kwargs"]

    def test_max_containers_passed_when_set(self):
        """``max_containers`` lands in @app.function kwargs when set."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest", max_containers=50)
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        assert mock_modal._captured["fn_kwargs"]["max_containers"] == 50

    def test_max_containers_omitted_when_none(self):
        """Default ``max_containers=None`` → no kwarg."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        assert "max_containers" not in mock_modal._captured["fn_kwargs"]

    def test_secrets_resolved_via_from_name(self):
        """Each secret name → modal.Secret.from_name(name); list flows to fn."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest", secrets=["hf-read", "aws-s3"])
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        # Two from_name calls (image_registry_secret unset).
        assert mock_modal.Secret.from_name.call_count == 2
        mock_modal.Secret.from_name.assert_any_call("hf-read")
        mock_modal.Secret.from_name.assert_any_call("aws-s3")
        secrets_kwarg = mock_modal._captured["fn_kwargs"]["secrets"]
        assert len(secrets_kwarg) == 2

    def test_secrets_omitted_when_empty(self):
        """Default ``secrets=[]`` → no ``secrets`` kwarg, no from_name calls."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        mock_modal.Secret.from_name.assert_not_called()
        assert "secrets" not in mock_modal._captured["fn_kwargs"]

    def test_volumes_resolved_via_from_name(self):
        """Volume names resolve via Volume.from_name(create_if_missing, version=2)."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(
            image="test:latest",
            volumes={"/cache": "hf-cache", "/weights": "foundry-weights"},
        )
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        assert mock_modal.Volume.from_name.call_count == 2
        mock_modal.Volume.from_name.assert_any_call(
            "hf-cache", create_if_missing=True, version=2
        )
        mock_modal.Volume.from_name.assert_any_call(
            "foundry-weights", create_if_missing=True, version=2
        )
        volumes_kwarg = mock_modal._captured["fn_kwargs"]["volumes"]
        assert set(volumes_kwarg.keys()) == {"/cache", "/weights"}

    def test_volumes_omitted_when_empty(self):
        """Default ``volumes={}`` → no ``volumes`` kwarg, no from_name calls."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        mock_modal.Volume.from_name.assert_not_called()
        assert "volumes" not in mock_modal._captured["fn_kwargs"]

    def test_env_applied_to_image(self):
        """``env`` chains an ``image.env(...)`` call into the image build."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(
            image="test:latest", env={"HF_XET_HIGH_PERFORMANCE": "1"}
        )
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        mock_image = mock_modal.Image.from_registry.return_value
        mock_image.env.assert_called_once_with({"HF_XET_HIGH_PERFORMANCE": "1"})

    def test_env_applied_before_add_local_python_source(self):
        """``image.env(...)`` must precede ``add_local_python_source(...)``.

        Modal forbids further build steps after ``add_local_*``; reversing
        the order raises ``modal.exception.InvalidError`` at image build.
        """
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(
            image="test:latest",
            env={"K": "V"},
            local_python_sources=["pkg"],
        )
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        mock_image = mock_modal.Image.from_registry.return_value
        names = [c[0] for c in mock_image.method_calls]
        assert "env" in names and "add_local_python_source" in names
        assert names.index("env") < names.index("add_local_python_source")

    def test_env_skipped_when_empty(self):
        """Empty ``env`` → no ``image.env(...)`` call, image cache stays clean."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        mock_image = mock_modal.Image.from_registry.return_value
        mock_image.env.assert_not_called()

    def test_defaults_no_new_kwargs(self):
        """Defaults: none of the new fields appear in @app.function kwargs."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router._ensure_running("test_op")

        fn_kwargs = mock_modal._captured["fn_kwargs"]
        for key in ("cpu", "max_containers", "secrets", "volumes"):
            assert key not in fn_kwargs
        mock_modal.Image.from_registry.return_value.env.assert_not_called()

    def test_output_snapshot_restored_locally(self, tmp_path):
        """Output files from remote are restored in the sandbox."""
        mock_modal = _make_mock_modal()
        # Simulate remote returning an output snapshot
        output_snapshot = {"result.json": b'{"done": true}'}
        mock_modal._captured["remote_fn"] = lambda **kw: (
            {"success": True},
            output_snapshot,
            {},
            b"",
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
        files, empty_dirs = snapshot_sandbox_for_artifact(str(local_root), local_input)
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

        raw_result, output_files, _, _tool_output = execute_on_modal(
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
                subprocess.run(["true"], cwd=execute_input.execute_dir, check=True)

        files, _, fresh_root, fresh_input, fresh_artifact_exec = (
            self._snapshot_local_and_build_fresh_input(tmp_path)
        )
        assert not fresh_artifact_exec.exists()  # precondition

        _, execute_on_modal = self._capture_execute_on_modal(
            ModalComputeConfig(image="test:latest")
        )

        from artisan.execution.compute.modal import _ContainerFailure

        try:
            execute_on_modal(
                operation_bytes=cloudpickle.dumps(_CwdOp()),
                execute_input_bytes=cloudpickle.dumps(fresh_input),
                sandbox=files,
                # sandbox_dirs intentionally omitted — pre-fix behavior.
                sandbox_root=str(fresh_root),
                tool_files={},
            )
        except _ContainerFailure as wrapper:
            # _execute_on_modal wraps the original exception; the
            # subprocess.Popen failure is what we're validating here.
            assert isinstance(wrapper.original, FileNotFoundError)
        else:
            msg = (
                "Expected _ContainerFailure wrapping FileNotFoundError when "
                "sandbox_dirs is dropped; the execute/artifact_0 shell should "
                "not exist on the fresh root."
            )
            raise AssertionError(msg)


class TestModalSubPhaseTimings:
    """Tests for router.warm() and sub-phase timing instrumentation."""

    def _make_execute_input(self, tmp_path, name: str) -> ExecuteInput:
        exec_dir = tmp_path / "execute" / name
        exec_dir.mkdir(parents=True)
        return ExecuteInput(inputs={}, execute_dir=str(exec_dir), log_path="/tmp/log")

    def test_warm_initializes_app_once(self, tmp_path):
        """warm() triggers init; a second warm() and a later batch call reuse it."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.name = "warm_op"
        operation.environments = Environments()
        operation.tool = None

        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()

        with patch.dict("sys.modules", {"modal": mock_modal}):
            router.warm("warm_op")
            router.warm("warm_op")  # idempotent
            router.route_execute_batch(operation, [], str(sandbox))

        assert mock_modal.App.call_count == 1

    def test_route_execute_batch_records_serialize_and_dispatch(self, tmp_path):
        """Passing timings records serialize and dispatch as floats; omitting is safe."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.name = "batch_op"
        operation.environments = Environments()
        operation.tool = None

        sandbox = tmp_path / "sandbox"
        (sandbox / "materialized_inputs").mkdir(parents=True)
        ei = self._make_execute_input(tmp_path, "artifact_0")

        timings: dict[str, Any] = {}
        with patch.dict("sys.modules", {"modal": mock_modal}):
            router.route_execute_batch(operation, [ei], str(sandbox), timings=timings)

        assert isinstance(timings["serialize"], float)
        assert timings["serialize"] >= 0.0
        assert isinstance(timings["dispatch"], float)
        assert timings["dispatch"] >= 0.0

        # Omitting the kwarg does not raise.
        with patch.dict("sys.modules", {"modal": mock_modal}):
            router.route_execute_batch(operation, [ei], str(sandbox))

    def test_route_execute_batch_empty_inputs(self, tmp_path):
        """Empty inputs still record both phases; handle yields nothing."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        operation = MagicMock()
        operation.name = "batch_op"
        operation.environments = Environments()
        operation.tool = None

        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()

        timings: dict[str, Any] = {}
        with patch.dict("sys.modules", {"modal": mock_modal}):
            handle = router.route_execute_batch(
                operation, [], str(sandbox), timings=timings
            )
            results = list(handle)

        assert isinstance(timings["serialize"], float)
        assert isinstance(timings["dispatch"], float)
        assert results == []
        assert handle.container_timings == []

    def test_execute_on_modal_returns_container_timings(self, tmp_path):
        """The remote function returns a 3-tuple with ordered epoch floats."""
        mock_modal = _make_mock_modal()
        config = ModalComputeConfig(image="test:latest")
        router = ModalComputeRouter(config)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            fn = router._ensure_running("test_op")

        class _Op:
            name = "test_op"

            def execute(self, execute_input):
                return "ok"

        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()
        ei = ExecuteInput(inputs={}, execute_dir=str(execute_dir), log_path="/tmp/log")

        raw_result, output_files, ct, tool_output_bytes = fn._original_fn(
            operation_bytes=cloudpickle.dumps(_Op()),
            execute_input_bytes=cloudpickle.dumps(ei),
            sandbox=None,
            sandbox_dirs=None,
            sandbox_root=None,
            tool_files={},
        )

        assert raw_result == "ok"
        assert isinstance(output_files, dict)
        assert tool_output_bytes == b""  # no sandbox_root → no log
        assert set(ct.keys()) == {
            "container_start_epoch",
            "execute_start_epoch",
            "execute_end_epoch",
        }
        assert all(isinstance(v, float) for v in ct.values())
        assert ct["container_start_epoch"] <= ct["execute_start_epoch"]
        assert ct["execute_start_epoch"] <= ct["execute_end_epoch"]

    def test_batch_handle_populates_container_timings(self, tmp_path):
        """Iterating the handle appends one ct entry per yielded result."""
        from artisan.execution.compute.modal import BatchExecuteHandle

        cts = [
            {
                "container_start_epoch": 100.0,
                "execute_start_epoch": 100.5,
                "execute_end_epoch": 101.0,
            },
            {
                "container_start_epoch": 100.1,
                "execute_start_epoch": 100.7,
                "execute_end_epoch": 101.3,
            },
            {
                "container_start_epoch": 100.2,
                "execute_start_epoch": 100.9,
                "execute_end_epoch": 101.6,
            },
        ]
        items = [
            ("r0", {}, cts[0], b""),
            ("r1", {}, cts[1], b""),
            ("r2", {}, cts[2], b""),
        ]

        fc = MagicMock()
        fc.iter.return_value = iter(items)

        eis = [self._make_execute_input(tmp_path, f"a{i}") for i in range(3)]
        handle = BatchExecuteHandle(function_call=fc, execute_inputs=eis, count=3)

        results = list(handle)
        assert results == ["r0", "r1", "r2"]
        assert len(handle.container_timings) == 3
        for observed, expected in zip(handle.container_timings, cts, strict=True):
            assert observed == expected

    def test_batch_handle_partial_failure_timings(self, tmp_path):
        """When one invocation raises mid-stream, surviving ct entries are kept."""
        from artisan.execution.compute.modal import BatchExecuteHandle

        cts = [
            {
                "container_start_epoch": 0.0,
                "execute_start_epoch": 0.1,
                "execute_end_epoch": 0.2,
            },
            {
                "container_start_epoch": 0.3,
                "execute_start_epoch": 0.4,
                "execute_end_epoch": 0.5,
            },
        ]

        def _generator():
            yield ("r0", {}, cts[0], b"")
            yield ("r1", {}, cts[1], b"")
            msg = "modal blew up"
            raise RuntimeError(msg)

        fc = MagicMock()
        fc.iter.return_value = _generator()

        eis = [self._make_execute_input(tmp_path, f"a{i}") for i in range(3)]
        handle = BatchExecuteHandle(function_call=fc, execute_inputs=eis, count=3)

        results = list(handle)
        assert results[0] == "r0"
        assert results[1] == "r1"
        assert isinstance(results[2], RuntimeError)
        assert len(handle.container_timings) == 2


# ---------------------------------------------------------------------------
# Tool-output capture: helpers, failure-path, batch concat
# ---------------------------------------------------------------------------


class TestUnitToolOutputRead:
    """Direct tests of `_read_unit_tool_output`."""

    def test_none_sandbox_root_returns_empty(self):
        from artisan.execution.compute.modal import _read_unit_tool_output

        assert _read_unit_tool_output(None) == b""

    def test_missing_file_returns_empty(self, tmp_path):
        from artisan.execution.compute.modal import _read_unit_tool_output

        assert _read_unit_tool_output(str(tmp_path)) == b""

    def test_normal_read(self, tmp_path):
        from artisan.execution.compute.modal import _read_unit_tool_output

        (tmp_path / "tool_output.log").write_bytes(b"line1\nline2\n")
        assert _read_unit_tool_output(str(tmp_path)) == b"line1\nline2\n"

    def test_tail_truncation(self, tmp_path):
        from artisan.execution.compute.modal import _read_unit_tool_output
        from artisan.execution.transport.log_constants import (
            MAX_TOOL_OUTPUT_BYTES,
        )

        oversize = b"x" * (MAX_TOOL_OUTPUT_BYTES + 100)
        (tmp_path / "tool_output.log").write_bytes(oversize)
        result = _read_unit_tool_output(str(tmp_path))
        assert len(result) == MAX_TOOL_OUTPUT_BYTES
        assert result == oversize[-MAX_TOOL_OUTPUT_BYTES:]


class TestWriteToolOutput:
    """Direct tests of `_write_tool_output`."""

    def test_none_path_noop(self):
        from artisan.execution.compute.modal import _write_tool_output

        _write_tool_output(None, b"some bytes")  # must not raise

    def test_empty_bytes_noop(self, tmp_path):
        from artisan.execution.compute.modal import _write_tool_output

        target = tmp_path / "log"
        _write_tool_output(str(target), b"")
        assert not target.exists()

    def test_creates_parent_dir(self, tmp_path):
        from artisan.execution.compute.modal import _write_tool_output

        target = tmp_path / "deep" / "nested" / "tool_output.log"
        _write_tool_output(str(target), b"hello")
        assert target.read_bytes() == b"hello"

    def test_overwrites_existing(self, tmp_path):
        from artisan.execution.compute.modal import _write_tool_output

        target = tmp_path / "tool_output.log"
        target.write_bytes(b"old")
        _write_tool_output(str(target), b"new")
        assert target.read_bytes() == b"new"


class TestWriteConcatenatedUnitLog:
    """Direct tests of `_write_concatenated_unit_log`."""

    def test_separators_and_order(self, tmp_path):
        from artisan.execution.compute.modal import _write_concatenated_unit_log

        target = tmp_path / "tool_output.log"
        _write_concatenated_unit_log(
            str(target),
            [(0, b"alpha"), (1, b"beta"), (2, b"gamma")],
        )
        contents = target.read_bytes()
        assert contents == (
            b"=== artifact 0 ===\nalpha\n\n"
            b"=== artifact 1 ===\nbeta\n\n"
            b"=== artifact 2 ===\ngamma"
        )

    def test_truncation_with_prefix(self, tmp_path):
        from artisan.execution.compute.modal import _write_concatenated_unit_log
        from artisan.execution.transport.log_constants import (
            MAX_TOOL_OUTPUT_BYTES,
        )

        target = tmp_path / "tool_output.log"
        # Two parts each twice the cap → final blob far exceeds the cap.
        big = b"x" * (MAX_TOOL_OUTPUT_BYTES * 2)
        _write_concatenated_unit_log(str(target), [(0, big), (1, big)])
        contents = target.read_bytes()
        assert contents.startswith(b"[truncated]\n")
        # Tail must be the last bytes of the un-truncated blob (all 'x').
        assert contents[-10:] == b"x" * 10
        assert len(contents) == len(b"[truncated]\n") + MAX_TOOL_OUTPUT_BYTES

    def test_empty_parts_noop(self, tmp_path):
        from artisan.execution.compute.modal import _write_concatenated_unit_log

        target = tmp_path / "tool_output.log"
        _write_concatenated_unit_log(str(target), [])
        assert not target.exists()

    def test_none_path_noop(self):
        from artisan.execution.compute.modal import _write_concatenated_unit_log

        _write_concatenated_unit_log(None, [(0, b"x")])  # must not raise


class TestContainerFailureRoundTrip:
    """`_execute_on_modal` wraps execute() failures in `_ContainerFailure`."""

    def _capture(self):
        mock_modal = _make_mock_modal()
        router = ModalComputeRouter(ModalComputeConfig(image="t:1"))
        with patch.dict("sys.modules", {"modal": mock_modal}):
            fn = router._ensure_running("op")
        return fn._original_fn

    def test_failure_after_partial_log(self, tmp_path):
        from artisan.execution.compute.modal import _ContainerFailure

        sandbox_root = tmp_path / "sandbox"
        sandbox_root.mkdir()

        class _Op:
            name = "op"

            def execute(self, execute_input):
                # Simulate run_command(stream_output=True) writing a
                # partial log before the tool blew up.
                with open(execute_input.log_path, "w") as f:
                    f.write("start\nmiddle\n")
                msg = "boom"
                raise ValueError(msg)

        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()
        ei = ExecuteInput(
            execute_dir=str(execute_dir),
            inputs={},
            log_path=str(sandbox_root / "tool_output.log"),
        )

        execute_on_modal = self._capture()

        try:
            execute_on_modal(
                operation_bytes=cloudpickle.dumps(_Op()),
                execute_input_bytes=cloudpickle.dumps(ei),
                sandbox=None,
                sandbox_dirs=None,
                sandbox_root=str(sandbox_root),
                tool_files={},
            )
        except _ContainerFailure as wrapper:
            assert isinstance(wrapper.original, ValueError)
            assert wrapper.tool_output_bytes == b"start\nmiddle\n"
        else:
            msg = "expected _ContainerFailure"
            raise AssertionError(msg)

    def test_failure_before_writing(self, tmp_path):
        from artisan.execution.compute.modal import _ContainerFailure

        sandbox_root = tmp_path / "sandbox"
        sandbox_root.mkdir()

        class _Op:
            name = "op"

            def execute(self, execute_input):
                msg = "boom"
                raise RuntimeError(msg)

        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()
        ei = ExecuteInput(
            execute_dir=str(execute_dir),
            inputs={},
            log_path=str(sandbox_root / "tool_output.log"),
        )

        execute_on_modal = self._capture()

        try:
            execute_on_modal(
                operation_bytes=cloudpickle.dumps(_Op()),
                execute_input_bytes=cloudpickle.dumps(ei),
                sandbox=None,
                sandbox_dirs=None,
                sandbox_root=str(sandbox_root),
                tool_files={},
            )
        except _ContainerFailure as wrapper:
            assert isinstance(wrapper.original, RuntimeError)
            assert wrapper.tool_output_bytes == b""
        else:
            msg = "expected _ContainerFailure"
            raise AssertionError(msg)


class TestRouteExecuteToolOutput:
    """`route_execute` writes bytes on success and on `_ContainerFailure`."""

    def _build(self, tmp_path):
        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        execute_dir = tmp_path / "execute"
        execute_dir.mkdir()
        ei = ExecuteInput(
            inputs={},
            execute_dir=str(execute_dir),
            log_path=str(sandbox / "tool_output.log"),
        )
        operation = MagicMock()
        operation.environments = Environments()
        operation.tool = None
        return sandbox, ei, operation

    def test_writes_bytes_on_success(self, tmp_path):
        mock_modal = _make_mock_modal()
        mock_modal._captured["remote_fn"] = lambda **kw: (
            "ok",
            {},
            {},
            b"captured stdout\n",
        )
        config = ModalComputeConfig(image="t:1")
        router = ModalComputeRouter(config)
        sandbox, ei, operation = self._build(tmp_path)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            result = router.route_execute(operation, ei, str(sandbox))

        assert result == "ok"
        assert (sandbox / "tool_output.log").read_bytes() == b"captured stdout\n"

    def test_writes_partial_and_reraises_on_failure(self, tmp_path):
        from artisan.execution.compute.modal import _ContainerFailure

        partial = b"partial output before crash\n"

        def _raise(**_kw):
            raise _ContainerFailure(ValueError("boom"), partial)

        mock_modal = _make_mock_modal()
        mock_modal._captured["remote_fn"] = _raise
        config = ModalComputeConfig(image="t:1")
        router = ModalComputeRouter(config)
        sandbox, ei, operation = self._build(tmp_path)

        with patch.dict("sys.modules", {"modal": mock_modal}):
            try:
                router.route_execute(operation, ei, str(sandbox))
            except ValueError as exc:
                assert str(exc) == "boom"
            else:
                msg = "expected ValueError"
                raise AssertionError(msg)

        assert (sandbox / "tool_output.log").read_bytes() == partial


class TestBatchExecuteHandleToolOutput:
    """`BatchExecuteHandle.__iter__` writes the concatenated unit log."""

    def _ei(self, tmp_path, name, log_path):
        d = tmp_path / name
        d.mkdir()
        return ExecuteInput(inputs={}, execute_dir=str(d), log_path=log_path)

    def test_concatenates_per_artifact_bytes(self, tmp_path):
        from artisan.execution.compute.modal import BatchExecuteHandle

        log_path = tmp_path / "tool_output.log"
        items = [
            (
                "r0",
                {},
                {
                    "container_start_epoch": 0,
                    "execute_start_epoch": 0,
                    "execute_end_epoch": 0,
                },
                b"a-out",
            ),
            (
                "r1",
                {},
                {
                    "container_start_epoch": 0,
                    "execute_start_epoch": 0,
                    "execute_end_epoch": 0,
                },
                b"b-out",
            ),
            (
                "r2",
                {},
                {
                    "container_start_epoch": 0,
                    "execute_start_epoch": 0,
                    "execute_end_epoch": 0,
                },
                b"c-out",
            ),
        ]
        fc = MagicMock()
        fc.iter.return_value = iter(items)
        eis = [self._ei(tmp_path, f"a{i}", str(log_path)) for i in range(3)]
        handle = BatchExecuteHandle(function_call=fc, execute_inputs=eis, count=3)

        results = list(handle)
        assert results == ["r0", "r1", "r2"]
        contents = log_path.read_bytes()
        assert b"=== artifact 0 ===\na-out" in contents
        assert b"=== artifact 1 ===\nb-out" in contents
        assert b"=== artifact 2 ===\nc-out" in contents
        # Order preserved
        assert contents.index(b"artifact 0") < contents.index(b"artifact 1")
        assert contents.index(b"artifact 1") < contents.index(b"artifact 2")

    def test_partial_log_from_failed_artifact(self, tmp_path):
        from artisan.execution.compute.modal import (
            BatchExecuteHandle,
            _ContainerFailure,
        )

        log_path = tmp_path / "tool_output.log"
        ct = {
            "container_start_epoch": 0,
            "execute_start_epoch": 0,
            "execute_end_epoch": 0,
        }

        def _gen():
            yield ("r0", {}, ct, b"good-output")
            raise _ContainerFailure(ValueError("kaboom"), b"partial-output")

        fc = MagicMock()
        fc.iter.return_value = _gen()
        eis = [self._ei(tmp_path, f"a{i}", str(log_path)) for i in range(2)]
        handle = BatchExecuteHandle(function_call=fc, execute_inputs=eis, count=2)

        results = list(handle)
        assert results[0] == "r0"
        assert isinstance(results[1], ValueError)
        contents = log_path.read_bytes()
        assert b"=== artifact 0 ===\ngood-output" in contents
        assert b"=== artifact 1 ===\npartial-output" in contents

    def test_finally_fires_on_early_break(self, tmp_path):
        from artisan.execution.compute.modal import BatchExecuteHandle

        log_path = tmp_path / "tool_output.log"
        ct = {
            "container_start_epoch": 0,
            "execute_start_epoch": 0,
            "execute_end_epoch": 0,
        }

        def _gen():
            yield ("r0", {}, ct, b"first")
            yield ("r1", {}, ct, b"second")
            yield ("r2", {}, ct, b"third")

        fc = MagicMock()
        fc.iter.return_value = _gen()
        eis = [self._ei(tmp_path, f"a{i}", str(log_path)) for i in range(3)]
        handle = BatchExecuteHandle(function_call=fc, execute_inputs=eis, count=3)

        # Consume only the first result, then close the generator.
        gen = iter(handle)
        first = next(gen)
        gen.close()  # triggers the `finally` block

        assert first == "r0"
        # Only the first artifact's bytes were buffered before close.
        contents = log_path.read_bytes()
        assert b"=== artifact 0 ===\nfirst" in contents
        assert b"second" not in contents

    def test_no_write_when_no_parts(self, tmp_path):
        from artisan.execution.compute.modal import BatchExecuteHandle

        log_path = tmp_path / "tool_output.log"
        ct = {
            "container_start_epoch": 0,
            "execute_start_epoch": 0,
            "execute_end_epoch": 0,
        }
        items = [
            ("r0", {}, ct, b""),
            ("r1", {}, ct, b""),
        ]
        fc = MagicMock()
        fc.iter.return_value = iter(items)
        eis = [self._ei(tmp_path, f"a{i}", str(log_path)) for i in range(2)]
        handle = BatchExecuteHandle(function_call=fc, execute_inputs=eis, count=2)

        list(handle)
        assert not log_path.exists()
