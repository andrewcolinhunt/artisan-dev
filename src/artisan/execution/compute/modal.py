"""Modal compute router — route execute() to a Modal container."""

from __future__ import annotations

import threading
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

import cloudpickle

from artisan.execution.compute.base import ComputeRouter
from artisan.schemas.operation_config.compute import ModalComputeConfig
from artisan.schemas.specs.input_models import ExecuteInput


class ModalComputeRouter(ComputeRouter):
    """Route execute() to a Modal container.

    Serializes the operation and execute input via cloudpickle,
    ships them to a Modal function, and returns the result. The
    Modal app is lazily created and held open so subsequent calls
    within the same step hit warm containers.

    Attributes:
        _config: Modal compute configuration.
        _app: Cached Modal app (created lazily).
        _fn: Cached Modal function (created lazily).
        _ctx: The ``app.run()`` context manager (held open).
        _init_lock: Guards lazy initialization for thread safety.
    """

    def __init__(self, config: ModalComputeConfig) -> None:
        self._config = config
        self._app: Any = None
        self._fn: Any = None
        self._ctx: Any = None
        self._init_lock = threading.Lock()

    def route_execute(
        self,
        operation: Any,
        execute_input: ExecuteInput,
        sandbox_root: str,
    ) -> Any:
        """Serialize and ship execute() to Modal.

        Args:
            operation: The operation instance.
            execute_input: Frozen input container for execute().
            sandbox_root: Path to the sandbox directory tree.

        Returns:
            The raw result from execute().
        """
        from artisan.execution.transport.sandbox_transport import (
            restore_sandbox,
            snapshot_sandbox,
        )
        from artisan.execution.transport.tool_transport import snapshot_tool_files

        fn = self._ensure_running(operation.name)
        operation = self._force_local_environment(operation)

        sandbox_files, sandbox_dirs = snapshot_sandbox(sandbox_root)
        tool_files = snapshot_tool_files(operation)

        result, output_snapshot = fn.remote(
            operation_bytes=cloudpickle.dumps(operation),
            execute_input_bytes=cloudpickle.dumps(execute_input),
            sandbox=sandbox_files,
            sandbox_dirs=sandbox_dirs,
            sandbox_root=sandbox_root,
            tool_files=tool_files,
        )

        if output_snapshot:
            restore_sandbox(execute_input.execute_dir, output_snapshot)

        return result

    def route_execute_batch(
        self,
        operation: Any,
        execute_inputs: list[ExecuteInput],
        sandbox_root: str,
    ) -> Iterable[Any]:
        """Batch-execute via Modal experimental_spawn_map().

        Serializes the operation ONCE, then per-artifact execute_inputs
        and sandboxes. Dispatches a single experimental_spawn_map() call.

        Note: experimental_spawn_map() is the variant that returns a
        FunctionCall handle. The stable spawn_map() returns None.
        """
        from artisan.execution.transport.sandbox_transport import (
            snapshot_sandbox_for_artifact,
        )
        from artisan.execution.transport.tool_transport import snapshot_tool_files

        fn = self._ensure_running(operation.name)
        forced_op = self._force_local_environment(operation)
        op_bytes = cloudpickle.dumps(forced_op)

        inputs_bytes = [cloudpickle.dumps(ei) for ei in execute_inputs]
        # Split the per-artifact (files, empty_dirs) tuples into
        # parallel lists for experimental_spawn_map's positional
        # zip semantics.
        snapshots = [
            snapshot_sandbox_for_artifact(sandbox_root, ei) for ei in execute_inputs
        ]
        sandboxes = [files for files, _ in snapshots]
        sandbox_dirs_list = [dirs for _, dirs in snapshots]
        tool_files = snapshot_tool_files(operation)

        fc = fn.experimental_spawn_map(
            [op_bytes] * len(execute_inputs),
            inputs_bytes,
            sandboxes,
            sandbox_dirs_list,
            [sandbox_root] * len(execute_inputs),
            [tool_files] * len(execute_inputs),
        )

        return BatchExecuteHandle(
            function_call=fc,
            execute_inputs=execute_inputs,
            count=len(execute_inputs),
        )

    def close(self) -> None:
        """Exit app.run() and release Modal resources."""
        if self._ctx is not None:
            self._ctx.__exit__(None, None, None)
            self._ctx = None
            self._app = None
            self._fn = None

    def __del__(self) -> None:
        self.close()

    def _force_local_environment(self, operation: Any) -> Any:
        """Override environment to local for Modal execution.

        When execute() runs on Modal, the Modal container IS the
        environment. Docker/Apptainer wrapping must not apply.
        """
        from artisan.schemas.operation_config.environment_spec import (
            LocalEnvironmentSpec,
        )

        if not isinstance(operation.environments.current(), LocalEnvironmentSpec):
            return operation.model_copy(
                update={
                    "environments": operation.environments.model_copy(
                        update={"active": "local"}
                    )
                }
            )
        return operation

    def _ensure_running(self, operation_name: str) -> Any:
        """Lazily create the Modal app and hydrate the function.

        Creates an ephemeral ``modal.App``, decorates the execute
        function, and enters ``app.run()`` to hydrate it. The context
        is held open so subsequent calls hit warm containers.

        Thread-safe via double-checked locking — multiple threads from
        the cross-unit thread pool may call this concurrently during
        initial setup. After initialization, ``fn.remote()`` and
        ``fn.experimental_spawn_map()`` are safe for concurrent use.

        Args:
            operation_name: Used to name the Modal app for dashboard
                visibility (e.g. ``artisan-data_transformer``).
        """
        if self._fn is not None:
            return self._fn

        with self._init_lock:
            if self._fn is not None:
                return self._fn
            return self._init_app(operation_name)

    def _init_app(self, operation_name: str) -> Any:
        """Create the Modal app, function, and enter app.run()."""
        import modal

        app = modal.App(f"artisan-{operation_name}")

        image_kwargs: dict[str, Any] = {}
        if self._config.image_registry_secret is not None:
            image_kwargs["secret"] = modal.Secret.from_name(
                self._config.image_registry_secret
            )

        image = modal.Image.from_registry(
            self._config.image, **image_kwargs
        ).add_local_python_source(*self._config.local_python_sources)

        fn_kwargs: dict[str, Any] = {
            "image": image,
            "gpu": self._config.gpu,
            "memory": self._config.memory_gb * 1024,
            "timeout": self._config.timeout,
            "retries": self._config.retries,
            "serialized": True,
        }
        if self._config.min_containers > 0:
            fn_kwargs["min_containers"] = self._config.min_containers
        if self._config.scaledown_window is not None:
            fn_kwargs["scaledown_window"] = self._config.scaledown_window

        @app.function(**fn_kwargs)
        def _execute_on_modal(
            operation_bytes: bytes,
            execute_input_bytes: bytes,
            sandbox: dict[str, bytes] | None = None,
            sandbox_dirs: list[str] | None = None,
            sandbox_root: str | None = None,
            tool_files: dict[str, bytes] | None = None,
        ) -> tuple[Any, dict[str, bytes]]:
            import cloudpickle as cp

            from artisan.execution.transport.sandbox_transport import (
                restore_sandbox,
                snapshot_outputs,
            )
            from artisan.execution.transport.tool_transport import (
                restore_tool_files,
            )

            # Always call restore_sandbox so empty-dir shells (e.g. the
            # per-artifact execute/artifact_i/ that the local lifecycle
            # mkdirs but that has no files) get recreated. `sandbox`
            # may be None for in-memory ops; restore_sandbox handles
            # that gracefully via the `or {}` fallbacks.
            if sandbox or sandbox_dirs:
                restore_sandbox(
                    sandbox_root,
                    sandbox or {},
                    empty_dirs=sandbox_dirs,
                )
            if tool_files:
                restore_tool_files(tool_files)

            operation = cp.loads(operation_bytes)
            execute_input = cp.loads(execute_input_bytes)

            raw_result = operation.execute(execute_input)

            output_files = snapshot_outputs(execute_input.execute_dir)
            return raw_result, output_files

        self._app = app
        self._fn = _execute_on_modal
        self._ctx = app.run()
        self._ctx.__enter__()
        return self._fn


@dataclass
class BatchExecuteHandle:
    """Handle for an in-flight batch execute on Modal.

    Iterates results in input order, restoring per-artifact sandboxes
    inline. Failed invocations yield the exception at that index
    instead of aborting iteration.
    """

    function_call: Any  # modal.FunctionCall
    execute_inputs: list[ExecuteInput]
    count: int

    def cancel(self) -> None:
        """Cancel all in-flight Modal invocations."""
        self.function_call.cancel(terminate_containers=True)

    def __iter__(self) -> Iterator[Any]:
        """Yield results in input order, restoring sandboxes inline."""
        from artisan.execution.transport.sandbox_transport import restore_sandbox

        it = self.function_call.iter()
        for i in range(self.count):
            try:
                raw_result, output_snap = next(it)
                if output_snap:
                    restore_sandbox(
                        self.execute_inputs[i].execute_dir,
                        output_snap,
                    )
                yield raw_result
            except StopIteration:
                yield RuntimeError("Batch ended early")
            except Exception as exc:
                yield exc
