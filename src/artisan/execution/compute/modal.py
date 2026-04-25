"""Modal compute_provider router — route execute() to a Modal container."""

from __future__ import annotations

import threading
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any

import cloudpickle

from artisan.execution.compute.base import ComputeRouter
from artisan.schemas.operation_config.compute import ModalComputeConfig
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.specs.input_models import ExecuteInput
from artisan.utils.timing import phase_timer

# Modal API defaults applied when ComputeResources fields are None.
# These match the values previously baked into ModalComputeConfig.
_MODAL_DEFAULT_MEMORY_GB = 8
_MODAL_DEFAULT_TIMEOUT = 3600


class ModalComputeRouter(ComputeRouter):
    """Route execute() to a Modal container.

    Serializes the operation and execute input via cloudpickle,
    ships them to a Modal function, and returns the result. The
    Modal app is lazily created and held open so subsequent calls
    within the same step hit warm containers.

    Attributes:
        _config: Modal compute_provider configuration.
        _app: Cached Modal app (created lazily).
        _fn: Cached Modal function (created lazily).
        _ctx: The ``app.run()`` context manager (held open).
        _init_lock: Guards lazy initialization for thread safety.
    """

    def __init__(
        self,
        config: ModalComputeConfig,
        compute_resources: ComputeResources | None = None,
    ) -> None:
        self._config = config
        # Hardware spec (gpu, memory_gb, timeout) lives on ComputeResources
        # so the same fields can apply to any future provider. None values
        # fall back to Modal API defaults at dispatch time.
        self._compute_resources = compute_resources or ComputeResources()
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

        result, output_snapshot, _container_timings = fn.remote(
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
        timings: dict[str, Any] | None = None,
    ) -> Iterable[Any]:
        """Batch-execute via Modal experimental_spawn_map().

        Serializes the operation ONCE, then per-artifact execute_inputs
        and sandboxes. Dispatches a single experimental_spawn_map() call.

        If ``timings`` is provided, records ``serialize`` (cloudpickle +
        sandbox/tool snapshots) and ``dispatch`` (the spawn_map call)
        phase timings into it via :func:`phase_timer`.

        Note: experimental_spawn_map() is the variant that returns a
        FunctionCall handle. The stable spawn_map() returns None.
        """
        from artisan.execution.transport.sandbox_transport import (
            snapshot_sandbox_for_artifact,
        )
        from artisan.execution.transport.tool_transport import snapshot_tool_files

        fn = self._ensure_running(operation.name)
        forced_op = self._force_local_environment(operation)

        t = timings if timings is not None else {}

        with phase_timer("serialize", t):
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

        with phase_timer("dispatch", t):
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

    def warm(self, operation_name: str) -> None:
        """Enter ``app.run()`` and hydrate the function.

        Idempotent wrapper around :meth:`_ensure_running` — callers that
        skip ``warm()`` still work (lazy init fires on first dispatch);
        calling ``warm()`` explicitly lets the caller record router
        startup as a separately-labeled phase.

        Args:
            operation_name: Used to name the Modal app for dashboard
                visibility (e.g. ``artisan-data_transformer``).
        """
        self._ensure_running(operation_name)

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
                return self._fn  # type: ignore[unreachable]  # double-checked locking: another thread may have set it
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

        memory_gb = (
            self._compute_resources.memory_gb
            if self._compute_resources.memory_gb is not None
            else _MODAL_DEFAULT_MEMORY_GB
        )
        timeout = (
            self._compute_resources.timeout
            if self._compute_resources.timeout is not None
            else _MODAL_DEFAULT_TIMEOUT
        )
        fn_kwargs: dict[str, Any] = {
            "image": image,
            "gpu": self._compute_resources.gpu,
            "memory": memory_gb * 1024,
            "timeout": timeout,
            "retries": self._config.retries,
            "serialized": True,
        }
        if self._config.min_containers > 0:
            fn_kwargs["min_containers"] = self._config.min_containers
        if self._config.scaledown_window is not None:
            fn_kwargs["scaledown_window"] = self._config.scaledown_window

        @app.function(**fn_kwargs)  # type: ignore[misc,unused-ignore]  # modal.App.function is untyped; env-dependent
        def _execute_on_modal(
            operation_bytes: bytes,
            execute_input_bytes: bytes,
            sandbox: dict[str, bytes] | None = None,
            sandbox_dirs: list[str] | None = None,
            sandbox_root: str | None = None,
            tool_files: dict[str, bytes] | None = None,
        ) -> tuple[Any, dict[str, bytes], dict[str, float]]:
            import time

            import cloudpickle as cp

            from artisan.execution.transport.sandbox_transport import (
                restore_sandbox,
                snapshot_outputs,
            )
            from artisan.execution.transport.tool_transport import (
                restore_tool_files,
            )

            container_start = time.time()

            # Always call restore_sandbox so empty-dir shells (e.g. the
            # per-artifact execute/artifact_i/ that the local lifecycle
            # mkdirs but that has no files) get recreated. `sandbox`
            # may be None for in-memory ops; restore_sandbox handles
            # that gracefully via the `or {}` fallbacks.
            if sandbox or sandbox_dirs:
                restore_sandbox(
                    sandbox_root,  # type: ignore[arg-type]  # sandbox_root truthy-guarded by outer condition
                    sandbox or {},
                    empty_dirs=sandbox_dirs,
                )
            if tool_files:
                restore_tool_files(tool_files)

            operation = cp.loads(operation_bytes)
            execute_input = cp.loads(execute_input_bytes)

            execute_start = time.time()
            raw_result = operation.execute(execute_input)
            execute_end = time.time()

            output_files = snapshot_outputs(execute_input.execute_dir)
            container_timings = {
                "container_start_epoch": container_start,
                "execute_start_epoch": execute_start,
                "execute_end_epoch": execute_end,
            }
            return raw_result, output_files, container_timings

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
    instead of aborting iteration. Per-artifact container-side epoch
    timestamps are appended to ``container_timings`` as each result is
    drained, so callers can aggregate cold-start / execute-span stats
    after iteration completes.
    """

    function_call: Any  # modal.FunctionCall
    execute_inputs: list[ExecuteInput]
    count: int
    container_timings: list[dict[str, float]] = field(default_factory=list)

    def cancel(self) -> None:
        """Cancel all in-flight Modal invocations."""
        self.function_call.cancel(terminate_containers=True)

    def __iter__(self) -> Iterator[Any]:
        """Yield results in input order, restoring sandboxes inline."""
        from artisan.execution.transport.sandbox_transport import restore_sandbox

        it = self.function_call.iter()
        for i in range(self.count):
            try:
                raw_result, output_snap, ct = next(it)
                self.container_timings.append(ct)
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
