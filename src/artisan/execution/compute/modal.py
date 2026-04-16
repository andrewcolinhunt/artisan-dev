"""Modal compute router — route execute() to a Modal container."""

from __future__ import annotations

from typing import Any

import cloudpickle

from artisan.execution.compute.base import ComputeRouter
from artisan.schemas.operation_config.compute import ModalComputeConfig
from artisan.schemas.specs.input_models import ExecuteInput


class ModalComputeRouter(ComputeRouter):
    """Route execute() to a Modal container.

    Serializes the operation and execute input via cloudpickle,
    ships them to a Modal function, and returns the result. A lazy
    ``modal.App`` is created on first call and reused for the
    router's lifetime so repeated calls hit warm containers.

    Attributes:
        _config: Modal compute configuration.
        _fn: Cached Modal function (created lazily).
    """

    def __init__(self, config: ModalComputeConfig) -> None:
        self._config = config
        self._fn: Any = None

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

        fn = self._get_or_create_fn()
        operation = self._force_local_environment(operation)

        sandbox_snapshot = snapshot_sandbox(sandbox_root)
        tool_files = snapshot_tool_files(operation)

        result, output_snapshot = fn.remote(
            operation_bytes=cloudpickle.dumps(operation),
            execute_input_bytes=cloudpickle.dumps(execute_input),
            sandbox=sandbox_snapshot,
            sandbox_root=sandbox_root,
            tool_files=tool_files,
        )

        if output_snapshot:
            restore_sandbox(sandbox_root, output_snapshot)

        return result

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

    def _get_or_create_fn(self) -> Any:
        """Lazily create the Modal function from config.

        Creates an ephemeral ``modal.App`` with the config's image,
        GPU, memory, timeout, and retry settings. Cached for the
        router's lifetime so repeated calls hit warm containers.
        """
        if self._fn is not None:
            return self._fn

        import modal

        app = modal.App()
        image = modal.Image.from_registry(self._config.image)

        @app.function(
            image=image,
            gpu=self._config.gpu,
            memory=self._config.memory_gb * 1024,
            timeout=self._config.timeout,
            retries=self._config.retries,
        )
        def _execute_on_modal(
            operation_bytes: bytes,
            execute_input_bytes: bytes,
            sandbox: dict[str, bytes] | None = None,
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

            if sandbox:
                restore_sandbox(sandbox_root, sandbox)
            if tool_files:
                restore_tool_files(tool_files)

            operation = cp.loads(operation_bytes)
            execute_input = cp.loads(execute_input_bytes)

            raw_result = operation.execute(execute_input)

            output_files = snapshot_outputs(execute_input.execute_dir)
            return raw_result, output_files

        self._fn = _execute_on_modal
        return self._fn
