"""Check the selected Modal SDK and assigned function compatibility."""

from __future__ import annotations

from pathlib import Path

from artisan_modal.bindings import ModalFunctionRuntime, ModalRuntimePayload

RUNTIME_ROOT = Path("/pkg")


def capture_modal_runtime() -> ModalRuntimePayload:
    """Read SDK version and import location without scanning its installed files."""
    import modal

    if (
        modal.__version__ != "1.4.1"
        or Path(modal.__file__).resolve() != RUNTIME_ROOT / "modal/__init__.py"
    ):
        message = "Modal startup requires the qualified SDK version and runtime mount"
        raise ValueError(message)
    return ModalRuntimePayload(sdk_version="1.4.1")


def verify_modal_runtime(expected: ModalRuntimePayload) -> None:
    """Require the prepared SDK version and expected platform import location."""
    if capture_modal_runtime() != expected:
        message = "Modal SDK metadata differs from explicit preparation"
        raise ValueError(message)


def capture_modal_function() -> ModalFunctionRuntime:
    """Read the exact pinned SDK's assigned image/resources without trusting inputs.

    This narrow internal SDK accessor is qualified with version 1.4.1 at the
    platform runtime location. Callers verify that metadata before using it.
    """
    from modal._runtime.container_io_manager import _ContainerIOManager

    manager = _ContainerIOManager._singleton
    if manager is None:
        message = "Modal function identity requires an active SDK worker"
        raise ValueError(message)
    definition = manager.function_def
    return ModalFunctionRuntime(
        image_id=definition.image_id,
        milli_cpu=definition.resources.milli_cpu,
        memory_mb=definition.resources.memory_mb,
        gpu_count=definition.resources.gpu_config.count,
        gpu_type=definition.resources.gpu_config.gpu_type,
        max_inputs=definition.max_concurrent_inputs or 1,
        timeout_seconds=definition.timeout_secs,
    )


def verify_modal_function(expected: ModalFunctionRuntime) -> None:
    """Reject a different image, allocation or concurrent-input configuration."""
    if capture_modal_function() != expected:
        message = "Modal function differs from its admitted image/resources"
        raise ValueError(message)
