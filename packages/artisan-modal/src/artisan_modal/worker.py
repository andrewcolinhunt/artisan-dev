"""Fixed image startup entry, independent of scientific payload deserialization."""

from __future__ import annotations

import sys
from pathlib import Path

from artisan.orchestration.runner_api import (
    LocalLauncherIdentity,
    PreparedNativeEnvironment,
    capture_runtime_compatibility,
    select_runtime_compatibility,
    verify_command_witness,
    verify_native_environment,
    verify_wheel_installation,
)
from artisan_modal._worker_entrypoint import verify_activation
from artisan_modal.bindings import LAYOUT_FILE, PIXI, ImageLayout, ImageStartup
from artisan_modal.runtime import capture_modal_runtime


def startup(package_names: tuple[str, ...] = ()) -> str:
    """Record completed image targets and environment-wide worker metadata."""
    layout, activation = verify_worker_activation()
    if not Path(sys.executable).resolve().is_relative_to(layout.worker.prefix):
        message = "Modal did not start the prepared worker interpreter"
        raise ValueError(message)
    if layout.witness is not None:
        verify_command_witness(layout.witness)
    targets = []
    for environment in layout.environments:
        target = PreparedNativeEnvironment(
            source=environment.source,
            wheels=environment.wheels,
            platform="linux-64",
            workspace=environment.workspace,
            prefix=environment.prefix,
        )
        verify_native_environment(target)
        if target.wheels:
            verify_wheel_installation(
                tuple(
                    target.workspace / ".artisan-wheels" / wheel.filename
                    for wheel in target.wheels
                ),
                interpreter=target.prefix / "bin/python",
            )
        if target.source.digest == layout.worker_source:
            compatibility = capture_runtime_compatibility()
            if package_names:
                select_runtime_compatibility(compatibility, package_names)
            target = target.model_copy(update={"compatibility": compatibility})
        targets.append(target)
    return ImageStartup(
        layout=layout,
        targets=tuple(targets),
        launcher=LocalLauncherIdentity.capture(PIXI, Path("/usr/bin/env")),
        runtime=capture_modal_runtime(),
        activation_sha256=activation,
    ).model_dump_json()


def verify_worker_activation() -> tuple[ImageLayout, str]:
    """Verify the actual startup before preparation measurement or live decode."""
    layout = ImageLayout.model_validate_json(LAYOUT_FILE.read_bytes())
    return layout, verify_activation(layout.activation_config())
