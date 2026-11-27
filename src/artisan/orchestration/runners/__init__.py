"""Built-in placement runner; external providers export their own runners."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from artisan.execution.environments.container_runtime import (
        ApptainerRuntime,
        DockerRuntime,
    )
    from artisan.orchestration.runners.local import LocalRunner

__all__ = ["ApptainerRuntime", "DockerRuntime", "LocalRunner"]


def __getattr__(
    name: str,
) -> type[LocalRunner] | type[ApptainerRuntime] | type[DockerRuntime]:
    if name == "LocalRunner":
        from artisan.orchestration.runners.local import LocalRunner

        return LocalRunner
    if name in {"ApptainerRuntime", "DockerRuntime"}:
        from artisan.execution.environments.container_runtime import (
            ApptainerRuntime,
            DockerRuntime,
        )

        return ApptainerRuntime if name == "ApptainerRuntime" else DockerRuntime
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
