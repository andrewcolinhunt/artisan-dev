"""Standalone execution of creator definitions and scientific compositions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from artisan.execution.session import ExecutionSession

__all__ = ["ExecutionSession"]


def __getattr__(name: str) -> type[ExecutionSession]:
    # Storage imports execution kernels while its own query modules initialize.
    if name == "ExecutionSession":
        from artisan.execution.session import ExecutionSession

        return ExecutionSession
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
