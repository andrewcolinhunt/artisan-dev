"""Standalone execution of creator definitions and scientific compositions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from artisan.execution.executor import Executor

__all__ = ["Executor"]


def __getattr__(name: str) -> type[Executor]:
    # Storage imports execution kernels while its own query modules initialize.
    if name == "Executor":
        from artisan.execution.executor import Executor

        return Executor
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
