"""Standalone execution of creator definitions and scientific compositions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from artisan.composites.base.called_operations import CalledOperation
    from artisan.execution.call import run_operation
    from artisan.execution.executor import Executor

__all__ = ["CalledOperation", "Executor", "run_operation"]


def __getattr__(name: str) -> object:
    if name == "CalledOperation":
        from artisan.composites.base.called_operations import CalledOperation

        return CalledOperation
    if name == "Executor":
        from artisan.execution.executor import Executor

        return Executor
    if name == "run_operation":
        from artisan.execution.call import run_operation

        return run_operation
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
