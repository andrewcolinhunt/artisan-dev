"""Resolve explicitly declared operation classes and capture their code identity."""

from __future__ import annotations

import importlib
import inspect
from pathlib import Path
from typing import Any

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.execution.replay import OperationIdentity
from artisan.utils.hashing import compute_content_digest


def resolve_operation(target: str) -> type[OperationDefinition]:
    """Import a module:qualname and require an OperationDefinition subclass."""
    module_name, separator, qualname = target.partition(":")
    if not separator or not module_name or not qualname:
        msg = "Operation target must be module:qualname"
        raise ValueError(msg)
    value: Any = importlib.import_module(module_name)
    for component in qualname.split("."):
        value = getattr(value, component)
    if not isinstance(value, type) or not issubclass(value, OperationDefinition):
        msg = "Operation target must be an OperationDefinition subclass"
        raise TypeError(msg)
    return value


def operation_identity(operation: type[OperationDefinition]) -> OperationIdentity:
    """Capture declared identity without claiming to fingerprint dependencies."""
    digest = None
    try:
        path = inspect.getsourcefile(operation)
        if path is not None:
            digest = compute_content_digest(Path(path).read_bytes())
    except (OSError, TypeError):
        pass
    return OperationIdentity(
        module=operation.__module__,
        qualname=operation.__qualname__,
        name=operation.name,
        version=str(operation.version),
        module_digest=digest,
    )
