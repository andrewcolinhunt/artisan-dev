"""Operation discovery — import built-in + manual modules, report outcomes.

``discover()`` is called explicitly by the MCP server's ``lifespan`` startup
and by Python-side callers. We deliberately do not run it on
``import artisan`` to keep that import cheap for non-agent users.

The underlying ``OperationDefinition._registry`` self-populates as a side
effect of any import that loads a subclass (via
``__pydantic_init_subclass__``). ``discover()`` adds attribution — which
module each registration came from — on top of that.
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Sequence
from typing import Literal

from artisan.errors import ApplicationRegistrationError
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.registry.models import (
    DiscoveryError,
    DiscoveryReport,
    DiscoverySource,
    NameCollision,
)
from artisan.utils.application_modules import (
    module_is_within,
    validate_application_modules,
)

# Example/demo ops are deliberately excluded: they would otherwise surface
# in every discover() call as first-class recommendable operations. Load
# them explicitly via extra_modules or ARTISAN_LOAD_MODULES when needed.
_BUILTIN_MODULES = ("artisan.operations.curator",)
_ENV_VAR = "ARTISAN_LOAD_MODULES"
_failed_imports: dict[str, str] = {}


def discover(
    extra_modules: list[str] | None = None, *, include_environment: bool = True
) -> DiscoveryReport:
    """Import built-in + manual modules, returning an attribution report.

    Idempotent: re-importing already-loaded modules is a no-op (Python's
    module cache) and ``__pydantic_init_subclass__`` does not re-fire for
    classes already defined.

    Args:
        extra_modules: Additional dotted module paths to import. Merged
            with the comma-separated list in ``ARTISAN_LOAD_MODULES`` and
            deduplicated.
        include_environment: Merge operator environment only at trusted entry
            points. Set false when the received module list is already resolved.

    Returns:
        ``DiscoveryReport`` with one ``DiscoverySource`` per imported
        module, ``DiscoveryError`` per failed import (not raised), and
        ``NameCollision`` per duplicate-name registration attempt.
    """
    collisions_before = len(OperationDefinition._name_collisions)
    sources: list[DiscoverySource] = []
    errors: list[DiscoveryError] = []

    for module_path in _BUILTIN_MODULES:
        source = _import_and_attribute(module_path, "builtin", errors)
        if source is not None:
            sources.append(source)

    modules = (
        _manual_modules(extra_modules)
        if include_environment
        else list(dict.fromkeys(extra_modules or []))
    )
    for module_path in modules:
        source = _import_and_attribute(module_path, "manual", errors)
        if source is not None:
            sources.append(source)

    new_collisions = [
        NameCollision(name=name, first_module=first, second_module=second)
        for name, first, second in OperationDefinition._name_collisions[
            collisions_before:
        ]
    ]

    return DiscoveryReport(
        operations_count=len(OperationDefinition._registry),
        sources=sources,
        errors=errors,
        name_collisions=new_collisions,
    )


def _manual_modules(extra_modules: list[str] | None) -> list[str]:
    """Merge env-var + kwarg module lists, preserving order, deduplicated."""
    env_value = os.environ.get(_ENV_VAR, "")
    env_modules = [m.strip() for m in env_value.split(",") if m.strip()]
    combined = list(env_modules) + list(extra_modules or [])
    seen: set[str] = set()
    deduped: list[str] = []
    for module in combined:
        if module not in seen:
            seen.add(module)
            deduped.append(module)
    return deduped


def _import_and_attribute(
    module_path: str,
    kind: Literal["builtin", "manual"],
    errors: list[DiscoveryError],
) -> DiscoverySource | None:
    """Import one module, capturing failures and attributing new registrations.

    Attribution is by snapshot diff: any op that wasn't in the registry
    before this import and whose ``__module__`` is ``module_path`` (or a
    submodule) is credited to this source.
    """
    registry_before = dict(OperationDefinition._registry)
    collisions_before = list(OperationDefinition._name_collisions)
    try:
        importlib.import_module(module_path)
    except Exception as exc:
        _failed_imports[module_path] = type(exc).__name__
        OperationDefinition._registry.clear()
        OperationDefinition._registry.update(registry_before)
        OperationDefinition._name_collisions[:] = collisions_before
        errors.append(
            DiscoveryError(
                module=module_path,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
        )
        return None

    new_registrations = [
        name
        for name, cls in OperationDefinition._registry.items()
        if name not in registry_before
        and (
            cls.__module__ == module_path
            or cls.__module__.startswith(f"{module_path}.")
        )
    ]
    return DiscoverySource(
        kind=kind,
        module=module_path,
        classes_registered=sorted(new_registrations),
    )


def bootstrap_applications(modules: Sequence[str], *, purpose: str = "runtime") -> None:
    """Load already resolved trusted applications strictly, without ambient merge.

    Args:
        modules: Installed dotted module names explicitly authorized by the caller.
        purpose: Runtime boundary used in configuration diagnostics.

    Raises:
        ApplicationRegistrationError: An import fails, a relevant registration
            collides, or a previous failed import requires a fresh interpreter.
    """
    resolved = validate_application_modules(modules)
    selected = (*_BUILTIN_MODULES, *resolved)
    for failed in _failed_imports:
        if any(
            module_is_within(failed, name) or module_is_within(name, failed)
            for name in selected
        ):
            msg = f"Application {failed} previously failed during import; restart the interpreter before {purpose}"
            raise ApplicationRegistrationError(msg)
    report = discover(list(resolved), include_environment=False)
    if report.errors:
        failure = report.errors[0]
        msg = f"Application {failure.module} failed to load for {purpose} ({failure.error_type}); restart the interpreter"
        raise ApplicationRegistrationError(msg) from None
    for _, _, rejected_module in OperationDefinition._name_collisions:
        if any(module_is_within(rejected_module, name) for name in selected):
            msg = f"Conflicting operation registrations in authorized applications prevent {purpose}"
            raise ApplicationRegistrationError(msg)
