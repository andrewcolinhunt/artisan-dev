"""Register caller-authorized applications from qualified worker import payloads."""

from __future__ import annotations

import importlib.machinery
import importlib.metadata
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from types import ModuleType

from packaging.utils import canonicalize_name

from artisan.execution.environments.observed_sources import observed_editable_sources
from artisan.execution.worker_requirements import _owns_module, _standard_library
from artisan.registry import bootstrap_applications
from artisan.schemas.execution.worker_transport import (
    DistributionMetadata,
    RuntimeCompatibility,
)
from artisan.utils.application_modules import validate_application_modules

_live_applications: ContextVar[tuple[str, ...] | None] = ContextVar(
    "artisan_live_applications", default=None
)


@contextmanager
def worker_application_scope(modules: tuple[str, ...]) -> Iterator[None]:
    """Bind the verified live preamble to the subsequent admitted entry."""
    token = _live_applications.set(modules)
    try:
        yield
    finally:
        _live_applications.reset(token)


def validate_worker_applications(
    modules: Sequence[str],
    *,
    qualified_modules: Sequence[str],
) -> tuple[str, ...]:
    """Check the resolved application selection without reopening controller code."""
    selected = validate_application_modules(modules)
    live = _live_applications.get()
    if live is not None and selected != live:
        msg = "Application bootstrap differs from the live call selection"
        raise ValueError(msg)
    if selected != validate_application_modules(qualified_modules):
        msg = "Application bootstrap differs from the qualified software selection"
        raise ValueError(msg)
    return selected


def bootstrap_worker_applications(
    modules: Sequence[str],
    *,
    qualified_modules: Sequence[str],
    compatibility: RuntimeCompatibility | None,
) -> None:
    """Check worker import coverage before unpickling or durable row hydration.

    Names come from the live dispatch, never stored artifacts, replay metadata,
    or environment. Resolving parent packages without executing them prevents an
    unrelated shadow package from gaining import authority through a valid name.
    """
    selected = validate_worker_applications(
        modules, qualified_modules=qualified_modules
    )
    if not selected:
        return
    if compatibility is None:
        msg = "Application bootstrap requires qualified worker code"
        raise ValueError(msg)
    _verify_imports(selected, compatibility)
    bootstrap_applications(selected, purpose="qualified worker")


def _verify_imports(
    selected: tuple[str, ...], compatibility: RuntimeCompatibility
) -> None:
    owners = importlib.metadata.packages_distributions()
    packages = {package.name: package for package in compatibility.packages}
    sources = observed_editable_sources(tuple(packages))
    checked: set[str] = set()
    for application in selected:
        search: Sequence[str] | None = None
        for count in range(1, len(application.split(".")) + 1):
            name = ".".join(application.split(".")[:count])
            spec = (
                importlib.machinery.BuiltinImporter.find_spec(name)
                or importlib.machinery.FrozenImporter.find_spec(name)
                or importlib.machinery.PathFinder.find_spec(name, search)
            )
            if spec is None or spec.origin is None:
                msg = f"Application module {name!r} has no qualified import payload"
                raise ValueError(msg)
            module = ModuleType(name)
            module.__file__, module.__spec__ = spec.origin, spec
            loaded = sys.modules.get(name)
            if (
                loaded is not None
                and not _standard_library(module)
                and getattr(loaded, "__file__", None) != spec.origin
            ):
                msg = f"Application module {name!r} differs from its import path"
                raise ValueError(msg)
            search = spec.submodule_search_locations
            if (
                loaded is not None
                and search is not None
                and tuple(getattr(loaded, "__path__", ())) != tuple(search)
            ):
                msg = f"Application package {name!r} changed its qualified import path"
                raise ValueError(msg)
            if name in checked or _standard_library(module):
                continue
            _verify_payload(
                module,
                owners.get(name.partition(".")[0], ()),
                packages,
                sources,
            )
            checked.add(name)


def _verify_payload(
    module: ModuleType,
    owners: Sequence[str],
    packages: Mapping[str, DistributionMetadata],
    sources: Mapping[str, Mapping[str, Path]],
) -> None:
    matches = [
        owner
        for owner in sorted({canonicalize_name(name) for name in (*owners, *sources)})
        if owner in packages
        and importlib.metadata.version(owner) == packages[owner].version
        and _owns_module(module, owner, sources)
    ]
    if len(matches) != 1:
        msg = f"Application module {module.__name__!r} is outside qualified worker code"
        raise ValueError(msg)
