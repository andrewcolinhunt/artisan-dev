"""Collect transport packages from trusted live definitions and schema registrations."""

from __future__ import annotations

import importlib.metadata
import json
import sys
import sysconfig
from collections.abc import Iterator, Mapping, Sequence
from importlib.resources.abc import Traversable
from pathlib import Path
from types import ModuleType
from typing import cast, get_args

from packaging.utils import canonicalize_name
from pydantic import BaseModel

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.composites.base.snapshots import parameter_class
from artisan.execution.environments.observed_sources import (
    observed_editable_owners,
    observed_editable_sources,
)
from artisan.execution.environments.source import source_entry
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES
from artisan.schemas.operation_config.environment import Environment
from artisan.utils.application_modules import validate_application_modules


def observed_controller_sources(
    plan: CompiledPlan,
    provider_types: Sequence[type],
    *,
    application_modules: Sequence[str] = (),
) -> tuple[tuple[str, ...], dict[str, dict[str, Path]]]:
    """Qualify current installed ownership without a canonical source declaration."""
    return observed_definition_sources(
        _plan_definitions(plan), provider_types, application_modules=application_modules
    )


def observed_definition_sources(
    definitions: Sequence[type[OperationDefinition] | type[CompositeDefinition]],
    provider_types: Sequence[type],
    *,
    application_modules: Sequence[str] = (),
) -> tuple[tuple[str, ...], dict[str, dict[str, Path]]]:
    """Collect observed import ownership for trusted live definition roots."""
    owners = importlib.metadata.packages_distributions()
    candidates = set(TRANSPORT_PACKAGES)
    modules = _required_modules(definitions, provider_types, application_modules)
    for module in modules:
        candidates.update(owners.get(module.partition(".")[0], ()))
    candidates.update(
        observed_editable_owners(
            tuple(
                sys.modules[name]
                for name in modules
                if name in sys.modules
                and not owners.get(name.partition(".")[0])
                and not _standard_library(sys.modules[name])
            )
        )
    )
    sources = observed_editable_sources(tuple(sorted(candidates)))
    names = collect_definition_packages(
        definitions,
        provider_types,
        editable_sources=sources,
        application_modules=application_modules,
    )
    return names, {name: paths for name, paths in sources.items() if name in names}


def collect_worker_packages(
    plan: CompiledPlan,
    provider_types: Sequence[type],
    *,
    editable_sources: Mapping[str, Mapping[str, Path]] | None = None,
    application_modules: Sequence[str] = (),
) -> tuple[str, ...]:
    """Collect a frozen creator plan's imported code and schema dependencies."""
    return collect_definition_packages(
        _plan_definitions(plan),
        provider_types,
        editable_sources=editable_sources,
        application_modules=application_modules,
    )


def collect_definition_packages(
    definitions: Sequence[type[OperationDefinition] | type[CompositeDefinition]],
    provider_types: Sequence[type],
    *,
    editable_sources: Mapping[str, Mapping[str, Path]] | None = None,
    application_modules: Sequence[str] = (),
) -> tuple[str, ...]:
    """Derive required code packages without importing recorded names.

    Definitions, their parameter models, the active artifact registry and the
    selected provider and SDK roots are caller-authorized live code. Registry coverage is
    conservative because input configuration references can name any registered
    type. Installed metadata identifies distributions; the loaded module must
    actually belong to that distribution's files or qualified editable selection.
    Runtime compatibility subsequently checks the selected package versions.

    Unpackaged definitions and module shadowing fail before transport. Editable
    selections come from verified canonical-source preparation or independently
    observed installed import roots, never a user dependency list or a serialized
    worker header.
    """
    modules = _required_modules(definitions, provider_types, application_modules)
    owners = importlib.metadata.packages_distributions()
    selections: dict[str, Mapping[str, Path]] = {
        canonicalize_name(name): files
        for name, files in (editable_sources or {}).items()
    }
    packages = set(TRANSPORT_PACKAGES)
    for name in sorted(modules):
        module = sys.modules.get(name)
        if module is None or name == "__main__":
            msg = f"Worker code {name!r} must come from an imported installed package"
            raise ValueError(msg)
        if _standard_library(module):
            continue
        candidates = {*owners.get(name.partition(".")[0], ()), *selections}
        matches = {
            canonicalize_name(candidate)
            for candidate in candidates
            if _owns_module(module, candidate, selections)
        }
        if len(matches) != 1:
            msg = f"Worker module {name!r} has no unique qualified distribution owner"
            raise ValueError(msg)
        packages.update(matches)
    return tuple(sorted(packages))


def qualified_controller_sources(
    declaration: Environment, target: PreparedNativeEnvironment
) -> dict[str, dict[str, Path]]:
    """Bind controller editables to the prepared worker's exact source selection.

    Environment resolution has already checked the declared source fingerprint.
    This maps that selection to the controller's import paths without hashing
    the same source again. Ordinary installed wheels use metadata ownership.
    """
    root = cast("Path | Traversable", getattr(declaration.manifest, "parent", None))
    sources = {}
    for package in target.source.packages:
        try:
            distribution = importlib.metadata.distribution(package.name)
        except importlib.metadata.PackageNotFoundError:
            # Worker-only dependencies need not exist in the controller.
            # Required transport packages are checked by the code collector.
            continue
        metadata = json.loads(distribution.read_text("direct_url.json") or "{}")
        if not metadata.get("dir_info", {}).get("editable", False):
            continue
        files = {}
        for payload in package.payloads:
            entry = source_entry(root, str(Path(package.root) / payload.source))
            if not isinstance(entry, Path):
                msg = "Editable worker code requires a filesystem source package"
                raise ValueError(msg)
            files[payload.path] = entry
        sources[package.name] = files
    return sources


def _required_modules(
    definitions: Sequence[type[OperationDefinition] | type[CompositeDefinition]],
    provider_types: Sequence[type],
    application_modules: Sequence[str],
) -> set[str]:
    if not provider_types:
        msg = "Worker coverage requires trusted provider code roots"
        raise ValueError(msg)
    for provider in provider_types:
        _require_imported_definition(provider)
    classes = list(provider_types)
    for definition in definitions:
        _require_imported_definition(definition)
        classes.append(definition)
        params = parameter_class(definition)
        if params is not None:
            classes.append(params)
    modules = {
        ".".join(name.split(".")[:count])
        for name in validate_application_modules(application_modules)
        for count in range(1, len(name.split(".")) + 1)
    }
    for artifact_definition in ArtifactTypeDef.get_all().values():
        classes.extend((artifact_definition, artifact_definition.model))
        policy = artifact_definition.reference_policy()
        if policy is not None:
            modules.add(policy.__module__)
    seen: set[type] = set()
    while classes:
        selected = classes.pop()
        if selected in seen:
            continue
        seen.add(selected)
        classes.extend(selected.__bases__)
        modules.add(selected.__module__)
        if issubclass(selected, BaseModel):
            for field in selected.model_fields.values():
                classes.extend(_annotation_classes(field.annotation))
    return modules


def _plan_definitions(
    plan: CompiledPlan,
) -> tuple[type[OperationDefinition] | type[CompositeDefinition], ...]:
    definitions = []
    for node in _plans(plan):
        node.instantiate()
        definitions.append(node.definition)
    return tuple(definitions)


def _plans(plan: CompiledPlan) -> Iterator[CompiledPlan]:
    yield plan
    for call in plan.calls:
        yield from _plans(call.plan)


def _annotation_classes(annotation: object) -> Iterator[type]:
    if isinstance(annotation, type):
        yield annotation
    for argument in get_args(annotation):
        yield from _annotation_classes(argument)


def _require_imported_definition(definition: type) -> None:
    value: object = sys.modules.get(definition.__module__)
    for part in definition.__qualname__.split("."):
        value = getattr(value, part, None)
    if value is not definition:
        msg = "Prepared workers require importable packaged definitions"
        raise ValueError(msg)


def _standard_library(module: ModuleType) -> bool:
    if module.__name__.partition(".")[0] not in sys.stdlib_module_names:
        return module.__name__ == "builtins"
    if getattr(module.__spec__, "origin", None) in {"built-in", "frozen"}:
        return True
    filename = getattr(module, "__file__", None)
    if filename is None:
        return False
    path = Path(filename).resolve()
    return path.is_relative_to(sysconfig.get_path("stdlib")) and not any(
        path.is_relative_to(sysconfig.get_path(key)) for key in ("purelib", "platlib")
    )


def _owns_module(
    module: ModuleType,
    name: str,
    selections: Mapping[str, Mapping[str, Path]],
) -> bool:
    filename = getattr(module, "__file__", None)
    if filename is None:
        return False
    path = Path(filename).resolve()
    selected = selections.get(canonicalize_name(name))
    if selected is not None:
        return path in {source.resolve() for source in selected.values()}
    distribution = importlib.metadata.distribution(name)
    return any(
        path == Path(str(distribution.locate_file(item))).resolve()
        for item in distribution.files or ()
        if ".." not in item.parts
    )
