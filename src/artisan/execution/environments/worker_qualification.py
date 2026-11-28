"""Persist selected worker transport qualification outside immutable software roots."""

from __future__ import annotations

import tempfile
from collections.abc import Sequence
from pathlib import Path

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.execution.local_assignment import qualify_local_worker
from artisan.execution.runtime_compatibility import (
    select_runtime_compatibility,
    verify_runtime_compatibility,
)
from artisan.execution.worker_requirements import (
    collect_definition_packages,
    collect_worker_packages,
    qualified_controller_sources,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.operation_config.environment import Environment


def controller_worker_sources(
    plan: CompiledPlan,
    provider_types: Sequence[type],
    declaration: Environment,
    worker: PreparedNativeEnvironment,
    *,
    application_modules: Sequence[str] = (),
) -> tuple[tuple[str, ...], dict[str, dict[str, Path]]]:
    """Collect trusted code and retain only its qualified controller editable mappings."""
    sources = qualified_controller_sources(declaration, worker)
    names = collect_worker_packages(
        plan,
        provider_types,
        editable_sources=sources,
        application_modules=application_modules,
    )
    return names, {name: files for name, files in sources.items() if name in names}


def controller_definition_sources(
    definitions: Sequence[type[OperationDefinition] | type[CompositeDefinition]],
    provider_types: Sequence[type],
    declaration: Environment,
    worker: PreparedNativeEnvironment,
    *,
    application_modules: Sequence[str] = (),
) -> tuple[tuple[str, ...], dict[str, dict[str, Path]]]:
    """Use the same prepared source ownership for trusted non-plan definitions."""
    sources = qualified_controller_sources(declaration, worker)
    names = collect_definition_packages(
        definitions,
        provider_types,
        editable_sources=sources,
        application_modules=application_modules,
    )
    return names, {name: files for name, files in sources.items() if name in names}


def prepare_worker_qualification(
    worker: PreparedNativeEnvironment,
    names: tuple[str, ...],
    *,
    cache_root: Path,
    pixi: Path,
    system_env: Path,
    verify_current: bool = True,
) -> PreparedNativeEnvironment:
    """Capture reusable environment metadata once, then select required packages."""
    path = _qualification_path(cache_root, worker)
    if path.exists():
        return lookup_worker_qualification(
            worker, names, cache_root=cache_root, verify_current=verify_current
        )
    qualified = qualify_local_worker(worker, pixi=pixi, system_env=system_env)
    assert qualified.compatibility is not None
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(qualified.model_dump_json().encode())
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return _select_qualification(qualified, names, verify_current=verify_current)


def lookup_worker_qualification(
    worker: PreparedNativeEnvironment,
    names: tuple[str, ...],
    *,
    cache_root: Path,
    verify_current: bool = True,
) -> PreparedNativeEnvironment:
    """Select package coverage from completed preparation without provisioning."""
    path = _qualification_path(cache_root, worker)
    if not path.is_file():
        msg = "Required worker transport packages are not qualified; prepare explicitly"
        raise ValueError(msg)
    qualified = PreparedNativeEnvironment.model_validate_json(path.read_bytes())
    compatibility = qualified.compatibility
    if (
        qualified.model_copy(update={"compatibility": None})
        != worker.model_copy(update={"compatibility": None})
        or compatibility is None
    ):
        msg = (
            "Worker qualification differs from the selected prepared target or packages"
        )
        raise ValueError(msg)
    return _select_qualification(qualified, names, verify_current=verify_current)


def _select_qualification(
    worker: PreparedNativeEnvironment,
    names: tuple[str, ...],
    *,
    verify_current: bool = True,
) -> PreparedNativeEnvironment:
    assert worker.compatibility is not None
    selected = select_runtime_compatibility(worker.compatibility, names)
    if verify_current:
        verify_runtime_compatibility(selected)
    return worker.model_copy(update={"compatibility": selected})


def _qualification_path(cache_root: Path, worker: PreparedNativeEnvironment) -> Path:
    return cache_root / "worker-qualification-v2" / worker.digest / "runtime.json"
