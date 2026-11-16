"""Resolve curator worker software without a creator recipe or install."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from artisan.execution.curator_definition import capture_curator_definition
from artisan.execution.environments.native import lookup_native_environment
from artisan.execution.environments.observed_worker import (
    ObservedWorker,
    qualify_observed_worker,
)
from artisan.execution.environments.worker_qualification import (
    controller_definition_sources,
    lookup_worker_qualification,
)
from artisan.execution.local_assignment import (
    PreparedLocalBindings,
    bind_local_worker,
)
from artisan.execution.worker_requirements import observed_definition_sources
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.execution.gpu import NativeGPUAllocation
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.operation_config.environment import Environment


def resolve_curator_software(
    operation: OperationDefinition,
    provider_types: Sequence[type],
    *,
    environment: Environment | None,
    cache_root: Path,
    pixi: Path,
    system_env: Path,
    native_gpu: NativeGPUAllocation | None = None,
    application_modules: tuple[str, ...] = (),
) -> PreparedLocalBindings:
    """Check prepared coverage or observe the current worker's metadata.

    Required packages come from trusted live code and registered artifact types.
    A prepared worker must already have explicit transport qualification; this
    resolver never installs or repairs it. An observed worker remains ineligible
    for scientific cache reuse.
    """
    capture_curator_definition(operation)
    worker: PreparedNativeEnvironment | ObservedWorker
    definitions = (type(operation),)
    if environment is None:
        names, _ = observed_definition_sources(
            definitions, provider_types, application_modules=application_modules
        )
        worker = qualify_observed_worker(names)
    else:
        worker = lookup_native_environment(environment, cache_root=cache_root)
        names, _ = controller_definition_sources(
            definitions,
            provider_types,
            environment,
            worker,
            application_modules=application_modules,
        )
        worker = lookup_worker_qualification(worker, names, cache_root=cache_root)
    return bind_local_worker(
        worker,
        pixi=pixi,
        system_env=system_env,
        native_gpu=native_gpu,
        application_modules=application_modules,
    )
