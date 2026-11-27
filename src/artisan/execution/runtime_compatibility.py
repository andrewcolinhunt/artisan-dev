"""Check Python and distribution metadata without reading installed payloads."""

from __future__ import annotations

import importlib.metadata
import platform
import re
import sys
from collections.abc import Sequence
from pathlib import Path

from artisan.schemas.execution.worker_transport import (
    DistributionMetadata,
    RuntimeCompatibility,
)


def capture_distribution_metadata(name: str) -> DistributionMetadata:
    """Read an installed distribution's name and version without importing it."""
    distribution = importlib.metadata.distribution(name)
    return DistributionMetadata(name=name, version=distribution.version)


def capture_runtime_compatibility(
    names: Sequence[str] | None = None,
) -> RuntimeCompatibility:
    """Capture selected packages, or all installed metadata during preparation."""
    if names is None:
        packages = _installed_metadata()
    else:
        packages = tuple(
            capture_distribution_metadata(name) for name in _selected_names(names)
        )
    return RuntimeCompatibility(
        python_implementation=platform.python_implementation(),
        python_version=platform.python_version(),
        cache_tag=sys.implementation.cache_tag or "unknown",
        packages=packages,
    )


def select_runtime_compatibility(
    available: RuntimeCompatibility, names: Sequence[str]
) -> RuntimeCompatibility:
    """Select required metadata from an already prepared environment's coverage."""
    selected = _selected_names(names)
    packages = {package.name: package for package in available.packages}
    if missing := set(selected) - packages.keys():
        msg = f"Prepared worker lacks required packages: {sorted(missing)}"
        raise ValueError(msg)
    return RuntimeCompatibility.model_validate(
        {
            **available.model_dump(),
            "packages": tuple(packages[name] for name in selected),
        }
    )


def verify_runtime_compatibility(expected: RuntimeCompatibility) -> None:
    """Reject incompatible interpreter or package versions before live transport."""
    actual_python = (
        platform.python_implementation(),
        platform.python_version(),
        sys.implementation.cache_tag or "unknown",
    )
    if actual_python != (
        expected.python_implementation,
        expected.python_version,
        expected.cache_tag,
    ):
        msg = "Worker Python does not match the prepared transport runtime"
        raise ValueError(msg)
    for package in expected.packages:
        try:
            observed = capture_distribution_metadata(package.name)
        except importlib.metadata.PackageNotFoundError as error:
            msg = f"Worker package {package.name!r} is not installed"
            raise ValueError(msg) from error
        if observed != package:
            msg = f"Worker package {package.name!r} differs from prepared metadata"
            raise ValueError(msg)


def _installed_metadata() -> tuple[DistributionMetadata, ...]:
    packages: dict[tuple[str, Path], DistributionMetadata] = {}
    for distribution in importlib.metadata.distributions():
        package = DistributionMetadata(
            name=distribution.metadata["Name"], version=distribution.version
        )
        root = Path(str(distribution.locate_file(""))).resolve()
        key = (package.name, root)
        previous = packages.get(key)
        if previous is not None and previous != package:
            msg = f"Installed package {package.name!r} has conflicting versions"
            raise ValueError(msg)
        packages[key] = package
    names = sorted({name for name, _ in packages})
    return tuple(capture_distribution_metadata(name) for name in names)


def _selected_names(names: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(re.sub(r"[-_.]+", "-", name).lower() for name in names)
    if len(normalized) != len(set(normalized)):
        msg = "Duplicate runtime package selection"
        raise ValueError(msg)
    return tuple(sorted(normalized))
