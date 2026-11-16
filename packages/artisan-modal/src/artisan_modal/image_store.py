"""Reuse completed images and resolve each declared source once per submission."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from artisan.orchestration.runner_api import (
    Environment,
    EnvironmentSource,
    PreparedNativeEnvironment,
    verify_container_source,
    wheel_builder_digest,
)
from artisan_modal.bindings import PreparedModalImage
from artisan_modal.preparation import BASE_IMAGE


class MissingPreparedImage(ValueError):
    """No complete image covers the requested software definition."""


@dataclass(frozen=True)
class ImageSelection:
    """One resolved image and its distinct authored environment selections."""

    image: PreparedModalImage
    targets: dict[Environment, PreparedNativeEnvironment]


def publish_image(binding: PreparedModalImage, *, cache_root: Path) -> Path:
    """Publish completed image preparation independently of endpoint deployment."""
    layout = binding.startup.layout
    request = {
        "worker": layout.worker_source,
        "targets": [target.digest for target in binding.startup.targets],
        "base": binding.base_image,
        "builder": binding.builder_version,
    }
    digest = hashlib.sha256(
        json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    destination = cache_root / "modal" / "images-v2" / digest / "binding.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(uuid.uuid4().hex + ".tmp")
    try:
        temporary.write_text(binding.model_dump_json())
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def _source_matches(
    declaration: Environment,
    source: EnvironmentSource,
    observed: dict[tuple[Environment, EnvironmentSource], bool],
) -> bool:
    key = declaration, source
    if key not in observed:
        try:
            verify_container_source(declaration, source)
        except (FileNotFoundError, ValueError):
            observed[key] = False
        else:
            observed[key] = True
    return observed[key]


def _select_target(
    binding: PreparedModalImage,
    declaration: Environment,
    observed: dict[tuple[Environment, EnvironmentSource], bool],
) -> PreparedNativeEnvironment:
    matches = [
        target
        for target in binding.startup.targets
        if target.source.name == declaration.name
        and _source_matches(declaration, target.source, observed)
    ]
    if not matches:
        message = "No unchanged image environment; prepare explicitly"
        raise MissingPreparedImage(message)
    if len(matches) != 1:
        message = "Prepared image has conflicting environment selections"
        raise ValueError(message)
    return matches[0]


def _current_builders(binding: PreparedModalImage) -> bool:
    builders = {
        platform: wheel_builder_digest(platform)
        for platform in {
            target.platform for target in binding.startup.targets if target.wheels
        }
    }
    return all(
        wheel.builder_digest == builders[target.platform]
        for target in binding.startup.targets
        for wheel in target.wheels
    )


def _select_image(
    binding: PreparedModalImage,
    worker: Environment,
    tools: Sequence[Environment],
    package_names: tuple[str, ...],
    observed: dict[tuple[Environment, EnvironmentSource], bool],
) -> ImageSelection:
    targets = {
        declaration: _select_target(binding, declaration, observed)
        for declaration in dict.fromkeys((worker, *tools))
    }
    selected = targets[worker]
    compatibility = selected.compatibility
    if (
        selected != binding.startup.worker
        or compatibility is None
        or set(package_names) - {package.name for package in compatibility.packages}
    ):
        message = "Prepared image lacks worker package coverage; prepare explicitly"
        raise MissingPreparedImage(message)
    return ImageSelection(binding, targets)


def lookup_image(
    worker: Environment,
    tools: Sequence[Environment],
    *,
    package_names: tuple[str, ...],
    cache_root: Path,
    base_image: str = BASE_IMAGE,
) -> ImageSelection:
    """Resolve complete image coverage without SDK calls or provisioning.

    Distinct call paths reuse their declaration's selected target. The local
    source observations last for this lookup only; subsequent submissions read
    current authored inputs again. Invalid or ambiguous bindings remain errors.
    """
    observed: dict[tuple[Environment, EnvironmentSource], bool] = {}
    matches = []
    for path in sorted(cache_root.glob("modal/images-v2/*/binding.json")):
        binding = PreparedModalImage.model_validate_json(path.read_bytes())
        if binding.base_image != base_image or not _current_builders(binding):
            continue
        try:
            selected = _select_image(binding, worker, tools, package_names, observed)
        except MissingPreparedImage:
            continue
        matches.append(selected)
    if not matches:
        message = "No compatible prepared Modal image; prepare explicitly"
        raise MissingPreparedImage(message)
    if len(matches) != 1:
        message = "Ambiguous prepared Modal images; use an unambiguous cache"
        raise ValueError(message)
    return matches[0]
