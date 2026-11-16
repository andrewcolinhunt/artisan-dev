"""Image startup evidence must match every exact declared source and location."""

from __future__ import annotations

import hashlib

import pytest
from artisan_modal.bindings import (
    ImageEnvironment,
    ImageLayout,
    ImageStartup,
    ModalRuntimePayload,
)
from pydantic import ValidationError

from artisan.orchestration.runner_api import (
    EnvironmentSource,
    LocalLauncherIdentity,
    PreparedNativeEnvironment,
)


def _environment(name: str) -> ImageEnvironment:
    digest = hashlib.sha256(name.encode()).hexdigest()
    return ImageEnvironment(
        source=EnvironmentSource.model_validate(
            {
                "manifest_name": "pixi.toml",
                "name": name,
                "files": [
                    {"path": path, "sha256": digest}
                    for path in ("pixi.lock", "pixi.toml")
                ],
            }
        )
    )


def _startup() -> ImageStartup:
    environments = tuple(
        sorted(
            (_environment("worker"), _environment("tool")),
            key=lambda item: item.source.digest,
        )
    )
    layout = ImageLayout(
        worker_source=next(
            item.source.digest for item in environments if item.source.name == "worker"
        ),
        environments=environments,
        pixi_sha256="a" * 64,
        worker_entrypoint_sha256="f" * 64,
    )
    targets = tuple(
        PreparedNativeEnvironment.model_validate(
            {
                "source": item.source,
                "platform": "linux-64",
                "workspace": item.workspace,
                "prefix": item.prefix,
            }
        )
        for item in environments
    )
    return ImageStartup(
        layout=layout,
        activation_sha256=hashlib.sha256(layout.activation_config()).hexdigest(),
        targets=targets,
        launcher=LocalLauncherIdentity(
            "pixi 0.83.0", "a" * 64, "c" * 64, ("glibc", "2.36")
        ),
        runtime=ModalRuntimePayload(sdk_version="1.4.1"),
    )


def test_startup_binds_exact_worker_tool_layout_and_rejects_source_or_launcher_substitution() -> (
    None
):
    startup = _startup()
    assert startup.worker.source.name == "worker"
    assert ImageStartup.model_validate_json(startup.model_dump_json()) == startup
    payload = startup.model_dump(mode="json")
    payload["targets"].reverse()
    with pytest.raises(ValidationError, match="exact image layout"):
        ImageStartup.model_validate(payload)
    payload = startup.model_dump(mode="json")
    payload["launcher"]["pixi_sha256"] = "f" * 64
    with pytest.raises(ValidationError, match="captured build input"):
        ImageStartup.model_validate(payload)


def test_image_layout_rejects_duplicate_or_missing_worker_and_unknown_fields() -> None:
    environment = _environment("tool")
    for environments, worker in (
        ((environment, environment), environment.source.digest),
        ((environment,), "f" * 64),
    ):
        with pytest.raises(ValidationError, match="unique sorted sources"):
            ImageLayout(
                worker_source=worker,
                environments=environments,
                pixi_sha256="a" * 64,
                worker_entrypoint_sha256="f" * 64,
            )
    with pytest.raises(ValidationError, match="Extra inputs"):
        ImageLayout.model_validate({**_startup().layout.model_dump(), "fallback": True})
