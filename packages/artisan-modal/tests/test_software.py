"""A provider digest must remain bound to actual worker-side measurements."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

import pytest
from artisan_modal import software
from artisan_modal.bindings import PIXI, ModalFunctionRuntime, PreparedModalImage
from artisan_modal.software import ModalSoftware

from artisan.orchestration.runner_api import Environment, PreparedLocalBindings


def binding(image: PreparedModalImage) -> ModalSoftware:
    local = PreparedLocalBindings(
        image.startup.worker,
        (),
        PIXI,
        Path("/usr/bin/env"),
        image.startup.launcher,
    )
    function = ModalFunctionRuntime(
        image_id=image.image_id,
        milli_cpu=1000,
        memory_mb=1024,
        gpu_count=0,
        gpu_type="",
        max_inputs=1,
        timeout_seconds=60,
    )
    return ModalSoftware(image, local, function)


def test_worker_verification_measures_provider_before_trusted_local_science(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image, _, _ = prepared
    selected = binding(image)
    seen = []
    monkeypatch.setattr(
        software, "verify_modal_runtime", lambda value: seen.append(value)
    )
    monkeypatch.setattr(
        software, "verify_modal_function", lambda value: seen.append(value)
    )
    monkeypatch.setattr(
        PreparedLocalBindings, "verify_worker", lambda self: seen.append(self)
    )
    selected.verify_worker()
    assert seen == [
        image.startup.runtime,
        selected.function,
        selected.local,
    ]
    assert selected.software_cacheable
    assert selected.application_modules == ()
    assert selected.compatibility == image.startup.worker.compatibility
    altered = selected.function.model_copy(update={"timeout_seconds": 61})
    assert (
        replace(selected, function=altered).software_digest != selected.software_digest
    )


def test_actual_provider_mismatch_cannot_be_hidden_by_controller_digest(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image, _, _ = prepared
    selected = binding(image)
    expected = selected.software_digest
    monkeypatch.setattr(software, "verify_modal_runtime", Mock())
    monkeypatch.setattr(
        software,
        "verify_modal_function",
        Mock(side_effect=ValueError("actual image differs")),
    )
    entered = Mock()
    monkeypatch.setattr(PreparedLocalBindings, "verify_worker", entered)
    assert selected.software_digest == expected
    with pytest.raises(ValueError, match="actual image differs"):
        selected.verify_worker()
    entered.assert_not_called()


def test_software_rejects_foreign_image_launcher_or_unqualified_gpu(
    prepared: tuple[PreparedModalImage, Environment, Environment],
) -> None:
    image, _, _ = prepared
    selected = binding(image)
    with pytest.raises(ValueError, match="identities differ"):
        replace(
            selected,
            function=selected.function.model_copy(update={"image_id": "im-other"}),
        )
    with pytest.raises(ValueError, match="native image layout"):
        replace(selected, local=replace(selected.local, pixi=Path("/unqualified/pixi")))
    with pytest.raises(ValueError, match="native image layout"):
        replace(
            selected,
            local=replace(
                selected.local,
                worker=selected.local.worker.model_copy(
                    update={"workspace": Path("/unqualified/workspace")}
                ),
            ),
        )
    with pytest.raises(ValueError, match="qualified allocation profile"):
        replace(
            selected,
            function=selected.function.model_copy(
                update={"gpu_count": 1, "gpu_type": "A10G"}
            ),
        )
