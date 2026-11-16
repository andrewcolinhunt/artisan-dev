"""Prepared workers check SDK metadata without scanning installed SDK files."""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

import pytest
from artisan_modal import runtime


def test_runtime_verification_uses_metadata_without_reading_installed_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "pkg"
    origin = root / "modal/__init__.py"
    origin.parent.mkdir(parents=True)
    origin.write_text("qualified sdk")
    module = ModuleType("modal")
    module.__dict__.update(__version__="1.4.1", __file__=str(origin))
    monkeypatch.setitem(sys.modules, "modal", module)
    monkeypatch.setattr(runtime, "RUNTIME_ROOT", root)

    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("runtime verification read installed SDK bytes")

    monkeypatch.setattr(Path, "open", forbidden)
    expected = runtime.capture_modal_runtime()
    runtime.verify_modal_runtime(expected)
    module.__dict__["__version__"] = "different"
    with pytest.raises(ValueError, match="qualified SDK version and runtime mount"):
        runtime.capture_modal_runtime()
    module.__dict__["__version__"] = "1.4.1"
    module.__dict__["__file__"] = str(tmp_path / "shadow/modal/__init__.py")
    with pytest.raises(ValueError, match="qualified SDK version and runtime mount"):
        runtime.capture_modal_runtime()


def test_function_identity_requires_active_exact_image_resources_and_one_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from modal._runtime.container_io_manager import _ContainerIOManager
    from pydantic import ValidationError

    monkeypatch.setattr(_ContainerIOManager, "_singleton", None)
    with pytest.raises(ValueError, match="active SDK worker"):
        runtime.capture_modal_function()
    definition = SimpleNamespace(
        image_id="im-qualified",
        resources=SimpleNamespace(
            milli_cpu=1000,
            memory_mb=1024,
            gpu_config=SimpleNamespace(count=0, gpu_type=""),
        ),
        max_concurrent_inputs=0,
        timeout_secs=60,
    )
    monkeypatch.setattr(
        _ContainerIOManager, "_singleton", SimpleNamespace(function_def=definition)
    )
    expected = runtime.capture_modal_function()
    runtime.verify_modal_function(expected)
    definition.image_id = "im-different"
    with pytest.raises(ValueError, match="admitted image/resources"):
        runtime.verify_modal_function(expected)
    definition.image_id = expected.image_id
    definition.resources.milli_cpu = 2000
    with pytest.raises(ValueError, match="admitted image/resources"):
        runtime.verify_modal_function(expected)
    definition.resources.milli_cpu = expected.milli_cpu
    definition.max_concurrent_inputs = 2
    with pytest.raises(ValidationError, match="max_inputs"):
        runtime.capture_modal_function()
