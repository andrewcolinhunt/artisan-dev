"""Trusted startup freezes module selection and refuses partial registrations."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from artisan.errors import ApplicationRegistrationError
from artisan.operations.base import OperationDefinition
from artisan.registry import bootstrap_applications, discover
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.utils.application_modules import resolve_application_modules
from artisan.utils.process_call import execute_process_call, serialize_process_call


@pytest.mark.parametrize("value", ["app", [""], ["a..b"], ["a/b"], ["a;secret"], [2]])
@pytest.mark.parametrize("model", [RuntimeEnvironment, PipelineConfig])
def test_application_modules_reject_invalid_names(value: object, model: type) -> None:
    kwargs = {"delta_root": "/delta", "staging_root": "/staging"}
    if model is PipelineConfig:
        kwargs["name"] = "test"
    with pytest.raises(ApplicationRegistrationError):
        model(**kwargs, application_modules=value)


def test_resolution_freezes_environment_before_explicit_modules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ARTISAN_LOAD_MODULES", " a.b, ,c.d,a.b ")
    assert resolve_application_modules(["c.d", "e.f"]) == ("a.b", "c.d", "e.f")
    runtime = RuntimeEnvironment(
        delta_root="/delta", staging_root="/staging", application_modules=("a.b",)
    )
    monkeypatch.setenv("ARTISAN_LOAD_MODULES", "malicious.child")
    restored = RuntimeEnvironment.model_validate_json(runtime.model_dump_json())
    call = serialize_process_call(lambda value: value.application_modules, restored)
    assert execute_process_call(call) == ("a.b",)
    assert (
        PipelineConfig(
            name="test", delta_root="/delta", staging_root="/staging"
        ).application_modules
        == ()
    )


def test_bootstrap_ignores_ambient_modules_and_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = tmp_path / "marker"
    (tmp_path / "startup_allowed.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('loaded')\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("ARTISAN_LOAD_MODULES", "does_not_exist_for_worker")
    bootstrap_applications(("startup_allowed",))
    assert marker.read_text() == "loaded"
    marker.unlink()
    bootstrap_applications(("startup_allowed",))
    assert not marker.exists()


def test_failed_report_import_cannot_be_retried_as_cached_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = "startup_partial_failure"
    (tmp_path / f"{module}.py").write_text(
        "raise RuntimeError('unsafe-secret-value')\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    assert discover([module], include_environment=False).errors
    with pytest.raises(ApplicationRegistrationError, match="restart") as caught:
        bootstrap_applications((module,))
    assert "unsafe-secret-value" not in str(caught.value.to_dict())
    assert caught.value.__cause__ is None


def test_bootstrap_import_failure_has_no_raw_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = "startup_fresh_failure"
    (tmp_path / f"{module}.py").write_text("raise ValueError('unsafe-secret-value')\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    with pytest.raises(ApplicationRegistrationError, match="ValueError") as caught:
        bootstrap_applications((module,), purpose="repair")
    assert "unsafe-secret-value" not in str(caught.value.to_dict())
    assert caught.value.__cause__ is None


def test_bootstrap_rejects_collision_already_seen_by_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = "startup_collision"
    monkeypatch.setitem(
        sys.modules,
        module,
        importlib.util.module_from_spec(importlib.machinery.ModuleSpec(module, None)),
    )
    monkeypatch.setattr(
        OperationDefinition, "_name_collisions", [("op", module, f"{module}.child")]
    )
    assert not discover([module], include_environment=False).name_collisions
    with pytest.raises(ApplicationRegistrationError, match="Conflicting"):
        bootstrap_applications((module,))


def test_missing_package_is_a_configuration_error() -> None:
    with pytest.raises(ApplicationRegistrationError) as caught:
        bootstrap_applications(("missing_application_for_bootstrap",))
    assert caught.value.error_type == "config"
    assert caught.value.envelope.recovery_hint == "CHECK_INPUT"
