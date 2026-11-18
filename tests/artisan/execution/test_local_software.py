"""Local Python subprocesses retain software locations after HOME changes."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from fixtures import lightweight_commands
from fixtures.lightweight_commands import NativeParent

from artisan.composites.base.compiler import compile_definition
from artisan.execution.compute.prepared import (
    PreparedExecuteRouter,
    freeze_runtime_injections,
)
from artisan.execution.environments.command_witness import lookup_command_witness
from artisan.execution.environments.native import lookup_native_environment
from artisan.execution.environments.observed_worker import (
    ObservedCommandEnvironment,
    observe_current_worker,
)
from artisan.execution.executor import Executor
from artisan.execution.local_assignment import bind_local_software
from artisan.execution.local_software import (
    LOCAL_SOFTWARE_ENV,
    LocalSoftware,
    local_software,
    local_software_scope,
)
from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES
from artisan.schemas.specs.input_models import FrozenInputContext
from artisan.utils.command_supervisor import managed_invocation
from artisan.utils.external_tools import run_command


@pytest.fixture
def software(tmp_path: Path) -> LocalSoftware:
    return LocalSoftware(
        tmp_path / "prepared-cache",
        tmp_path / "launchers/pixi",
        Path("/usr/bin/env"),
        {"SERVICE_TOKEN": "synthetic-runtime-value"},
    )


def test_local_software_scope_restores_outer_settings_after_failure(
    software: LocalSoftware, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(LOCAL_SOFTWARE_ENV, "not a valid handoff")
    child = replace(software, cache_root=software.cache_root / "child")
    with local_software_scope(software):
        assert local_software() is software
        with (
            pytest.raises(RuntimeError, match="phase failed"),
            local_software_scope(child),
        ):
            assert local_software() is child
            msg = "phase failed"
            raise RuntimeError(msg)
        assert local_software() is software
    with pytest.raises(ValueError):
        local_software()


def test_handoff_contains_paths_and_runtime_keys_without_secret_values(
    software: LocalSoftware, monkeypatch: pytest.MonkeyPatch
) -> None:
    values = software.environment()
    assert "synthetic-runtime-value" not in values[LOCAL_SOFTWARE_ENV]
    for key, value in {**values, **software.runtime_env}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("HOME", "/different-home")
    monkeypatch.setenv("XDG_CACHE_HOME", "/different-cache")
    assert local_software() == software
    with pytest.raises(TypeError):
        local_software().runtime_env["SERVICE_TOKEN"] = "changed"  # type: ignore[index]
    monkeypatch.delenv("SERVICE_TOKEN")
    with pytest.raises(ValueError, match="missing declared runtime"):
        local_software()


def test_handoff_rejects_software_override_injection_and_relative_paths(
    software: LocalSoftware, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(ValueError, match="controls"):
        freeze_runtime_injections(software.environment())
    payload = json.loads(software.environment()[LOCAL_SOFTWARE_ENV])
    payload["cache_root"] = "relative-cache"
    monkeypatch.setenv(LOCAL_SOFTWARE_ENV, json.dumps(payload))
    with pytest.raises(ValueError, match="absolute paths"):
        local_software()


def test_router_context_exposes_selected_locations_to_native_phases(
    software: LocalSoftware,
) -> None:
    router = PreparedExecuteRouter(
        None,
        pixi=software.pixi,
        system_env=software.system_env,
        runtime_env=software.runtime_env,
        local_software=software,
    )
    with router.context():
        assert local_software() is software


def test_managed_python_reads_original_locations_after_home_changes(
    software: LocalSoftware, tmp_path: Path
) -> None:
    worker = observe_current_worker(TRANSPORT_PACKAGES)
    environment = ObservedCommandEnvironment(
        worker,
        tmp_path / "command",
        software.runtime_env,
        local_software=software,
    )
    program = """
import json, os, sys
from artisan.execution.local_software import local_software
software = local_software()
print(json.dumps({
    "cache_root": str(software.cache_root),
    "pixi": str(software.pixi),
    "system_env": str(software.system_env),
    "runtime": dict(software.runtime_env),
    "home": os.environ["HOME"],
    "pid": os.getpid(),
    "python": sys.executable,
}))
"""
    with managed_invocation():
        result = run_command(environment, ["python", "-I", "-c", program])
    measured = json.loads(result.stdout)
    assert measured["cache_root"] == str(software.cache_root)
    assert measured["pixi"] == str(software.pixi)
    assert measured["system_env"] == str(software.system_env)
    assert measured["runtime"] == dict(software.runtime_env)
    assert measured["home"] == str(tmp_path / "command/home")
    assert measured["pid"] != os.getpid()
    assert measured["python"] == sys.executable


def test_observed_worker_does_not_capture_internal_software_locator(
    software: LocalSoftware, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(LOCAL_SOFTWARE_ENV, software.environment()[LOCAL_SOFTWARE_ENV])
    worker = observe_current_worker(TRANSPORT_PACKAGES)
    assert LOCAL_SOFTWARE_ENV not in worker.environment


@pytest.fixture
def qualified_software(tmp_path: Path) -> LocalSoftware:
    cache = os.environ.get("ARTISAN_TEST_PREPARED_CACHE")
    if cache is None:
        pytest.skip("Explicit existing command-witness qualification cache required")
    source = local_software()
    lookup_command_witness(Path(cache))
    launchers = tmp_path / "selected-launchers"
    launchers.mkdir()
    pixi, system_env = launchers / "pixi", launchers / "env"
    pixi.symlink_to(source.pixi)
    system_env.symlink_to(source.system_env)
    return LocalSoftware(
        Path(cache).resolve(),
        pixi,
        system_env,
        {"SERVICE_TOKEN": "qualified-synthetic"},
    )


@pytest.mark.parametrize("kind", ["inherited", "prepared"])
def test_managed_python_calls_operation_with_real_command_binding(
    qualified_software: LocalSoftware,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    if kind == "prepared":
        lookup_native_environment(
            lightweight_commands.PreparedCommandProbe().environment,
            cache_root=qualified_software.cache_root,
        )
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "unprepared-cache"))
    worker = observe_current_worker(TRANSPORT_PACKAGES)
    environment = ObservedCommandEnvironment(
        worker,
        tmp_path / "python-worker",
        qualified_software.runtime_env,
        local_software=qualified_software,
    )
    with managed_invocation():
        result = run_command(
            environment,
            [
                "python",
                "-I",
                str(Path(lightweight_commands.__file__)),
                kind,
                str(tmp_path / "child-command"),
            ],
        )
    measured = json.loads(result.stdout)
    assert measured["pid"] != os.getpid()
    assert measured["python"] == sys.executable
    assert measured["home"] == str(tmp_path / "python-worker/home")
    assert measured["cache"] == str(qualified_software.cache_root)
    assert measured["pixi"] == str(qualified_software.pixi)
    if kind == "inherited":
        assert measured["values"]["pid"] != measured["pid"]
        assert measured["values"]["executable"] == sys.executable
        assert measured["values"]["token"] == "qualified-synthetic"
    else:
        assert measured["values"] == {"compiler_ran": True}


@pytest.mark.parametrize("phase", ["preprocess", "execute", "postprocess"])
def test_managed_native_parent_calls_command_with_its_selected_settings(
    qualified_software: LocalSoftware,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path / "unprepared-home"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "unprepared-cache"))
    operation = NativeParent(
        params=NativeParent.Params(phase=phase, work_dir=str(tmp_path / "child"))
    )
    plan = compile_definition(NativeParent, params=operation.params)
    bindings = bind_local_software(
        plan,
        observe_current_worker(TRANSPORT_PACKAGES),
        cache_root=qualified_software.cache_root,
        pixi=qualified_software.pixi,
        system_env=qualified_software.system_env,
    )
    router = bindings.adapter(operation, (), runtime_env=qualified_software.runtime_env)
    with Executor(working_root=tmp_path / "parent") as executor:
        result = executor.invoke(
            operation,
            FrozenInputContext("parent", {}),
            execute_router=router,
            batch_size=1,
            materialization_references={},
        )
    measured = result.artifacts["metrics"][0].values
    assert measured["pid"] != os.getpid()
    assert measured["executable"] == sys.executable
    assert measured["token"] == "qualified-synthetic"
